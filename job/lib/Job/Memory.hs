{-# LANGUAGE StrictData #-}
{-# LANGUAGE NoFieldSelectors #-}

module Job.Memory (queue) where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception.Safe qualified as Ex
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.Resource.Extra qualified as R
import Data.Acquire qualified as A
import Data.Fixed
import Data.Foldable
import Data.Function
import Data.List qualified as List
import Data.Map.Strict qualified as Map
import Data.Maybe
import Data.Time qualified as Time
import GHC.IO.Exception
import GHC.Stack

import Job

--------------------------------------------------------------------------------

keepAliveBeatMicroseconds :: Int
keepAliveBeatMicroseconds = 5_000_000

autoRetryDelaySeconds :: Time.NominalDiffTime
autoRetryDelaySeconds = 2

-- | Get the next 'Work'able starting at or after the given time, if any.
next' :: Time.UTCTime -> Map.Map Id (Meta, job) -> Maybe (Id, Meta, job)
next' t =
   listToMaybe . snd . prune' \jId meta job ->
      ( False
      , case meta.alive of
         Nothing | meta.wait <= t -> [(jId, meta, job)]
         _ -> []
      )

-- prune' :: (Id -> Meta -> job -> Writer a Bool)
--        -> Map Id (Meta, job)
--        -> Writer a (Map Id (Meta, job))
prune'
   :: (Monoid a)
   => (Id -> Meta -> job -> (Bool, a))
   -> Map.Map Id (Meta, job)
   -> (Map.Map Id (Meta, job), a)
prune' f =
   foldl'
      ( \(!m, !al) (jId, (meta, job)) ->
         let (keep, ar) = f jId meta job
         in  (if keep then Map.insert jId (meta, job) m else m, al <> ar)
      )
      mempty
      . List.sortOn (\(jId, (meta, _)) -> (meta, jId))
      . Map.toList

-- | An in-memory 'Queue'.
queue :: forall job. A.Acquire (Queue job)
queue = do
   -- tjobs: All the jobs in this 'Queue', running or not. Notice that if
   -- 'prune' removes some currently running jobs, they may be added back if
   -- requested by the 'pull' or 'retry'.
   tjobs :: TVar (Map.Map Id (Meta, job)) <-
      R.mkAcquire1 (newTVarIO mempty) \tv ->
         -- Discard all jobs. Alive or not.
         -- TODO: Should we wait until running jobs finish?
         atomically $ writeTVar tv mempty
   -- twork: If 'pull' couldn't find a suitable job in 'tjobs' right away, then
   -- it'll block waiting for a STM action pushed to 'twork'. To be applied to
   -- the 'Id' and 'Meta' and @job@ already marked as 'active' in 'tjobs'.
   twork :: TQueue (STM (Maybe ((Id, Meta, job) -> STM ()))) <-
      R.mkAcquire1 newTQueueIO \t ->
         atomically $ void $ flushTQueue t
   -- connect: connect jobs ready to be executed in 'tjobs', with workers
   -- waiting for a job in 'twork'. If no job or no worker, do nothing.
   let connect :: Time.UTCTime -> STM ()
       connect now = fix \again -> do
         tryReadTQueue twork >>= traverse_ \myf ->
            myf >>= \case
               Nothing -> again
               Just f -> do
                  jobs <- readTVar tjobs
                  case next' now jobs of
                     Nothing -> unGetTQueue twork myf
                     Just (jId, meta, job) -> do
                        writeTVar tjobs $
                           Map.insert
                              jId
                              ( Meta
                                 { alive = Just now
                                 , nice = meta.nice
                                 , try = meta.try
                                 , wait = meta.wait
                                 }
                              , job
                              )
                              jobs
                        f (jId, meta, job)
                        again
   -- This background thread perform 'shift's every 60 seconds.
   -- Not really necessary, just in case a timer died for some reason.
   void do
      R.mkAcquire1
         ( forkIO $ forever do
            threadDelay 60_000_000 -- 60 seconds
            now <- Time.getCurrentTime
            atomically $ connect now
         )
         (Ex.uninterruptibleMask_ . killThread)
   -- tactive: Whether the 'Queue' is still active (i.e., not vanished).
   tactive :: TVar Bool <- R.mkAcquire1 (newTVarIO True) \tv ->
      atomically $ writeTVar tv False
   let ensureActive :: STM ()
       ensureActive = do
         active <- readTVar tactive
         when (not active) do
            throwSTM $ resourceVanishedWithCallStack "Job.Memory.queue"
   pure
      Queue
         { push = \nice wait job -> do
            jId <- newId
            atomically do
               ensureActive
               let meta = Meta{alive = Nothing, try = 0, nice, wait}
               modifyTVar' tjobs $ Map.insert jId (meta, job)
            tde <- registerDelayUTCTime wait
            void $ Ex.mask_ $ forkIO do
               atomically $ readTVar tde >>= check
               now <- Time.getCurrentTime
               atomically $ connect now
            pure jId
         , ---------
           prune = \f -> atomically do
            ensureActive
            readTVar tactive >>= \case
               False -> pure mempty -- It's alright to “fail silently”
               True -> do
                  (!m, !a) <- prune' f <$> readTVar tjobs
                  writeTVar tjobs m
                  pure a
         , ---------
           ready = do
            now <- Time.getCurrentTime
            jobs <- readTVarIO tjobs
            pure $ isJust $ next' now jobs
         , ---------
           pull = do
            -- gjob: Will block until a job is ready for being worked on.
            gjob :: STM (Id, Meta, job) <- fmap snd do
               R.mkAcquire1
                  ( atomically do
                     ensureActive
                     tjob :: TMVar (Id, Meta, job) <- newEmptyTMVar
                     twrk <- newTVar $ Just $ putTMVar tjob
                     writeTQueue twork $ readTVar twrk
                     pure
                        ( writeTVar twrk Nothing
                        , do
                           ensureActive
                           readTVar twrk >>= \case
                              Just _ -> takeTMVar tjob
                              Nothing ->
                                 throwSTM $
                                    resourceVanishedWithCallStack
                                       "Job.Memory.queue/pull"
                        )
                  )
                  (atomically . fst)
            -- tout: If 'finish' is called, then @'Just' 'Nothing'@, if
            -- 'retry' is called, then @'Just' ('Just' _)@.
            tout :: TVar (Maybe (Maybe (Nice, Time.UTCTime))) <-
               liftIO $ newTVarIO Nothing
            (jId :: Id, meta :: Meta, job :: job) <- do
               R.mkAcquireType1
                  ( do
                     now <- Time.getCurrentTime
                     atomically $ ensureActive >> connect now
                     atomically $ gjob
                  )
                  ( \(jId, meta0, job) rt -> do
                     ymeta1 <-
                        readTVarIO tout >>= \case
                           -- no 'retry', and no 'finish',
                           -- and release with exception.
                           Nothing
                              | A.ReleaseExceptionWith _ <- rt -> do
                                 now <- Time.getCurrentTime
                                 pure $
                                    Just $
                                       Meta
                                          { nice = meta0.nice
                                          , alive = Nothing
                                          , try = succ meta0.try
                                          , wait =
                                             Time.addUTCTime
                                                autoRetryDelaySeconds
                                                now
                                          }
                           -- explicit 'retry'.
                           Just (Just (nice, wait)) ->
                              pure $
                                 Just $
                                    Meta
                                       { alive = Nothing
                                       , try = succ meta0.try
                                       , ..
                                       }
                           -- explicit 'finish',
                           -- or release without exception.
                           _ -> pure Nothing
                     case ymeta1 of
                        Nothing ->
                           atomically $ modifyTVar' tjobs $ Map.delete jId
                        Just !meta1 -> do
                           atomically do
                              ensureActive
                              modifyTVar' tjobs $ Map.insert jId (meta1, job)
                           tde <- registerDelayUTCTime meta1.wait
                           void $ Ex.mask_ $ forkIO do
                              atomically $ readTVar tde >>= check
                              now <- Time.getCurrentTime
                              atomically $ connect now
                  )
            -- While working on this job, send heartbeats
            void do
               R.mkAcquire1
                  ( forkIO $ fix \again -> do
                     eactive <- Ex.tryAny do
                        threadDelay keepAliveBeatMicroseconds
                        now <- Time.getCurrentTime
                        atomically do
                           a <- readTVar tactive
                           when a $
                              modifyTVar' tjobs $
                                 Map.insert
                                    jId
                                    ( Meta
                                       { alive = Just now
                                       , nice = meta.nice
                                       , try = meta.try
                                       , wait = meta.wait
                                       }
                                    , job
                                    )
                           pure a
                     case eactive of
                        Right False -> pure ()
                        _ -> again
                  )
                  (Ex.uninterruptibleMask_ . killThread)
            pure
               Work
                  { id = jId
                  , job
                  , meta
                  , retry = \n w ->
                     atomically $ writeTVar tout $ Just (Just (n, w))
                  , finish = atomically $ writeTVar tout $ Just Nothing
                  }
         }

--------------------------------------------------------------------------------

-- | Like 'registerDelay', but waits until a specified 'Time.UTCTime'.
registerDelayUTCTime :: Time.UTCTime -> IO (TVar Bool)
registerDelayUTCTime wait = do
   start <- Time.getCurrentTime
   if wait <= start
      then newTVarIO True
      else do
         tvtmp <-
            registerDelayMicro $
               ceilingPicoToMicro $
                  Time.nominalDiffTimeToSeconds $
                     Time.diffUTCTime wait start
         tvout <- newTVarIO False
         void $ Ex.mask_ $ forkIO do
            atomically $ readTVar tvtmp >>= check
            fix \again -> do
               now <- Time.getCurrentTime
               if wait <= now
                  then atomically $ writeTVar tvout True
                  else do
                     -- Only happens if there were leap seconds involved.
                     -- We just loop innefficiently for a bit.
                     threadDelay 10_000 -- 10 milliseconds
                     again
         pure tvout

ceilingPicoToMicro :: Pico -> Micro
ceilingPicoToMicro (MkFixed p) = MkFixed (ceiling (toRational p / 1_000_000))

-- | Like 'registerDelay', but not limited to @'maxBound' :: 'Int'@.
registerDelayMicro :: Micro -> IO (TVar Bool)
registerDelayMicro us
   | us <= 0 = newTVarIO True
   | otherwise = do
      tv <- liftIO $ newTVarIO False
      void $ Ex.mask_ $ forkIO do
         threadDelayMicro us
         atomically $ writeTVar tv True
      pure tv

-- | Like 'threadDelay', but not limited to @'maxBound' :: 'Int'@.
threadDelayMicro :: Micro -> IO ()
threadDelayMicro = fix \again us -> when (us > 0) do
   let d = floor (min us stepMax * 1_000_000)
   threadDelay d
   again (us - stepMax)
  where
   stepMax :: Micro
   stepMax = MkFixed (fromIntegral (maxBound :: Int))

--------------------------------------------------------------------------------

resourceVanishedWithCallStack :: (HasCallStack) => String -> IOError
resourceVanishedWithCallStack s =
   (userError s)
      { ioe_location = prettyCallStack (popCallStack callStack)
      , ioe_type = ResourceVanished
      }
