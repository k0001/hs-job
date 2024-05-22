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
import Data.Function
import Data.IntMap.Strict qualified as IntMap
import Data.Map.Strict qualified as Map
import Data.Maybe
import Data.Time qualified as Time
import Data.Word
import GHC.IO.Exception
import GHC.Stack

import Job

--------------------------------------------------------------------------------

keepAliveBeatMicroseconds :: Int
keepAliveBeatMicroseconds = 5_000_000

autoRetryDelaySeconds :: Time.NominalDiffTime
autoRetryDelaySeconds = 2

-- | Internal.
data Val a = Val
   { job :: a
   , try :: Word32
   , nice :: Nice
   , wait :: Time.UTCTime
   , alive :: Maybe Time.UTCTime
   }

-- | Get the next 'Work'able 'Val' starting at or after the given time, if any.
next' :: Time.UTCTime -> Map.Map Id (Val a) -> Maybe (Id, Val a)
next' t =
   Map.foldlWithKey
      ( \yo jId v ->
         if isNothing v.alive && v.wait <= t
            then case yo of
               Just (_, ov)
                  | ov.nice <= v.nice
                  , ov.try <= v.try
                  , ov.wait <= v.wait ->
                     yo
               _ -> Just (jId, v)
            else yo
      )
      Nothing

prune'
   :: (Monoid a)
   => (Prune job -> (Bool, a))
   -> Map.Map Id (Val job)
   -> (a, Map.Map Id (Val job))
prune' f =
   Map.foldlWithKey
      ( \(!al, !m) jId v@Val{..} ->
         let (!keep, !ar) = f Prune{id = jId, ..}
         in  (al <> ar, if keep then Map.insert jId v m else m)
      )
      (mempty, mempty)

queue :: forall job. A.Acquire (Queue job)
queue = do
   -- tjobs: All the jobs in this 'Queue', running or not. Notice that if
   -- 'prune' removes some currently running jobs, they may be added back if
   -- requested by the 'pull' or 'retry'.
   tjobs :: TVar (Map.Map Id (Val job)) <-
      R.mkAcquire1 (newTVarIO mempty) \tv ->
         -- Discard all jobs. Alive or not.
         -- TODO: Should we wait until running jobs finish?
         atomically $ writeTVar tv mempty
   -- twork: If 'pull' couldn't find a suitable job in 'tjobs' right away,
   -- then it'll block waiting for a STM action pushed to 'twork'. To be
   -- applied to the 'Id' and 'Val' already marked as 'active' in 'tjobs'.
   twork :: TVar (IntMap.IntMap ((Id, Val job) -> STM ())) <-
      R.mkAcquire1 (newTVarIO mempty) \t ->
         atomically $ writeTVar t mempty
   -- shift: connect jobs ready to be executed in 'tjobs', with workers
   -- waiting for a job in 'twork'. If no job or no worker, do nothing.
   let shift :: Time.UTCTime -> STM ()
       shift now = fix \again -> do
         work0 <- readTVar twork
         forM_ (IntMap.minView work0) \(f, work1) -> do
            jobs <- readTVar tjobs
            forM_ (next' now jobs) \(jId, Val{..}) -> do
               let v = Val{alive = Just now, ..}
               modifyTVar' tjobs $ Map.insert jId v
               writeTVar twork work1
               f (jId, v)
               again
   -- This background thread perform 'shift's every 60 seconds.  Not really
   -- necessary, just in case a timer died for some reason.
   void do
      R.mkAcquire1
         ( forkIO $ forever do
            threadDelay 60_000_000 -- 60 seconds
            now <- Time.getCurrentTime
            atomically $ shift now
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
            t0 <- Time.getCurrentTime
            join $ atomically do
               ensureActive
               let v = Val{alive = Nothing, try = 0, nice, wait, job}
               modifyTVar' tjobs $ Map.insert jId v
               if wait <= t0
                  then shift t0 >> pure (pure ())
                  else pure do
                     tde <- registerDelayUTCTime wait
                     void $ Ex.mask_ $ forkIO do
                        atomically $ readTVar tde >>= check
                        t1 <- Time.getCurrentTime
                        atomically $ shift t1
            pure jId
         , ---------
           prune = \f -> atomically do
            ensureActive
            readTVar tactive >>= \case
               False -> pure mempty -- It's alright to “fail silently”
               True -> do
                  (!b, !m) <- prune' f <$> readTVar tjobs
                  writeTVar tjobs m
                  pure b
         , ---------
           ready = do
            now <- Time.getCurrentTime
            jobs <- readTVarIO tjobs
            pure $ isJust $ next' now jobs
         , ---------
           pull = do
            -- gjob: Will block until a job is ready for being worked on.
            gjob :: STM (Id, Val job) <- fmap snd do
               R.mkAcquire1
                  ( atomically do
                     ensureActive
                     work0 <- readTVar twork
                     let k = succ $ maybe minBound fst $ IntMap.lookupMax work0
                     t <- newEmptyTMVar
                     writeTVar twork $ IntMap.insert k (putTMVar t) work0
                     pure (k, ensureActive >> takeTMVar t)
                  )
                  (atomically . modifyTVar' twork . IntMap.delete . fst)
            -- tout: If 'finish' is called, then @'Just' 'Nothing'@, if
            -- 'retry' is called, then @'Just' ('Just' _)@.
            tout :: TVar (Maybe (Maybe (Nice, Time.UTCTime))) <-
               liftIO $ newTVarIO Nothing
            (jId :: Id, v :: Val job) <- do
               R.mkAcquireType1
                  ( do
                     now <- Time.getCurrentTime
                     atomically $ ensureActive >> shift now
                     atomically $ gjob
                  )
                  \(jId, v) rt -> do
                     now <- Time.getCurrentTime
                     atomically $
                        readTVar tout >>= \case
                           -- no 'retry', and no 'finish', and release with exception.
                           Nothing | A.ReleaseExceptionWith _ <- rt -> do
                              ensureActive
                              let Val{job, nice} = v
                                  alive = Nothing
                                  try = succ v.try
                                  wait = Time.addUTCTime autoRetryDelaySeconds now
                              modifyTVar' tjobs $ Map.insert jId Val{..}
                           -- explicit 'retry'.
                           Just (Just (nice, wait)) -> do
                              ensureActive
                              let Val{job} = v
                                  alive = Nothing
                                  try = succ v.try
                              modifyTVar' tjobs $ Map.insert jId Val{..}
                           -- explicit 'finish', or release without exception.
                           _ -> modifyTVar' tjobs $ Map.delete jId
            -- While working on this job, send heartbeats
            void do
               R.mkAcquire1
                  ( forkIO $ fix \again -> do
                     eactive <- Ex.tryAny do
                        threadDelay keepAliveBeatMicroseconds
                        now <- Time.getCurrentTime
                        let v1 = case v of Val{..} -> Val{alive = Just now, ..}
                        atomically do
                           a <- readTVar tactive
                           when a $ modifyTVar' tjobs $ Map.insert jId v1
                           pure a
                     case eactive of
                        Right False -> pure ()
                        _ -> again
                  )
                  (Ex.uninterruptibleMask_ . killThread)
            pure
               Work
                  { id = jId
                  , retry = \n w ->
                     atomically $ writeTVar tout $ Just (Just (n, w))
                  , finish = atomically $ writeTVar tout $ Just Nothing
                  , job = v.job
                  , try = v.try
                  , nice = v.nice
                  , wait = v.wait
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
