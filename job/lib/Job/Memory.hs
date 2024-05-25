{-# LANGUAGE StrictData #-}
{-# LANGUAGE NoFieldSelectors #-}

module Job.Memory (queue) where

import Control.Concurrent
import Control.Concurrent.Async qualified as As
import Control.Concurrent.STM
import Control.Exception.Safe qualified as Ex
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Maybe (MaybeT (..), runMaybeT)
import Control.Monad.Trans.Resource.Extra qualified as R
import Control.Monad.Trans.State.Strict (put, runStateT)
import Data.Acquire qualified as A
import Data.Fixed
import Data.Function
import Data.List qualified as List
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Set (Set)
import Data.Set qualified as Set
import Data.Time qualified as Time
import Data.UUID.V7 (UUID)
import Data.UUID.V7 qualified as UUID7
import GHC.IO.Exception
import GHC.Stack

import Job

--------------------------------------------------------------------------------

keepAliveBeat :: Micro
keepAliveBeat = 4

retryDelay :: Time.NominalDiffTime
retryDelay = 2

data Env job = Env
   { jobs :: TVar (Map Id (Meta, job))
   , queued :: TVar (Set (Meta, Id))
   , worker :: TMVar (UUID, (Id, Meta, job) -> STM ())
   , active :: TVar Bool
   }

acqEnv :: A.Acquire (Env job)
acqEnv = do
   jobs <- R.mkAcquire1 (newTVarIO mempty) \t -> do
      atomically $ writeTVar t mempty
   queued <- R.mkAcquire1 (newTVarIO mempty) \t -> do
      atomically $ writeTVar t mempty
   worker <- R.mkAcquire1 newEmptyTMVarIO \t ->
      atomically $ void $ tryTakeTMVar t
   active <- R.mkAcquire1 (newTVarIO True) \tv ->
      atomically $ writeTVar tv False
   pure Env{..}

ensureActive :: Env job -> STM ()
ensureActive env =
   readTVar env.active >>= \case
      True -> pure ()
      False -> throwSTM $ resourceVanished "Job.Memory.queue"

connect1 :: forall job. Env job -> Time.UTCTime -> STM (Maybe Id)
connect1 env now =
   readTVar env.active >>= \case
      False -> pure Nothing
      True ->
         nextJob >>= mapM \x@(i, _, _) -> do
            (_, f) <- takeTMVar env.worker
            f x >> pure i
  where
   nextJob :: STM (Maybe (Id, Meta, job))
   nextJob = do
      q0 <- readTVar env.queued
      forM (Set.minView q0) \((m0@Meta{..}, i), q1) -> do
         writeTVar env.queued q1
         let m = Meta{alive = Just now, ..}
         jobs0 <- readTVar env.jobs
         (!jobs1, yj) <- flip runStateT Nothing do
            Map.alterF
               ( mapM \(m1, j) -> do
                  when (m0 /= m1) $ Ex.throwString "m0 /= m1"
                  put (Just j)
                  pure (m, j)
               )
               i
               jobs0
         case yj of
            Just j -> writeTVar env.jobs jobs1 >> pure (i, m, j)
            Nothing -> Ex.throwString "job in queue but not in map"

connectMany :: forall job. Env job -> IO [Id]
connectMany env = fmap ($ []) (go id)
  where
   go :: ([Id] -> [Id]) -> IO ([Id] -> [Id])
   go f = do
      -- We perform each connect1 in a separate transaction
      -- so that we don't hog concurrent access to the Env.
      now <- Time.getCurrentTime
      atomically (connect1 env now) >>= \case
         Just i -> go ((i :) . f)
         Nothing -> pure f

-- | An in-memory 'Queue'.
queue :: forall job. A.Acquire (Queue job)
queue = do
   env <- acqEnv
   void do
      -- This background thread perform 'connectMany' every 60 seconds.
      -- Not really necessary, just in case a timer died for some reason.
      R.mkAcquire1
         ( As.async $ forever do
            threadDelay 60_000_000
            void $ connectMany env
         )
         As.uninterruptibleCancel
   pure
      Queue
         { push = \nice wait job -> do
            let m = Meta{nice, wait, alive = Nothing, try = 0}
            i <- newId
            atomically do
               ensureActive env
               modifyTVar' env.jobs $ Map.insert i (m, job)
               modifyTVar' env.queued $ Set.insert (m, i)
            void $ As.async do
               threadDelayUTCTime wait
               void $ connectMany env
            pure i
         , ---------
           prune = \f ->
            atomically do
               readTVar env.active >>= \case
                  False -> pure mempty -- It's alright to “fail silently”
                  True -> do
                     jobs0 <- readTVar env.jobs
                     let (js, qs, a) =
                           List.foldl'
                              ( \(!js0, !qs0, !al) (i, (m, j)) ->
                                 case f i m j of
                                    (False, ar) -> (js0, qs0, al <> ar)
                                    (True, ar) ->
                                       ( (i, (m, j)) : js0
                                       , maybe ((m, i) : qs0) (const qs0) m.alive
                                       , al <> ar
                                       )
                              )
                              mempty
                              $ List.sortOn (\(i, (m, _)) -> (m, i))
                              $ Map.toList jobs0
                     writeTVar env.jobs $! Map.fromList js
                     writeTVar env.queued $! Set.fromList qs
                     pure a
         , ---------
           ready = join $ atomically do
            readTVar env.active >>= \case
               False -> pure (pure Nothing)
               True -> do
                  q <- readTVar env.queued
                  case Set.lookupMin q of
                     Nothing -> pure (pure (Just False))
                     Just (m, _) -> pure do
                        now <- Time.getCurrentTime
                        pure (Just (m.wait <= now))
         , ---------
           pull = runMaybeT do
            liftIO (readTVarIO env.active) >>= \case
               False -> MaybeT $ pure Nothing
               True -> pure ()
            tout :: TVar (Maybe (Maybe (Nice, Time.UTCTime))) <-
               liftIO $ newTVarIO Nothing
            (i :: Id, meta :: Meta, job :: job) <- MaybeT do
               R.mkAcquireType1
                  ( do
                     k0 <- UUID7.genUUID
                     Ex.bracketOnError
                        ( atomically do
                           readTVar env.active >>= \case
                              False -> pure Nothing
                              True -> do
                                 tj <- newEmptyTMVar
                                 -- Blocks until we become next worker
                                 putTMVar env.worker (k0, putTMVar tj)
                                 pure (Just tj)
                        )
                        ( mapM_ \_ -> atomically do
                           tryTakeTMVar env.worker >>= \case
                              Just (k1, f)
                                 | k0 /= k1 ->
                                    putTMVar env.worker (k1, f)
                              _ -> pure ()
                        )
                        ( \case
                           Nothing -> pure Nothing
                           Just tj -> atomically do
                              readTVar env.active >>= \case
                                 False -> pure Nothing
                                 True ->
                                    -- Blocks until we get job input
                                    Just <$> takeTMVar tj
                        )
                  )
                  ( \yx rt -> forM_ yx \(i, m0, job) -> do
                     ym1 <-
                        readTVarIO tout >>= \case
                           -- no 'retry', and no 'finish',
                           -- and release with exception.
                           Nothing
                              | A.ReleaseExceptionWith _ <- rt -> do
                                 now <- Time.getCurrentTime
                                 let alive = Nothing
                                     try = succ m0.try
                                     nice = m0.nice
                                     wait = Time.addUTCTime retryDelay now
                                 pure $ Just Meta{..}
                           -- explicit 'retry'.
                           Just (Just (nice, wait)) -> do
                              let alive = Nothing
                                  try = succ m0.try
                              pure $ Just Meta{..}
                           -- explicit 'finish',
                           -- or release without exception.
                           _ -> pure Nothing
                     case ym1 of
                        Nothing ->
                           atomically $ modifyTVar' env.jobs $ Map.delete i
                        Just !m1 -> do
                           atomically do
                              ensureActive env
                              modifyTVar' env.queued $ Set.insert (m1, i)
                              modifyTVar' env.jobs $
                                 Map.insert i (m1, job)
                           void $ As.async do
                              threadDelayUTCTime m1.wait
                              void $ connectMany env
                  )
            -- While working on this job, send heartbeats
            void $ lift do
               R.mkAcquire1
                  ( As.async $ fix \again -> do
                     eactive <- Ex.tryAny do
                        threadDelayMicro keepAliveBeat
                        now <- Time.getCurrentTime
                        let m1
                              | Meta{..} <- meta =
                                 Meta{alive = Just now, ..}
                        atomically do
                           a <- readTVar env.active
                           when a do
                              modifyTVar' env.jobs $
                                 Map.insert i (m1, job)
                           pure a
                     case eactive of
                        Right False -> pure ()
                        _ -> again
                  )
                  As.uninterruptibleCancel
            pure
               Work
                  { id = i
                  , job
                  , meta
                  , retry = \n w ->
                     atomically $ writeTVar tout $ Just (Just (n, w))
                  , finish = atomically $ writeTVar tout $ Just Nothing
                  }
         }

--------------------------------------------------------------------------------

-- | Like 'threadDelay', but waits until a specified 'Time.UTCTime'.
threadDelayUTCTime :: Time.UTCTime -> IO ()
threadDelayUTCTime wait = do
   t <- registerDelayUTCTime wait
   atomically $ readTVar t >>= check

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
         void $ Ex.mask_ $ As.async do
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
      void $ Ex.mask_ $ As.async do
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

resourceVanished :: (HasCallStack) => String -> IOError
resourceVanished s =
   (userError s)
      { ioe_location = prettyCallStack (popCallStack callStack)
      , ioe_type = ResourceVanished
      }
