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
import Data.Bifunctor
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

autoRescheduleDelaySeconds :: Time.NominalDiffTime
autoRescheduleDelaySeconds = 2

-- | Internal.
data Val a = Val
   { job :: a
   , try :: Word32
   , nice :: Nice
   , wait :: Time.UTCTime
   , alive :: Maybe Time.UTCTime
   }

mkPrune :: Id -> Val a -> Prune a
mkPrune jId Val{..} = Prune{id = jId, ..}

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
      ( \(!al, !m) jId v ->
         let (!keep, !ar) = f (mkPrune jId v)
         in  (al <> ar, if keep then Map.insert jId v m else m)
      )
      (mempty, mempty)

queue :: forall job. A.Acquire (Queue job)
queue = do
   tmids :: TVar (Map.Map Id (Val job)) <-
      R.mkAcquire1 (newTVarIO mempty) \tv ->
         -- Discard all jobs. Alive or not.
         -- TODO: Should we wait until running jobs finish?
         atomically $ writeTVar tv mempty
   tmwait :: TVar (Map.Map Time.UTCTime (Id -> Val job -> STM ())) <-
      R.mkAcquire1 (newTVarIO mempty) \tv ->
         atomically $ writeTVar tv mempty
   tactive :: TVar Bool <- R.mkAcquire1 (newTVarIO True) \tv ->
      atomically $ writeTVar tv False
   let throwIfVanished :: STM ()
       throwIfVanished = do
         active <- readTVar tactive
         when (not active) do
            throwSTM $ resourceVanishedWithCallStack "Job.Memory.queue"
       pushId
         :: Id
         -> Time.UTCTime
         -> Word32
         -> Nice
         -> Time.UTCTime
         -> job
         -> STM ()
       pushId jId now try nice wait job = do
         throwIfVanished
         let v0 = Val{alive = Nothing, job, ..}
         if wait <= now
            then do
               mwait0 <- readTVar tmwait
               case Map.minView mwait0 of
                  Just (f, mwait1) -> do
                     writeTVar tmwait mwait1
                     let v1 = Val{alive = Just now, ..}
                     modifyTVar' tmids $ Map.insert jId v1
                     f jId v1
                  Nothing -> modifyTVar' tmids $ Map.insert jId v0
            else modifyTVar' tmids $ Map.insert jId v0
   let relWork
         :: Id
         -> Val job
         -> Time.UTCTime
         -> A.ReleaseType
         -> Maybe (Maybe (Nice, Time.UTCTime)) -- tout
         -> STM ()
       relWork jId v now rt out = do
         modifyTVar' tmids $ Map.delete jId
         case out of
            Just Nothing -> pure ()
            Just (Just (nice, wait)) ->
               pushId jId now (succ v.try) nice wait v.job
            Nothing -> case rt of
               A.ReleaseExceptionWith _ -> do
                  let wait = Time.addUTCTime autoRescheduleDelaySeconds now
                  pushId jId now (succ v.try) v.nice wait v.job
               _ -> pure ()
   pure
      Queue
         { push = \nice wait job -> do
            jId <- newId
            now <- Time.getCurrentTime
            atomically $ pushId jId now 0 nice wait job
            pure jId
         , ---------
           prune = \f -> atomically do
            readTVar tactive >>= \case
               False -> pure mempty -- It's alright to “fail silently”
               True -> do
                  (!b, !m) <- prune' f <$> readTVar tmids
                  writeTVar tmids m
                  pure b
         , ---------
           pull = do
            -- tout: If 'finish' was called, then @'Just' 'Nothing'@, if
            -- 'retry' was called, then @'Just' ('Just' _)@.
            tout :: TVar (Maybe (Maybe (Nice, Time.UTCTime))) <-
               liftIO $ newTVarIO Nothing
            -- If there is 'Work' readily available, return it. Otherwise,
            -- returns a 'STM' action that will block until 'Work' is available.
            -- In both cases, the corresponding 'Val' was marked as being alive
            -- by the time we get our hands on the 'Work'.
            ew :: Either (STM (Id, Val job)) (Id, Val job) <-
               fmap (first fst) do
                  R.mkAcquireType1
                     ( do
                        now <- Time.getCurrentTime
                        atomically do
                           throwIfVanished
                           mwait0 <- readTVar tmwait
                           yw <- case Map.null mwait0 of
                              False -> pure Nothing
                              True -> do
                                 mids <- readTVar tmids
                                 forM (next' now mids) \(jId, Val{..}) -> do
                                    let v = Val{alive = Just now, ..}
                                    modifyTVar' tmids $ Map.insert jId v
                                    pure (jId, v)
                           case yw of
                              Just w -> pure $ Right w
                              Nothing -> do
                                 t :: TMVar (Id, Val a) <- newEmptyTMVar
                                 modifyTVar' tmwait $
                                    Map.insert now (curry $ putTMVar t)
                                 pure $
                                    Left
                                       ( throwIfVanished >> takeTMVar t
                                       , modifyTVar' tmwait $ Map.delete now
                                       )
                     )
                     ( \ew rt -> case ew of
                        Left (_, r) -> atomically r
                        Right (jId, v) -> do
                           now <- Time.getCurrentTime
                           atomically $ relWork jId v now rt =<< readTVar tout
                     )
            -- If we didn't manage to immediately get 'Work' on the last step,
            -- then block here waiting and set up the corresponding release.
            (jId :: Id, v :: Val job) <- case ew of
               Right x -> pure x
               Left sx -> R.mkAcquireType1 (atomically sx) \(jId, v) rt -> do
                  now <- Time.getCurrentTime
                  atomically $ relWork jId v now rt =<< readTVar tout
            -- While the 'Work' is alive, send heartbeats every 5 seconds.
            _keepAlive :: ThreadId <-
               R.mkAcquire1
                  ( -- This code runs with exceptions masked because of mkAcquire1.
                    -- TODO: Stop if not 'tactive'.
                    forkIO $ forever $ Ex.tryAny do
                     threadDelay keepAliveBeatMicroseconds
                     now <- Time.getCurrentTime
                     atomically do
                        modifyTVar' tmids $
                           Map.update (\Val{..} -> Just Val{alive = Just now, ..}) jId
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

resourceVanishedWithCallStack :: (HasCallStack) => String -> IOError
resourceVanishedWithCallStack s =
   (userError s)
      { ioe_location = prettyCallStack (popCallStack callStack)
      , ioe_type = ResourceVanished
      }
