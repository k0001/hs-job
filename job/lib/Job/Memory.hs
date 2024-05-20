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
   :: (Monoid b)
   => (Prune a -> Maybe b)
   -> Map.Map Id (Val a)
   -> (b, Map.Map Id (Val a))
prune' f =
   Map.foldlWithKey
      ( \(!bl, !m) jId v ->
         case f (mkPrune jId v) of
            Nothing -> (bl, Map.insert jId v m)
            Just br -> (bl <> br, m)
      )
      (mempty, mempty)

queue :: A.Acquire (Queue a)
queue = do
   tmids :: TVar (Map.Map Id (Val a)) <-
      R.mkAcquire1 (newTVarIO mempty) \tv ->
         -- Discard all jobs. Alive or not.
         -- TODO: Should we wait until running jobs finish?
         atomically $ writeTVar tv mempty
   tmwait :: TVar (Map.Map Time.UTCTime (Id -> Val a -> STM ())) <-
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
         :: Id -> Time.UTCTime -> Word32 -> Nice -> Time.UTCTime -> a -> STM ()
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
         -> Time.UTCTime
         -> A.ReleaseType
         -> Maybe (Maybe (Nice, Time.UTCTime)) -- tout
         -> STM ()
       relWork jId now rt out = do
         m0 <- readTVar tmids
         let (yv, !m1) = Map.updateLookupWithKey (\_ _ -> Nothing) jId m0
         writeTVar tmids m1
         forM_ yv \v ->
            -- Note: We always enter this forM_
            case out of
               Just Nothing -> pure ()
               Just (Just (nice, wait)) ->
                  pushId jId now (succ v.try) nice wait v.job
               Nothing -> case rt of
                  A.ReleaseExceptionWith _ -> do
                     let wait = Time.addUTCTime 2 now
                     pushId jId now (succ v.try) v.nice wait v.job
                  _ -> pure ()
   pure
      Queue
         { push = \nice wait job -> liftIO do
            jId <- newId
            now <- Time.getCurrentTime
            atomically $ pushId jId now 0 nice wait job
            pure jId
         , ---------
           prune = \f -> liftIO $ atomically do
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
            ew :: Either (STM (Id, Val a)) (Id, Val a) <- fmap (first fst) do
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
                              modifyTVar' tmwait $ Map.insert now (curry $ putTMVar t)
                              pure $
                                 Left
                                    ( throwIfVanished >> takeTMVar t
                                    , modifyTVar' tmwait $ Map.delete now
                                    )
                  )
                  ( \ew rt -> case ew of
                     Left (_, r) -> atomically r
                     Right (jId, _) -> do
                        now <- Time.getCurrentTime
                        atomically $ relWork jId now rt =<< readTVar tout
                  )
            -- If we didn't manage to immediately get 'Work' on the last step,
            -- then block here waiting and set up the corresponding release.
            (jId :: Id, v :: Val a) <- case ew of
               Right x -> pure x
               Left sx -> R.mkAcquireType1 (atomically sx) \(jId, _) rt -> do
                  now <- Time.getCurrentTime
                  atomically $ relWork jId now rt =<< readTVar tout
            -- While the 'Work' is alive, send heartbeats every 5 seconds.
            _keepalive :: ThreadId <-
               R.mkAcquire1
                  ( -- This code runs with exceptions masked because of mkAcquire1.
                    -- TODO: Stop if not 'tactive'.
                    forkIO $ forever $ Ex.tryAny do
                     threadDelay 5_000_000 -- 5 seconds
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
                     liftIO $ atomically $ writeTVar tout $ Just (Just (n, w))
                  , finish = liftIO $ atomically $ writeTVar tout $ Just Nothing
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
