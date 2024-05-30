{-# LANGUAGE QuasiQuotes #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Job.Sq (migrations, queue) where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception.Safe qualified as Ex
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.Class
import Control.Monad.Trans.Maybe
import Control.Monad.Trans.Resource.Extra qualified as R
import Data.Acquire qualified as A
import Data.Aeson qualified as Ae
import Data.Function
import Data.Functor
import Data.Functor.Contravariant
import Data.Functor.Contravariant.Divisible
import Data.Int
import Data.Time qualified as Time
import GHC.IO.Exception
import GHC.Stack
import Sq qualified

import Job

--------------------------------------------------------------------------------

migrations :: [Sq.Migration]
migrations =
   [ Sq.migration "add job table" do
      Sq.zero @()
         ( Sq.writeStatement
            mempty
            mempty
            [Sq.sql|
             CREATE TABLE job
               ( rowid INTEGER NOT NULL PRIMARY KEY
               , id TEXT NOT NULL UNIQUE
               , nice INTEGER NOT NULL
               , wait TEXT NOT NULL
               , try INTEGER NOT NULL
               , alive TEXT NULL
               , job TEXT NOT NULL
               ) |]
         )
         ()
   , Sq.migration "add job index ix_id" do
      Sq.zero @()
         ( Sq.writeStatement
            mempty
            mempty
            "CREATE UNIQUE INDEX ix_id ON job (id)"
         )
         ()
   , Sq.migration "add job index ix_wait" do
      Sq.zero @()
         ( Sq.writeStatement
            mempty
            mempty
            "CREATE INDEX ix_wait ON job (wait)"
         )
         ()
   , Sq.migration "add job index ix_alive" do
      Sq.zero @()
         ( Sq.writeStatement
            mempty
            mempty
            "CREATE INDEX ix_alive ON job (alive)"
         )
         ()
   ]

inIdMetaJob :: (Ae.ToJSON job) => Sq.Input (Id, Meta, job)
inIdMetaJob =
   divide
      (\(i, m, j) -> (i, (m, j)))
      "id"
      (divided inMeta (Sq.encode "job" Sq.encodeAeson))

inMeta :: Sq.Input Meta
inMeta =
   mconcat
      [ contramap (.nice) "nice"
      , contramap (.try) "try"
      , contramap (.alive) "alive"
      , contramap (.wait) "wait"
      ]

outMeta :: Sq.Output Meta
outMeta = do
   nice <- "nice"
   try <- "try"
   alive <- "alive"
   wait <- "wait"
   pure Meta{..}

outIdMetaJob :: (Ae.FromJSON job) => Sq.Output (Id, Meta, job)
outIdMetaJob = do
   ji <- "id"
   meta <- outMeta
   job <- Sq.decode "job" Sq.decodeAeson
   pure (ji, meta, job)

stInsert
   :: (Ae.ToJSON job)
   => Sq.Statement 'Sq.Write (Id, Meta, job) Int64
stInsert =
   Sq.writeStatement
      inIdMetaJob
      "rowid"
      [Sq.sql|
       INSERT INTO job (id, nice, wait, try, alive, job)
       VALUES ($id, $nice, $wait, $try, $alive, $job)
       RETURNING rowid
      |]

stDelete :: Sq.Statement 'Sq.Write Id Int64
stDelete =
   Sq.writeStatement
      "id"
      "rowid"
      "DELETE FROM job WHERE id=$id RETURNING rowid"

stHeartbeat :: Sq.Statement 'Sq.Write (Id, Maybe Time.UTCTime) Int64
stHeartbeat =
   Sq.writeStatement
      (divided "id" "alive")
      "rowid"
      "UPDATE job SET alive=$alive WHERE id=$id RETURNING rowid"

stUpdate
   :: (Ae.ToJSON job)
   => Sq.Statement 'Sq.Write (Id, Meta, job) Int64
stUpdate =
   Sq.writeStatement
      inIdMetaJob
      "rowid"
      [Sq.sql|
       UPDATE job
       SET nice=$nice, wait=$wait, try=$try, alive=$alive, job=$job
       WHERE id=$id
       RETURNING rowid
      |]

stById :: (Ae.FromJSON job) => Sq.Statement 'Sq.Read Id (Meta, job)
stById =
   Sq.readStatement
      "id"
      ((,) <$> outMeta <*> Sq.decode "job" Sq.decodeAeson)
      [Sq.sql|
       SELECT nice, wait, try, alive, job
       FROM job
       WHERE id=$id
      |]

stHasNext :: Sq.Statement 'Sq.Read Time.UTCTime Bool
stHasNext =
   Sq.readStatement
      "now"
      ("out" <&> \(n :: Int) -> n > 0)
      [Sq.sql|
       SELECT count(*) AS out
       FROM job
       WHERE alive IS NULL AND wait <= $now
       LIMIT 1
      |]

stNext
   :: (Ae.FromJSON job)
   => Sq.Statement 'Sq.Read Time.UTCTime (Id, Meta, job)
stNext =
   Sq.readStatement
      "now"
      outIdMetaJob
      [Sq.sql|
       SELECT id, nice, wait, try, alive, job
       FROM job
       WHERE alive IS NULL AND wait <= $now
       ORDER BY nice, wait, try, id, rowid
       LIMIT 1
      |]

stPrune :: (Ae.FromJSON job) => Sq.Statement 'Sq.Read () (Id, Meta, job)
stPrune =
   Sq.readStatement
      mempty
      outIdMetaJob
      [Sq.sql|
       SELECT id, nice, wait, try, alive, job
       FROM job
       ORDER BY alive NULLS FIRST, nice, wait, try, id, rowid
      |]

keepAliveBeatMicro :: Int
keepAliveBeatMicro = 4_000_000

retryDelay :: Time.NominalDiffTime
retryDelay = 2

data Exit = ExitDefault | ExitFinish | ExitRetry Nice Time.UTCTime

queue
   :: forall job
    . (Ae.ToJSON job, Ae.FromJSON job)
   => Sq.Pool 'Sq.Write
   -> Queue job
queue db =
   Queue
      { push = \nice wait job -> do
         ji <- newId
         Sq.commit db do
            let m = Meta{alive = Nothing, try = 0, ..}
            _ <- Sq.one stInsert (ji, m, job)
            pure ji
      , pull = do
         tye <- liftIO $ newTVarIO $ Just ExitDefault
         (ji, meta, job) <-
            R.mkAcquireType1
               ( -- Block until job available.
                 -- TODO: use sqlite3_commit_hook or sqlite3_update_hook
                 -- instead of polling.
                 fix \again ->
                  maybe (threadDelay 1_000_000 >> again) pure
                     =<< runMaybeT do
                        t0 <- lift Time.getCurrentTime
                        -- We use a read tx for running stHasNext first
                        -- because it leads to better concurrency.
                        void $ MaybeT $ Sq.read db do
                           Sq.one stHasNext t0 <&> \case
                              True -> Just ()
                              False -> Nothing
                        MaybeT $ Sq.commit db do
                           Sq.maybe stNext t0 >>= traverse \(ji, Meta{..}, job) -> do
                              let m1 = Meta{alive = Just t0, ..}
                              void $ Sq.one stHeartbeat (ji, Just t0)
                              pure (ji, m1, job)
               )
               ( \(ji, m0, job0) rt -> do
                  exit <- atomically do
                     readTVar tye >>= \case
                        Nothing -> Ex.throwString "impossible"
                        Just exit -> writeTVar tye Nothing >> pure exit
                  now <- Time.getCurrentTime
                  let alive = Nothing
                  Sq.commit db $ case exit of
                     ExitDefault | A.ReleaseExceptionWith _ <- rt -> do
                        let wait = Time.addUTCTime retryDelay now
                        Sq.maybe stById ji >>= \case
                           Nothing -> do
                              let nice = m0.nice
                                  m1 = Meta{try = succ m0.try, ..}
                              void $ Sq.one stInsert (ji, m1, job0)
                           Just (Meta{try, nice}, job1 :: job) -> do
                              -- job0 and job1 should be the same.
                              -- However, just in case, we preserve job1.
                              let m1 = Meta{try = succ try, ..}
                              void $ Sq.one stUpdate (ji, m1, job1)
                     ExitRetry nice wait -> do
                        Sq.maybe stById ji >>= \case
                           Nothing -> do
                              let m1 = Meta{try = succ m0.try, ..}
                              void $ Sq.one stInsert (ji, m1, job0)
                           Just (Meta{try}, job1 :: job) -> do
                              -- job0 and job1 should be the same.
                              -- However, just in case, we preserve job1.
                              let m1 = Meta{try = succ try, ..}
                              void $ Sq.one stUpdate (ji, m1, job1)
                     _ -> void $ Sq.one stDelete ji
               )
         void $
            R.mkAcquire1
               ( forkIO $ fix \again -> Ex.handleAny (const again) do
                  threadDelay keepAliveBeatMicro
                  readTVarIO tye >>= \case
                     Nothing -> pure ()
                     Just _ -> do
                        now <- Time.getCurrentTime
                        void $ Sq.commit db $ Sq.one stHeartbeat (ji, Just now)
                        again
               )
               (Ex.uninterruptibleMask_ . killThread)
         pure
            Work
               { job
               , meta
               , id = ji
               , retry = \ !nice !wait -> atomically do
                  readTVar tye >>= \case
                     Nothing -> Ex.throwM $ resourceVanished "Job.Sq.queue"
                     Just _ -> writeTVar tye $ Just $ ExitRetry nice wait
               , finish = atomically do
                  readTVar tye >>= \case
                     Nothing -> Ex.throwM $ resourceVanished "Job.Sq.queue"
                     Just _ -> writeTVar tye $ Just ExitFinish
               }
      , prune = \f -> Sq.commit db do
         (_, xs0) <- Sq.list stPrune ()
         foldM
            ( \ !al (i, m, j) -> do
               let (keep, ar) = f i m j
               if keep
                  then void $ Sq.one stUpdate (i, m, j)
                  else void $ Sq.one stDelete i
               pure (al <> ar)
            )
            mempty
            xs0
      }

--------------------------------------------------------------------------------

instance Sq.EncodeDefault Id where
   encodeDefault = contramap (.uuid7) Sq.encodeDefault

instance Sq.DecodeDefault Id where
   decodeDefault =
      Sq.decodeRefine
         (maybe (Left "Not valid UUIDv7") Right . idFromUUID7)
         Sq.decodeDefault

deriving via Int32 instance Sq.EncodeDefault Nice
deriving via Int32 instance Sq.DecodeDefault Nice

--------------------------------------------------------------------------------

resourceVanished :: (HasCallStack) => String -> IOError
resourceVanished s =
   (userError s)
      { ioe_location = prettyCallStack (popCallStack callStack)
      , ioe_type = ResourceVanished
      }
