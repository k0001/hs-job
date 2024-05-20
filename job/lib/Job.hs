{-# LANGUAGE StrictData #-}
{-# LANGUAGE NoFieldSelectors #-}

module Job
   ( -- * Nice
    Nice (..)

    -- * Id
   , Id
   , unsafeIdFromUUID7
   , idFromUUID7
   , newId

    -- * Job
   , Work (..)
   , Queue (..)
   , Prune (..)
   ) where

import Control.Monad
import Control.Monad.IO.Class
import Data.Acquire qualified as A
import Data.Int
import Data.Time qualified as Time
import Data.UUID.V7 (UUID)
import Data.UUID.V7 qualified as UUID7
import Data.Word
import GHC.Records
import GHC.Stack

--------------------------------------------------------------------------------

-- | Nice value for a job.
--
-- * The __lower__ the value, the less “nice” a job is to other jobs.  Meaning
-- it will have priority over other jobs, and possibly be allocated more
-- resources.
--
-- * The __higher__ the value, the “nicer” a job is to other jobs. Meaning it
-- will have less priority over other jobs, and possibly allow competing jobs
-- to take more resources.
--
-- * Use @0@ as default 'Nice' value.
newtype Nice = Nice {int32 :: Int32}
   deriving newtype (Eq, Ord, Show, Enum, Bounded)

--------------------------------------------------------------------------------

-- | Unique identifier for the scheduled @job@ (and re-scheduled @job@, see
-- 'pull' and 'retry').
newtype Id
   = -- | Unsafe because there's no guarantee that the UUID is V7,
     -- which this library expects. Use 'idFromUUID7' if possible.
     UnsafeId UUID
   deriving newtype (Eq, Ord, Show)

instance HasField "uuid7" Id UUID where
   getField (UnsafeId u) = u

unsafeIdFromUUID7 :: (HasCallStack) => UUID -> Id
unsafeIdFromUUID7 = maybe (error "unsafeIdFromUUID7") id . idFromUUID7

idFromUUID7 :: UUID -> Maybe Id
idFromUUID7 u = UnsafeId u <$ guard (UUID7.validate u)

newId :: (MonadIO m) => m Id
newId = UnsafeId <$> UUID7.genUUID

--------------------------------------------------------------------------------

data Work job = Work
   { id :: Id
   -- ^ Unique identifier for the scheduled @job@ (and re-scheduled @job@, see
   -- 'pull' and 'retry').
   , job :: job
   -- ^ The actual @job@ to be carried out.
   , try :: Word32
   -- ^ How many tries have been attempted already (excluding the current one).
   , nice :: Nice
   -- ^ 'Nice' value used while scheduling the @job@.
   , wait :: Time.UTCTime
   -- ^ Time until the 'Queue' was supposed to wait before considering
   -- working on the @job@.
   , retry :: forall m. (MonadIO m) => Nice -> Time.UTCTime -> m ()
   -- ^ Once this 'Work' is released, reschedule to be executed at the
   -- specified 'Time.UTCTime' at the earliest.
   --
   -- See the documentation for 'Queue'\'s 'pull'.
   --
   -- @
   -- 'retry' _ _ '>>' 'retry' n t  ==  'retry' n2 t2
   -- 'finish'    '>>' 'retry' n t  ==  'retry' n t
   -- 'retry' n t '>>' 'finish'     ==  'finish'
   -- @
   , finish :: forall m. (MonadIO m) => m ()
   -- ^ Once this 'Work' is released, remove it from the execution queue.
   --
   -- See the documentation for 'Queue'\'s 'pull'.
   --
   -- @
   -- 'finish'    '>>' 'retry' n t  ==  'retry' n t
   -- 'retry' n t '>>' 'finish'     ==  'finish'
   -- 'finish'    '>>' 'finish'     ==  'finish'
   -- @
   }
   deriving stock (Functor)

-- | A @job@ 'Queue'.
--
-- * @job@s can be 'push'ed to the 'Queue' for eventual execution, 'pull'ed
-- from the 'Queue' for immediate execution, and the 'Queue' itself can be
-- pruned.
--
-- * @"Job.Memory".'Job.Memory.queue'@ is an in-memory implementation that can
-- serve as reference.
--
-- * Other backends are expected to provide a 'Queue' implementation.
data Queue job = Queue
   { push
      :: forall m
       . (MonadIO m)
      => Nice
      -> Time.UTCTime
      -> job
      -> m Id
   -- ^ Push new @job@ to the queue so to be executed after the specified
   -- 'Time.UTCTime', which may be in the past.
   , pull :: A.Acquire (Work job)
   -- ^ Pull some 'Work' from the queue.
   --
   -- * Blocks until 'Work' is available.
   --
   -- * On 'A.ReleaseExceptionWith', the @job@ is automatically rescheduled
   -- for re-execution after a few seconds. This behavior can be overriden
   -- by using 'Work'\'s 'retry' or 'finish'.
   --
   -- * On 'A.ReleaseNormal' or 'A.ReleaseEarly', the @job@ is automatically
   -- removed from the 'Queue'. This behavior can be overriden
   -- by using 'Work'\'s 'retry' or 'finish'.
   , prune
      :: forall b m
       . (Monoid b, MonadIO m)
      => (Prune job -> Maybe b)
      -> m b
   -- ^ Remove from the 'Queue' those @job@s for which the given function
   -- returns 'Just'. Allows collecting some additional output in @b@.
   -- The given @job@s are in no particular order.
   }

-- | Wrapper for all the @job@-related data accessible through 'Queue'\'s
-- 'prune' function.
data Prune job = Prune
   { id :: Id
   -- ^ Unique identifier for the scheduled @job@ (and re-scheduled @job@, see
   -- 'pull' and 'retry').
   , job :: job
   -- ^ The actual @job@ to be carried out.
   , try :: Word32
   -- ^ How many tries have been attempted already (excluding the current
   -- one, in case 'alive' is 'Just').
   , nice :: Nice
   -- ^ 'Nice' value used while scheduling the @job@.
   , wait :: Time.UTCTime
   -- ^ Time until the 'Queue' is or was supposed to wait before considering
   -- working on the @job@.
   , alive :: Maybe Time.UTCTime
   -- ^ If 'Just', the @job@ is currently being 'Work'ed on, allegedly. The
   -- last time the @job@ sent a heartbeat is attached.
   }
   deriving (Eq, Ord, Show)
