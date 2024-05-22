{-# LANGUAGE StrictData #-}
{-# LANGUAGE NoFieldSelectors #-}

module Job
   ( -- * Nice
    Nice (..)
   , nice0

    -- * Id
   , Id
   , unsafeIdFromUUID7
   , idFromUUID7
   , newId

    -- * Queue
   , Queue (..)
   , push
   , prune

    -- * Work
   , Work (..)
   , retry
   , finish

    -- * Prune
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
-- * Use 'nice0' (i.e,. @'Nice' 0@) as default 'Nice' value.
newtype Nice = Nice {int32 :: Int32}
   deriving newtype (Eq, Ord, Show, Enum, Bounded)

nice0 :: Nice
nice0 = Nice 0

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

-- | A @job@ together with its 'Queue' execution context details.
--
-- As soon as you get your hands on a 'Work', which you do through 'pull',
-- start working on it right away.
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
   , retry :: Nice -> Time.UTCTime -> IO ()
   -- ^ Once this 'Work' is released, reschedule to be executed at the
   -- specified 'Time.UTCTime' at the earliest.
   --
   -- See the documentation for 'Queue'\'s 'pull'.
   --
   -- @
   -- 'retry' _ _ '>>' 'retry' n t  ==  'retry' n t
   -- 'finish'    '>>' 'retry' n t  ==  'retry' n t
   -- 'retry' n t '>>' 'finish'     ==  'finish'
   -- @
   , finish :: IO ()
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

-- | Like the 'retry' field in 'Work', except with a bit more
-- polymorphic type and intended to be used as a top-level function.
retry
   :: forall job m
    . (MonadIO m)
   => Work job
   -> Nice
   -> Time.UTCTime
   -> m ()
retry Work{retry = f} n t = liftIO $ f n t

-- | Like the 'finish' field in 'Work', except with a bit more
-- polymorphic type and intended to be used as a top-level function.
finish
   :: forall job m
    . (MonadIO m)
   => Work job
   -> m ()
finish Work{finish = m} = liftIO m

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
   { push :: Nice -> Time.UTCTime -> job -> IO Id
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
   , ready :: IO Bool
   -- ^ Returns immediately whether there is any @job@ waiting and ready to be
   -- 'Work'ed on right away. In other words, 'ready' returns 'True' in those
   -- cases where 'pull' would acquire 'Work' right away.
   , prune :: forall a. (Monoid a) => (Prune job -> (Bool, a)) -> IO a
   -- ^ Prune @job@s from the 'Queue', keeping only those for which the given
   -- function returns 'True' (like 'List.filter'). Allows collecting some
   -- additional 'Monoid'al output.  The given @job@s are in no particular
   -- order. __IMPORTANT:__ If you remove a @job@ that is currently active,
   -- it might be 'push'ed back to the 'Queue' later if required by 'retry'
   -- or a 'Work' exception.
   }

-- | Like the 'push' field in 'Queue', except with a bit more polymorphic type
-- and intended to be used as a top-level function.
push
   :: forall job m
    . (MonadIO m)
   => Queue job
   -> Nice
   -> Time.UTCTime
   -> job
   -> m Id
push Queue{push = f} n t j = liftIO $ f n t j

-- | Like the 'prune' field in 'Queue', except with a bit more polymorphic type
-- and intended to be used as a top-level function.
prune
   :: forall job a m
    . (Monoid a, MonadIO m)
   => Queue job
   -> (Prune job -> (Bool, a))
   -> m a
prune Queue{prune = f} g = liftIO $ f g

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
