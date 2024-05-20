module Main (main) where

import Data.Acquire qualified as A
import Data.Time qualified as Time
import Job qualified
import Job.Memory qualified

--------------------------------------------------------------------------------

main :: IO ()
main = A.withAcquire (Job.Memory.queue @String) \q -> do
   t0 <- Time.getCurrentTime

   [] <- Job.prune q \p -> (False, [p])

   i0 <- Job.push q (Job.Nice 0) t0 "j0"
   [x0] <- Job.prune q \p -> (False, [p])
   True <- pure $ x0.id == i0
   True <- pure $ x0.job == "j0"
   [] <- Job.prune q \p -> (False, [p])

   i1 <- Job.push q (Job.Nice 0) t0 "j1"
   [x1] <- Job.prune q \p -> (True, [p])
   True <- pure $ x1.id == i1
   True <- pure $ x1.job == "j1"
   [x2] <- Job.prune q \p -> (True, [p])
   True <- pure $ x2.id == i1
   True <- pure $ x2.job == "j1"

   pure ()
