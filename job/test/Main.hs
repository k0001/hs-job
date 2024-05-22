{-# OPTIONS_GHC -Wno-incomplete-patterns #-}

module Main (main) where

import Control.Exception qualified as Ex
import Control.Monad
import Data.Acquire qualified as A
import Data.Time qualified as Time
import Job
import Job.Memory qualified

--------------------------------------------------------------------------------

main :: IO ()
main = A.withAcquire (Job.Memory.queue @String) \q -> do
   t0 <- Time.getCurrentTime

   putStrLn "a"
   [] <- prune q \p -> (False, [p])

   putStrLn "b"
   i0 <- push q nice0 t0 "j0"
   [x0] <- prune q \p -> (False, [p])
   True <- pure $ x0.id == i0
   True <- pure $ x0.job == "j0"
   True <- pure $ x0.nice == nice0
   True <- pure $ x0.wait == t0
   True <- pure $ x0.try == 0
   True <- pure $ x0.alive == Nothing
   [] <- prune q \p -> (False, [p])

   putStrLn "c"
   i1 <- push q nice0 t0 "j1"
   [x1] <- prune q \p -> (True, [p])
   True <- pure $ x1.id == i1
   True <- pure $ x1.job == "j1"
   True <- pure $ x1.nice == nice0
   True <- pure $ x1.wait == t0
   True <- pure $ x1.try == 0
   True <- pure $ x1.alive == Nothing

   putStrLn "d"
   [x2] <- prune q \p -> (True, [p])
   True <- pure $ x2.id == i1
   True <- pure $ x2.job == "j1"
   True <- pure $ x2.nice == nice0
   True <- pure $ x2.wait == t0
   True <- pure $ x2.try == 0
   True <- pure $ x2.alive == Nothing

   putStrLn "e"
   t1 <- Time.getCurrentTime
   A.withAcquire q.pull \w -> do
      putStrLn "e'"
      True <- pure $ w.id == i1
      True <- pure $ w.job == "j1"
      True <- pure $ w.nice == nice0
      True <- pure $ w.wait == t0
      True <- pure $ w.try == 0
      retry w (Nice 1) t1

   putStrLn "f"
   [x3] <- prune q \p -> (True, [p])
   True <- pure $ x3.id == i1
   True <- pure $ x3.job == "j1"
   True <- pure $ x3.nice == Nice 1
   True <- pure $ x3.wait == t1
   True <- pure $ x3.try == 1
   True <- pure $ x3.alive == Nothing

   putStrLn "g"
   A.withAcquire q.pull \w -> do
      putStrLn "g'"
      True <- pure $ w.id == i1
      True <- pure $ w.job == "j1"
      True <- pure $ w.nice == Nice 1
      True <- pure $ w.wait == t1
      True <- pure $ w.try == 1
      retry w (Nice 1) t1

   putStrLn "h"
   [x4] <- prune q \p -> (True, [p])
   True <- pure $ x4.id == i1
   True <- pure $ x4.job == "j1"
   True <- pure $ x4.nice == Nice 1
   True <- pure $ x4.wait == t1
   True <- pure $ x4.try == 2
   True <- pure $ x4.alive == Nothing

   putStrLn "i"
   A.withAcquire q.pull \w -> do
      putStrLn "i'"
      True <- pure $ w.id == i1
      True <- pure $ w.job == "j1"
      True <- pure $ w.nice == Nice 1
      True <- pure $ w.wait == t1
      True <- pure $ w.try == 2
      pure ()

   putStrLn "j"
   [] <- prune q \p -> (True, [p])
   t2 <- Time.addUTCTime 0.3 <$> Time.getCurrentTime
   i2 <- push q nice0 t2 "j2"
   [x5] <- prune q \p -> (True, [p])
   True <- pure $ x5.id == i2
   True <- pure $ x5.job == "j2"
   True <- pure $ x5.nice == nice0
   True <- pure $ x5.wait == t2
   True <- pure $ x5.try == 0
   True <- pure $ x5.alive == Nothing

   putStrLn "k"
   Ex.handle (\case Err 1 -> pure ()) do
      A.withAcquire q.pull \w -> do
         putStrLn "k'"
         True <- pure $ w.id == i2
         True <- pure $ w.job == "j2"
         True <- pure $ w.nice == nice0
         True <- pure $ w.wait == t2
         True <- pure $ w.try == 0
         void $ Ex.throwIO $ Err 1

   putStrLn "h"
   [x6] <- prune q \p -> (True, [p])
   True <- pure $ x6.id == i2
   True <- pure $ x6.job == "j2"
   True <- pure $ x6.nice == nice0
   True <- pure $ x6.wait > t2
   True <- pure $ x6.try == 1
   True <- pure $ x6.alive == Nothing

   putStrLn "i"
   pure ()

data Err = Err Int
   deriving stock (Eq, Show)
   deriving anyclass (Ex.Exception)
