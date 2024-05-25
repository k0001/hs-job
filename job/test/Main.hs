{-# OPTIONS_GHC -Wno-incomplete-patterns #-}

module Main (main) where

import Control.Concurrent
import Control.Exception qualified as Ex
import Control.Monad
import Data.Acquire qualified as A
import Data.IORef
import Data.List qualified as List
import Data.Time qualified as Time
import Job
import Job.Memory qualified
import System.Random.Stateful qualified as R
import System.Timeout

--------------------------------------------------------------------------------

infix 4 ==.
(==.) :: (Eq a, Show a) => a -> a -> IO ()
(==.) l r =
   if l == r
      then mempty
      else fail $ (showsPrec 11 l . showString " /= " . showsPrec 11 r) ""

infix 4 >.
(>.) :: (Ord a, Show a) => a -> a -> IO ()
(>.) l r =
   if l > r
      then mempty
      else fail $ (showsPrec 11 l . showString " <= " . showsPrec 11 r) ""

main :: IO ()
main = A.withAcquire (Job.Memory.queue @String) \q -> do
   t0 <- Time.getCurrentTime

   putStrLn "a"
   [] <- prune q \i m j -> (False, [(i, m, j)])

   putStrLn "b"
   i0 <- push q nice0 t0 "j0"
   [(x0i, x0m, x0j)] <- prune q \i m j -> (False, [(i, m, j)])
   x0i ==. i0
   x0j ==. "j0"
   x0m.nice ==. nice0
   x0m.wait ==. t0
   x0m.try ==. 0
   x0m.alive ==. Nothing
   [] <- prune q \i m j -> (False, [(i, m, j)])

   putStrLn "c"
   i1 <- push q nice0 t0 "j1"
   [(x1i, x1m, x1j)] <- prune q \i m j -> (True, [(i, m, j)])
   x1i ==. i1
   x1j ==. "j1"
   x1m.nice ==. nice0
   x1m.wait ==. t0
   x1m.try ==. 0
   x1m.alive ==. Nothing

   putStrLn "d"
   [(x2i, x2m, x2j)] <- prune q \i m j -> (True, [(i, m, j)])
   x2i ==. i1
   x2j ==. "j1"
   x2m.nice ==. nice0
   x2m.wait ==. t0
   x2m.try ==. 0
   x2m.alive ==. Nothing

   putStrLn "e"
   t1 <- Time.getCurrentTime
   A.withAcquire q.pull \w -> do
      putStrLn "e'"
      w.id ==. i1
      w.job ==. "j1"
      w.meta.nice ==. nice0
      w.meta.wait ==. t0
      w.meta.try ==. 0
      retry w (Nice 1) t1

   putStrLn "f"
   [(x3i, x3m, x3j)] <- prune q \i m j -> (True, [(i, m, j)])
   x3i ==. i1
   x3j ==. "j1"
   x3m.nice ==. Nice 1
   x3m.wait ==. t1
   x3m.try ==. 1
   x3m.alive ==. Nothing

   putStrLn "g"
   A.withAcquire q.pull \w -> do
      putStrLn "g'"
      w.id ==. i1
      w.job ==. "j1"
      w.meta.nice ==. Nice 1
      w.meta.wait ==. t1
      w.meta.try ==. 1
      retry w (Nice 1) t1

   putStrLn "h"
   [(x4i, x4m, x4j)] <- prune q \i m j -> (True, [(i, m, j)])
   x4i ==. i1
   x4j ==. "j1"
   x4m.nice ==. Nice 1
   x4m.wait ==. t1
   x4m.try ==. 2
   x4m.alive ==. Nothing

   putStrLn "i"
   A.withAcquire q.pull \w -> do
      putStrLn "i'"
      w.id ==. i1
      w.job ==. "j1"
      w.meta.nice ==. Nice 1
      w.meta.wait ==. t1
      w.meta.try ==. 2
      pure ()

   putStrLn "j"
   [] <- prune q \i m j -> (True, [(i, m, j)])
   t2 <- Time.addUTCTime 0.3 <$> Time.getCurrentTime
   i2 <- push q nice0 t2 "j2"
   [(x5i, x5m, x5j)] <- prune q \i m j -> (True, [(i, m, j)])
   x5i ==. i2
   x5j ==. "j2"
   x5m.nice ==. nice0
   x5m.wait ==. t2
   x5m.try ==. 0
   x5m.alive ==. Nothing

   putStrLn "k"
   Ex.handle (\case Err 1 -> pure ()) do
      A.withAcquire q.pull \w -> do
         putStrLn "k'"
         w.id ==. i2
         w.job ==. "j2"
         w.meta.nice ==. nice0
         w.meta.wait ==. t2
         w.meta.try ==. 0
         void $ Ex.throwIO $ Err 1

   putStrLn "l"
   [(x6i, x6m, x6j)] <- prune q \i m j -> (True, [(i, m, j)])
   x6i ==. i2
   x6j ==. "j2"
   x6m.nice ==. nice0
   x6m.wait >. t2
   x6m.try ==. 1
   x6m.alive ==. Nothing

   putStrLn "m"
   i3 <- push q nice0 t0 "j3"
   i4 <- push q (pred nice0) t1 "j4"
   i5 <- push q nice0 t0 "j5"
   jIds0 <- prune q \i _ _ -> (True, [i])
   jIds0 ==. [i4, i3, i5, i2]

   putStrLn "n"
   forM_ (zip jIds0 (List.drop 1 (List.tails jIds0))) \(jId, tl) ->
      A.withAcquire q.pull \w -> do
         w.id ==. jId
         jIds1 <- prune q \i _ _ -> (True, [i])
         jIds1 ==. tl <> [jId]
   [] <- prune q \i m j -> (True, [(i, m, j)])

   putStrLn "o"
   do
      t3 <- Time.getCurrentTime
      rwout <- newIORef ([] :: [String])
      let wins :: [String] = fmap show [0 .. 1000 :: Word]
      forM_ wins \win -> do
         wrd :: Word <- R.uniformM R.globalStdGen
         let pct = toRational wrd / toRational (maxBound :: Word)
             ndt = fromRational (pct * 0.5)
         push q nice0 (Time.addUTCTime ndt t3) win
      putStrLn "o'"
      replicateM_ 4 $ forkIO $ void $ timeout 3_000_000 $ forever do
         A.withAcquire q.pull \w ->
            atomicModifyIORef' rwout \xs -> (w.job : xs, ())
      threadDelay 3_100_000
      [] <- prune q \i m j -> (True, [(i, m, j)])
      wout <- readIORef rwout
      length wout ==. length wins
      List.sort wout ==. List.sort wins

   putStrLn "done"

data Err = Err Int
   deriving stock (Eq, Show)
   deriving anyclass (Ex.Exception)
