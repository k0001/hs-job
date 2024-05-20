module Main (main) where

import Df1 qualified
import Di qualified
import Di.Core qualified
import Test.Tasty qualified as Tasty
import Test.Tasty.Hedgehog qualified as Tasty
import Test.Tasty.Runners qualified as Tasty

--------------------------------------------------------------------------------

main :: IO ()
main = Tasty.defaultMainWithIngredients
      [ Tasty.consoleTestReporter
      , Tasty.listingTests
      ]
      $ Tasty.localOption (Tasty.HedgehogTestLimit (Just 1000))
      $ tt

tt :: Tasty.TestTree
tt = undefined
