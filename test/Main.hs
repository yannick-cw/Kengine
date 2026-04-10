module Main (main) where

import Test.Hspec
import Test.Kengine qualified

main :: IO ()
main = hspec $ parallel $ do
  Test.Kengine.spec
