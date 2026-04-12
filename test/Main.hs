module Main (main) where

import Test.Engine qualified
import Test.Hspec
import Test.Mapping qualified
import Test.Types qualified

main :: IO ()
main = hspec $ parallel $ do
  Test.Types.spec
  Test.Mapping.spec
  Test.Engine.spec
