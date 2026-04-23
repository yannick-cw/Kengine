module Main (main) where

import Test.Engine qualified
import Test.Hspec
import Test.Mapping qualified
import Test.Store.Binary qualified
import Test.Store.InMemory qualified
import Test.Store.Persistence qualified
import Test.Types qualified

main :: IO ()
main = hspec $ parallel $ do
  Test.Types.spec
  Test.Mapping.spec
  Test.Engine.tokenizeSpec
  Test.Engine.docSpec
  Test.Engine.searchSpec
  Test.Store.InMemory.spec
  Test.Store.Persistence.spec
  Test.Store.Binary.spec
