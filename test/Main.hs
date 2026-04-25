module Main (main) where

import Test.Hspec
import Test.Index.Document qualified
import Test.Mapping qualified
import Test.Persistence.Binary qualified
import Test.Persistence.FileStore qualified
import Test.Search qualified
import Test.Store qualified
import Test.Tokenize qualified
import Test.Types qualified

main :: IO ()
main = hspec $ parallel $ do
  Test.Types.spec
  Test.Mapping.spec
  Test.Tokenize.spec
  Test.Index.Document.spec
  Test.Search.spec
  Test.Store.spec
  Test.Persistence.FileStore.spec
  Test.Persistence.Binary.spec
