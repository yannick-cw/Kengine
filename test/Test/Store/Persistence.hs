module Test.Store.Persistence (spec) where

import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Except (runExceptT)
import Data.Foldable (for_, traverse_)
import Data.Map qualified as Map
import Hedgehog (diff, evalEither, forAll)
import Hedgehog.Gen qualified as Gen
import Hedgehog.Range qualified as Range
import Kengine.Store.Persistence (FileStore (..), mkFileStore')
import System.Directory (removeDirectoryRecursive)
import System.IO.Temp (withSystemTempDirectory)
import Test.Helpers.Generators (
  genDocsForMapping,
  genValidIndexName,
  genValidMapping,
 )
import Test.Hspec (Spec, around, describe, it)
import Test.Hspec.Hedgehog (hedgehog)

spec :: Spec
spec = do
  describe "Persistence" $ around (withSystemTempDirectory "kengine-test") $ do
    it "roundtrip any mapping" $ \dir -> hedgehog $ do
      idxNameMapping <-
        forAll $ Gen.list (Range.linear 1 10) ((,) <$> genValidIndexName <*> genValidMapping)
      resultMappings <- liftIO $ runExceptT $ do
        let store = mkFileStore' dir
        for_ idxNameMapping (uncurry store.storeMapping)
        mappings <- store.readMappings
        liftIO $ removeDirectoryRecursive dir
        pure mappings

      mappings <- evalEither resultMappings
      diff (length mappings) (==) (length idxNameMapping)
    it "roundtrip any docs" $ \dir -> hedgehog $ do
      idxWithDocs <-
        forAll $
          Gen.list
            (Range.linear 1 10)
            ( (,)
                <$> genValidIndexName
                <*> (genValidMapping >>= genDocsForMapping)
            )
      resultDocs <- liftIO $ runExceptT $ do
        let store = mkFileStore' dir
        traverse_ (\(idx, docs) -> traverse_ (store.storeDoc idx) docs) idxWithDocs
        docs <- store.readDocs
        liftIO $ removeDirectoryRecursive dir
        pure docs
      docs <- evalEither resultDocs

      diff docs (==) (Map.fromList idxWithDocs)
