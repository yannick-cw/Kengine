module Test.Persistence.FileStore (spec) where

import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Except (runExceptT)
import Data.Foldable (traverse_)
import Hedgehog (diff, evalEither, forAll)
import Hedgehog.Gen qualified as Gen
import Hedgehog.Range qualified as Range
import Kengine.Persistence.FileStore (FileStore (..), mkFileStore')
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
  describe "FileStore" $ around (withSystemTempDirectory "kengine-test") $ do
    it "roundtrip any mapping" $ \dir -> hedgehog $ do
      (idx, mapping) <-
        forAll ((,) <$> genValidIndexName <*> genValidMapping)
      resultMapping <- liftIO $ runExceptT $ do
        let store = mkFileStore' dir
        store.storeMapping idx mapping
        resMapping <- store.readMapping idx
        liftIO $ removeDirectoryRecursive dir
        pure resMapping

      resMapping <- evalEither resultMapping
      diff resMapping (==) (Just mapping)
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
        res <- traverse ((\idx -> (idx,) <$> store.readDocs idx) . fst) idxWithDocs
        liftIO $ removeDirectoryRecursive dir
        pure res
      docs <- evalEither resultDocs

      diff docs (==) idxWithDocs
