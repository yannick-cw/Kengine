module Test.Store.Persistence (spec) where

import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Except (runExceptT)
import Data.Foldable (for_, traverse_)
import Data.Map qualified as Map
import Hedgehog (annotate, annotateShow, diff, failure, forAll)
import Hedgehog.Gen qualified as Gen
import Hedgehog.Range qualified as Range
import Kengine.Errors (IOE)
import Kengine.Store.Persistence (FileStore (..), mkFileStore')
import Kengine.Types (DocId (..), Document (Document))
import System.Directory (removeDirectoryRecursive)
import Test.Helpers.Generators (
  genDocForMapping,
  genDocsForMapping,
  genValidIndexName,
  genValidMapping,
 )
import Test.Hspec (Spec, describe, it)
import Test.Hspec.Hedgehog (hedgehog)

testDir :: FilePath
testDir = "test-data"

spec :: Spec
spec = do
  describe "Persistence" $ do
    it "roundtrip any mapping" $ hedgehog $ do
      idxNameMapping <-
        forAll $ Gen.list (Range.linear 1 10) ((,) <$> genValidIndexName <*> genValidMapping)
      resultMappings <- liftIO $ runIOE $ do
        let store = mkFileStore' testDir
        for_ idxNameMapping (uncurry store.storeMapping)
        mappings <- store.readMappings
        liftIO $ removeDirectoryRecursive testDir
        pure mappings

      diff (length resultMappings) (==) (length idxNameMapping)
    it "roundtrip any docs" $ hedgehog $ do
      idxWithDocs <-
        forAll $
          Gen.list
            (Range.linear 1 10)
            ( (,)
                <$> genValidIndexName
                <*> (genValidMapping >>= genDocsForMapping)
            )
      resultDocs <- liftIO $ runExceptT $ do
        let store = mkFileStore' testDir
        for_
          idxWithDocs
          (\(idx, docs) -> traverse_ (store.storeDoc idx) docs)
        docs <- store.readDocs
        liftIO $ removeDirectoryRecursive testDir
        pure docs

      case resultDocs of
        Right docs -> diff docs (==) (Map.fromList idxWithDocs)
        Left err -> do
          annotateShow err
          failure

runIOE :: (Show e) => IOE e a -> IO a
runIOE m = runExceptT m >>= either (fail . show) pure
