module Test.Store.Persistence (spec) where

import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Except (runExceptT)
import Data.Foldable (for_)
import Hedgehog (diff, forAll)
import Hedgehog.Gen qualified as Gen
import Hedgehog.Range qualified as Range
import Kengine.Errors (IOE)
import Kengine.Store.Persistence (FileStore (..), mkFileStore')
import System.Directory (removeDirectoryRecursive)
import Test.Helpers.Generators (genValidIndexName, genValidMapping)
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

runIOE :: (Show e) => IOE e a -> IO a
runIOE m = runExceptT m >>= either (fail . show) pure
