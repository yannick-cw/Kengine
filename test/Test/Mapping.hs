module Test.Mapping (spec) where

import Hedgehog (annotateShow, assert, diff, forAll)
import Hedgehog.Gen qualified as Gen
import Hedgehog.Range qualified as Range
import Kengine.Mapping (validateMapping)
import Kengine.Types (Mapping (..))
import Test.Helpers.Generators (genValidIndexName, genValidMapping)
import Test.Hspec (Spec, describe, it)
import Test.Hspec.Hedgehog (hedgehog)
import Validation qualified as V

spec :: Spec
spec =
  describe "Mapping" $ do
    it "parses all valid mappings" $ hedgehog $ do
      mapping <- forAll genValidMapping
      indexName <- forAll genValidIndexName
      let valRes = validateMapping indexName [] mapping
      diff valRes (==) (V.Success mapping)
    it "fails on duplicate fields" $ hedgehog $ do
      mapping <- forAll $ (\m -> m{fields = m.fields <> m.fields}) <$> genValidMapping
      indexName <- forAll genValidIndexName
      let valRes = validateMapping indexName [] mapping
      annotateShow valRes
      assert (V.isFailure valRes)
    it "fails on existing index for that name" $ hedgehog $ do
      mapping <- forAll genValidMapping
      indexName <- forAll genValidIndexName
      existingIndexes <-
        forAll $
          (<> [indexName]) <$> Gen.list (Range.linear 0 10) genValidIndexName
      let valRes = validateMapping indexName existingIndexes mapping
      annotateShow valRes
      assert (V.isFailure valRes)
