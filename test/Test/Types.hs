module Test.Types (spec) where

import Data.Aeson (
  Result (Error, Success),
  eitherDecode,
  fromJSON,
 )
import Hedgehog (annotate, failure, forAll, success)
import Hedgehog.Gen qualified as Gen
import Kengine.Types (Mapping)
import Test.Helpers.Generators (
  genInvalidNameJson,
  genInvalidSTypeJson,
  genValidFieldJson,
  genValidMappingJson,
 )
import Test.Hspec (Spec, describe, it)
import Test.Hspec.Hedgehog (hedgehog)

spec :: Spec
spec =
  describe "Types" $ do
    it "rejects empty mappings" $ hedgehog $ do
      let json = "{\"fields\": []}"
      annotate "empty mapping is rejected"
      case eitherDecode json :: Either String Mapping of
        Left _ -> success
        Right _ -> failure
    it "accepts valid mappings" $ hedgehog $ do
      mapping <- forAll $ genValidMappingJson genValidFieldJson
      annotate "valid mapping is parsed"
      case fromJSON mapping :: Result Mapping of
        Error _ -> failure
        Success _ -> success
    it "rejects invalid field names or types" $ hedgehog $ do
      mapping <-
        forAll $ genValidMappingJson (Gen.choice [genInvalidNameJson, genInvalidSTypeJson])
      annotate "invalid mapping is rejected"
      case fromJSON mapping :: Result Mapping of
        Error _ -> success
        Success _ -> failure
