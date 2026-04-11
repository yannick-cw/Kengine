module Test.Kengine (spec) where

import Data.Aeson (
  Result (Error, Success),
  Value (..),
  eitherDecode,
  fromJSON,
  object,
  (.=),
 )
import Data.Char (isAlphaNum)
import Data.Text (Text)
import Data.Text qualified as T
import Hedgehog (Gen, annotate, failure, forAll, success)
import Hedgehog.Gen qualified as Gen
import Hedgehog.Range qualified as Range
import Kengine.Types (
  Mapping,
 )
import Test.Hspec (Spec, describe, it)
import Test.Hspec.Hedgehog (hedgehog)

spec :: Spec
spec =
  describe "Kengine" $ do
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
    it "rejects empty mappings" $ hedgehog $ do
      mapping <-
        forAll $ genValidMappingJson (Gen.choice [genInvalidNameJson, genInvalidSTypeJson])
      annotate "valid mapping is parsed"
      case fromJSON mapping :: Result Mapping of
        Error _ -> success
        Success _ -> failure

genValidMappingJson :: Gen Value -> Gen Value
genValidMappingJson fieldGen = do
  fields <- Gen.list (Range.linear 1 10) fieldGen
  pure $ object ["fields" .= fields]

genValidName :: Gen Text
genValidName =
  Gen.text
    (Range.linear 1 30)
    (Gen.element $ ['a' .. 'z'] <> ['A' .. 'Z'] <> ['0' .. '9'] <> ['_', '-'])

genValidType :: Gen Text
genValidType = Gen.element ["Text" :: Text, "Keyword", "Bool", "Number"]

genValidFieldJson :: Gen Value
genValidFieldJson = do
  name <- genValidName
  sType <- genValidType
  pure $ object ["fieldName" .= name, "sType" .= sType]

genInvalidNameJson :: Gen Value
genInvalidNameJson = do
  name <-
    Gen.filter
      (\t -> not (T.all (\c -> isAlphaNum c || c == '-' || c == '_') t) || T.null t)
      (Gen.text (Range.linear 0 30) Gen.unicode)
  sType <- genValidType
  pure $ object ["fieldName" .= name, "sType" .= sType]

genInvalidSTypeJson :: Gen Value
genInvalidSTypeJson = do
  name <- genValidName
  sType <- Gen.text (Range.linear 0 30) Gen.unicode
  pure $ object ["fieldName" .= name, "sType" .= sType]
