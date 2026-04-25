module Test.Index.Document (spec) where

import Data.Aeson qualified as AE
import Data.Aeson.Key qualified as AE.Key
import Data.Map qualified as Map
import Data.Scientific qualified as S
import Data.Text qualified as T
import Hedgehog (annotateShow, diff, failure, forAll, success)
import Hedgehog.Gen qualified as Gen
import Kengine.Errors (KengineError (ParseError))
import Kengine.Index.Document (parseDocument)
import Kengine.Types (
  FieldName,
  FieldValue (BoolVal, KeywordVal, NumberVal, TextVal),
 )
import Refined (unrefine)
import Test.Helpers.Generators (
  genDocForMapping,
  genValidMapping,
  genValidMappingRequiredField,
 )
import Test.Hspec (Spec, describe, it)
import Test.Hspec.Hedgehog (hedgehog)

spec :: Spec
spec = do
  describe "validate a document" $ do
    it "accepts valid documents for a mapping" $ hedgehog $ do
      mapping <- forAll genValidMapping
      doc <- forAll $ genDocForMapping mapping
      let jsonObj = docToJson doc
      let validatedDoc = parseDocument jsonObj mapping
      annotateShow validatedDoc
      diff validatedDoc (==) (Right doc)
    it "rejects invalid documents missing fields" $ hedgehog $ do
      mapping <- forAll (genValidMappingRequiredField True)
      doc <- forAll $ genDocForMapping mapping
      toDropKeys <- forAll (Gen.filter (not . null) (Gen.subset (Map.keysSet doc)))
      let missingFieldsDoc = Map.filterWithKey (\k _ -> k `notElem` toDropKeys) doc
      let jsonObj = docToJson missingFieldsDoc
      let validatedDoc = parseDocument jsonObj mapping
      annotateShow validatedDoc
      case validatedDoc of
        Left (ParseError msg) ->
          diff "not found" T.isInfixOf msg
        _ -> failure
    it "rejects invalid docunents with wrong type" $ hedgehog $ do
      mapping <- forAll (genValidMappingRequiredField True)
      doc <- forAll $ genDocForMapping mapping
      let docWithWrongTpe = fudgeFieldVal <$> doc
      let jsonObj = docToJson docWithWrongTpe
      let validatedDoc = parseDocument jsonObj mapping
      annotateShow validatedDoc
      case validatedDoc of
        Left (ParseError _) -> success
        _ -> failure

docToJson :: Map.Map FieldName FieldValue -> AE.Value
docToJson doc =
  AE.object $
    (\(name, fieldValue) -> (AE.Key.fromText $ unrefine name, fieldValToJSON fieldValue))
      <$> Map.toList doc

fieldValToJSON :: FieldValue -> AE.Value
fieldValToJSON (TextVal txt) = AE.String txt
fieldValToJSON (KeywordVal txt) = AE.String txt
fieldValToJSON (BoolVal bool) = AE.Bool bool
fieldValToJSON (NumberVal num) = AE.Number (S.fromFloatDigits num)

fudgeFieldVal :: FieldValue -> FieldValue
fudgeFieldVal (TextVal _) = BoolVal False
fudgeFieldVal (KeywordVal _) = NumberVal 22
fudgeFieldVal (BoolVal _) = TextVal "ello"
fudgeFieldVal (NumberVal _) = KeywordVal "key"
