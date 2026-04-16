module Test.Engine (tokenizeSpec, docSpec) where

import Data.Aeson qualified as AE
import Data.Aeson.Key qualified as AE.Key
import Data.Char qualified as C
import Data.Foldable (for_)
import Data.List qualified as L
import Data.Map qualified as Map
import Data.Scientific qualified as S
import Data.Text qualified as T
import Hedgehog (
  annotate,
  annotateShow,
  assert,
  diff,
  failure,
  forAll,
  success,
 )
import Hedgehog.Gen qualified as Gen
import Kengine.Engine (Token (..), parseDocument, tokenize)
import Kengine.Errors (SearchError (..))
import Kengine.Types (
  Document (Document),
  FieldName,
  FieldValue (BoolVal, KeywordVal, NumberVal, TextVal),
  Term (..),
 )
import Refined (unrefine)
import Test.Helpers.Generators (
  genDocForMapping,
  genNonAlphaText,
  genText,
  genTokenizableText,
  genValidMapping,
 )
import Test.Hspec (Spec, describe, it, shouldBe)
import Test.Hspec.Hedgehog (hedgehog)

tokenizeSpec :: Spec
tokenizeSpec = do
  describe "tokenize" $ do
    it "lowercases all tokens" $ hedgehog $ do
      term <- forAll genText
      let tokenized = tokenize (Term term)
      let characters = tokenized >>= (\(Token tkn) -> T.unpack tkn)
      for_ characters $ \c -> do
        annotate ("found non lower char: " <> [c])
        assert (C.toLower c == c)
    it "splits on non alphanum chars" $ hedgehog $ do
      termsWithSep <- forAll genTokenizableText
      let term = Term $ T.intercalate "" termsWithSep
      let tokenized = tokenize term
      annotate "Tokenized must have same length as input"
      annotateShow tokenized
      diff (L.length tokenized) (==) (L.length termsWithSep)
    it "pure non alpha is discarded" $ hedgehog $ do
      nonAlphaTxt <- forAll $ genNonAlphaText
      let tokenized = tokenize $ Term nonAlphaTxt
      annotateShow tokenized
      diff tokenized (==) []
    it "tokenizes" $ do
      let tokenized = tokenize $ Term "Bio-Milch 3,5%"
      tokenized `shouldBe` (Token <$> ["bio", "milch", "3", "5"])

docSpec :: Spec
docSpec = do
  describe "validate a document" $ do
    it "accepts valid documents for a mapping" $ hedgehog $ do
      mapping <- forAll genValidMapping
      d@(Document doc) <- forAll $ genDocForMapping mapping
      let jsonObj = docToJson doc
      let validatedDoc = parseDocument jsonObj mapping
      annotateShow validatedDoc
      diff validatedDoc (==) (Right d)
    it "rejects invalid docunents missing fields" $ hedgehog $ do
      mapping <- forAll genValidMapping
      (Document doc) <- forAll $ genDocForMapping mapping
      toDropKeys <- forAll (Gen.filter (not . null) (Gen.subset (Map.keysSet doc)))
      let missingFieldsDoc = Map.filterWithKey (\k _ -> k `notElem` toDropKeys) doc
      let jsonObj = docToJson missingFieldsDoc
      let validatedDoc = parseDocument jsonObj mapping
      annotateShow validatedDoc
      case validatedDoc of
        Left (SearchError msg) ->
          diff "not found" T.isInfixOf msg
        Right _ -> failure
    it "rejects invalid docunents with wrong type" $ hedgehog $ do
      mapping <- forAll genValidMapping
      (Document doc) <- forAll $ genDocForMapping mapping
      let docWithWrongTpe = fudgeFieldVal <$> doc
      let jsonObj = docToJson docWithWrongTpe
      let validatedDoc = parseDocument jsonObj mapping
      annotateShow validatedDoc
      case validatedDoc of
        Left (SearchError _) -> success
        Right _ -> failure

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
