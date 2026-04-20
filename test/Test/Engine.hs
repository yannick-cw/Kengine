{-# LANGUAGE TemplateHaskell #-}

module Test.Engine (tokenizeSpec, docSpec, searchSpec) where

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
import Kengine.Engine (Token (..), parseDocument, searchQ, tokenize)
import Kengine.Errors (KengineError (SearchError))
import Kengine.Types (
  DocId (DocId),
  Document (Document),
  FieldName,
  FieldValue (BoolVal, KeywordVal, NumberVal, TextVal),
  MetaData (MetaData),
  Query (..),
  Score (..),
  SearchResult (..),
  Term (..),
  TermFrequency (..),
 )
import Refined (unrefine)
import Refined qualified as R
import Test.Helpers.Generators (
  genDocForMapping,
  genNonAlphaText,
  genText,
  genTokenizableText,
  genValidMapping,
  genValidMappingRequiredField,
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
      nonAlphaTxt <- forAll genNonAlphaText
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
    it "rejects invalid documents missing fields" $ hedgehog $ do
      mapping <- forAll (genValidMappingRequiredField True)
      (Document doc) <- forAll $ genDocForMapping mapping
      toDropKeys <- forAll (Gen.filter (not . null) (Gen.subset (Map.keysSet doc)))
      let missingFieldsDoc = Map.filterWithKey (\k _ -> k `notElem` toDropKeys) doc
      let jsonObj = docToJson missingFieldsDoc
      let validatedDoc = parseDocument jsonObj mapping
      annotateShow validatedDoc
      case validatedDoc of
        Left (SearchError msg) ->
          diff "not found" T.isInfixOf msg
        _ -> failure
    it "rejects invalid docunents with wrong type" $ hedgehog $ do
      mapping <- forAll (genValidMappingRequiredField True)
      (Document doc) <- forAll $ genDocForMapping mapping
      let docWithWrongTpe = fudgeFieldVal <$> doc
      let jsonObj = docToJson docWithWrongTpe
      let validatedDoc = parseDocument jsonObj mapping
      annotateShow validatedDoc
      case validatedDoc of
        Left (SearchError _) -> success
        _ -> failure

searchSpec :: Spec
searchSpec = do
  describe "scoring" $ do
    it "calculates correct bm25 score" $
      --   BM25(t, d) = IDF(t) * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * (dl / avgdl)))
      --   where:
      --     tf    = term frequency of t in d
      --     dl    = document length (total terms in d)
      --     avgdl = average document length across all documents
      --     k1    = 1.2 (saturation parameter: higher = less saturation)
      --     b     = 0.75 (length normalization: 0 = no normalization, 1 = full)
      --     IDF(t) = log((N - df(t) + 0.5) / (df(t) + 0.5) + 1)
      -- N: 2, df("test") = 2 docs, k1 = 1.2 b = 0.75
      -- idf = log((20 - 2 + 0.5) / (2 + 0.5) + 1) = log(8.4)
      -- avgdl = 7.5
      -- doc1: tf = 10, dl = 5 => log(8.4) * (10 * (1.2 + 1)) / (10 + 1.2 * (1-0.75 + 0.75 * (5 / 7.5)))
      -- doc2: tf = 5, dl = 10 => log(8.4) * (5 * (1.2 + 1)) / (5 + 1.2 * (1-0.75 + 0.75 * (10 / 7.5)))

      let
        fieldName :: FieldName = $$(R.refineTH "search_field")
        docStore =
          Map.fromList $
            (\idx -> (DocId idx, Document (Map.singleton fieldName (TextVal "")))) <$> [1 .. 20]
        fieldIndex =
          Map.singleton
            fieldName
            (Map.fromList [(Token "test", Map.fromList [(DocId 1, TF 10), (DocId 2, TF 5)])])
        fieldMeta =
          Map.singleton
            fieldName
            (Map.fromList [(DocId 1, MetaData 5), (DocId 2, MetaData 10)])
        searchRes = searchQ (Query "test") docStore fieldIndex fieldMeta
        idf = log ((2 - 2 + 0.5) / (2 + 0.5) + 1)
        doc1 = idf * (10 * (1.2 + 1)) / (10 + 1.2 * (1 - 0.75 + 0.75 * (5 / 7.5)))
        doc2 = idf * (5 * (1.2 + 1)) / (5 + 1.2 * (1 - 0.75 + 0.75 * (10 / 7.5)))
       in
        (\(SearchResult _ (Score s)) -> s) <$> searchRes
          `shouldBe` [doc1, doc2]
    it "matches ALL query terms" $
      let
        docStore =
          Map.fromList $ (\idx -> (DocId idx, Document Map.empty)) <$> [1 .. 20]
        fieldName :: FieldName = $$(R.refineTH "search_field")
        fieldIndex =
          Map.singleton
            fieldName
            ( Map.fromList
                [ (Token "test", Map.fromList [(DocId 1, TF 10), (DocId 2, TF 5)])
                , (Token "and", Map.fromList [(DocId 1, TF 10)])
                ]
            )
        fieldMeta =
          Map.singleton
            fieldName
            (Map.fromList [(DocId 1, MetaData 5), (DocId 2, MetaData 10)])
        searchRes = searchQ (Query "and test") docStore fieldIndex fieldMeta
       in
        length searchRes `shouldBe` 1

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
