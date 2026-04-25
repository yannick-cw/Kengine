module Kengine.Engine (parseDocument, tokenize, Token (..), searchQ, updateIndex) where

import Data.Aeson ((.:), (.:?))
import Data.Aeson qualified as AE
import Data.Aeson.Key qualified as AE.Types.Key
import Data.Aeson.Types qualified as AE.Types
import Data.Bifunctor (first)
import Data.Char qualified as C
import Data.List qualified as L
import Data.List.NonEmpty (toList)
import Data.List.NonEmpty qualified as NEL
import Data.Map qualified as Map
import Data.Maybe qualified as M
import Data.Ord (Down (Down))
import Data.Text (Text)
import Data.Text qualified as T
import Kengine.Errors (KengineError (SearchError))
import Kengine.Types (
  BM25 (..),
  DocId,
  DocStore,
  Document (Document),
  Field (..),
  FieldIndex,
  FieldMetadata,
  FieldName,
  FieldValue (BoolVal, KeywordVal, NumberVal, TextVal),
  InvertedIndex,
  Mapping (..),
  MetaData (..),
  Score (Score),
  SearchResult,
  Term (..),
  TermFrequency (TF),
  Token (Token),
  fromDoc,
 )
import Kengine.Types qualified as KT
import Refined (unrefine)

-- "Bio-Milch 3,5%" becomes ["bio", "milch", "3", "5"]
tokenize :: Term -> [Token]
tokenize = fmap (Token . T.toLower) . filter (not . T.null) . splitNonAlpha

splitNonAlpha :: Term -> [Text]
splitNonAlpha (Term t) = T.split (\tkn -> not (C.isAscii tkn && C.isAlphaNum tkn)) t

parseDocument :: AE.Value -> Mapping -> Either KengineError (Map.Map FieldName FieldValue)
parseDocument jVal Mapping{fields} =
  let
    document = AE.withObject "Document" (docParser fields)
   in
    first (SearchError . T.pack) (AE.Types.parseEither document jVal)

-- todo simplify
docParser ::
  NEL.NonEmpty Field -> AE.Object -> AE.Types.Parser (Map.Map FieldName FieldValue)
docParser fields obj =
  let
    parsedFieldVals =
      traverse
        ( \((Field{sType, fieldName, required}) :: Field) ->
            let key = AE.Types.Key.fromText (unrefine fieldName)
             in if required
                  then
                    Just . (fieldName,) <$> case sType of
                      KT.Text -> TextVal <$> obj .: key
                      KT.Keyword -> KeywordVal <$> obj .: key
                      KT.Bool -> BoolVal <$> obj .: key
                      KT.Number -> NumberVal <$> obj .: key
                  else
                    fmap (fieldName,) <$> case sType of
                      KT.Text -> fmap TextVal <$> obj .:? key
                      KT.Keyword -> fmap KeywordVal <$> obj .:? key
                      KT.Bool -> fmap BoolVal <$> obj .:? key
                      KT.Number -> fmap NumberVal <$> obj .:? key
        )
        fields
   in
    Map.fromList . M.catMaybes . toList <$> parsedFieldVals

searchQ ::
  [Token] ->
  DocStore ->
  FieldIndex ->
  FieldMetadata ->
  [SearchResult]
searchQ tokenizedQ docStore fieldIndex fieldMeta =
  let
    -- each entry is one map of docs of scores for one field
    perFieldResults :: [Map.Map DocId Score]
    perFieldResults = Map.elems $ Map.mapWithKey searchOneField fieldIndex
    perDocScore = Map.fromListWith (+) (Map.toList =<< perFieldResults)
   in
    L.sortOn Down $ Map.elems $ Map.intersectionWith fromDoc perDocScore docStore
  where
    -- inverted index for that specific field
    searchOneField :: FieldName -> InvertedIndex -> Map.Map DocId Score
    searchOneField fName invertedIndex =
      let
        metadataForFieldname :: Map.Map DocId MetaData
        metadataForFieldname = Map.findWithDefault Map.empty fName fieldMeta

        -- Just when all tokens find a doc - Nothing when any token can not be found
        -- core search logic decision - we could also concat instead that would allow
        -- if single terms would not be found
        -- each NEL entry is for one search token
        matchingDocs :: Maybe (NEL.NonEmpty (Map.Map DocId TermFrequency))
        matchingDocs = traverse (invertedIndex Map.!?) tokenizedQ >>= NEL.nonEmpty

        -- TF -> BM25 score for each doc
        docsWithScore :: Maybe (NEL.NonEmpty (Map.Map DocId BM25))
        docsWithScore = fmap (calcbm25 metadataForFieldname) <$> matchingDocs

        -- crucial, we always insercet -> only documents survice that had a results for EACH search token
        docsWithAllTkns = maybe Map.empty (foldl1 (Map.intersectionWith (+))) docsWithScore
       in
        (\(BM25 score) -> Score score) <$> docsWithAllTkns

    calcbm25 :: Map.Map DocId MetaData -> Map.Map DocId TermFrequency -> Map.Map DocId BM25
    calcbm25 docsMeta tfMap =
      --   BM25(t, d) = IDF(t) * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * (dl / avgdl)))
      --   where:
      --     tf    = term frequency of t in d
      --     dl    = document length (total terms in d)
      --     avgdl = average document length across all documents
      --     k1    = 1.2 (saturation parameter: higher = less saturation)
      --     b     = 0.75 (length normalization: 0 = no normalization, 1 = full)
      --     IDF(t) = log((N - df(t) + 0.5) / (df(t) + 0.5) + 1)
      -- N: 20, df("test") = 2 docs, k1 = 1.2 b = 0.75
      Map.mapWithKey
        (\docId (TF tf) -> BM25 $ totalDocScore (fromIntegral tf) docId)
        tfMap
      where
        n_total :: Float
        n_total = fromIntegral $ length docsMeta
        df_t :: Float
        df_t = fromIntegral $ length tfMap
        idf :: Float
        idf = log ((n_total - df_t + 0.5) / (df_t + 0.5) + 1)
        dl :: DocId -> Float
        dl docId = maybe 0 (\(MetaData count) -> fromIntegral count) (Map.lookup docId docsMeta)
        avgdl :: Map.Map DocId MetaData -> Float
        avgdl m
          | Map.null m = 1 -- safety net - should be 0
          | otherwise = fromIntegral total / fromIntegral (Map.size m)
          where
            total = sum $ (\(MetaData count) -> count) <$> m
        totalDocScore :: Float -> DocId -> Float
        totalDocScore tf docId = idf * (tf * (1.2 + 1)) / (tf + 1.2 * (1 - 0.75 + 0.75 * (dl docId / avgdl docsMeta)))

updateIndex ::
  FieldIndex ->
  FieldMetadata ->
  Document ->
  (FieldIndex, FieldMetadata)
updateIndex fieldIndex fieldMeta (Document docId fieldNameToValue) =
  let
    tokensPerField =
      Map.mapMaybe
        ( \case
            (TextVal txt) -> Just (tokenize $ Term txt)
            _ -> Nothing
        )
        fieldNameToValue
    allMetadataPerField :: FieldMetadata
    allMetadataPerField = toPerFieldMetadata <$> tokensPerField
    -- simple union is enough - this is a new doc, the doc id can not exist yet
    mergedMeta = Map.unionWith Map.union allMetadataPerField fieldMeta

    -- takes FieldName -> [Token] and merges duplicates to create term frequency of
    -- each token per field
    newInvertedIndexForTouchedFields =
      fmap (Map.singleton docId) . Map.fromListWith (+) . fmap (,TF 1) <$> tokensPerField

    mergedFields =
      Map.unionWith
        (Map.unionWith Map.union)
        newInvertedIndexForTouchedFields
        fieldIndex
   in
    (mergedFields, mergedMeta)
  where
    toPerFieldMetadata :: [Token] -> Map.Map DocId MetaData
    toPerFieldMetadata tkns = Map.singleton docId (MetaData{totalTokens = length tkns})
