module Kengine.Engine (parseDocument, tokenize, Token (..), searchQ) where

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
import Kengine.Errors (SearchError (SearchError))
import Kengine.Types (
  DocId,
  DocStore,
  Document (Document),
  Field (..),
  FieldDocResult,
  FieldIndex,
  FieldMetadata,
  FieldName,
  FieldValue (BoolVal, KeywordVal, NumberVal, TextVal),
  InvertedIndex,
  Mapping (..),
  MetaData (..),
  Query (..),
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

parseDocument :: AE.Value -> Mapping -> Either SearchError Document
parseDocument jVal Mapping{fields} =
  let
    document = AE.withObject "Document" (docParser fields)
   in
    first (SearchError . T.pack) (AE.Types.parseEither document jVal)

-- todo simplify
docParser :: NEL.NonEmpty Field -> AE.Object -> AE.Types.Parser Document
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
    Document . Map.fromList . M.catMaybes . toList <$> parsedFieldVals

-- TF(t, d) = number of times term t appears in document d
-- IDF(t)   = log(N / df(t))
-- where N = total document count, df(t) = number of documents containing term t
-- here: tfidf d1 = 10 * log(20/2); d2 = 5 * log(20/2);
searchQ ::
  Query ->
  DocStore ->
  FieldIndex ->
  FieldMetadata ->
  [SearchResult]
searchQ (Query query) docStore fieldIndex fieldMeta =
  let
    perFieldResults = Map.elems $ Map.mapWithKey searchOneField fieldIndex
    perDocScore = Map.fromListWith (+) (Map.toList =<< perFieldResults)
   in
    L.sortOn Down $ Map.elems $ Map.intersectionWith fromDoc perDocScore docStore
  where
    searchOneField :: FieldName -> InvertedIndex -> Map.Map DocId Score
    searchOneField fName invertedIndex =
      let
        tokenizedQ = tokenize $ Term query
        docs =
          traverse -- todo safe map call
            ( \tkn ->
                calcbm25 (Map.findWithDefault Map.empty fName fieldMeta)
                  <$> Map.lookup tkn invertedIndex
            )
            tokenizedQ
        docsWithAllTkns = case docs of
          (Just (fstDocIds : restDocIds)) -> foldl' (Map.intersectionWith (+)) fstDocIds restDocIds
          _ -> Map.empty
       in
        Score <$> docsWithAllTkns
    calcbm25 :: Map.Map DocId MetaData -> Map.Map DocId TermFrequency -> Map.Map DocId Float
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
        (\docId (TF tf) -> totalDocScore (fromIntegral tf) docId docsMeta)
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
        totalDocScore :: Float -> DocId -> Map.Map DocId MetaData -> Float
        totalDocScore tf docId metadata = idf * (tf * (1.2 + 1)) / (tf + 1.2 * (1 - 0.75 + 0.75 * (dl docId / avgdl metadata)))
