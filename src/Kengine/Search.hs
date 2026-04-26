module Kengine.Search (searchQ) where

import Data.List qualified as L
import Data.List.NonEmpty qualified as NEL
import Data.Map qualified as Map
import Data.Ord (Down (Down))
import Kengine.Errors (Result)
import Kengine.Types (
  BM25 (..),
  DocFieldStats (..),
  DocId,
  DocStore,
  Document,
  FieldIndex,
  FieldName,
  FieldStats,
  InvertedIndex,
  Score (Score),
  SearchResult (..),
  TermFrequency (TF),
  Token,
 )

-- todo we should not pass docstore and metadata at all - it should come from a unified search interface that internally
-- resolves wherever search comes from
searchQ ::
  [Token] ->
  DocStore ->
  FieldIndex ->
  FieldStats ->
  (DocId -> Result (Maybe Document)) ->
  Result [SearchResult]
searchQ tokenizedQ docStore fieldIndex fieldMeta docDiskLookup =
  let
    -- each entry is one map of docs of scores for one field
    perFieldResults :: [Map.Map DocId Score]
    perFieldResults = Map.elems $ Map.mapWithKey searchOneField fieldIndex
    perDocScore = Map.fromListWith (+) (Map.toList =<< perFieldResults)
   in
    L.sortOn Down
      . Map.elems
      <$> Map.traverseMaybeWithKey
        ( \docId score ->
            fmap (\doc -> SearchResult{doc, score})
              <$> case Map.lookup docId docStore of
                Just doc -> pure $ Just doc
                -- we need disk lookup
                Nothing -> docDiskLookup docId
        )
        perDocScore
  where
    -- Map.elems $
    --   Map.intersectionWith (\score doc -> SearchResult{doc, score}) perDocScore docStore

    -- inverted index for that specific field
    searchOneField :: FieldName -> InvertedIndex -> Map.Map DocId Score
    searchOneField fName invertedIndex =
      let
        metadataForFieldname :: Map.Map DocId DocFieldStats
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

    calcbm25 ::
      Map.Map DocId DocFieldStats -> Map.Map DocId TermFrequency -> Map.Map DocId BM25
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
        dl docId = maybe 0 (\(DocFieldStats count) -> fromIntegral count) (Map.lookup docId docsMeta)
        avgdl :: Map.Map DocId DocFieldStats -> Float
        avgdl m
          | Map.null m = 1 -- safety net - should be 0
          | otherwise = fromIntegral total / fromIntegral (Map.size m)
          where
            total = sum $ (\(DocFieldStats count) -> count) <$> m
        totalDocScore :: Float -> DocId -> Float
        totalDocScore tf docId = idf * (tf * (1.2 + 1)) / (tf + 1.2 * (1 - 0.75 + 0.75 * (dl docId / avgdl docsMeta)))
