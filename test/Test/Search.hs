{-# LANGUAGE TemplateHaskell #-}

module Test.Search (spec) where

import Control.Monad.Trans.Except (runExceptT)
import Data.Map qualified as Map
import Kengine.Errors (IOE)
import Kengine.Search (searchQ)
import Kengine.Types (
  DocFieldStats (DocFieldStats),
  DocId (DocId),
  Document (..),
  FieldName,
  FieldValue (TextVal),
  Score (..),
  SearchResult (..),
  TermFrequency (..),
  Token (..),
 )
import Refined qualified as R
import Test.Hspec (Spec, describe, it, shouldBe)

spec :: Spec
spec = do
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
            ( \idx -> (DocId idx, Document{docId = DocId idx, body = Map.singleton fieldName (TextVal "")})
            )
              <$> [1 .. 20]
        fieldIndex =
          Map.singleton
            fieldName
            (Map.fromList [(Token "test", Map.fromList [(DocId 1, TF 10), (DocId 2, TF 5)])])
        fieldMeta =
          Map.singleton
            fieldName
            (Map.fromList [(DocId 1, DocFieldStats 5), (DocId 2, DocFieldStats 10)])
        searchResIO = searchQ [Token "test"] docStore fieldIndex fieldMeta (\_ -> pure Nothing)
        idf = log ((2 - 2 + 0.5) / (2 + 0.5) + 1)
        doc1 = idf * (10 * (1.2 + 1)) / (10 + 1.2 * (1 - 0.75 + 0.75 * (5 / 7.5)))
        doc2 = idf * (5 * (1.2 + 1)) / (5 + 1.2 * (1 - 0.75 + 0.75 * (10 / 7.5)))
       in
        do
          searchRes <- runIOE searchResIO
          (\SearchResult{score = Score s} -> s) <$> searchRes `shouldBe` [doc1, doc2]

    it "matches ALL query terms" $
      let
        docStore =
          Map.fromList $
            (\idx -> (DocId idx, Document{docId = DocId idx, body = Map.empty})) <$> [1 .. 20]
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
            (Map.fromList [(DocId 1, DocFieldStats 5), (DocId 2, DocFieldStats 10)])
        searchResIO = searchQ [Token "and", Token "test"] docStore fieldIndex fieldMeta (\_ -> pure Nothing)
       in
        do
          searchRes <- runIOE searchResIO
          length searchRes `shouldBe` 1

runIOE :: (Show e) => IOE e a -> IO a
runIOE m = runExceptT m >>= either (fail . show) pure
