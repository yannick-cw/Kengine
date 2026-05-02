module Kengine.Index.Update (updateIndex) where

import Data.Map qualified as Map
import Data.Maybe qualified as M
import Data.Set qualified as S
import Data.Text qualified as T
import Kengine.Tokenize (tokenize)
import Kengine.Types (
  DocFieldStats (..),
  DocId,
  Document (..),
  FieldIndex,
  FieldStats,
  FieldTrigrams,
  FieldValue (KeywordVal, TextVal),
  TermFrequency (TF),
  Token (Token),
  Trigram,
  mergeFieldIndex,
  mergeFieldStats,
  mergeTrigrams,
 )
import Refined (refineFail)

updateIndex ::
  FieldIndex ->
  FieldStats ->
  FieldTrigrams ->
  Document ->
  (FieldIndex, FieldStats, FieldTrigrams)
updateIndex fieldIndex fieldMeta trigrams Document{docId, body} =
  let
    tokensPerField =
      Map.mapMaybe
        ( \case
            (TextVal txt) -> Just (tokenize txt)
            (KeywordVal txt) -> Just [Token $ T.toLower txt]
            _ -> Nothing
        )
        body
    allMetadataPerField :: FieldStats
    allMetadataPerField = toPerFieldMetadata <$> tokensPerField
    -- simple union is enough - this is a new doc, the doc id can not exist yet
    mergedMeta = mergeFieldStats allMetadataPerField fieldMeta

    -- takes FieldName -> [Token] and merges duplicates to create term frequency of
    -- each token per field
    newInvertedIndexForTouchedFields =
      fmap (Map.singleton docId) . Map.fromListWith (+) . fmap (,TF 1) <$> tokensPerField

    -- for each token per field
    -- builds a new map of trigram -> token, union to bigger map of fieldname -> trigram -> [token]
    newFieldTrigrams = do
      (fieldName, tokens) <- Map.toList tokensPerField
      token <- tokens
      trigram <- tokenToTrigams token
      pure (Map.singleton fieldName (Map.singleton trigram (S.singleton token)))

    mergedFields = mergeFieldIndex newInvertedIndexForTouchedFields fieldIndex
    mergedTrigrams = foldl' mergeTrigrams trigrams newFieldTrigrams
   in
    (mergedFields, mergedMeta, mergedTrigrams)
  where
    toPerFieldMetadata :: [Token] -> Map.Map DocId DocFieldStats
    toPerFieldMetadata tkns = Map.singleton docId (DocFieldStats{totalTokens = length tkns})
    tokenToTrigams :: Token -> [Trigram]
    tokenToTrigams (Token tkn) = M.mapMaybe (refineFail . T.take 3) (T.tails tkn)
