module Kengine.Index.Update (updateIndex) where

import Data.Map qualified as Map
import Data.Text qualified as T
import Kengine.Tokenize (tokenize)
import Kengine.Types (
  DocFieldStats (..),
  DocId,
  Document (..),
  FieldIndex,
  FieldStats,
  FieldValue (KeywordVal, TextVal),
  TermFrequency (TF),
  Token (Token),
  mergeFieldIndex,
  mergeFieldStats,
 )

updateIndex ::
  FieldIndex ->
  FieldStats ->
  Document ->
  (FieldIndex, FieldStats)
updateIndex fieldIndex fieldMeta Document{docId, body} =
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

    mergedFields = mergeFieldIndex newInvertedIndexForTouchedFields fieldIndex
   in
    (mergedFields, mergedMeta)
  where
    toPerFieldMetadata :: [Token] -> Map.Map DocId DocFieldStats
    toPerFieldMetadata tkns = Map.singleton docId (DocFieldStats{totalTokens = length tkns})
