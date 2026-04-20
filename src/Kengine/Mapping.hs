module Kengine.Mapping (validateMapping, Validated) where

import Data.Bifunctor (Bifunctor (first))
import Data.List.NonEmpty qualified as NE
import Data.Map qualified as Map
import Data.Text (Text, intercalate)
import Data.Text qualified as T
import Kengine.Errors (KengineError (SearchError))
import Kengine.Types (
  Field (..),
  IndexName,
  Mapping (..),
 )
import Refined (unrefine)
import Validation qualified as V (Validation (..), failureIf)

type Validated a = V.Validation (NE.NonEmpty Text) a

validateMapping ::
  IndexName -> [IndexName] -> Mapping -> V.Validation KengineError Mapping
validateMapping newIndexName existingNames mapping =
  mapping
    <$ first
      (SearchError . intercalate " - " . NE.toList)
      (duplicatedField mapping <> mappingExists newIndexName existingNames)

mappingExists :: IndexName -> [IndexName] -> Validated ()
mappingExists n existingIndexNames =
  V.failureIf
    (n `elem` existingIndexNames)
    ("Index with name: `" <> T.show n <> "` exists already.")

duplicatedField :: Mapping -> Validated ()
duplicatedField m =
  let
    byFieldName = groupBy (unrefine . (.fieldName)) m.fields
    duplicates = Map.filter (\eles -> length eles > 1) byFieldName
    duplicateNames = Map.keys duplicates
   in
    V.failureIf
      (not $ null duplicateNames)
      ("Duplicate field names: " <> intercalate ", " duplicateNames)

groupBy :: (Ord k) => (a -> k) -> NE.NonEmpty a -> Map.Map k [a]
groupBy f = foldl' (\m a -> Map.insertWith (<>) (f a) [a] m) Map.empty
