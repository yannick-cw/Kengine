module Kengine.Index.Document (parseDocument) where

import Data.Aeson ((.:), (.:?))
import Data.Aeson qualified as AE
import Data.Aeson.Key qualified as AE.Types.Key
import Data.Aeson.Types qualified as AE.Types
import Data.Bifunctor (first)
import Data.List.NonEmpty (toList)
import Data.List.NonEmpty qualified as NEL
import Data.Map qualified as Map
import Data.Maybe qualified as M
import Data.Text qualified as T
import Kengine.Errors (KengineError (ParseError))
import Kengine.Types (
  Field (..),
  FieldName,
  FieldValue (BoolVal, KeywordVal, NumberVal, TextVal),
  Mapping (..),
 )
import Kengine.Types qualified as KT
import Refined (unrefine)

parseDocument :: AE.Value -> Mapping -> Either KengineError (Map.Map FieldName FieldValue)
parseDocument jVal Mapping{fields} =
  let
    document = AE.withObject "Document" (docParser fields)
   in
    first (ParseError . T.pack) (AE.Types.parseEither document jVal)

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
