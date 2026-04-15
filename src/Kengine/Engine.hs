module Kengine.Engine (parseDocument, tokenize, Token (..)) where

import Data.Aeson qualified as AE
import Data.Aeson.Key qualified as AE.Types.Key
import Data.Aeson.KeyMap qualified as AE.KeyMap
import Data.Char qualified as C
import Data.List.NonEmpty (toList)
import Data.Map qualified as Map
import Data.Scientific qualified as S
import Data.Text (Text)
import Data.Text qualified as T
import Kengine.Errors (IndexError (IndexError))
import Kengine.Types (
  Document (Document),
  Field (..),
  FieldName,
  FieldValue (BoolVal, KeywordVal, NumberVal, TextVal),
  Mapping (..),
  SearchType (..),
  Term (..),
 )
import Kengine.Types qualified as KT
import Refined (unrefine)

newtype Token = Token Text deriving newtype (Show, Eq)

-- "Bio-Milch 3,5%" becomes ["bio", "milch", "3", "5"]
tokenize :: Term -> [Token]
tokenize = fmap (Token . T.toLower) . filter (not . T.null) . splitNonAlpha

splitNonAlpha :: Term -> [Text]
splitNonAlpha (Term t) = T.split (\tkn -> not (C.isAscii tkn && C.isAlphaNum tkn)) t

parseDocument :: AE.Value -> Mapping -> Either IndexError Document
parseDocument (AE.Object obj) Mapping{fields} =
  let
    findValue :: FieldName -> SearchType -> Either IndexError FieldValue
    findValue name expectedType =
      maybe
        (Left $ IndexError $ "Missing required field: " <> unrefine name)
        (parseSearchType expectedType)
        (AE.KeyMap.lookup (AE.Types.Key.fromText (unrefine name)) obj)
    parsedDoc =
      traverse
        (\(Field{sType, fieldName}) -> (fieldName,) <$> findValue fieldName sType)
        fields
   in
    Document . Map.fromList . toList <$> parsedDoc
parseDocument _ _ = Left $ IndexError "No valid json object given."

parseSearchType :: SearchType -> AE.Value -> Either IndexError FieldValue
parseSearchType KT.Text (AE.String txt) = Right $ TextVal txt
parseSearchType KT.Keyword (AE.String txt) = Right $ KeywordVal txt
parseSearchType KT.Bool (AE.Bool bool) = Right $ BoolVal bool
parseSearchType KT.Number (AE.Number num) = Right $ NumberVal (S.toRealFloat num)
parseSearchType tpe val = Left $ IndexError ("Type: " <> T.show tpe <> " expected, but got: " <> T.show val)
