module Kengine.Engine (parseDocument, tokenize, Token (..)) where

import Data.Aeson ((.:))
import Data.Aeson qualified as AE
import Data.Aeson.Key qualified as AE.Types.Key
import Data.Aeson.Types qualified as AE.Types
import Data.Bifunctor (first)
import Data.Char qualified as C
import Data.List.NonEmpty (toList)
import Data.List.NonEmpty qualified as L
import Data.Map qualified as Map
import Data.Scientific qualified as S
import Data.Text (Text)
import Data.Text qualified as T
import Kengine.Errors (IndexError (IndexError))
import Kengine.Types (
  Document (Document),
  Field (..),
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
parseDocument jVal Mapping{fields} =
  let
    document = AE.withObject "Document" (docParser fields)
   in
    first (IndexError . T.pack) (AE.Types.parseEither document jVal)

docParser :: L.NonEmpty Field -> AE.Object -> AE.Types.Parser Document
docParser fields obj =
  let
    parsedFieldVals =
      traverse
        ( \((Field{sType, fieldName}) :: Field) ->
            let key = AE.Types.Key.fromText (unrefine fieldName)
             in (fieldName,) <$> case sType of
                  KT.Text -> TextVal <$> obj .: key
                  KT.Keyword -> KeywordVal <$> obj .: key
                  KT.Bool -> BoolVal <$> obj .: key
                  KT.Number -> NumberVal <$> obj .: key
        )
        fields
   in
    Document . Map.fromList . toList <$> parsedFieldVals
