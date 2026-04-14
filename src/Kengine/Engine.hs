module Kengine.Engine (parseDocument, tokenize, Token (..)) where

import Data.Aeson qualified as AE
import Data.Char qualified as C
import Data.Text (Text)
import Data.Text qualified as T
import Kengine.Errors (IndexError (IndexError))
import Kengine.Types (Document, Mapping, Term (..))

newtype Token = Token Text deriving newtype (Show, Eq)

-- "Bio-Milch 3,5%" becomes ["bio", "milch", "3", "5"]
tokenize :: Term -> [Token]
tokenize = fmap (Token . T.toLower) . filter (not . T.null) . splitNonAlpha

splitNonAlpha :: Term -> [Text]
splitNonAlpha (Term t) = T.split (\tkn -> not (C.isAscii tkn && C.isAlphaNum tkn)) t

-- todo implement this next
parseDocument :: AE.Value -> Mapping -> Either IndexError Document
parseDocument _ _ = Left $ IndexError "fail"
