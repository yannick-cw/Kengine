module Kengine.Tokenize (tokenize) where

import Data.Char qualified as C
import Data.Text (Text)
import Data.Text qualified as T
import Kengine.Types (Token (..))

-- "Bio-Milch 3,5%" becomes ["bio", "milch", "3", "5"]
tokenize :: Text -> [Token]
tokenize = fmap (Token . T.toLower) . filter (not . T.null) . splitNonAlpha

splitNonAlpha :: Text -> [Text]
splitNonAlpha = T.split (\tkn -> not (C.isAscii tkn && C.isAlphaNum tkn))
