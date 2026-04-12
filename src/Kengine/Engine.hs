module Kengine.Engine (tokenize, Token (..)) where

import Data.Text (Text)
import Data.Text qualified as T
import Kengine.Types (Term (..))

-- Implement as a pure function: `tokenize :: Text -> [Text]`
-- - Split on anything not alphanumeric (Unicode-aware)
-- - Lowercase all tokens
-- - Discard empty tokens
-- - "Bio-Milch 3,5%" becomes ["bio", "milch", "3", "5"]

newtype Token = Token Text

tokenize :: Term -> [Token]
tokenize (Term m) = [Token $ T.toLower m]
