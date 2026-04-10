module Kengine.Types (KCodec (..), Key (..)) where

import Data.ByteString (ByteString)
import Data.Text (Text)

newtype Key = Key Text deriving stock (Eq, Ord, Show)

class KCodec a where
  encode :: a -> ByteString
  decode :: ByteString -> Either String a
