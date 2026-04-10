{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Kengine.Codec.Json () where

import Data.Aeson (ToJSON)
import Data.Aeson qualified as Aeson
import Data.Aeson.Types (FromJSON)
import Data.ByteString (ByteString, toStrict)
import Kengine.Types (KCodec (..))

instance (ToJSON a, FromJSON a) => KCodec a where
  encode :: a -> ByteString
  encode = toStrict . Aeson.encode
  decode :: ByteString -> Either String a
  decode = Aeson.eitherDecodeStrict
