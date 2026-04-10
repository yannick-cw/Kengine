module Kengine (DB, open, put, get, Key (..)) where

import Kengine.Codec.Json ()
import Kengine.Store.InMemory (Store (..), mkStore)
import Kengine.Types (KCodec (..), Key (..))

newtype DB = DB Store
open :: IO DB
open = DB <$> mkStore

put :: (KCodec value) => DB -> Key -> value -> IO ()
put (DB store) k v = store.putVal k (encode v)

get :: (KCodec value) => DB -> Key -> IO (Maybe (Either String value))
get (DB store) k = do
  value <- store.getVal k
  pure $ decode <$> value
