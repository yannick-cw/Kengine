module Kengine.Store.InMemory (mkStore, Store (..)) where

import Data.ByteString (ByteString)
import Data.IORef qualified as Ref
import Data.Map qualified as Map
import Kengine.Types (Key)

data Store = Store
  { getVal :: Key -> IO (Maybe ByteString)
  , putVal :: Key -> ByteString -> IO ()
  }

mkStore :: IO Store
mkStore = do
  ref <- Ref.newIORef Map.empty
  pure
    Store
      { getVal = \k -> Map.lookup k <$> Ref.readIORef ref
      , putVal = \k v -> Ref.modifyIORef ref (Map.insert k v)
      }
