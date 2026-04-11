module Kengine.Store.InMemory (Store (..), mkStore) where

import Data.Aeson (Object)
import Data.Map qualified as Map
import GHC.Conc (TVar)
import GHC.Conc qualified as TVar
import Kengine.Types (IndexName, IndexResponse, Mapping, Query, SearchResults)

data Store = Store
  { createIndex :: IndexName -> Mapping -> IO IndexResponse
  , indexDoc :: IndexName -> Object -> IO IndexResponse
  , search :: IndexName -> Query -> IO SearchResults
  }

mkStore :: IO Store
mkStore = do
  _ :: TVar (Map.Map String String) <- TVar.newTVarIO Map.empty
  pure undefined

-- Store
--   { getVal = \k -> Map.lookup k <$> Ref.readIORef ref
--   , putVal = \k v -> Ref.modifyIORef ref (Map.insert k v)
--   }
