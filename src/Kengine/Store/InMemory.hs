module Kengine.Store.InMemory (Store (..), mkStore) where

import Control.Monad.Trans.Except (ExceptT (..))
import Data.Aeson qualified as AE
import Data.Map qualified as Map
import GHC.Conc qualified as TVar
import Kengine.Errors (IOE, IndexError, SearchError)
import Kengine.Mapping (validateMapping)
import Kengine.Types (
  DocId (DocId),
  Document (Document),
  IndexName,
  IndexResponse (..),
  IndexResponseStatus (..),
  Mapping (..),
  Query,
  SearchResults (..),
 )
import Validation qualified as V (validationToEither)

data Store = Store
  { createIndex :: IndexName -> Mapping -> IOE IndexError IndexResponse
  , indexDoc :: IndexName -> AE.Value -> IOE IndexError IndexResponse
  , search :: IndexName -> Query -> IOE SearchError SearchResults
  }

mkStore :: IO Store
mkStore = do
  mappingVar :: TVar.TVar (Map.Map IndexName Mapping) <- TVar.newTVarIO Map.empty
  docStore :: TVar.TVar (Map.Map DocId Document) <- TVar.newTVarIO Map.empty
  pure
    Store
      { createIndex = \name mapping -> do
          _ <- ExceptT $ TVar.atomically $ do
            existingMappings <- TVar.readTVar mappingVar
            let validMapping =
                  V.validationToEither $ validateMapping name (Map.keys existingMappings) mapping
            traverse
              (\m -> TVar.writeTVar mappingVar (Map.insert name m existingMappings))
              validMapping
          pure IndexResponse{status = Created}
      , indexDoc = \_ _ -> pure IndexResponse{status = Indexed}
      , search = \_ _ -> pure SearchResults{results = []}
      }
