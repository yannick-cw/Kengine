module Kengine.Store.InMemory (Store (..), mkStore) where

import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Except (ExceptT (..))
import Data.Aeson qualified as AE
import Data.List qualified as L
import Data.Map qualified as Map
import Data.Maybe qualified as M
import Data.Ord (Down (Down))
import GHC.Conc qualified as TVar
import Kengine.Engine (parseDocument, searchQ, updateIndex)
import Kengine.Errors (IOE, SearchError (SearchError))
import Kengine.Mapping (validateMapping)
import Kengine.Types (
  DocId (DocId),
  DocStore,
  Document,
  FieldIndex,
  FieldMetadata,
  IndexData (..),
  IndexName,
  IndexResponse (..),
  IndexResponseStatus (..),
  IndexView,
  Mapping (..),
  Query,
  SearchResults (..),
 )
import Refined (unrefine)
import Validation qualified as V (validationToEither)

data Store = Store
  { createIndex :: IndexName -> Mapping -> IOE SearchError IndexResponse
  , indexDoc :: IndexName -> AE.Value -> IOE SearchError IndexResponse
  , search :: IndexName -> Query -> IOE SearchError SearchResults
  }

mkStore :: IO Store
mkStore = do
  indexViewVar :: TVar.TVar IndexView <- TVar.newTVarIO Map.empty
  pure
    Store
      { createIndex = createIndex indexViewVar
      , indexDoc = \name jval -> do
          idxView <- liftIO $ TVar.readTVarIO indexViewVar
          (IndexData mapping docStoreVar invertedIndexVar fieldMetaDataVar) <-
            lookupIndex name idxView
          (newId, doc) <- indexDoc mapping docStoreVar jval
          storeTokens invertedIndexVar fieldMetaDataVar newId doc
          pure IndexResponse{status = Indexed}
      , search = \name query -> do
          idxView <- liftIO $ TVar.readTVarIO indexViewVar
          (IndexData _ docStoreVar fieldIndexVar fieldMetaDataVar) <- lookupIndex name idxView
          fieldIndex <- liftIO $ TVar.readTVarIO fieldIndexVar
          docStore <- liftIO $ TVar.readTVarIO docStoreVar
          fieldMetadata <- liftIO $ TVar.readTVarIO fieldMetaDataVar
          pure $ SearchResults{results = searchQ query docStore fieldIndex fieldMetadata}
      }

lookupIndex :: IndexName -> IndexView -> IOE SearchError IndexData
lookupIndex name indexView =
  ExceptT . pure $
    maybe
      (Left $ SearchError ("No index found: " <> unrefine name))
      Right
      (Map.lookup name indexView)

createIndex ::
  TVar.TVar IndexView ->
  IndexName ->
  Mapping ->
  IOE SearchError IndexResponse
createIndex indexViewVar name mapping = do
  _ <- ExceptT $ TVar.atomically $ do
    existingMappings <- TVar.readTVar indexViewVar
    let validMapping =
          V.validationToEither $ validateMapping name (Map.keys existingMappings) mapping
    newIndexData <- traverse createIndexData validMapping
    traverse
      (\idxData -> TVar.writeTVar indexViewVar (Map.insert name idxData existingMappings))
      newIndexData
  pure IndexResponse{status = Created}
  where
    createIndexData :: Mapping -> TVar.STM IndexData
    createIndexData validMapping = do
      docStoreVar <- TVar.newTVar Map.empty
      fieldIndexVar <- TVar.newTVar Map.empty
      fieldMetadataVar <- TVar.newTVar Map.empty
      pure $ IndexData validMapping docStoreVar fieldIndexVar fieldMetadataVar

indexDoc ::
  Mapping ->
  TVar.TVar DocStore ->
  AE.Value ->
  IOE SearchError (DocId, Document)
indexDoc mapping docStoreVar doc = do
  docToIndex <- ExceptT . pure $ parseDocument doc mapping
  liftIO $ TVar.atomically $ do
    docStore <- TVar.readTVar docStoreVar
    let hightestDocId = M.listToMaybe $ L.sortOn Down (Map.keys docStore)
    let nextDocId = maybe (DocId 1) (1 +) hightestDocId
    TVar.writeTVar docStoreVar (Map.insert nextDocId docToIndex docStore)
    pure (nextDocId, docToIndex)

storeTokens ::
  TVar.TVar FieldIndex ->
  TVar.TVar FieldMetadata ->
  DocId ->
  Document ->
  IOE SearchError ()
storeTokens fieldIndexVar fieldMetadataVar docId doc = do
  liftIO $ TVar.atomically $ do
    fieldIndex <- TVar.readTVar fieldIndexVar
    fieldMeta <- TVar.readTVar fieldMetadataVar
    let (updatedIndex, updatedMeta) = updateIndex docId doc fieldIndex fieldMeta
    TVar.writeTVar fieldIndexVar updatedIndex
    TVar.writeTVar fieldMetadataVar updatedMeta
