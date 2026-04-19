{- HLINT ignore "Use tuple-section" -}
module Kengine.Store.InMemory (Store (..), mkStore) where

import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Except (ExceptT (..))
import Data.Aeson qualified as AE
import Data.Bifunctor (Bifunctor (second))
import Data.List qualified as L
import Data.Map qualified as Map
import Data.Maybe qualified as M
import Data.Ord (Down (..))
import GHC.Conc qualified as TVar
import Kengine.Engine (parseDocument, searchQ, tokenize)
import Kengine.Errors (IOE, SearchError (SearchError))
import Kengine.Mapping (validateMapping)
import Kengine.Types (
  DocId (DocId),
  DocStore,
  Document (Document),
  FieldDocResult,
  FieldIndex,
  FieldMetadata,
  FieldValue (TextVal),
  IndexData (..),
  IndexName,
  IndexResponse (..),
  IndexResponseStatus (..),
  IndexView,
  InvertedIndex,
  Mapping (..),
  MetaData (MetaData),
  Query,
  SearchResults (..),
  Term (Term),
  TermFrequency (..),
  Token,
  fromDoc,
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
    let nextDocId = maybe (DocId 1) (\(_, DocId dId) -> DocId (dId + 1)) (L.unsnoc (Map.keys docStore))
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

updateIndex ::
  DocId ->
  Document ->
  FieldIndex ->
  FieldMetadata ->
  (FieldIndex, FieldMetadata)
updateIndex docId (Document docs) fieldIndex fieldMeta =
  let
    tokenizedFields =
      Map.mapMaybe
        ( \case
            (TextVal txt) -> Just (tokenize $ Term txt)
            _ -> Nothing
        )
        docs
    newMeta = (\tkns -> Map.singleton docId (MetaData $ length tkns)) <$> tokenizedFields
    tknsWithCounts = fmap (,TF 1) <$> tokenizedFields
    newFieldIndx =
      Map.fromListWith (Map.unionWith (+)) . fmap (second (Map.singleton docId))
        <$> tknsWithCounts
    mergedFields =
      Map.unionWith
        (Map.unionWith (Map.unionWith (+)))
        newFieldIndx
        fieldIndex
    mergedMeta = Map.unionWith (Map.unionWith (+)) newMeta fieldMeta
   in
    (mergedFields, mergedMeta)
