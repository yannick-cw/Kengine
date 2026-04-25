module Kengine.Store.InMemory (Store (..), mkStore) where

import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Except (ExceptT (..), throwE)
import Data.Aeson qualified as AE
import Data.Foldable (traverse_)
import Data.List qualified as L
import Data.Map qualified as Map
import Data.Maybe qualified as M
import Data.Ord (Down (Down))
import Data.Text qualified as T
import GHC.Conc qualified as TVar
import Kengine.Engine (parseDocument, searchQ, tokenize, updateIndex)
import Kengine.Errors (IOE, KengineError (FileError, SearchError))
import Kengine.Mapping (validateMapping)
import Kengine.Store.Persistence (FileStore (..))
import Kengine.Types (
  DocId (DocId),
  DocStore,
  Document (..),
  FieldIndex,
  FieldMetadata,
  FieldName,
  IndexData (..),
  IndexName,
  IndexResponse (..),
  IndexResponseStatus (..),
  IndexView,
  Mapping (..),
  Offset,
  Query (..),
  SearchResults (..),
  SparseIndex,
  Term (..),
  Token,
 )
import Refined (unrefine)
import Validation qualified as V (validationToEither)

data Store = Store
  { createIndex :: IndexName -> Mapping -> IOE KengineError IndexResponse
  , indexDoc :: IndexName -> AE.Value -> IOE KengineError IndexResponse
  , search :: IndexName -> Query -> IOE KengineError SearchResults
  , flushState :: IOE KengineError ()
  }

mkStore :: FileStore -> IOE KengineError Store
mkStore
  FileStore
    { storeMapping
    , readIdxs
    , readMapping
    , readDocs
    , storeDoc
    , readSnapshot
    , unsafeFlushState
    , readDiskFieldIndex
    } = do
    allIndexes <- readIdxs
    liftIO $
      print
        ("Loading existing indexes... " <> T.unwords (unrefine <$> allIndexes))
    indexView <-
      traverse
        ( \idx -> do
            maybeMapping <- readMapping idx
            mapping <-
              maybe
                (throwE $ FileError ("No mapping found for index: " <> unrefine idx))
                pure
                maybeMapping
            snapshotData <- readSnapshot idx
            allDocs <- readDocs idx
            liftIO $ TVar.atomically ((idx,) <$> createInitialIdxData mapping snapshotData allDocs)
        )
        allIndexes
    indexViewVar <- liftIO $ TVar.newTVarIO (Map.fromList indexView)
    pure
      Store
        { createIndex = \name m -> do
            response <- createIndex indexViewVar name m
            storeMapping name m
            pure response
        , indexDoc = \name jval -> do
            idxView <- liftIO $ TVar.readTVarIO indexViewVar
            (IndexData mapping docStoreVar invertedIndexVar fieldMetaDataVar _ _) <-
              lookupIndex name idxView
            doc <- indexDoc mapping docStoreVar jval
            storeDoc name doc
            liftIO $ TVar.atomically (storeTokens invertedIndexVar fieldMetaDataVar doc)
            pure IndexResponse{status = Indexed}
        , search = \name (Query query) -> do
            idxView <- liftIO $ TVar.readTVarIO indexViewVar
            (IndexData _ docStoreVar memtableVar fieldMetaDataVar sparseIdxVar persistedFieldNamesVar) <-
              lookupIndex name idxView
            inMemFieldIndex <- liftIO $ TVar.readTVarIO memtableVar
            docStore <- liftIO $ TVar.readTVarIO docStoreVar
            fieldMetadata <- liftIO $ TVar.readTVarIO fieldMetaDataVar
            persistedFieldNames <- liftIO $ TVar.readTVarIO persistedFieldNamesVar
            sparseIdx <- liftIO $ TVar.readTVarIO sparseIdxVar
            let qTokens = tokenize (Term query)
            let offsets = resolveSparseIdx persistedFieldNames qTokens sparseIdx
            fromFileInvertedIdx <- traverse (readDiskFieldIndex name persistedFieldNames) offsets
            let completeFromFileIdx = foldl' (Map.unionWith (Map.unionWith Map.union)) Map.empty fromFileInvertedIdx
            let completeFieldIndex = Map.unionWith (Map.unionWith Map.union) inMemFieldIndex completeFromFileIdx
            pure $
              SearchResults{results = searchQ qTokens docStore completeFieldIndex fieldMetadata}
        , flushState = do
            idxView <- liftIO $ TVar.readTVarIO indexViewVar
            _ <- Map.traverseWithKey unsafeFlushState idxView
            pure ()
        }

lookupIndex :: IndexName -> IndexView -> IOE KengineError IndexData
lookupIndex name indexView =
  ExceptT . pure $
    maybe
      (Left $ SearchError ("No index found: " <> unrefine name))
      Right
      (Map.lookup name indexView)

resolveSparseIdx :: [FieldName] -> [Token] -> SparseIndex -> [Offset]
resolveSparseIdx persistedFieldNames tkns idx =
  snd
    <$> M.catMaybes
      ( do
          fn <- persistedFieldNames
          tkn <- tkns
          pure $ Map.lookupLE (fn, tkn) idx
      )

createIndex ::
  TVar.TVar IndexView ->
  IndexName ->
  Mapping ->
  IOE KengineError IndexResponse
createIndex indexViewVar name mapping = do
  _ <- ExceptT $ TVar.atomically $ do
    existingMappings <- TVar.readTVar indexViewVar
    let validMapping =
          V.validationToEither $ validateMapping name (Map.keys existingMappings) mapping
    newIndexData <- traverse createEmptyIdxData validMapping
    traverse
      (\idxData -> TVar.writeTVar indexViewVar (Map.insert name idxData existingMappings))
      newIndexData
  pure IndexResponse{status = Created}

createEmptyIdxData :: Mapping -> TVar.STM IndexData
createEmptyIdxData validMapping = do
  docStoreVar <- TVar.newTVar Map.empty
  memtableVar <- TVar.newTVar Map.empty
  fieldMetadataVar <- TVar.newTVar Map.empty
  sparseIdxVar <- TVar.newTVar Map.empty
  fieldNamesVar <- TVar.newTVar []
  pure $
    IndexData validMapping docStoreVar memtableVar fieldMetadataVar sparseIdxVar fieldNamesVar

createInitialIdxData ::
  Mapping ->
  (DocStore, SparseIndex, FieldMetadata, [FieldName]) ->
  [Document] ->
  TVar.STM IndexData
createInitialIdxData validMapping (docsFromSnapshot, sparseIndex, fieldMeta, fieldNames) docsFromLog = do
  docStoreVar <-
    TVar.newTVar $ Map.union docsFromSnapshot docsFromAppendLog
  memtableVar <- TVar.newTVar Map.empty
  fieldMetadataVar <- TVar.newTVar fieldMeta
  sparseIndexVar <- TVar.newTVar sparseIndex
  fieldNamesVar <- TVar.newTVar fieldNames
  -- this should in practice just be `docsFromAppendLog` - but in case of crash
  -- append log docs would have not been cleaned up
  let docsNewerThanSnapshot = Map.difference docsFromAppendLog docsFromSnapshot
  traverse_ (storeTokens memtableVar fieldMetadataVar) docsNewerThanSnapshot
  pure $
    IndexData
      validMapping
      docStoreVar
      memtableVar
      fieldMetadataVar
      sparseIndexVar
      fieldNamesVar
  where
    docsFromAppendLog :: Map.Map DocId Document
    docsFromAppendLog = Map.fromList ((\d -> (d.docId, d)) <$> docsFromLog)

indexDoc ::
  Mapping ->
  TVar.TVar DocStore ->
  AE.Value ->
  IOE KengineError Document
indexDoc mapping docStoreVar doc = do
  docToIndex <- ExceptT . pure $ parseDocument doc mapping
  liftIO $ TVar.atomically $ do
    docStore <- TVar.readTVar docStoreVar
    let hightestDocId = M.listToMaybe $ L.sortOn Down (Map.keys docStore)
    let nextDocId = maybe (DocId 1) (1 +) hightestDocId
    let newDoc = Document nextDocId docToIndex
    TVar.writeTVar docStoreVar (Map.insert nextDocId newDoc docStore)
    pure newDoc

storeTokens ::
  TVar.TVar FieldIndex ->
  TVar.TVar FieldMetadata ->
  Document ->
  TVar.STM ()
storeTokens memtableVar fieldMetadataVar doc = do
  do
    fieldIndex <- TVar.readTVar memtableVar
    fieldMeta <- TVar.readTVar fieldMetadataVar
    let (updatedIndex, updatedMeta) = updateIndex doc fieldIndex fieldMeta
    TVar.writeTVar memtableVar updatedIndex
    TVar.writeTVar fieldMetadataVar updatedMeta
