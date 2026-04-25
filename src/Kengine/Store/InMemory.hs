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
  Memtable (..),
  Offset,
  Query (..),
  SearchResults (..),
  Segment (Segment),
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
    indexDatas <-
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
            (idx,) <$> liftIO (TVar.newTVarIO $ createInitialIdxData mapping snapshotData allDocs)
        )
        allIndexes
    indexViewVar <- liftIO $ TVar.newTVarIO (Map.fromList indexDatas)
    pure
      Store
        { createIndex = \name m -> do
            response <- createIndex indexViewVar name m
            storeMapping name m
            pure response
        , indexDoc = \name jval -> do
            idxView <- liftIO $ TVar.readTVarIO indexViewVar
            indexDataVar <- lookupIndex name idxView
            doc <- indexDoc indexDataVar jval
            storeDoc name doc
            liftIO $ TVar.atomically $ do
              (IndexData m memtable s) <- TVar.readTVar indexDataVar
              let (updatedFieldIdx, updatedFieldMeta) = updateIndex memtable.fieldIdx memtable.fieldMeta doc
              let updatedIdxData = IndexData m memtable{fieldIdx = updatedFieldIdx, fieldMeta = updatedFieldMeta} s
              TVar.writeTVar indexDataVar updatedIdxData
            pure IndexResponse{status = Indexed}
        , search = \name (Query query) -> do
            idxView <- liftIO $ TVar.readTVarIO indexViewVar
            idxDataVar <- lookupIndex name idxView
            (IndexData _ memtable segment) <- liftIO $ TVar.readTVarIO idxDataVar
            let qTokens = tokenize (Term query)
            let offsets = resolveSparseIdx qTokens segment
            fromFileInvertedIdx <- traverse (readDiskFieldIndex name segment) offsets
            let completeFromFileIdx = foldl' (Map.unionWith (Map.unionWith Map.union)) Map.empty fromFileInvertedIdx
            let completeFieldIndex = Map.unionWith (Map.unionWith Map.union) memtable.fieldIdx completeFromFileIdx
            pure $
              SearchResults
                { results = searchQ qTokens memtable.docStore completeFieldIndex memtable.fieldMeta
                }
        , flushState = do
            idxView <- liftIO $ TVar.readTVarIO indexViewVar
            _ <- Map.traverseWithKey unsafeFlushState idxView
            pure ()
        }

lookupIndex :: IndexName -> IndexView -> IOE KengineError (TVar.TVar IndexData)
lookupIndex name indexView =
  ExceptT . pure $
    maybe
      (Left $ SearchError ("No index found: " <> unrefine name))
      Right
      (Map.lookup name indexView)

resolveSparseIdx :: [Token] -> Segment -> [Offset]
resolveSparseIdx tkns (Segment sparseIdx fieldnames) =
  snd
    <$> M.catMaybes
      ( do
          fn <- fieldnames
          tkn <- tkns
          pure $ Map.lookupLE (fn, tkn) sparseIdx
      )

createIndex ::
  TVar.TVar IndexView ->
  IndexName ->
  Mapping ->
  IOE KengineError IndexResponse
createIndex indexViewVar name mapping = do
  _ <- ExceptT $ TVar.atomically $ do
    indexView <- TVar.readTVar indexViewVar
    let validMapping =
          V.validationToEither $ validateMapping name (Map.keys indexView) mapping
    let newIndexData = createEmptyIdxData <$> validMapping
    newIndexTVar <- traverse TVar.newTVar newIndexData
    traverse
      (\idxData -> TVar.writeTVar indexViewVar (Map.insert name idxData indexView))
      newIndexTVar
  pure IndexResponse{status = Created}

createEmptyIdxData :: Mapping -> IndexData
createEmptyIdxData validMapping = do
  IndexData
    validMapping
    Memtable{docStore = Map.empty, fieldIdx = Map.empty, fieldMeta = Map.empty}
    (Segment Map.empty [])

createInitialIdxData ::
  Mapping ->
  (DocStore, SparseIndex, FieldMetadata, [FieldName]) ->
  [Document] ->
  IndexData
createInitialIdxData validMapping (docsFromSnapshot, sparseIndex, fieldMeta, fieldNames) docsFromLog =
  let
    docStore = Map.union docsFromSnapshot docsFromAppendLog
    -- this should in practice just be `docsFromAppendLog` - but in case of crash
    -- append log docs would have not been cleaned up
    docsNewerThanSnapshot = Map.difference docsFromAppendLog docsFromSnapshot
    (updatedFieldIdx, updatedFieldMeta) =
      foldl'
        (\(fieldIdx, meta) nextDoc -> updateIndex fieldIdx meta nextDoc)
        (Map.empty, fieldMeta)
        docsNewerThanSnapshot
   in
    IndexData
      validMapping
      Memtable{docStore, fieldIdx = updatedFieldIdx, fieldMeta = updatedFieldMeta}
      (Segment sparseIndex fieldNames)
  where
    docsFromAppendLog :: Map.Map DocId Document
    docsFromAppendLog = Map.fromList ((\d -> (d.docId, d)) <$> docsFromLog)

indexDoc ::
  TVar.TVar IndexData ->
  AE.Value ->
  IOE KengineError Document
indexDoc indexDataVar doc = do
  (IndexData mapping _ _) <- liftIO $ TVar.readTVarIO indexDataVar
  docToIndex <- ExceptT . pure $ parseDocument doc mapping
  liftIO $ TVar.atomically $ do
    (IndexData m memtable s) <- TVar.readTVar indexDataVar
    let hightestDocId = M.listToMaybe $ L.sortOn Down (Map.keys memtable.docStore)
    let nextDocId = maybe (DocId 1) (1 +) hightestDocId
    let newDoc = Document nextDocId docToIndex
    let newDocStore = Map.insert nextDocId newDoc memtable.docStore
    TVar.writeTVar indexDataVar (IndexData m memtable{docStore = newDocStore} s)
    pure newDoc
