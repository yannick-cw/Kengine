module Kengine.Store (Store (..), mkStore) where

import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Except (ExceptT (..), throwE)
import Data.Aeson qualified as AE
import Data.List qualified as L
import Data.Map qualified as Map
import Data.Maybe qualified as M
import Data.Ord (Down (Down))
import Data.Text qualified as T
import GHC.Conc qualified as TVar
import Kengine.Errors (IOE, KengineError (FileError, SearchError))
import Kengine.Index.Document (parseDocument)
import Kengine.Index.Update (updateIndex)
import Kengine.Mapping (validateMapping)
import Kengine.Persistence.FileStore (FileStore (..))
import Kengine.Persistence.Flush (flushSegment)
import Kengine.Search (searchQ)
import Kengine.Tokenize (tokenize)
import Kengine.Types (
  BlockLocation,
  DocId (DocId),
  DocStore,
  Document (..),
  FieldName,
  FieldStats,
  IndexData (..),
  IndexName,
  IndexResponse (..),
  IndexResponseStatus (..),
  IndexView,
  Mapping (..),
  Memtable (..),
  Query (..),
  SearchResults (..),
  Segment (Segment),
  SparseIndex,
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
mkStore fs = do
  indexViewVar <- loadIndexes fs
  pure
    Store
      { createIndex = createIndex' indexViewVar fs
      , indexDoc = indexDoc' indexViewVar fs
      , search = search' indexViewVar fs
      , flushState = flushState' indexViewVar fs
      }

loadIndexes :: FileStore -> IOE KengineError (TVar.TVar IndexView)
loadIndexes fs = do
  allIndexes <- fs.readIdxs
  liftIO $
    print
      ("Loading existing indexes... " <> T.unwords (unrefine <$> allIndexes))
  indexDatas <-
    traverse
      ( \idx -> do
          mapping <- requireMapping fs idx
          snapshotData <- fs.readSnapshot idx
          allDocs <- fs.readDocs idx
          (idx,) <$> liftIO (TVar.newTVarIO $ createInitialIdxData mapping snapshotData allDocs)
      )
      allIndexes
  liftIO $ TVar.newTVarIO (Map.fromList indexDatas)

requireMapping :: FileStore -> IndexName -> IOE KengineError Mapping
requireMapping fs idx = do
  maybeMapping <- fs.readMapping idx
  maybe
    (throwE $ FileError ("No mapping found for index: " <> unrefine idx))
    pure
    maybeMapping

createIndex' ::
  TVar.TVar IndexView ->
  FileStore ->
  IndexName ->
  Mapping ->
  IOE KengineError IndexResponse
createIndex' indexViewVar fs name mapping = do
  _ <- ExceptT $ TVar.atomically $ do
    indexView <- TVar.readTVar indexViewVar
    let validMapping =
          V.validationToEither $ validateMapping name (Map.keys indexView) mapping
    let newIndexData = createEmptyIdxData <$> validMapping
    newIndexTVar <- traverse TVar.newTVar newIndexData
    traverse
      (\idxData -> TVar.writeTVar indexViewVar (Map.insert name idxData indexView))
      newIndexTVar
  fs.storeMapping name mapping
  pure IndexResponse{status = Created}

indexDoc' ::
  TVar.TVar IndexView ->
  FileStore ->
  IndexName ->
  AE.Value ->
  IOE KengineError IndexResponse
indexDoc' indexViewVar fs name jval = do
  idxView <- liftIO $ TVar.readTVarIO indexViewVar
  indexDataVar <- lookupIndex name idxView
  doc <- buildDoc indexDataVar jval
  fs.storeDoc name doc
  liftIO $ TVar.atomically $ do
    idxData <- TVar.readTVar indexDataVar
    let (updatedFieldIdx, updatedFieldMeta) = updateIndex idxData.memtable.fieldIdx idxData.memtable.fieldMeta doc
    let updatedMemtable = idxData.memtable{fieldIdx = updatedFieldIdx, fieldMeta = updatedFieldMeta}
    TVar.writeTVar indexDataVar idxData{memtable = updatedMemtable}
  pure IndexResponse{status = Indexed}

search' ::
  TVar.TVar IndexView ->
  FileStore ->
  IndexName ->
  Query ->
  IOE KengineError SearchResults
search' indexViewVar fs name (Query query) = do
  idxView <- liftIO $ TVar.readTVarIO indexViewVar
  idxDataVar <- lookupIndex name idxView
  idxData <- liftIO $ TVar.readTVarIO idxDataVar
  let qTokens = tokenize query
  let offsets = resolveSparseIdx qTokens idxData.segment
  fromFileInvertedIdx <- traverse (fs.readDiskFieldIndex name idxData.segment) offsets
  let completeFromFileIdx = foldl' (Map.unionWith (Map.unionWith Map.union)) Map.empty fromFileInvertedIdx
  let completeFieldIndex = Map.unionWith (Map.unionWith Map.union) idxData.memtable.fieldIdx completeFromFileIdx
  pure $
    SearchResults
      { results =
          searchQ qTokens idxData.memtable.docStore completeFieldIndex idxData.memtable.fieldMeta
      }

flushState' :: TVar.TVar IndexView -> FileStore -> IOE KengineError ()
flushState' indexViewVar fs = do
  idxView <- liftIO $ TVar.readTVarIO indexViewVar
  _ <- Map.traverseWithKey (flushSegment fs) idxView
  pure ()

lookupIndex :: IndexName -> IndexView -> IOE KengineError (TVar.TVar IndexData)
lookupIndex name indexView =
  ExceptT . pure $
    maybe
      (Left $ SearchError ("No index found: " <> unrefine name))
      Right
      (Map.lookup name indexView)

resolveSparseIdx :: [Token] -> Segment -> [BlockLocation]
resolveSparseIdx tkns (Segment sparseIdx fieldnames) =
  snd
    <$> M.catMaybes
      ( do
          fn <- fieldnames
          tkn <- tkns
          pure $ Map.lookupLE (fn, tkn) sparseIdx
      )

createEmptyIdxData :: Mapping -> IndexData
createEmptyIdxData validMapping =
  IndexData
    { mapping = validMapping
    , memtable = Memtable{docStore = Map.empty, fieldIdx = Map.empty, fieldMeta = Map.empty}
    , segment = Segment Map.empty []
    }

createInitialIdxData ::
  Mapping ->
  (DocStore, SparseIndex, FieldStats, [FieldName]) ->
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
      { mapping = validMapping
      , memtable = Memtable{docStore, fieldIdx = updatedFieldIdx, fieldMeta = updatedFieldMeta}
      , segment = Segment sparseIndex fieldNames
      }
  where
    docsFromAppendLog :: Map.Map DocId Document
    docsFromAppendLog = Map.fromList ((\d -> (d.docId, d)) <$> docsFromLog)

buildDoc ::
  TVar.TVar IndexData ->
  AE.Value ->
  IOE KengineError Document
buildDoc indexDataVar doc = do
  idxData <- liftIO $ TVar.readTVarIO indexDataVar
  docToIndex <- ExceptT . pure $ parseDocument doc idxData.mapping
  liftIO $ TVar.atomically $ do
    freshIdxData <- TVar.readTVar indexDataVar
    let hightestDocId = M.listToMaybe $ L.sortOn Down (Map.keys freshIdxData.memtable.docStore)
    let nextDocId = maybe (DocId 1) (1 +) hightestDocId
    let newDoc = Document{docId = nextDocId, body = docToIndex}
    let newDocStore = Map.insert nextDocId newDoc freshIdxData.memtable.docStore
    TVar.writeTVar
      indexDataVar
      freshIdxData{memtable = freshIdxData.memtable{docStore = newDocStore}}
    pure newDoc
