module Kengine.Store (Store (..), mkStore) where

import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Except (ExceptT (..), throwE)
import Data.Aeson qualified as AE
import Data.Map qualified as Map
import Data.Maybe qualified as M
import Data.Text qualified as T
import Data.Text.Lazy qualified as LT
import GHC.Conc qualified as TVar
import Kengine.Debug.Layout qualified as Layout
import Kengine.Errors (IOE, KengineError (FileError, SearchError), Result)
import Kengine.Index.Document (parseDocument)
import Kengine.Index.Update (updateIndex)
import Kengine.Mapping (validateMapping)
import Kengine.Persistence.Binary (Header (..))
import Kengine.Persistence.FileStore (FileStore (..))
import Kengine.Persistence.Flush (flushSegment)
import Kengine.Search (searchQ)
import Kengine.Tokenize (tokenize)
import Kengine.Types (
  BlockLocation,
  DocId (DocId),
  DocSparseIndex,
  Document (..),
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
  Segment (..),
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
  , debugLayout :: IndexName -> IOE KengineError LT.Text
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
      , debugLayout = Layout.renderLayout fs indexViewVar
      }

loadIndexes :: FileStore -> IOE KengineError (TVar.TVar IndexView)
loadIndexes FileStore{readIdxs, readMapping, readSnapshot, readDocs} = do
  allIndexes <- readIdxs
  liftIO $
    print
      ("Loading existing indexes... " <> T.unwords (unrefine <$> allIndexes))
  indexDatas <-
    traverse
      ( \idx -> do
          mapping <- requireMapping readMapping idx
          snapshotData <- readSnapshot idx
          allDocs <- readDocs idx
          (idx,) <$> liftIO (TVar.newTVarIO $ createInitialIdxData mapping snapshotData allDocs)
      )
      allIndexes
  liftIO $ TVar.newTVarIO (Map.fromList indexDatas)

requireMapping ::
  (IndexName -> IOE KengineError (Maybe Mapping)) ->
  IndexName ->
  IOE KengineError Mapping
requireMapping readMapping idx = do
  maybeMapping <- readMapping idx
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
createIndex' indexViewVar FileStore{storeMapping} name mapping = do
  _ <- ExceptT $ TVar.atomically $ do
    indexView <- TVar.readTVar indexViewVar
    let validMapping =
          V.validationToEither $ validateMapping name (Map.keys indexView) mapping
    let newIndexData = createEmptyIdxData <$> validMapping
    newIndexTVar <- traverse TVar.newTVar newIndexData
    traverse
      (\idxData -> TVar.writeTVar indexViewVar (Map.insert name idxData indexView))
      newIndexTVar
  storeMapping name mapping
  pure IndexResponse{status = Created}

indexDoc' ::
  TVar.TVar IndexView ->
  FileStore ->
  IndexName ->
  AE.Value ->
  IOE KengineError IndexResponse
indexDoc' indexViewVar FileStore{storeDoc} name jval = do
  idxView <- liftIO $ TVar.readTVarIO indexViewVar
  indexDataVar <- lookupIndex name idxView
  doc <- buildDoc indexDataVar jval
  storeDoc name doc
  liftIO $ TVar.atomically $ do
    idxData@IndexData{memtable = memtable@Memtable{fieldIdx, fieldMeta}} <-
      TVar.readTVar indexDataVar
    let (updatedFieldIdx, updatedFieldMeta) = updateIndex fieldIdx fieldMeta doc
    let updatedMemtable = memtable{fieldIdx = updatedFieldIdx, fieldMeta = updatedFieldMeta}
    TVar.writeTVar indexDataVar idxData{memtable = updatedMemtable}
  pure IndexResponse{status = Indexed}

search' ::
  TVar.TVar IndexView ->
  FileStore ->
  IndexName ->
  Query ->
  IOE KengineError SearchResults
search' indexViewVar FileStore{readDiskFieldIndex, readDiskDoc} name (Query query) = do
  idxView <- liftIO $ TVar.readTVarIO indexViewVar
  idxDataVar <- lookupIndex name idxView
  IndexData{memtable = Memtable{docStore, fieldIdx, fieldMeta}, segment} <-
    liftIO $ TVar.readTVarIO idxDataVar
  let qTokens = tokenize query
  let offsets = resolveSparseIdx qTokens segment
  fromFileInvertedIdx <- traverse (readDiskFieldIndex name segment) offsets
  let completeFromFileIdx = foldl' (Map.unionWith (Map.unionWith Map.union)) Map.empty fromFileInvertedIdx
  let completeFieldIndex = Map.unionWith (Map.unionWith Map.union) fieldIdx completeFromFileIdx
  -- todo better in the future to get for all docId candidates once
  SearchResults
    <$> searchQ qTokens docStore completeFieldIndex fieldMeta (diskDocLookup segment)
  where
    diskDocLookup :: Segment -> DocId -> Result (Maybe Document)
    diskDocLookup segment docId =
      maybe
        (pure Nothing)
        (fmap (Map.lookup docId) . readDiskDoc name)
        (resolveDocSparseIdx docId segment)

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
resolveSparseIdx tkns Segment{sparseIndex, fieldNames} =
  snd
    <$> M.catMaybes
      ( do
          fn <- fieldNames
          tkn <- tkns
          pure $ Map.lookupLE (fn, tkn) sparseIndex
      )

resolveDocSparseIdx :: DocId -> Segment -> Maybe BlockLocation
resolveDocSparseIdx tkns Segment{docsSparseIndex} = snd <$> Map.lookupLE tkns docsSparseIndex

createEmptyIdxData :: Mapping -> IndexData
createEmptyIdxData validMapping =
  IndexData
    { mapping = validMapping
    , memtable = Memtable{docStore = Map.empty, fieldIdx = Map.empty, fieldMeta = Map.empty}
    , segment = Segment Map.empty [] Map.empty
    , maxDocId = DocId 0
    }

createInitialIdxData ::
  Mapping ->
  Maybe (Header, SparseIndex, DocSparseIndex, FieldStats) ->
  [Document] ->
  IndexData
createInitialIdxData validMapping snapshot docsFromLog =
  let
    -- todo just for now header.docCount as max id - wont hold for deletions
    (sparseIndex, fieldMeta, fieldNames, docSparse, maxSnapshotId) = case snapshot of
      Nothing -> (Map.empty, Map.empty, [], Map.empty, 0)
      Just (h, si, dsi, fs) -> (si, fs, h.fieldNames, dsi, fromIntegral h.docCount)
    -- this should in practice just be `docsFromAppendLog` - but in case of crash
    -- append log docs would have not been cleaned up
    (updatedFieldIdx, updatedFieldMeta) =
      foldl'
        (\(fieldIdx, meta) nextDoc -> updateIndex fieldIdx meta nextDoc)
        (Map.empty, fieldMeta)
        docsFromAppendLog
    maxDocId = foldl' max (DocId maxSnapshotId) ((.docId) <$> docsFromLog)
   in
    IndexData
      { mapping = validMapping
      , memtable =
          Memtable
            { docStore = docsFromAppendLog
            , fieldIdx = updatedFieldIdx
            , fieldMeta = updatedFieldMeta
            }
      , segment = Segment sparseIndex fieldNames docSparse
      , maxDocId
      }
  where
    docsFromAppendLog :: Map.Map DocId Document
    docsFromAppendLog = Map.fromList ((\d -> (d.docId, d)) <$> docsFromLog)

buildDoc ::
  TVar.TVar IndexData ->
  AE.Value ->
  IOE KengineError Document
buildDoc indexDataVar jval = do
  IndexData{mapping = parseMapping} <- liftIO $ TVar.readTVarIO indexDataVar
  docToIndex <- ExceptT . pure $ parseDocument jval parseMapping
  liftIO $ TVar.atomically $ do
    idxData@IndexData{memtable, maxDocId} <- TVar.readTVar indexDataVar
    let Memtable{docStore} = memtable
    let nextDocId = maxDocId + 1
    let newDoc = Document{docId = nextDocId, body = docToIndex}
    let newDocStore = Map.insert nextDocId newDoc docStore
    TVar.writeTVar
      indexDataVar
      idxData{memtable = memtable{docStore = newDocStore}, maxDocId = nextDocId}
    pure newDoc
