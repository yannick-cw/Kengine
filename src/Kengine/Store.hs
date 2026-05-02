module Kengine.Store (Store (..), mkStore) where

import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Except (ExceptT (..), throwE)
import Data.Aeson qualified as AE
import Data.List qualified as L
import Data.List.NonEmpty qualified as Nel
import Data.Map qualified as Map
import Data.Maybe qualified as M
import Data.Set qualified as S
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
import Kengine.Search (expandTrigrams, searchQ)
import Kengine.Tokenize (tokenize)
import Kengine.Types (
  BlockLocation,
  DocId (DocId),
  DocStore,
  Document (..),
  Field,
  FieldIndex,
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
  SearchTerm (..),
  Segment (..),
  Token,
  mergeFieldIndex,
  mergeFieldStats,
 )
import Refined (unrefine)
import Validation qualified as V (validationToEither)

data Store = Store
  { createIndex :: IndexName -> Mapping -> IOE KengineError IndexResponse
  , indexDoc :: IndexName -> AE.Value -> IOE KengineError IndexResponse
  , updateMapping :: IndexName -> Field -> IOE KengineError IndexResponse
  , search :: IndexName -> Query -> IOE KengineError SearchResults
  , searchFuzzy :: IndexName -> Query -> IOE KengineError SearchResults
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
      , updateMapping = updateMapping' indexViewVar fs
      , search = search' indexViewVar fs False
      , searchFuzzy = search' indexViewVar fs True
      , flushState = flushState' indexViewVar fs
      , debugLayout = Layout.renderLayout fs indexViewVar
      }

loadIndexes :: FileStore -> IOE KengineError (TVar.TVar IndexView)
loadIndexes FileStore{readIdxs, readMapping, readSnapshots, readDocs} = do
  allIndexes <- readIdxs
  liftIO $
    print
      ("Loading existing indexes... " <> T.unwords (unrefine <$> allIndexes))
  indexDatas <-
    traverse
      ( \idx -> do
          mapping <- requireMapping readMapping idx
          snapshotData <- readSnapshots idx
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
    idxData@IndexData{memtable = memtable@Memtable{fieldIdx, fieldMeta, trigrams}} <-
      TVar.readTVar indexDataVar
    let (updatedFieldIdx, updatedFieldMeta, updatedTrigrams) = updateIndex fieldIdx fieldMeta trigrams doc
    let updatedMemtable =
          memtable
            { fieldIdx = updatedFieldIdx
            , fieldMeta = updatedFieldMeta
            , trigrams = updatedTrigrams
            }
    TVar.writeTVar indexDataVar idxData{memtable = updatedMemtable}
  pure IndexResponse{status = Indexed}

updateMapping' ::
  TVar.TVar IndexView ->
  FileStore ->
  IndexName ->
  Field ->
  IOE KengineError IndexResponse
updateMapping' indexViewVar FileStore{storeMapping} name newField = do
  idxView <- liftIO $ TVar.readTVarIO indexViewVar
  indexDataVar <- lookupIndex name idxView
  newMapping <- ExceptT $ TVar.atomically $ do
    idxData@IndexData{mapping} <- TVar.readTVar indexDataVar
    let validMapping =
          V.validationToEither $
            validateMapping name [] mapping{fields = Nel.cons newField mapping.fields}
    traverse
      ( \m -> do
          TVar.writeTVar indexDataVar idxData{mapping = m}
          pure m
      )
      validMapping
  -- danger zone here: when crashing before storing mapping, mapping is lost
  storeMapping name newMapping
  pure IndexResponse{status = MappingUpdated}

search' ::
  TVar.TVar IndexView ->
  FileStore ->
  Bool ->
  IndexName ->
  Query ->
  Result SearchResults
search' indexViewVar FileStore{readDiskFieldIndex, readDiskDoc} fuzzy name (Query query) = do
  idxView <- liftIO $ TVar.readTVarIO indexViewVar
  idxDataVar <- lookupIndex name idxView
  IndexData{memtable = Memtable{docStore, fieldIdx, fieldMeta, trigrams}, segments} <-
    liftIO $ TVar.readTVarIO idxDataVar
  let qTokens =
        if fuzzy
          then expandTrigrams trigrams <$> tokenize query
          else
            (\tkn -> (SearchTerm{originalToken = tkn, alternatives = S.empty})) <$> tokenize query
  inveretdIdxsFromAllSegments <-
    traverse (resolveInvertedIdxsForOneSegment qTokens) segments
  -- merge from left to right, starting with in mem and then newest segment - leftmost - first
  let completeFieldIndex = mergeIdxs $ mergeIdxs <$> [fieldIdx] : inveretdIdxsFromAllSegments
  let lookupDocsInAllSegments docIds = mconcat <$> traverse (diskDocLookup docIds) segments
  SearchResults . take 100
    <$> searchQ qTokens docStore completeFieldIndex fieldMeta lookupDocsInAllSegments
  where
    diskDocLookup :: [DocId] -> Segment -> Result DocStore
    diskDocLookup docIds segment = do
      let blockLocations = L.nub $ M.mapMaybe (`resolveDocSparseIdx` segment) docIds
      mconcat <$> traverse (readDiskDoc name segment) blockLocations
    resolveInvertedIdxsForOneSegment :: [SearchTerm] -> Segment -> Result [FieldIndex]
    resolveInvertedIdxsForOneSegment tokens segment = do
      let offsets =
            resolveSparseIdx
              ( tokens
                  >>= (\SearchTerm{originalToken, alternatives} -> S.toList $ S.insert originalToken alternatives)
              )
              segment
      traverse (readDiskFieldIndex name segment) offsets
    mergeIdxs :: [FieldIndex] -> FieldIndex
    mergeIdxs = foldl' mergeFieldIndex Map.empty

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
    , memtable =
        Memtable
          { docStore = Map.empty
          , fieldIdx = Map.empty
          , fieldMeta = Map.empty
          , trigrams = Map.empty
          }
    , segments = []
    , maxDocId = DocId 0
    }

createInitialIdxData ::
  Mapping ->
  [(Header, FieldStats, Segment)] ->
  [Document] ->
  IndexData
createInitialIdxData validMapping segments docsFromLog =
  let
    fieldMeta = foldl' mergeFieldStats Map.empty ((\(_, m, _) -> m) <$> segments)
    -- todo just for now header.docCount as max id - wont hold for deletions
    maxDocIdFromSegments = sum $ (\(h, _, _) -> DocId $ fromIntegral h.docCount) <$> segments
    (updatedFieldIdx, updatedFieldMeta, updatedTrigrams) =
      foldl'
        (\(fieldIdx, meta, trigrams) nextDoc -> updateIndex fieldIdx meta trigrams nextDoc)
        (Map.empty, fieldMeta, Map.empty)
        docsFromAppendLog
    maxDocId = foldl' max maxDocIdFromSegments ((.docId) <$> docsFromLog)
   in
    IndexData
      { mapping = validMapping
      , memtable =
          Memtable
            { docStore = docsFromAppendLog
            , fieldIdx = updatedFieldIdx
            , fieldMeta = updatedFieldMeta
            , trigrams = updatedTrigrams
            }
      , -- todo needs file path as param to segement
        segments = (\(_, _, s) -> s) <$> segments
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
