module Kengine.Store.InMemory (Store (..), mkStore) where

import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Except (ExceptT (..))
import Data.Aeson qualified as AE
import Data.List qualified as L
import Data.Map qualified as Map
import Data.Maybe qualified as M
import Data.Set qualified as S
import GHC.Conc qualified as TVar
import Kengine.Engine (Token, parseDocument, tokenize)
import Kengine.Errors (IOE, SearchError (SearchError))
import Kengine.Mapping (validateMapping)
import Kengine.Types (
  DocId (DocId),
  Document (Document),
  FieldValue (TextVal),
  IndexName,
  IndexResponse (..),
  IndexResponseStatus (..),
  Mapping (..),
  Query (Query),
  SearchResults (..),
  Term (Term),
 )
import Refined (unrefine)
import Validation qualified as V (validationToEither)

data Store = Store
  { createIndex :: IndexName -> Mapping -> IOE SearchError IndexResponse
  , indexDoc :: IndexName -> AE.Value -> IOE SearchError IndexResponse
  , search :: IndexName -> Query -> IOE SearchError SearchResults
  }

type InvertedIndex = Map.Map Token (S.Set DocId)
type DocStore = Map.Map DocId Document
data IndexData = IndexData Mapping (TVar.TVar DocStore) (TVar.TVar InvertedIndex)
type IndexView = (Map.Map IndexName IndexData)

mkStore :: IO Store
mkStore = do
  indexViewVar :: TVar.TVar IndexView <- TVar.newTVarIO Map.empty
  pure
    Store
      { createIndex = createIndex indexViewVar
      , indexDoc = \name jval -> do
          idxView <- liftIO $ TVar.readTVarIO indexViewVar
          (IndexData mapping docStoreVar invertedIndexVar) <- lookupIndex name idxView
          (newId, doc) <- indexDoc mapping docStoreVar jval
          storeTokens invertedIndexVar newId doc
          pure IndexResponse{status = Indexed}
      , search = \name query -> do
          idxView <- liftIO $ TVar.readTVarIO indexViewVar
          (IndexData _ docStoreVar invertedIndexVar) <- lookupIndex name idxView
          invertedIndex <- liftIO $ TVar.readTVarIO invertedIndexVar
          docStore <- liftIO $ TVar.readTVarIO docStoreVar
          search docStore invertedIndex query
      }

lookupIndex :: IndexName -> IndexView -> IOE SearchError IndexData
lookupIndex name indexView =
  ExceptT . pure $
    maybe
      (Left $ SearchError ("No index found: " <> unrefine name))
      Right
      (Map.lookup name indexView)

search ::
  DocStore ->
  InvertedIndex ->
  Query ->
  IOE SearchError SearchResults
search docStore invertedIndex query = do
  let results = searchQ query invertedIndex docStore
  pure SearchResults{results}

-- TODO test
searchQ ::
  Query ->
  InvertedIndex ->
  DocStore ->
  [Document]
searchQ (Query query) invertedIndex docStore =
  let
    tokenizedQ = tokenize $ Term query
    docs = traverse (`Map.lookup` invertedIndex) tokenizedQ
    docsWithAllTkns = case docs of
      Nothing -> S.empty
      (Just []) -> S.empty
      (Just (fstDocIds : restDocIds)) -> foldl' S.intersection fstDocIds restDocIds
   in
    M.mapMaybe (`Map.lookup` docStore) (S.toList docsWithAllTkns)

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
      docStoreVar :: TVar.TVar DocStore <- TVar.newTVar Map.empty
      invertedIndexVar :: TVar.TVar InvertedIndex <- TVar.newTVar Map.empty
      pure $ IndexData validMapping docStoreVar invertedIndexVar

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
  TVar.TVar InvertedIndex ->
  DocId ->
  Document ->
  IOE SearchError ()
storeTokens invertedIndexVar docId doc = do
  liftIO $ TVar.atomically $ do
    invertedIndex <- TVar.readTVar invertedIndexVar
    let updatedIndex = updateIndex docId doc invertedIndex
    TVar.writeTVar invertedIndexVar updatedIndex

-- todo test
updateIndex ::
  DocId ->
  Document ->
  InvertedIndex ->
  InvertedIndex
updateIndex docId (Document docs) invertedIndex =
  let
    textFields =
      M.mapMaybe
        ( \case
            (_, TextVal txt) -> Just (Term txt)
            _ -> Nothing
        )
        (Map.toList docs)
    tokens = textFields >>= tokenize
   in
    L.foldl'
      (\invIndex nextTkn -> Map.insertWith S.union nextTkn (S.singleton docId) invIndex)
      invertedIndex
      tokens
