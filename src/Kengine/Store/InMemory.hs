module Kengine.Store.InMemory (Store (..), mkStore) where

import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Except (ExceptT (..))
import Data.Aeson qualified as AE
import Data.List qualified as L
import Data.Map qualified as Map
import Data.Maybe qualified as M
import GHC.Conc qualified as TVar
import Kengine.Engine (parseDocument, searchQ, tokenize)
import Kengine.Errors (IOE, SearchError (SearchError))
import Kengine.Mapping (validateMapping)
import Kengine.Types (
  DocId (DocId),
  DocStore,
  Document (Document),
  FieldValue (TextVal),
  IndexData (..),
  IndexName,
  IndexResponse (..),
  IndexResponseStatus (..),
  IndexView,
  InvertedIndex,
  Mapping (..),
  Query,
  SearchResults (..),
  Term (Term),
  TermFrequency (..),
  Token,
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
          (IndexData mapping docStoreVar invertedIndexVar) <- lookupIndex name idxView
          (newId, doc) <- indexDoc mapping docStoreVar jval
          storeTokens invertedIndexVar newId doc
          pure IndexResponse{status = Indexed}
      , search = \name query -> do
          idxView <- liftIO $ TVar.readTVarIO indexViewVar
          (IndexData _ docStoreVar invertedIndexVar) <- lookupIndex name idxView
          invertedIndex <- liftIO $ TVar.readTVarIO invertedIndexVar
          docStore <- liftIO $ TVar.readTVarIO docStoreVar
          pure $ SearchResults (searchQ query invertedIndex docStore)
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
    L.foldl' updateInvertedIndex invertedIndex tokens
  where
    -- creates a new map if that token appears first - otherwise updates the existing inner map
    updateInvertedIndex :: InvertedIndex -> Token -> InvertedIndex
    updateInvertedIndex invertedIdx tkn =
      Map.alter
        (maybe (Just $ Map.singleton docId (TF 1)) (Just . updateTermFrequency))
        tkn
        invertedIdx
    -- creates a new map if docId for that term appears first - otherwise adds 1
    updateTermFrequency :: Map.Map DocId TermFrequency -> Map.Map DocId TermFrequency
    updateTermFrequency = Map.alter (maybe (Just $ TF 1) (\tf -> Just (tf + 1))) docId
