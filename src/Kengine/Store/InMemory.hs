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
          (IndexData mapping docStoreVar invertedIndexVar) <- lookupIndex name idxView
          (newId, doc) <- indexDoc mapping docStoreVar jval
          storeTokens invertedIndexVar newId doc
          pure IndexResponse{status = Indexed}
      , search = \name query -> do
          idxView <- liftIO $ TVar.readTVarIO indexViewVar
          (IndexData _ docStoreVar fieldIndexVar) <- lookupIndex name idxView
          fieldIndex <- liftIO $ TVar.readTVarIO fieldIndexVar
          docStore <- liftIO $ TVar.readTVarIO docStoreVar
          pure $ SearchResults{results = searchQ query docStore fieldIndex}
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
      fieldIndexVar :: TVar.TVar FieldIndex <- TVar.newTVar Map.empty
      pure $ IndexData validMapping docStoreVar fieldIndexVar

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
  DocId ->
  Document ->
  IOE SearchError ()
storeTokens fieldIndexVar docId doc = do
  liftIO $ TVar.atomically $ do
    fieldIndex <- TVar.readTVar fieldIndexVar
    let updatedIndex = updateIndex docId doc fieldIndex
    TVar.writeTVar fieldIndexVar updatedIndex

updateIndex ::
  DocId ->
  Document ->
  FieldIndex ->
  FieldIndex
updateIndex docId (Document docs) fieldIndex =
  let
    tokenizedFields =
      Map.mapMaybe
        ( \case
            (TextVal txt) -> Just (tokenize $ Term txt)
            _ -> Nothing
        )
        docs
    tknsWithCounts = fmap (,TF 1) <$> tokenizedFields
    newFieldIndx =
      Map.fromListWith (Map.unionWith (+)) . fmap (second (Map.singleton docId))
        <$> tknsWithCounts
   in
    Map.unionWith
      (Map.unionWith (Map.unionWith (+)))
      newFieldIndx
      fieldIndex
