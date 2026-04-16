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
import Kengine.Errors (IOE, IndexError (IndexError), SearchError)
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
  { createIndex :: IndexName -> Mapping -> IOE IndexError IndexResponse
  , indexDoc :: IndexName -> AE.Value -> IOE IndexError IndexResponse
  , search :: IndexName -> Query -> IOE SearchError SearchResults
  }

-- TODO right now nothing is index specific!!

type InvertedIndex = Map.Map Token (S.Set DocId)
type DocStore = Map.Map DocId Document
type Mappings = (Map.Map IndexName Mapping)

mkStore :: IO Store
mkStore = do
  mappingVar :: TVar.TVar Mappings <- TVar.newTVarIO Map.empty
  docStoreVar :: TVar.TVar DocStore <- TVar.newTVarIO Map.empty
  invertedIndexVar :: TVar.TVar InvertedIndex <- TVar.newTVarIO Map.empty
  pure
    Store
      { createIndex = createIndex mappingVar
      , indexDoc = \name jval -> do
          (newId, doc) <- indexDoc mappingVar docStoreVar name jval
          storeTokens invertedIndexVar newId doc
          pure IndexResponse{status = Indexed}
      , search = search docStoreVar invertedIndexVar
      }

-- TODO everything needs to be index specific
search ::
  TVar.TVar DocStore ->
  TVar.TVar InvertedIndex ->
  IndexName ->
  Query ->
  IOE SearchError SearchResults
search docStoreVar invertedIndexVar _ query = do
  invertedIndex <- liftIO $ TVar.readTVarIO invertedIndexVar
  docStore <- liftIO $ TVar.readTVarIO docStoreVar
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
  TVar.TVar (Map.Map IndexName Mapping) ->
  IndexName ->
  Mapping ->
  IOE IndexError IndexResponse
createIndex mappingVar name mapping = do
  _ <- ExceptT $ TVar.atomically $ do
    existingMappings <- TVar.readTVar mappingVar
    let validMapping =
          V.validationToEither $ validateMapping name (Map.keys existingMappings) mapping
    traverse
      (\m -> TVar.writeTVar mappingVar (Map.insert name m existingMappings))
      validMapping
  pure IndexResponse{status = Created}

indexDoc ::
  TVar.TVar (Map.Map IndexName Mapping) ->
  TVar.TVar (Map.Map DocId Document) ->
  IndexName ->
  AE.Value ->
  IOE IndexError (DocId, Document)
indexDoc mappingVar docStoreVar name doc = do
  mappingStore <- liftIO $ TVar.readTVarIO mappingVar
  mapping <- lookupMappig mappingStore
  docToIndex <- ExceptT . pure $ parseDocument doc mapping
  liftIO $ TVar.atomically $ do
    docStore <- TVar.readTVar docStoreVar
    let nextDocId = maybe (DocId 1) (\(_, DocId dId) -> DocId (dId + 1)) (L.unsnoc (Map.keys docStore))
    TVar.writeTVar docStoreVar (Map.insert nextDocId docToIndex docStore)
    pure (nextDocId, docToIndex)
  where
    lookupMappig mappingStore =
      ExceptT . pure $
        maybe
          (Left $ IndexError ("No index found: " <> unrefine name))
          Right
          (Map.lookup name mappingStore)

storeTokens ::
  TVar.TVar InvertedIndex ->
  DocId ->
  Document ->
  IOE IndexError ()
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
