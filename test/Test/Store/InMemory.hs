{-# LANGUAGE TemplateHaskell #-}

module Test.Store.InMemory (spec) where

import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Except (runExceptT)
import Data.Aeson (ToJSON (toJSON))
import Data.Aeson qualified as AE
import Data.Foldable (for_)
import Data.List.NonEmpty qualified as NEL
import Data.Map qualified as Map
import Data.Text (Text)
import Hedgehog (annotateShow, diff, evalEither, forAll)
import Hedgehog.Gen qualified as Gen
import Hedgehog.Range qualified as Range
import Kengine.Errors (IOE)
import Kengine.Store.InMemory (Store (..), mkStore)
import Kengine.Store.Persistence (FileStore (..), mkFileStore')
import Kengine.Types (
  Document,
  Field (..),
  FieldName,
  FieldValue (..),
  IndexName,
  Mapping (..),
  Query (..),
  SearchResult (..),
  SearchResults (..),
 )
import Kengine.Types qualified as K
import Refined qualified as R
import System.IO.Temp (withSystemTempDirectory)
import Test.Helpers.Generators (genDocForMapping, genValidIndexName, genValidMapping)
import Test.Hspec (Spec, describe, it, shouldBe)
import Test.Hspec.Hedgehog (hedgehog)

emptyFileStore :: FileStore
emptyFileStore =
  FileStore
    { storeMapping = \_ _ -> pure ()
    , readMapping = \_ -> pure Nothing
    , readIdxs = pure []
    , storeDoc = \_ _ -> pure ()
    , readDocs = \_ -> pure []
    , unsafeFlushState = \_ _ -> pure ()
    , readSnapshot = \_ -> undefined
    }

spec :: Spec
spec = do
  describe "Store for search" $ do
    it "works e2e" $ withSystemTempDirectory "kengine-test" $ \dir -> do
      let store = mkFileStore' dir
      let fields =
            Field{sType = K.Text, fieldName = $$(R.refineTH "some_text"), required = True}
              NEL.:| [Field{sType = K.Text, fieldName = $$(R.refineTH "other_field"), required = True}]
      let mapping = Mapping{fields}
      let indexName :: IndexName = $$(R.refineTH "test-index")
      let doc1 =
            AE.object
              [ "some_text" AE..= ("these terms are in both" :: Text)
              , "other_field" AE..= ("apple" :: Text)
              ]
      let doc2 =
            AE.object
              [ "some_text" AE..= ("these terms are in both" :: Text)
              , "other_field" AE..= ("banana" :: Text)
              ]
      let doc3 =
            AE.object
              [ "some_text" AE..= ("this is new" :: Text)
              , "other_field" AE..= ("ok" :: Text)
              ]
      runIOE $ do
        Store{createIndex, indexDoc, search} <- mkStore store
        _ <- createIndex indexName mapping
        for_ [doc1, doc2] (indexDoc indexName)
        searchRes <- search indexName (Query "these terms are in both")
        bananaDoc <- search indexName (Query "banana")
        -- search terms match AND per field - cross field gives no match
        nonMatch <- search indexName (Query "banana term")
        Store{search = fresSearch, flushState} <- mkStore store
        searchResAfterRestart <- fresSearch indexName (Query "these terms are in both")
        bananaDocAfterRestart <- fresSearch indexName (Query "banana")
        flushState
        indexDoc indexName doc3
        Store{search = afterCompactSearch} <- mkStore store
        searchResAfterFlush <- afterCompactSearch indexName (Query "these terms are in both")
        bananaDocAfterFlush <- afterCompactSearch indexName (Query "banana")
        doc3Match <- afterCompactSearch indexName (Query "this is new")
        liftIO $ length searchRes.results `shouldBe` 2
        liftIO $ length bananaDoc.results `shouldBe` 1
        liftIO $ length nonMatch.results `shouldBe` 0
        liftIO $ searchRes `shouldBe` searchResAfterRestart
        liftIO $ bananaDoc `shouldBe` bananaDocAfterRestart
        -- only compare docs, store is different with third doc in the mix
        liftIO $ srToDoc searchRes `shouldBe` srToDoc searchResAfterFlush
        liftIO $ srToDoc bananaDoc `shouldBe` srToDoc bananaDocAfterFlush
        liftIO $ length doc3Match.results `shouldBe` 1

    it "only find the doc if it includes the full query (AND query tokens)" $ do
      hedgehog $ do
        indexName <- forAll genValidIndexName
        let fieldName :: FieldName = $$(R.refineTH "some_text")
        mapping <-
          forAll $
            (\m -> m{fields = Field{sType = K.Text, fieldName, required = True} NEL.<| m.fields})
              <$> genValidMapping
        fields : rest <-
          forAll $ Gen.list (Range.linear 1 100) (genDocForMapping mapping)
        let query = "test query"
        let notMatchingQuery = Query "test query extraTkn"
        let fstDocWithQuery = Map.adjust (\_ -> TextVal query) fieldName fields
        let allDocs = toJSON . fmap fieldValueToJSON <$> (fstDocWithQuery : rest)
        annotateShow allDocs
        (searchRes, noMatch) <-
          evalEither
            =<< liftIO
              ( runExceptT $ do
                  Store{createIndex, indexDoc, search} <- mkStore emptyFileStore
                  _ <- createIndex indexName mapping
                  for_ allDocs (indexDoc indexName)
                  searchRes <- search indexName (Query query)
                  noMatch <- search indexName notMatchingQuery
                  pure (searchRes, noMatch)
              )

        diff (length searchRes.results) (==) 1
        diff (length noMatch.results) (==) 0

runIOE :: (Show e) => IOE e a -> IO a
runIOE m = runExceptT m >>= either (fail . show) pure

fieldValueToJSON :: FieldValue -> AE.Value
fieldValueToJSON (TextVal txt) = toJSON txt
fieldValueToJSON (KeywordVal txt) = toJSON txt
fieldValueToJSON (BoolVal b) = toJSON b
fieldValueToJSON (NumberVal n) = toJSON n

srToDoc :: SearchResults -> [Document]
srToDoc sr = (\(SearchResult d _) -> d) <$> sr.results
