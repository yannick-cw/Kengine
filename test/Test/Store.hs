{-# LANGUAGE TemplateHaskell #-}

module Test.Store (spec) where

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
import Kengine.Persistence.FileStore (mkFileStore')
import Kengine.Store (Store (..), mkStore)
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

fields :: NEL.NonEmpty Field
fields =
  Field{sType = K.Text, fieldName = $$(R.refineTH "some_text"), required = True}
    NEL.:| [Field{sType = K.Text, fieldName = $$(R.refineTH "other_field"), required = True}]
mapping :: Mapping
mapping = Mapping{fields}
indexName :: IndexName
indexName :: IndexName = $$(R.refineTH "test-index")
doc1 :: AE.Value
doc1 =
  AE.object
    [ "some_text" AE..= ("these terms are in both" :: Text)
    , "other_field" AE..= ("apple" :: Text)
    ]
doc2 :: AE.Value
doc2 =
  AE.object
    [ "some_text" AE..= ("these terms are in both" :: Text)
    , "other_field" AE..= ("banana" :: Text)
    ]
doc3 :: AE.Value
doc3 =
  AE.object
    [ "some_text" AE..= ("this is new" :: Text)
    , "other_field" AE..= ("ok" :: Text)
    ]

spec :: Spec
spec = do
  describe "Store for search" $ do
    it "works e2e" $ withSystemTempDirectory "kengine-test" $ \dir -> do
      let fileStore = mkFileStore' dir
      runIOE $ do
        Store{createIndex, indexDoc, search, flushState} <- mkStore fileStore
        _ <- createIndex indexName mapping
        for_ [doc1, doc2] (indexDoc indexName)
        searchRes <- search indexName (Query "these terms are in both")
        bananaDoc <- search indexName (Query "banana")
        -- search terms match AND per field - cross field gives no match
        nonMatch <- search indexName (Query "banana term")
        flushState
        indexDoc indexName doc3
        searchResAfterFlush <- search indexName (Query "these terms are in both")
        bananaDocAfterFlush <- search indexName (Query "banana")
        doc3Match <- search indexName (Query "this is new")
        liftIO $ length searchRes.results `shouldBe` 2
        liftIO $ length bananaDoc.results `shouldBe` 1
        liftIO $ length nonMatch.results `shouldBe` 0
        -- flush shouldn't change results for the pre-flush docs
        liftIO $ srToDoc searchRes `shouldBe` srToDoc searchResAfterFlush
        liftIO $ srToDoc bananaDoc `shouldBe` srToDoc bananaDocAfterFlush
        liftIO $ length doc3Match.results `shouldBe` 1

    it "finds indexed docs from before and after flushing (single store, live swap)" $
      withSystemTempDirectory "kengine-test" $ \dir -> do
        let fileStore = mkFileStore' dir
        runIOE $ do
          store <- mkStore fileStore
          _ <- store.createIndex indexName mapping
          store.indexDoc indexName doc1
          firstDocBeforeFlush <- store.search indexName (Query "these terms are in both")
          store.flushState
          store.indexDoc indexName doc2
          bothDocsAfterFlush <- store.search indexName (Query "these terms are in both")
          store.flushState
          bothDocsAfterSndFlush <- store.search indexName (Query "these terms are in both")
          liftIO $ length firstDocBeforeFlush.results `shouldBe` 1
          liftIO $ length bothDocsAfterFlush.results `shouldBe` 2
          liftIO $ length bothDocsAfterSndFlush.results `shouldBe` 2

    it "recovers indexed docs across restarts" $
      withSystemTempDirectory "kengine-test" $ \dir -> do
        let fileStore = mkFileStore' dir
        runIOE $ do
          store1 <- mkStore fileStore
          _ <- store1.createIndex indexName mapping
          store1.indexDoc indexName doc1
          store1.flushState
          store2 <- mkStore fileStore
          store2.indexDoc indexName doc2
          store2.flushState
          store3 <- mkStore fileStore
          bothDocs <- store3.search indexName (Query "these terms are in both")
          liftIO $ length bothDocs.results `shouldBe` 2

    it "only find the doc if it includes the full query (AND query tokens)" $ do
      hedgehog $ do
        idxName <- forAll genValidIndexName
        let fieldName :: FieldName = $$(R.refineTH "some_text")
        m <-
          forAll $
            (\m -> m{fields = Field{sType = K.Text, fieldName, required = True} NEL.<| m.fields})
              <$> genValidMapping
        fns : rest <-
          forAll $ Gen.list (Range.linear 1 100) (genDocForMapping m)
        let query = "test query"
        let notMatchingQuery = Query "test query extraTkn"
        let fstDocWithQuery = Map.adjust (\_ -> TextVal query) fieldName fns
        let allDocs = toJSON . fmap fieldValueToJSON <$> (fstDocWithQuery : rest)
        shuffled <- forAll $ Gen.shuffle allDocs
        splitIdx <- forAll $ Gen.int (Range.linear 0 (length shuffled))
        let (preFlush, postFlush) = splitAt splitIdx shuffled
        annotateShow allDocs
        (searchRes, noMatch) <-
          evalEither
            =<< liftIO
              ( withSystemTempDirectory "kengine-test" $ \dir ->
                  runExceptT $ do
                    Store{createIndex, indexDoc, search, flushState} <- mkStore (mkFileStore' dir)
                    _ <- createIndex idxName m
                    for_ preFlush (indexDoc idxName)
                    flushState
                    for_ postFlush (indexDoc idxName)
                    searchRes <- search idxName (Query query)
                    noMatch <- search idxName notMatchingQuery
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
srToDoc SearchResults{results} = (\SearchResult{doc} -> doc) <$> results
