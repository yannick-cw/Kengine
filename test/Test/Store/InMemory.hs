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
import Hedgehog (annotateShow, diff, forAll)
import Hedgehog.Gen qualified as Gen
import Hedgehog.Range qualified as Range
import Kengine.Errors (IOE)
import Kengine.Store.InMemory (Store (..), mkStore)
import Kengine.Types (
  Document (..),
  Field (..),
  FieldName,
  FieldValue (..),
  IndexName,
  Mapping (..),
  Query (..),
  SearchResults (..),
 )
import Kengine.Types qualified as K
import Refined qualified as R
import Test.Helpers.Generators (genDocForMapping, genValidIndexName, genValidMapping)
import Test.Hspec (Spec, describe, it, shouldBe)
import Test.Hspec.Hedgehog (hedgehog)

spec :: Spec
spec = do
  describe "Store for search" $ do
    it "works e2e" $ do
      store@Store{createIndex, indexDoc, search} <- mkStore
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
      (searchRes, bananaRes, nonMatch) <- runIOE $ do
        _ <- createIndex indexName mapping
        for_ [doc1, doc2] (indexDoc indexName)
        searchRes <- search indexName (Query "these terms are in both")
        bananaDoc <- search indexName (Query "banana")
        -- search terms match AND per field - cross field gives no match
        nonMatch <- search indexName (Query "banana term")
        pure (searchRes, bananaDoc, nonMatch)

      length searchRes.results `shouldBe` 2
      length bananaRes.results `shouldBe` 1
      length nonMatch.results `shouldBe` 0

    it "only find the doc if it includes the full query (AND query tokens)" $ do
      hedgehog $ do
        indexName <- forAll genValidIndexName
        let fieldName :: FieldName = $$(R.refineTH "some_text")
        mapping <-
          forAll $
            (\m -> m{fields = Field{sType = K.Text, fieldName, required = True} NEL.<| m.fields})
              <$> genValidMapping
        (Document fields) : rest <-
          forAll $ Gen.list (Range.linear 1 100) (genDocForMapping mapping)
        let query = "test query"
        let notMatchingQuery = Query "test query extraTkn"
        let fstDocWithQuery = Map.adjust (\_ -> TextVal query) fieldName fields
        let allDocs = toJSON <$> Document fstDocWithQuery : rest
        annotateShow allDocs
        (searchRes, noMatch) <- liftIO $ runIOE $ do
          Store{createIndex, indexDoc, search} <- liftIO mkStore
          _ <- createIndex indexName mapping
          for_ allDocs (indexDoc indexName)
          searchRes <- search indexName (Query query)
          noMatch <- search indexName notMatchingQuery
          pure (searchRes, noMatch)

        diff (length searchRes.results) (==) 1
        diff (length noMatch.results) (==) 0

runIOE :: (Show e) => IOE e a -> IO a
runIOE m = runExceptT m >>= either (fail . show) pure
