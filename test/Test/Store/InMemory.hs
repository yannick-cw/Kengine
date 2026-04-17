{-# LANGUAGE TemplateHaskell #-}

module Test.Store.InMemory (spec) where

import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Except (runExceptT)
import Data.Aeson qualified as AE
import Data.List.NonEmpty (NonEmpty (..))
import Data.Text (Text)
import Kengine.Errors (IOE, SearchError)
import Kengine.Store.InMemory (Store (..), mkStore)
import Kengine.Types (
  Field (..),
  IndexName,
  IndexResponse (..),
  Mapping (..),
  Query (..),
  SearchResults (..),
 )
import Kengine.Types qualified as K
import Refined qualified as R
import Test.Hspec (Spec, describe, expectationFailure, it, shouldBe)

spec :: Spec
spec = do
  describe "Store for search" $ do
    it "works e2e" $ do
      res <- runExceptT run
      case res of
        Left err -> expectationFailure (show err)
        Right () -> pure ()

run :: IOE SearchError ()
run = do
  Store{createIndex, indexDoc, search} <- liftIO mkStore
  let fields =
        Field{sType = K.Text, fieldName = $$(R.refineTH "some_text")}
          :| [Field{sType = K.Text, fieldName = $$(R.refineTH "other_field")}]
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
  indexRes <- createIndex indexName mapping
  insertDoc <- indexDoc indexName doc1 *> indexDoc indexName doc2
  searchRes <- search indexName (Query "these terms are in both")
  bananaDoc <- search indexName (Query "banana")
  liftIO $ do
    indexRes `shouldBe` (IndexResponse{status = K.Created})
    insertDoc `shouldBe` (IndexResponse{status = K.Indexed})
    length searchRes.results `shouldBe` 2
    length bananaDoc.results `shouldBe` 1
