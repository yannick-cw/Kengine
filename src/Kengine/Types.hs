module Kengine.Types (
  BM25 (..),
  Mapping (..),
  FieldMetadata,
  MetaData (..),
  Field (..),
  SearchType (..),
  FieldDocResult,
  FieldIndex,
  SearchResult (..),
  IndexName,
  Query (..),
  IndexResponse (..),
  IndexResponseStatus (..),
  SearchResults (..),
  ValidName,
  Term (..),
  DocId (..),
  Document (..),
  fromDoc,
  Score (..),
  FieldValue (..),
  FieldName,
  InvertedIndex,
  DocStore,
  IndexData (..),
  IndexView,
  Token (..),
  TermFrequency (..),
) where

import Data.Aeson (FromJSON (..), ToJSON (..), (.!=), (.:), (.:?))
import Data.Aeson qualified as AE
import Data.Bifunctor (first)
import Data.Binary (Binary (..))
import Data.Char (isAlphaNum)
import Data.List.NonEmpty qualified as L
import Data.Map qualified as Map
import Data.Text (Text)
import Data.Text qualified as T (all, null)
import Data.Text.Lazy qualified as LT
import Data.Typeable (typeRep)
import GHC.Conc qualified as TVar
import GHC.Generics (Generic)
import Refined (
  Predicate (validate),
  Refined,
  displayRefineException,
  refine,
  refineFail,
  success,
  throwRefineOtherException,
  unrefine,
 )
import Web.Scotty (Parsable (parseParam))

-- data types

newtype BM25 = BM25 Float deriving newtype (Num, Eq, Show)

-- how often a token appears in a document
newtype TermFrequency = TF Int
  deriving newtype (Num, Show)
  deriving stock (Generic)
instance Binary TermFrequency
type InvertedIndex = Map.Map Token (Map.Map DocId TermFrequency)
type FieldIndex = Map.Map FieldName InvertedIndex
type DocStore = Map.Map DocId Document
type IndexView = (Map.Map IndexName IndexData)
type FieldMetadata = Map.Map FieldName (Map.Map DocId MetaData)
newtype MetaData = MetaData {totalTokens :: Int}
  deriving newtype (Eq, Num, Show)
  deriving stock (Generic)
instance Binary MetaData

data IndexData
  = IndexData Mapping (TVar.TVar DocStore) (TVar.TVar FieldIndex) (TVar.TVar FieldMetadata)

-- Creation Types
newtype Mapping = Mapping {fields :: L.NonEmpty Field} deriving stock (Eq, Show, Generic)
instance FromJSON Mapping
instance ToJSON Mapping

data Field = Field {sType :: SearchType, fieldName :: FieldName, required :: Bool}
  deriving stock (Eq, Show, Generic)
instance FromJSON Field where
  parseJSON =
    AE.withObject
      "Field"
      ( \ob ->
          Field <$> ob .: "sType" <*> ob .: "fieldName" <*> ob .:? "required" .!= True
      )
instance ToJSON Field

data SearchType = Text | Keyword | Bool | Number deriving stock (Eq, Show, Generic)
instance FromJSON SearchType
instance ToJSON SearchType

newtype Query = Query Text
instance Parsable Query where
  parseParam = Right . Query . LT.toStrict

type FieldName = Refined ValidName Text
type IndexName = Refined ValidName Text
instance Parsable IndexName where
  parseParam = first (LT.pack . displayRefineException) . refine . LT.toStrict
instance Binary FieldName where
  put = put . unrefine
  get = refineFail =<< get

data ValidName
instance Predicate ValidName Text where
  validate r value
    | T.all (\c -> isAlphaNum c || c == '-' || c == '_') value && not (T.null value) = success
    | otherwise = throwRefineOtherException (typeRep r) "Allowed: [a-zA-Z0-9_-]"

--- Respone Types

data IndexResponseStatus = Created | Indexed deriving stock (Eq, Show)
instance ToJSON IndexResponseStatus where
  toJSON Created = "created"
  toJSON Indexed = "indexed"
newtype IndexResponse = IndexResponse {status :: IndexResponseStatus}
  deriving stock (Generic, Eq, Show)
instance ToJSON IndexResponse

newtype SearchResults = SearchResults {results :: [SearchResult]}
  deriving stock (Generic, Eq, Show)
instance ToJSON SearchResults

-- Document Types

newtype DocId = DocId Int
  deriving newtype (Eq, Ord, Num)
  deriving stock (Show, Generic)
instance Binary DocId
instance ToJSON DocId where
  toJSON (DocId i) = toJSON i
instance FromJSON DocId where
  parseJSON v = DocId <$> parseJSON v
newtype Term = Term Text

newtype Token = Token Text
  deriving newtype (Show, Eq, Ord)
  deriving stock (Generic)
instance Binary Token

data Document = Document {docId :: DocId, body :: Map.Map FieldName FieldValue}
  deriving stock (Show, Eq, Generic)
instance Binary Document
instance ToJSON Document
instance FromJSON Document

data FieldValue = TextVal Text | KeywordVal Text | BoolVal Bool | NumberVal Double
  deriving stock (Show, Eq, Generic)
instance FromJSON FieldValue
instance ToJSON FieldValue
instance Binary FieldValue

newtype Score = Score Float
  deriving newtype (Show, Eq, Num)
  deriving stock (Generic)
instance ToJSON Score where
  toJSON (Score s) = toJSON s
data SearchResult = SearchResult Document Score
  deriving stock (Show, Eq, Generic)
instance ToJSON SearchResult where
  toJSON (SearchResult Document{docId, body} score) =
    AE.object
      [ "id" AE..= docId
      , "score" AE..= score
      , "doc" AE..= Map.map unwrapFieldValue body
      ]
    where
      unwrapFieldValue :: FieldValue -> AE.Value
      unwrapFieldValue (TextVal txt) = toJSON txt
      unwrapFieldValue (KeywordVal txt) = toJSON txt
      unwrapFieldValue (BoolVal b) = toJSON b
      unwrapFieldValue (NumberVal n) = toJSON n

instance Ord SearchResult where
  (<=) (SearchResult _ (Score s1)) (SearchResult _ (Score s2)) = s1 <= s2

type FieldDocResult = Map.Map DocId (FieldName, Score)

fromDoc :: Score -> Document -> SearchResult
fromDoc score doc = SearchResult doc score
