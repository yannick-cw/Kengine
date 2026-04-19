module Kengine.Types (
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
  success,
  throwRefineOtherException,
 )
import Web.Scotty (Parsable (parseParam))

-- data types

-- how often a token appears in a document
newtype TermFrequency = TF Int deriving newtype (Num)
type InvertedIndex = Map.Map Token (Map.Map DocId TermFrequency)
type FieldIndex = Map.Map FieldName InvertedIndex
type DocStore = Map.Map DocId Document
type IndexView = (Map.Map IndexName IndexData)
type FieldMetadata = Map.Map FieldName (Map.Map DocId MetaData)
newtype MetaData = MetaData {totalTokens :: Int} deriving newtype (Eq, Num, Show)

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

newtype DocId = DocId Int deriving newtype (Eq, Ord)
newtype Term = Term Text

newtype Token = Token Text deriving newtype (Show, Eq, Ord)

newtype Document = Document (Map.Map FieldName FieldValue)
  deriving stock (Show, Eq, Generic)
instance ToJSON Document
data FieldValue = TextVal Text | KeywordVal Text | BoolVal Bool | NumberVal Double
  deriving stock (Show, Eq, Generic)
instance ToJSON FieldValue where
  toJSON (TextVal txt) = toJSON txt
  toJSON (KeywordVal txt) = toJSON txt
  toJSON (BoolVal b) = toJSON b
  toJSON (NumberVal n) = toJSON n

newtype Score = Score Float
  deriving newtype (Show, Eq, Num)
  deriving stock (Generic)
instance ToJSON Score where
  toJSON (Score s) = toJSON s
data SearchResult = SearchResult Document Score
  deriving stock (Show, Eq, Generic)
instance ToJSON SearchResult
instance Ord SearchResult where
  (<=) (SearchResult _ (Score s1)) (SearchResult _ (Score s2)) = s1 <= s2

type FieldDocResult = Map.Map DocId (FieldName, Score)

fromDoc :: Score -> Document -> SearchResult
fromDoc score doc = SearchResult doc score
