module Kengine.Engine (parseDocument, tokenize, Token (..), searchQ) where

import Data.Aeson ((.:), (.:?))
import Data.Aeson qualified as AE
import Data.Aeson.Key qualified as AE.Types.Key
import Data.Aeson.Types qualified as AE.Types
import Data.Bifunctor (first)
import Data.Char qualified as C
import Data.List qualified as L
import Data.List.NonEmpty (toList)
import Data.List.NonEmpty qualified as NEL
import Data.Map qualified as Map
import Data.Maybe qualified as M
import Data.Ord (Down (Down))
import Data.Text (Text)
import Data.Text qualified as T
import Kengine.Errors (SearchError (SearchError))
import Kengine.Types (
  DocId,
  DocStore,
  Document (Document),
  Field (..),
  FieldDocResult,
  FieldIndex,
  FieldName,
  FieldValue (BoolVal, KeywordVal, NumberVal, TextVal),
  InvertedIndex,
  Mapping (..),
  Query (..),
  Score (Score),
  SearchResult,
  Term (..),
  TermFrequency (TF),
  Token (Token),
  fromDoc,
 )
import Kengine.Types qualified as KT
import Refined (unrefine)

-- "Bio-Milch 3,5%" becomes ["bio", "milch", "3", "5"]
tokenize :: Term -> [Token]
tokenize = fmap (Token . T.toLower) . filter (not . T.null) . splitNonAlpha

splitNonAlpha :: Term -> [Text]
splitNonAlpha (Term t) = T.split (\tkn -> not (C.isAscii tkn && C.isAlphaNum tkn)) t

parseDocument :: AE.Value -> Mapping -> Either SearchError Document
parseDocument jVal Mapping{fields} =
  let
    document = AE.withObject "Document" (docParser fields)
   in
    first (SearchError . T.pack) (AE.Types.parseEither document jVal)

-- todo simplify
docParser :: NEL.NonEmpty Field -> AE.Object -> AE.Types.Parser Document
docParser fields obj =
  let
    parsedFieldVals =
      traverse
        ( \((Field{sType, fieldName, required}) :: Field) ->
            let key = AE.Types.Key.fromText (unrefine fieldName)
             in if required
                  then
                    Just . (fieldName,) <$> case sType of
                      KT.Text -> TextVal <$> obj .: key
                      KT.Keyword -> KeywordVal <$> obj .: key
                      KT.Bool -> BoolVal <$> obj .: key
                      KT.Number -> NumberVal <$> obj .: key
                  else
                    fmap (fieldName,) <$> case sType of
                      KT.Text -> fmap TextVal <$> obj .:? key
                      KT.Keyword -> fmap KeywordVal <$> obj .:? key
                      KT.Bool -> fmap BoolVal <$> obj .:? key
                      KT.Number -> fmap NumberVal <$> obj .:? key
        )
        fields
   in
    Document . Map.fromList . M.catMaybes . toList <$> parsedFieldVals

-- TF(t, d) = number of times term t appears in document d
-- IDF(t)   = log(N / df(t))
-- where N = total document count, df(t) = number of documents containing term t
-- here: tfidf d1 = 10 * log(20/2); d2 = 5 * log(20/2);
searchQ ::
  Query ->
  DocStore ->
  FieldIndex ->
  [SearchResult]
searchQ (Query query) docStore fieldIndex =
  let
    perFieldResults = Map.elems $ Map.mapWithKey searchOneField fieldIndex
    perDocScore = Map.fromListWith (+) (Map.toList =<< perFieldResults)
   in
    L.sortOn Down $ Map.elems $ Map.intersectionWith fromDoc perDocScore docStore
  where
    searchOneField :: FieldName -> InvertedIndex -> Map.Map DocId Score
    searchOneField fName invertedIndex =
      let
        tokenizedQ = tokenize $ Term query
        totalCount = Map.size $ Map.filter (\(Document doc) -> Map.member fName doc) docStore
        docs = traverse (\tkn -> calcIdf totalCount <$> Map.lookup tkn invertedIndex) tokenizedQ
        docsWithAllTkns = case docs of
          (Just (fstDocIds : restDocIds)) -> foldl' (Map.intersectionWith (+)) fstDocIds restDocIds
          _ -> Map.empty
       in
        Score <$> docsWithAllTkns
    calcIdf :: Int -> Map.Map DocId TermFrequency -> Map.Map DocId Float
    calcIdf totalCount tfMap =
      let
        numDocsForTkn = fromIntegral $ Map.size tfMap
       in
        Map.map
          (\(TF tf) -> fromIntegral tf * log (fromIntegral totalCount / numDocsForTkn))
          tfMap
