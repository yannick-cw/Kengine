module Test.Helpers.Generators (
  genValidMappingRequiredField,
  genValidMapping,
  genValidField,
  genDocsForMapping,
  genValidFieldName,
  genValidIndexName,
  genDocForMapping,
  validNameChar,
  genValidMappingJson,
  genValidFieldJson,
  genValidName,
  genValidType,
  genInvalidNameJson,
  genInvalidSTypeJson,
  genText,
  genNonAlphaText,
  genTextAlphaNum,
  genTokenizableText,
) where

import Data.Aeson (Value, object, (.=))
import Data.Char (isAlphaNum)
import Data.List.NonEmpty qualified as NE
import Data.Map qualified as Map
import Data.Maybe qualified as M
import Data.Text (Text)
import Data.Text qualified as T
import Hedgehog (Gen)
import Hedgehog.Gen qualified as Gen
import Hedgehog.Range qualified as Range
import Kengine.Types (
  DocId (DocId),
  Document (..),
  Field (..),
  FieldName,
  FieldValue (..),
  IndexName,
  Mapping (..),
  SearchType (..),
  ValidName,
 )
import Refined (Refined, refine)

-- type generators

genValidMappingRequiredField :: Bool -> Gen Mapping
genValidMappingRequiredField allRequired = do
  fields <- Gen.nonEmpty (Range.linear 1 10) (genValidField allRequired)
  pure Mapping{fields}

genValidMapping :: Gen Mapping
genValidMapping = genValidMappingRequiredField False

genValidField :: Bool -> Gen Field
genValidField allRequired = do
  fieldName <- genValidFieldName
  sType <- Gen.element [Text, Keyword, Bool, Number]
  required <- if allRequired then Gen.constant True else Gen.bool
  pure Field{sType, fieldName, required}

genValidFieldName :: Gen (Refined ValidName Text)
genValidFieldName = do
  t <- Gen.text (Range.linear 1 30) validNameChar
  case refine t of
    Right name -> pure name
    Left err -> fail (show err)

genValidIndexName :: Gen IndexName
genValidIndexName = genValidFieldName

validNameChar :: Gen Char
validNameChar = Gen.element $ ['a' .. 'z'] <> ['A' .. 'Z'] <> ['0' .. '9'] <> ['_', '-']

-- Raw generators

genText :: Gen Text
genText = Gen.text (Range.linear 0 100) Gen.unicode

genNonAlphaNum :: Gen Char
genNonAlphaNum = Gen.filter (not . isAlphaNum) Gen.unicode

genTextAlphaNum :: Gen Text
genTextAlphaNum = Gen.text (Range.linear 1 10) Gen.alphaNum

genValidMappingJson :: Gen Value -> Gen Value
genValidMappingJson fieldGen = do
  fields <- Gen.list (Range.linear 1 10) fieldGen
  pure $ object ["fields" .= fields]

genValidName :: Gen Text
genValidName = Gen.text (Range.linear 1 30) validNameChar

genValidType :: Gen Text
genValidType = Gen.element ["Text" :: Text, "Keyword", "Bool", "Number"]

genValidFieldJson :: Gen Value
genValidFieldJson = do
  name <- genValidName
  sType <- genValidType
  pure $ object ["fieldName" .= name, "sType" .= sType]

genInvalidNameJson :: Gen Value
genInvalidNameJson = do
  name <-
    Gen.filter
      (\t -> not (T.all (\c -> isAlphaNum c || c == '-' || c == '_') t) || T.null t)
      (Gen.text (Range.linear 0 30) Gen.unicode)
  sType <- genValidType
  pure $ object ["fieldName" .= name, "sType" .= sType]

genInvalidSTypeJson :: Gen Value
genInvalidSTypeJson = do
  name <- genValidName
  sType <- Gen.text (Range.linear 0 30) Gen.unicode
  pure $ object ["fieldName" .= name, "sType" .= sType]

genDocForMapping :: Mapping -> Gen (Map.Map FieldName FieldValue)
genDocForMapping (Mapping fields) = do
  fieldValues <- traverse genFieldValue (NE.toList fields)
  pure $ Map.fromList (M.catMaybes fieldValues)

-- shortcoming: all ids 1 right now
genDocsForMapping :: Mapping -> Gen [Document]
genDocsForMapping m = Gen.list (Range.linear 1 10) (Document (DocId 1) <$> genDocForMapping m)

genFieldValue :: Field -> Gen (Maybe (FieldName, FieldValue))
genFieldValue field = do
  let fieldValGen = case field.sType of
        Text -> TextVal <$> genTextAlphaNum
        Keyword -> KeywordVal <$> genValidName
        Bool -> BoolVal <$> Gen.bool
        Number -> NumberVal <$> Gen.double (Range.linearFrac 0 1000)
  val <-
    if field.required
      then Just <$> fieldValGen
      else Gen.maybe fieldValGen
  pure $ (field.fieldName,) <$> val

genNonAlphaText :: Gen Text
genNonAlphaText = Gen.text (Range.linear 1 3) genNonAlphaNum

genTokenizableText :: Gen [Text]
genTokenizableText = do
  n <- Gen.int (Range.linear 1 10)
  tokens <- Gen.list (Range.singleton n) genTextAlphaNum
  seps <- Gen.list (Range.singleton n) genNonAlphaText
  let tokensWithSep = uncurry (<>) <$> zip tokens seps
  pure tokensWithSep
