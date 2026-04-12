module Test.Helpers.Generators (
  genValidMapping,
  genValidField,
  genValidFieldName,
  genValidIndexName,
  validNameChar,
  genValidMappingJson,
  genValidFieldJson,
  genValidName,
  genValidType,
  genInvalidNameJson,
  genInvalidSTypeJson,
  genText,
  genNonAlphaNum,
  genTextAlphaNum,
  genTokenizableText,
) where

import Data.Aeson (Value, object, (.=))
import Data.Char (isAlphaNum)
import Data.Text (Text)
import Data.Text qualified as T
import Hedgehog (Gen)
import Hedgehog.Gen qualified as Gen
import Hedgehog.Range qualified as Range
import Kengine.Types (Field (..), IndexName, Mapping (..), SearchType (..), ValidName)
import Refined (Refined, refine)

-- type generators

genValidMapping :: Gen Mapping
genValidMapping = do
  fields <- Gen.nonEmpty (Range.linear 1 10) genValidField
  pure Mapping{fields}

genValidField :: Gen Field
genValidField = do
  fieldName <- genValidFieldName
  sType <- Gen.element [Text, Keyword, Bool, Number]
  pure Field{sType, fieldName}

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
genTextAlphaNum = Gen.text (Range.linear 0 10) Gen.alphaNum

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

genTokenizableText :: Gen [Text]
genTokenizableText = do
  n <- Gen.int (Range.linear 1 10)
  tokens <- Gen.list (Range.singleton n) genTextAlphaNum
  seps <- Gen.list (Range.singleton n) (Gen.text (Range.linear 1 3) genNonAlphaNum)
  let tokensWithSep = uncurry (<>) <$> zip tokens seps
  pure tokensWithSep
