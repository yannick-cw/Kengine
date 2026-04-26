module Test.Helpers.Generators (
  genValidMappingRequiredField,
  genFieldMeta,
  genSparseIndexEntry,
  genTokenEntry,
  genValidMapping,
  genValidField,
  genHeader,
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
  genState,
) where

import Data.Aeson (Value, object, (.=))
import Data.Char (isAlphaNum)
import Data.List.NonEmpty qualified as NE
import Data.Map qualified as Map
import Data.Maybe qualified as M
import Data.Text (Text)
import Data.Text qualified as T
import Data.Word (Word32)
import Hedgehog (Gen)
import Hedgehog.Gen qualified as Gen
import Hedgehog.Range qualified as Range
import Kengine.Persistence.Binary (
  FieldMeta (..),
  Header (..),
  SparseIndexEntry (..),
  TokenEntry (..),
 )
import Kengine.Types (
  DocFieldStats (..),
  DocId (DocId),
  DocStore,
  Document (..),
  Field (..),
  FieldIndex,
  FieldName,
  FieldStats,
  FieldValue (..),
  IndexName,
  Mapping (..),
  SearchType (..),
  TermFrequency (TF),
  Token (..),
  ValidName,
 )
import Refined (Refined, refine)

-- type generators

genValidMappingRequiredField :: Bool -> Gen Mapping
genValidMappingRequiredField allRequired = do
  fields <-
    NE.nubBy (\a b -> a.fieldName == b.fieldName)
      <$> Gen.nonEmpty (Range.linear 1 10) (genValidField allRequired)
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

genDocsForMapping :: Mapping -> Gen [Document]
genDocsForMapping m = do
  bodies <- Gen.list (Range.linear 1 10) (genDocForMapping m)
  pure $ zipWith (Document . DocId) [1 ..] bodies

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

-- binary - header
genW32 :: Gen Word32
genW32 = Gen.word32 (Range.linear 1 1000)

genHeader :: Gen Header
genHeader = do
  version <- Gen.word8 (Range.linear 0 10)
  mappingVersion <- genW32
  docCount <- genW32
  fieldNames <- Gen.list (Range.linear 1 1000) genValidFieldName
  termSparseOffset <- Gen.word64 (Range.linear 0 1000)
  docSparseOffset <- Gen.word64 (Range.linear 0 1000)
  metaSparseOffset <- Gen.word64 (Range.linear 0 1000)
  storedFieldsOffset <- Gen.word64 (Range.linear 0 1000)
  docMetadataOffset <- Gen.word64 (Range.linear 0 1000)
  pure
    Header
      { version
      , mappingVersion
      , docCount
      , fieldNames
      , termSparseOffset
      , docSparseOffset
      , metaSparseOffset
      , storedFieldsOffset
      , docMetadataOffset
      }

genDocId :: Gen DocId
genDocId = DocId . fromIntegral <$> genW32

genTF :: Gen TermFrequency
genTF = TF . fromIntegral <$> genW32

genTokenEntry :: Gen TokenEntry
genTokenEntry = do
  fieldId <- Gen.word16 (Range.linear 0 1000)
  token <- Token <$> genText
  docs <- Gen.list (Range.linear 1 100) ((,) <$> genDocId <*> genTF)
  pure TokenEntry{fieldId, token, docs}

genSparseIndexEntry :: Gen SparseIndexEntry
genSparseIndexEntry = do
  fieldId <- Gen.word16 (Range.linear 0 1000)
  token <- Token <$> genText
  firstBlockOffset <- Gen.word64 (Range.linear 1 10000)
  pure SparseIndexEntry{fieldId, token, firstBlockOffset}

genFieldMeta :: Gen FieldMeta
genFieldMeta = do
  fieldId <- Gen.word16 (Range.linear 0 1000)
  docId <- genDocId
  tokenCount <- genW32
  pure FieldMeta{fieldId, docId, tokenCount}

genState :: Gen (DocStore, FieldIndex, FieldStats)
genState = do
  mapping <- genValidMapping
  docs <- genDocsForMapping mapping
  let fieldNames = NE.toList (fmap (.fieldName) mapping.fields)
  let docStore = Map.fromList ((\d -> (d.docId, d)) <$> docs)
  fieldIndex <-
    Map.fromList <$> traverse (\fn -> (fn,) <$> genInvertedIndex) fieldNames
  metadata <-
    Map.fromList <$> traverse (\fn -> (fn,) <$> genMetaMap) fieldNames
  pure (docStore, fieldIndex, metadata)
  where
    genInvertedIndex = Gen.map (Range.linear 1 5) $ do
      tkn <- Token <$> genTextAlphaNum
      postings <- Gen.map (Range.linear 1 3) ((,) <$> genDocId <*> genTF)
      pure (tkn, postings)
    genMetaMap =
      Gen.map (Range.linear 1 5) $
        (,) <$> genDocId <*> (DocFieldStats <$> Gen.int (Range.linear 0 100))
