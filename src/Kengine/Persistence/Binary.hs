{-# OPTIONS_GHC -Wno-orphans #-}

module Kengine.Persistence.Binary (
  Header (..),
  decodeTokenEntry,
  putDocument,
  decodeSnapshot,
  getDocument,
  putHeader,
  SparseIndexEntry (..),
  getSparseIndexEntry,
  putSparseIndexEntry,
  encodeState,
  getHeader,
  putTokenEntry,
  getTokenEntry,
  TokenEntry (..),
  FieldMeta (..),
  putFieldMeta,
  getFieldMeta,
) where

import Control.Monad (replicateM, unless, void)
import Data.Binary (Binary (..), Word16)
import Data.Binary qualified as B
import Data.ByteString qualified as BS
import Data.Foldable (traverse_)
import Data.List (sortOn)
import Data.List.NonEmpty qualified as Nel
import Data.List.Split (chunksOf)
import Data.Map qualified as Map
import Data.Serialize qualified as C
import Data.Text.Encoding (decodeUtf8, encodeUtf8)
import Data.Word (Word32, Word64, Word8)
import Kengine.Types (
  BlockLocation (..),
  DocFieldStats (..),
  DocId (..),
  DocStore,
  Document (..),
  FieldIndex,
  FieldName,
  FieldStats,
  FieldValue,
  SparseIndex,
  TermFrequency (..),
  Token (..),
 )
import Refined (refineFail, unrefine)

instance Binary TermFrequency
instance Binary DocFieldStats
instance Binary DocId
instance Binary Token
instance Binary Document
instance Binary FieldValue
instance Binary FieldName where
  put = put . unrefine
  get = refineFail =<< get

encodeState :: DocStore -> FieldIndex -> FieldStats -> BS.ByteString
encodeState docStore fieldIndex metadata =
  let
    fieldNames = Map.keys fieldIndex
    fieldNameToId = Map.fromList $ zip fieldNames [0 :: Word16 ..]
    -- build header
    nonOffsetHeader = emptyHeader fieldNames (length docStore)
    -- measure header byte length
    headerOffset = BS.length $ C.runPut (putHeader nonOffsetHeader)

    (tokenBlocksBytes, sparseIndex) = encodeTokenBlocks fieldNameToId fieldIndex
    sparseIndexBytes = C.runPut $ traverse_ putSparseIndexEntry sparseIndex
    documentBytes = encodeDocs docStore
    metaBytes = encodeMetadata fieldNameToId metadata

    termSparseOffset = fromIntegral $ headerOffset + BS.length tokenBlocksBytes
    storedFieldsOffset = termSparseOffset + fromIntegral (BS.length sparseIndexBytes)
    docMetadataOffset = storedFieldsOffset + fromIntegral (BS.length documentBytes)

    finalHeader =
      C.runPut $
        putHeader
          nonOffsetHeader{termSparseOffset, storedFieldsOffset, docMetadataOffset}
   in
    finalHeader <> tokenBlocksBytes <> sparseIndexBytes <> documentBytes <> metaBytes

emptyHeader :: [FieldName] -> Int -> Header
emptyHeader fieldNames docCount =
  Header
    { version = 1
    , mappingVersion = 1
    , docCount = fromIntegral docCount
    , fieldNames
    , termSparseOffset = 0
    , storedFieldsOffset = 0
    , docMetadataOffset = 0
    }

encodeTokenBlocks ::
  Map.Map FieldName Word16 -> FieldIndex -> (BS.ByteString, [SparseIndexEntry])
encodeTokenBlocks fieldNameToId fieldIndex =
  let
    -- all token entries - flat - sorted by field + token
    tokenEntries = sortOn (\e -> (e.fieldId, e.token)) $ do
      (fieldName, tokensMap) <- Map.toList fieldIndex
      (token, docs) <- Map.toList tokensMap
      pure TokenEntry{fieldId = fieldNameToId Map.! fieldName, token, docs = Map.toList docs}
    tokenGroups = nelChunksOf 128 tokenEntries
    tokenBlocks = C.runPut . void <$> fmap (traverse putTokenEntry) tokenGroups
    blockSizes = BS.length <$> tokenBlocks
    relativeOffsets = scanl (+) 0 blockSizes
    -- build sparse index, needs offsets from above per 128 entries
    sparseIndex =
      zipWith
        ( \((TokenEntry{fieldId, token}) Nel.:| _) firstBlockOffset ->
            SparseIndexEntry{fieldId, token, firstBlockOffset}
        )
        tokenGroups
        (fromIntegral <$> relativeOffsets)
   in
    (BS.concat tokenBlocks, sparseIndex)

encodeDocs :: DocStore -> BS.ByteString
encodeDocs = C.runPut . traverse_ putDocument . sortOn (.docId) . Map.elems

encodeMetadata :: Map.Map FieldName Word16 -> FieldStats -> BS.ByteString
encodeMetadata fieldNameToId metadata =
  let
    metaEntries = sortOn (.docId) $ do
      (fieldName, metaMap) <- Map.toList metadata
      (docId, DocFieldStats tokenCount) <- Map.toList metaMap
      pure
        FieldMeta
          { fieldId = fieldNameToId Map.! fieldName
          , docId
          , tokenCount = fromIntegral tokenCount
          }
   in
    C.runPut $ traverse_ putFieldMeta metaEntries

getMany :: C.Get a -> C.Get [a]
getMany getOne = do
  atEnd <- C.isEmpty
  if atEnd
    then pure []
    else do
      r <- getOne
      rest <- getMany getOne
      pure $ r : rest

decodeSnapshot ::
  BS.ByteString -> Either String (Header, DocStore, FieldIndex, SparseIndex, FieldStats)
decodeSnapshot = C.runGet $ do
  header <- getHeader
  headerSize <- C.bytesRead
  let tokenSectionSize = fromIntegral header.termSparseOffset - headerSize
  tokenEntries <- C.isolate tokenSectionSize (getMany getTokenEntry)
  headerAndEntrySize <- C.bytesRead
  let sparseIndexSize = fromIntegral header.storedFieldsOffset - headerAndEntrySize
  sparseEntries <- C.isolate sparseIndexSize (getMany getSparseIndexEntry)
  docs <- replicateM (fromIntegral header.docCount) getDocument
  meta <- getMany getFieldMeta
  pure
    ( header
    , buildDocStore docs
    , buildFieldIndex header tokenEntries
    , buildSparseIndex header headerSize tokenSectionSize sparseEntries
    , buildFieldStats header meta
    )

buildDocStore :: [Document] -> DocStore
buildDocStore docs = Map.fromList $ (\d -> (d.docId, d)) <$> docs

buildFieldIndex :: Header -> [TokenEntry] -> FieldIndex
buildFieldIndex header tokenEntries =
  foldl'
    (Map.unionWith (Map.unionWith Map.union))
    Map.empty
    ( ( \e ->
          Map.singleton
            (header.fieldNames !! fromIntegral e.fieldId)
            (Map.singleton e.token (Map.fromList e.docs))
      )
        <$> tokenEntries
    )

buildFieldStats :: Header -> [FieldMeta] -> FieldStats
buildFieldStats header meta =
  foldl'
    (Map.unionWith Map.union)
    Map.empty
    ( ( \e ->
          Map.singleton
            (header.fieldNames !! fromIntegral e.fieldId)
            (Map.singleton e.docId (DocFieldStats $ fromIntegral e.tokenCount))
      )
        <$> meta
    )

buildSparseIndex :: Header -> Int -> Int -> [SparseIndexEntry] -> SparseIndex
buildSparseIndex header headerSize tokenSectionSize sparseEntries =
  let
    -- these are all the endpoints for each block, I drop the first offset and add the tokenEntrySize as last byte
    nextRelativeOffset =
      drop 1 $ ((.firstBlockOffset) <$> sparseEntries) ++ [fromIntegral tokenSectionSize]
    sparseIndexes =
      ( \(e, nextBlockStart) ->
          Map.singleton
            (header.fieldNames !! fromIntegral e.fieldId, e.token)
            -- we convert the relative block offsets to absolute offsets
            ( BlockLocation
                { firstByte = fromIntegral (e.firstBlockOffset + fromIntegral headerSize)
                , size = fromIntegral (nextBlockStart - e.firstBlockOffset)
                }
            )
      )
        <$> zip sparseEntries nextRelativeOffset
   in
    foldl' Map.union Map.empty sparseIndexes

decodeTokenEntry :: BS.ByteString -> Either String [TokenEntry]
decodeTokenEntry = C.runGet (getMany getTokenEntry)

data Header = Header
  { version :: Word8
  , mappingVersion :: Word32
  , docCount :: Word32
  , fieldNames :: [FieldName]
  , termSparseOffset :: Word64
  , storedFieldsOffset :: Word64
  , docMetadataOffset :: Word64
  }
  deriving stock (Eq, Show)

putHeader :: Header -> C.Put
putHeader
  Header
    { version
    , mappingVersion
    , docCount
    , fieldNames
    , termSparseOffset
    , storedFieldsOffset
    , docMetadataOffset
    } = do
    C.putByteString "KENG"
    C.putWord8 version
    C.putWord32be mappingVersion
    C.putWord32be docCount
    C.putWord16be (fromIntegral $ length fieldNames)
    traverse_ putFieldName fieldNames
    C.putWord64be termSparseOffset
    C.putWord64be storedFieldsOffset
    C.putWord64be docMetadataOffset
    where
      putFieldName :: FieldName -> C.Put
      putFieldName fn = do
        let bytes = encodeUtf8 (unrefine fn)
        C.putWord16be (fromIntegral $ BS.length bytes)
        C.putByteString bytes

getHeader :: C.Get Header
getHeader = do
  keng <- C.getBytes 4
  unless (keng == "KENG") (fail "Not a kengine header.")
  version <- C.getWord8
  mappingVersion <- C.getWord32be
  docCount <- C.getWord32be
  fieldCount <- C.getWord16be
  fieldNames <- replicateM (fromIntegral fieldCount) getFieldName
  termSparseOffset <- C.getWord64be
  storedFieldsOffset <- C.getWord64be
  docMetadataOffset <- C.getWord64be
  pure
    Header
      { version
      , mappingVersion
      , docCount
      , fieldNames
      , termSparseOffset
      , storedFieldsOffset
      , docMetadataOffset
      }
  where
    getFieldName :: C.Get FieldName
    getFieldName = do
      fieldNameLength <- C.getWord16be
      fnNameBytes <- C.getBytes (fromIntegral fieldNameLength)
      refineFail $ decodeUtf8 fnNameBytes

data TokenEntry = TokenEntry
  { fieldId :: Word16
  , token :: Token
  , docs :: [(DocId, TermFrequency)]
  }
  deriving stock (Eq, Show)

putTokenEntry :: TokenEntry -> C.Put
putTokenEntry TokenEntry{fieldId, token = (Token tkn), docs} = do
  C.putWord16be fieldId
  let tknBytes = encodeUtf8 tkn
  C.putWord16be (fromIntegral $ BS.length tknBytes)
  C.putByteString tknBytes
  C.putWord32be (fromIntegral $ length docs)
  traverse_ (uncurry putDoc) docs
  where
    putDoc :: DocId -> TermFrequency -> C.Put
    putDoc (DocId dId) (TF tf) = do
      C.putWord32be (fromIntegral dId)
      C.putWord32be (fromIntegral tf)

getTokenEntry :: C.Get TokenEntry
getTokenEntry = do
  fieldId <- C.getWord16be
  tokenLength <- C.getWord16be
  token <- Token . decodeUtf8 <$> C.getBytes (fromIntegral tokenLength)
  docsCount <- C.getWord32be
  docs <- replicateM (fromIntegral docsCount) getDoc
  pure TokenEntry{fieldId, token, docs}
  where
    getDoc :: C.Get (DocId, TermFrequency)
    getDoc = do
      docId <- C.getWord32be
      tf <- C.getWord32be
      pure (DocId (fromIntegral docId), TF (fromIntegral tf))

data SparseIndexEntry = SparseIndexEntry {fieldId :: Word16, token :: Token, firstBlockOffset :: Word64}
  deriving stock (Eq, Show)

putSparseIndexEntry :: SparseIndexEntry -> C.Put
putSparseIndexEntry SparseIndexEntry{fieldId, token = (Token tkn), firstBlockOffset} = do
  C.putWord16be fieldId
  let tknBytes = encodeUtf8 tkn
  C.putWord16be (fromIntegral $ BS.length tknBytes)
  C.putByteString tknBytes
  C.putWord64be firstBlockOffset

getSparseIndexEntry :: C.Get SparseIndexEntry
getSparseIndexEntry = do
  fieldId <- C.getWord16be
  tokenLength <- C.getWord16be
  token <- Token . decodeUtf8 <$> C.getBytes (fromIntegral tokenLength)
  firstBlockOffset <- C.getWord64be
  pure SparseIndexEntry{fieldId, token, firstBlockOffset}

putDocument :: Document -> C.Put
putDocument doc = do
  let DocId dId = doc.docId
  C.putWord32be (fromIntegral dId)
  let binBody = BS.toStrict $ B.encode doc.body
  C.putWord32be (fromIntegral $ BS.length binBody)
  C.putByteString binBody

getDocument :: C.Get Document
getDocument = do
  docId <- DocId . fromIntegral <$> C.getWord32be
  bodyLength <- C.getWord32be
  bodyBytes <- C.getBytes (fromIntegral bodyLength)
  let body = B.decode (BS.fromStrict bodyBytes)
  pure Document{docId, body}

data FieldMeta = FieldMeta {fieldId :: Word16, docId :: DocId, tokenCount :: Word32}
  deriving stock (Eq, Show)

putFieldMeta :: FieldMeta -> C.Put
putFieldMeta FieldMeta{fieldId, docId = (DocId dId), tokenCount} = do
  C.putWord32be (fromIntegral dId)
  C.putWord16be fieldId
  C.putWord32be tokenCount

getFieldMeta :: C.Get FieldMeta
getFieldMeta = do
  docId <- DocId . fromIntegral <$> C.getWord32be
  fieldId <- C.getWord16be
  tokenCount <- C.getWord32be
  pure FieldMeta{fieldId, docId, tokenCount}

nelChunksOf :: Int -> [e] -> [Nel.NonEmpty e]
nelChunksOf i l = Nel.fromList <$> chunksOf i l
