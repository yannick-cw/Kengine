{-# OPTIONS_GHC -Wno-orphans #-}

module Kengine.Persistence.Binary (
  Header (..),
  decodeTokenEntry,
  putDocument,
  decodeSnapshot,
  decodeDocument,
  getMany,
  getDocument,
  putHeader,
  SparseIndexEntry (..),
  getSparseIndexEntry,
  putSparseIndexEntry,
  encodeState,
  getHeader,
  putVarint,
  getVarint,
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
import Data.Bits
import Data.ByteString qualified as BS
import Data.Foldable (traverse_)
import Data.List (mapAccumL, sortOn)
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
  DocSparseIndex,
  DocStore,
  Document (..),
  FieldIndex,
  FieldName,
  FieldStats,
  FieldValue,
  SparseIndex,
  TermFrequency (..),
  Token (..),
  mergeFieldIndex,
  mergeFieldStats,
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

{- | On-disk segment layout (written here, read by 'decodeSnapshot').

  Most integer fields are varint-encoded (LEB128-style: high bit of each byte = "more
  bytes follow", low 7 bits = data). The exceptions are the magic and the five offset
  fields in the header — those stay fixed-width Word64 BE because their byte size has
  to be known before their values are computed in 'encodeState'.

  HEADER
    "KENG"               4 raw bytes (magic; do not change)
    version              varint
    mappingVersion       varint
    docCount             varint
    fieldCount           varint
    fieldNames           fieldCount * (varint len + utf8 bytes)
    termSparseOffset     u64 BE   -> start of SPARSE INDEX
    docSparseOffset      u64 BE   -> start of SPARSE DOC INDEX
    metaSparseOffset     u64 BE   -> end of file (used to size SPARSE DOC INDEX)
    storedFieldsOffset   u64 BE   -> start of STORED DOCS
    docMetadataOffset    u64 BE   -> start of FIELD METADATA

  TOKEN BLOCKS  (sorted by (fieldId, token); 16 entries per block)
    per entry:
      fieldId            varint
      token              varint len + utf8 bytes
      postingCount       varint
      postings           postingCount * (varint docId, varint tf)

  SPARSE INDEX  (one entry per token block; same sort order)
    fieldId              varint
    token                varint len + utf8 bytes
    blockOffset          varint   -> first byte of that block, relative to TOKEN BLOCKS start

  STORED DOCS  (sorted by docId; 128 docs per block)
    per doc:
      docId              varint
      bodyLen            varint
      body               bodyLen bytes (Data.Binary encoding of the doc body)

  FIELD METADATA  (one per (docId, text field); sorted by docId)
    docId                varint
    fieldId              varint
    tokenCount           varint

  SPARSE DOC INDEX  (one entry per doc block)
    docId                varint
    firstDocOffset       varint   -> first byte of that block, relative to STORED DOCS start
-}
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
    documentBlocks = encodeDocs docStore
    documentBytes = BS.concat $ snd <$> documentBlocks
    metaBytes = encodeMetadata fieldNameToId metadata
    docSparseBytes = encodeDocSparse documentBlocks

    termSparseOffset = fromIntegral $ headerOffset + BS.length tokenBlocksBytes
    storedFieldsOffset = termSparseOffset + fromIntegral (BS.length sparseIndexBytes)
    docMetadataOffset = storedFieldsOffset + fromIntegral (BS.length documentBytes)
    docSparseOffset = docMetadataOffset + fromIntegral (BS.length metaBytes)
    metaSparseOffset = docSparseOffset + fromIntegral (BS.length docSparseBytes)

    finalHeader =
      C.runPut $
        putHeader
          nonOffsetHeader
            { termSparseOffset
            , docSparseOffset
            , metaSparseOffset
            , storedFieldsOffset
            , docMetadataOffset
            }
   in
    finalHeader
      <> tokenBlocksBytes
      <> sparseIndexBytes
      <> documentBytes
      <> metaBytes
      <> docSparseBytes

emptyHeader :: [FieldName] -> Int -> Header
emptyHeader fieldNames docCount =
  Header
    { version = 1
    , mappingVersion = 1
    , docCount = fromIntegral docCount
    , fieldNames
    , termSparseOffset = 0
    , docSparseOffset = 0
    , metaSparseOffset = 0
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
    tokenGroups = nelChunksOf 16 tokenEntries
    tokenBlocks = C.runPut . void <$> fmap (traverse putTokenEntry) tokenGroups
    blockSizes = BS.length <$> tokenBlocks
    relativeOffsets = scanl (+) 0 blockSizes
    -- build sparse index, needs offsets from above per 16 entries
    sparseIndex =
      zipWith
        ( \((TokenEntry{fieldId, token}) Nel.:| _) firstBlockOffset ->
            SparseIndexEntry{fieldId, token, firstBlockOffset}
        )
        tokenGroups
        (fromIntegral <$> relativeOffsets)
   in
    (BS.concat tokenBlocks, sparseIndex)

encodeDocs :: DocStore -> [(DocId, BS.ByteString)]
encodeDocs docStore =
  let
    docsChunks = nelChunksOf 128 (sortOn (.docId) (Map.elems docStore))
    docBlocks =
      (\docBlock -> ((Nel.head docBlock).docId, C.runPut (traverse_ putDocument docBlock)))
        <$> docsChunks
   in
    docBlocks

encodeDocSparse :: [(DocId, BS.ByteString)] -> BS.ByteString
encodeDocSparse docBlocks =
  let
    allRelativeOffsets = scanl (+) 0 (BS.length . snd <$> docBlocks)
    sparseEntries =
      zipWith
        (\docId blockOffset -> DocSparseEntry docId (fromIntegral blockOffset))
        (fst <$> docBlocks)
        allRelativeOffsets
   in
    C.runPut $ traverse_ putDocSparseEntry sparseEntries

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
  BS.ByteString ->
  Either String (Header, DocStore, FieldIndex, SparseIndex, DocSparseIndex, FieldStats)
decodeSnapshot = C.runGet $ do
  header <- getHeader
  headerSize <- C.bytesRead
  let tokenSectionSize = fromIntegral header.termSparseOffset - headerSize
  tokenEntries <- C.isolate tokenSectionSize (getMany getTokenEntry)
  headerAndEntrySize <- C.bytesRead
  let sparseIndexSize = fromIntegral header.storedFieldsOffset - headerAndEntrySize
  sparseEntries <- C.isolate sparseIndexSize (getMany getSparseIndexEntry)
  docs <- replicateM (fromIntegral header.docCount) getDocument
  let metaEntriesSize = fromIntegral (header.docSparseOffset - header.docMetadataOffset)
  meta <- C.isolate metaEntriesSize (getMany getFieldMeta)
  let sparseDocIndexSize = fromIntegral (header.metaSparseOffset - header.docSparseOffset)
  sparseDocEntries <- C.isolate sparseDocIndexSize (getMany getDocSparseEntry)
  pure
    ( header
    , buildDocStore docs
    , buildFieldIndex header tokenEntries
    , buildSparseIndex
        header
        (fromIntegral headerSize)
        (fromIntegral tokenSectionSize)
        sparseEntries
    , buildDocSparseIndex header sparseDocEntries
    , buildFieldStats header meta
    )

buildDocStore :: [Document] -> DocStore
buildDocStore docs = Map.fromList $ (\d -> (d.docId, d)) <$> docs

buildFieldIndex :: Header -> [TokenEntry] -> FieldIndex
buildFieldIndex header tokenEntries =
  foldl'
    mergeFieldIndex
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
    mergeFieldStats
    Map.empty
    ( ( \e ->
          Map.singleton
            (header.fieldNames !! fromIntegral e.fieldId)
            (Map.singleton e.docId (DocFieldStats $ fromIntegral e.tokenCount))
      )
        <$> meta
    )

buildSparseIndex :: Header -> Word64 -> Word64 -> [SparseIndexEntry] -> SparseIndex
buildSparseIndex header headerSize tokenSectionSize sparseEntries =
  let
    blockLocations = buildBlockLocations headerSize tokenSectionSize ((.firstBlockOffset) <$> sparseEntries)
    indexes =
      zipWith
        Map.singleton
        ((\e -> (header.fieldNames !! fromIntegral e.fieldId, e.token)) <$> sparseEntries)
        blockLocations
   in
    foldl' Map.union Map.empty indexes

buildDocSparseIndex :: Header -> [DocSparseEntry] -> DocSparseIndex
buildDocSparseIndex header sparseEntries =
  let
    sectionSize = header.docMetadataOffset - header.storedFieldsOffset
    blockLocations =
      buildBlockLocations
        header.storedFieldsOffset
        sectionSize
        ((.firstDocOffset) <$> sparseEntries)
    indexes = zipWith Map.singleton ((.docId) <$> sparseEntries) blockLocations
   in
    foldl' Map.union Map.empty indexes

buildBlockLocations :: Word64 -> Word64 -> [Word64] -> [BlockLocation]
buildBlockLocations sectionFirstByte wholeSectionSize firstEntryOffsets =
  let
    -- these are all the endpoints for each block
    nextRelativeOffset = drop 1 $ firstEntryOffsets ++ [wholeSectionSize]
    sparseIndexes =
      ( \(entryOffset, nextBlockOffset) ->
          -- we convert the relative block offsets to absolute offsets
          ( BlockLocation
              { firstByte = fromIntegral (entryOffset + sectionFirstByte)
              , size = fromIntegral (nextBlockOffset - entryOffset)
              }
          )
      )
        <$> zip firstEntryOffsets nextRelativeOffset
   in
    sparseIndexes

decodeTokenEntry :: BS.ByteString -> Either String [TokenEntry]
decodeTokenEntry = C.runGet (getMany getTokenEntry)

decodeDocument :: BS.ByteString -> Either String [Document]
decodeDocument = C.runGet (getMany getDocument)

data Header = Header
  { version :: Word8
  , mappingVersion :: Word32
  , docCount :: Word32
  , fieldNames :: [FieldName]
  , termSparseOffset :: Word64
  , docSparseOffset :: Word64
  , metaSparseOffset :: Word64
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
    , docSparseOffset
    , metaSparseOffset
    , storedFieldsOffset
    , docMetadataOffset
    } = do
    C.putByteString "KENG"
    putVarint (fromIntegral version)
    putVarint (fromIntegral mappingVersion)
    putVarint (fromIntegral docCount)
    putVarint (fromIntegral $ length fieldNames)
    traverse_ putFieldName fieldNames
    -- header offsets stay fixed-width: their size is measured before they're known,
    -- so varint here would create a chicken-and-egg problem in encodeState.
    C.putWord64be termSparseOffset
    C.putWord64be docSparseOffset
    C.putWord64be metaSparseOffset
    C.putWord64be storedFieldsOffset
    C.putWord64be docMetadataOffset
    where
      putFieldName :: FieldName -> C.Put
      putFieldName fn = do
        let bytes = encodeUtf8 (unrefine fn)
        putVarint (fromIntegral $ BS.length bytes)
        C.putByteString bytes

getHeader :: C.Get Header
getHeader = do
  keng <- C.getBytes 4
  unless (keng == "KENG") (fail "Not a kengine header.")
  version <- fromIntegral <$> getVarint
  mappingVersion <- fromIntegral <$> getVarint
  docCount <- fromIntegral <$> getVarint
  fieldCount <- getVarint
  fieldNames <- replicateM (fromIntegral fieldCount) getFieldName
  termSparseOffset <- C.getWord64be
  docSparseOffset <- C.getWord64be
  metaSparseOffset <- C.getWord64be
  storedFieldsOffset <- C.getWord64be
  docMetadataOffset <- C.getWord64be
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
  where
    getFieldName :: C.Get FieldName
    getFieldName = do
      fieldNameLength <- getVarint
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
  putVarint (fromIntegral fieldId)
  let tknBytes = encodeUtf8 tkn
  putVarint (fromIntegral $ BS.length tknBytes)
  C.putByteString tknBytes
  putVarint (fromIntegral $ length docs)
  -- takes doc ids like [1001,1002,1010,1050] and compresses to [1001,1,8,40] offsets from last id
  -- 'delte encoding'
  let docIdsAsOffsets = snd $ mapAccumL (\lastDocId (docId, tf) -> (docId, (docId - lastDocId, tf))) 0 docs
  traverse_ (uncurry putDoc) docIdsAsOffsets
  where
    putDoc :: DocId -> TermFrequency -> C.Put
    putDoc (DocId dId) (TF tf) = do
      putVarint (fromIntegral dId)
      putVarint (fromIntegral tf)

getTokenEntry :: C.Get TokenEntry
getTokenEntry = do
  fieldId <- fromIntegral <$> getVarint
  tokenLength <- getVarint
  token <- Token . decodeUtf8 <$> C.getBytes (fromIntegral tokenLength)
  docsCount <- getVarint
  docs <- replicateM (fromIntegral docsCount) getDoc
  -- 'delte decoding' - from offsets [1001,1,8,40] to [1001,1002,1010,1050]
  let docIdsFromOffsets = scanl1 (\(lastDocId, _) (docId, tf) -> (lastDocId + docId, tf)) docs
  pure TokenEntry{fieldId, token, docs = docIdsFromOffsets}
  where
    getDoc :: C.Get (DocId, TermFrequency)
    getDoc = do
      docId <- getVarint
      tf <- getVarint
      pure (DocId (fromIntegral docId), TF (fromIntegral tf))

data SparseIndexEntry = SparseIndexEntry {fieldId :: Word16, token :: Token, firstBlockOffset :: Word64}
  deriving stock (Eq, Show)

putSparseIndexEntry :: SparseIndexEntry -> C.Put
putSparseIndexEntry SparseIndexEntry{fieldId, token = (Token tkn), firstBlockOffset} = do
  putVarint (fromIntegral fieldId)
  let tknBytes = encodeUtf8 tkn
  putVarint (fromIntegral $ BS.length tknBytes)
  C.putByteString tknBytes
  putVarint firstBlockOffset

getSparseIndexEntry :: C.Get SparseIndexEntry
getSparseIndexEntry = do
  fieldId <- fromIntegral <$> getVarint
  tokenLength <- getVarint
  token <- Token . decodeUtf8 <$> C.getBytes (fromIntegral tokenLength)
  firstBlockOffset <- getVarint
  pure SparseIndexEntry{fieldId, token, firstBlockOffset}

data DocSparseEntry = DocSparseEntry {docId :: DocId, firstDocOffset :: Word64}
  deriving stock (Eq, Show)

putDocSparseEntry :: DocSparseEntry -> C.Put
putDocSparseEntry DocSparseEntry{docId = (DocId dId), firstDocOffset} = do
  putVarint (fromIntegral dId)
  putVarint firstDocOffset

getDocSparseEntry :: C.Get DocSparseEntry
getDocSparseEntry = do
  docId <- DocId . fromIntegral <$> getVarint
  firstDocOffset <- getVarint
  pure DocSparseEntry{docId, firstDocOffset}

putDocument :: Document -> C.Put
putDocument Document{docId = DocId dId, body} = do
  putVarint (fromIntegral dId)
  let binBody = BS.toStrict $ B.encode body
  putVarint (fromIntegral $ BS.length binBody)
  C.putByteString binBody

getDocument :: C.Get Document
getDocument = do
  docId <- DocId . fromIntegral <$> getVarint
  bodyLength <- getVarint
  bodyBytes <- C.getBytes (fromIntegral bodyLength)
  body <- case B.decodeOrFail (BS.fromStrict bodyBytes) of
    Right (_, _, b) -> pure b
    Left (_, _, err) -> fail ("fail decoding doc from bin data " <> err)
  pure Document{docId, body}

data FieldMeta = FieldMeta {fieldId :: Word16, docId :: DocId, tokenCount :: Word32}
  deriving stock (Eq, Show)

putFieldMeta :: FieldMeta -> C.Put
putFieldMeta FieldMeta{fieldId, docId = (DocId dId), tokenCount} = do
  putVarint (fromIntegral dId)
  putVarint (fromIntegral fieldId)
  putVarint (fromIntegral tokenCount)

getFieldMeta :: C.Get FieldMeta
getFieldMeta = do
  docId <- DocId . fromIntegral <$> getVarint
  fieldId <- fromIntegral <$> getVarint
  tokenCount <- fromIntegral <$> getVarint
  pure FieldMeta{fieldId, docId, tokenCount}

nelChunksOf :: Int -> [e] -> [Nel.NonEmpty e]
nelChunksOf i l = Nel.fromList <$> chunksOf i l

getVarint :: C.Get Word64
getVarint = do
  nextByte <- C.getWord8
  -- leading 1 indicates more bytes with data, 0 that was it
  let hasMore = nextByte .&. 0b10000000 == 0b10000000
  -- don't want indicator bit in result, drop if 1
  let last7Bits :: Word8 = nextByte .&. 0b01111111
  if hasMore
    -- recurse, take the next words (nextWord64: 0...0 10101010 11001011) and shift 7 to left
    -- and then add in the current 7 bits on the rigt (1110101... 0000000 OR 0...0 1010101)
    then (\nextWord64 -> shiftL nextWord64 7 .|. fromIntegral last7Bits) <$> getVarint
    -- no more, just return the word64 (0...0 01101010)
    else pure (fromIntegral last7Bits)

putVarint :: Word64 -> C.Put
putVarint word =
  let
    -- we store the last 7 bits each step
    last7Bits = word .&. 0b01111111
    -- moving the to be stored bits out to the right
    shiftedRight7 = shiftR word 7
   in
    if shiftedRight7 == 0
      -- when no more bits remain in the Word64, done, from integral adds leading 0
      then C.putWord8 (fromIntegral last7Bits)
      -- when more to come, add a leading 1, indicates for the reader more bytes to be checked
      else C.putWord8 (fromIntegral last7Bits .|. 0b10000000) >> putVarint shiftedRight7
