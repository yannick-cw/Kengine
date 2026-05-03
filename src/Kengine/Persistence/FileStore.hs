module Kengine.Persistence.FileStore (FileStore (..), mkFileStore, mkFileStore', fileNameForNextSegment, fileNameFromSegNum) where

import Control.Monad.Trans.Except (except, throwE)
import Data.Aeson qualified as AE
import Data.Bifunctor (Bifunctor (first))
import Data.ByteString qualified as BS
import Data.ByteString.Lazy qualified as L
import Data.Char (isDigit)
import Data.Foldable (traverse_)
import Data.List (stripPrefix)
import Data.Map qualified as Map
import Data.Maybe qualified as M
import Data.Text qualified as T
import Data.Word (Word16, Word8)
import Kengine.Errors (IOE, KengineError (FileError), liftIOE)
import Kengine.Persistence.Binary (
  Header (..),
  TokenEntry (..),
  decodeDocument,
  decodeSnapshot,
  decodeTokenEntry,
  encodeState,
 )
import Kengine.Types (
  BlockLocation (..),
  DocId,
  DocSparseIndex,
  DocStore,
  Document (..),
  FieldIndex,
  FieldStats,
  FieldTrigrams,
  IndexName,
  Mapping,
  Segment (..),
  SegmentId (..),
  SparseIndex,
  mergeFieldIndex,
 )
import Refined (refineFail, unrefine)
import System.Directory (
  XdgDirectory (XdgData),
  createDirectoryIfMissing,
  doesFileExist,
  getXdgDirectory,
  listDirectory,
  removeFile,
  renameFile,
 )
import System.FilePath ((</>))
import System.IO (IOMode (ReadMode), SeekMode (AbsoluteSeek), hSeek, withFile)
import Text.Read (readMaybe)

dataDir :: IO FilePath
dataDir = getXdgDirectory XdgData "kengine"

data FileStore = FileStore
  { storeMapping :: IndexName -> Mapping -> IOE KengineError ()
  , readMapping :: IndexName -> IOE KengineError (Maybe Mapping)
  , readIdxs :: IOE KengineError [IndexName]
  , storeDoc :: IndexName -> Document -> IOE KengineError ()
  , readDocs :: IndexName -> IOE KengineError [Document]
  , readSnapshots ::
      IndexName -> IOE KengineError [(Header, FieldStats, FieldTrigrams, Segment)] -- todo can be optimized to actually not read full snapshot
  , readFullSnapshots ::
      IndexName -> IOE KengineError [(DocStore, FieldStats, FieldIndex, FieldTrigrams)]
  , indexDir :: IndexName -> FilePath
  , readDiskFieldIndex :: IndexName -> Segment -> BlockLocation -> IOE KengineError FieldIndex
  , readDiskDoc :: IndexName -> Segment -> BlockLocation -> IOE KengineError DocStore
  , writePendingSnapshot ::
      IndexName ->
      SegmentId ->
      (DocStore, FieldIndex, FieldStats, FieldTrigrams) ->
      IOE KengineError ()
  , readPendingSnapshotSegment :: IndexName -> SegmentId -> IOE KengineError Segment
  , commitPendingSnapshot :: IndexName -> SegmentId -> IOE KengineError ()
  , truncateWAL :: IndexName -> DocId -> IOE KengineError ()
  , deleteSegments :: IndexName -> [SegmentId] -> IOE KengineError ()
  }

mkFileStore :: IO FileStore
mkFileStore = mkFileStore' <$> dataDir

mkFileStore' :: FilePath -> FileStore
mkFileStore' dir =
  FileStore
    { storeMapping = storeMapping' dir
    , readMapping = readMapping' dir
    , readIdxs = readIdxs' dir
    , storeDoc = storeDoc' dir
    , readDocs = readDocs' dir
    , readSnapshots = readSnapshots' dir
    , readFullSnapshots = readFullSnapshots' dir
    , indexDir = pathToIdx dir
    , readDiskFieldIndex = readDiskFieldIndex' dir
    , readDiskDoc = readDiskDoc' dir
    , writePendingSnapshot = writePendingSnapshot' dir
    , readPendingSnapshotSegment = readPendingSnapshotSegment' dir
    , commitPendingSnapshot = commitPendingSnapshot' dir
    , truncateWAL = truncateWAL' dir
    , deleteSegments = deleteSegments' dir
    }

fileNameForNextSegment :: Maybe Segment -> FilePath
fileNameForNextSegment Nothing = "seg_1.bin"
fileNameForNextSegment (Just Segment{segNum}) = fileNameFromSegNum (segNum + 1)

fileNameFromSegNum :: SegmentId -> FilePath
fileNameFromSegNum (SegmentId n) = "seg_" ++ show n ++ ".bin"

mappingFile, walFile, segmentFilePrefix :: FilePath
mappingFile = "mapping.json"
walFile = "log.jsonl"
segmentFilePrefix = "seg_"

storeMapping' :: FilePath -> IndexName -> Mapping -> IOE KengineError ()
storeMapping' dir idx = writeFullFile jsonEnCodec dir idx mappingFile

readMapping' :: FilePath -> IndexName -> IOE KengineError (Maybe Mapping)
readMapping' dir idx = readForIdxFile jsonDeCodec (pathToIdx dir idx </> mappingFile)

readIdxs' :: FilePath -> IOE KengineError [IndexName]
readIdxs' dir = M.mapMaybe (refineFail . T.pack) <$> listDirs (dir </> "indexes")

storeDoc' :: FilePath -> IndexName -> Document -> IOE KengineError ()
storeDoc' dir idx = appendToFile jsonEnCodec dir idx walFile

readDocs' :: FilePath -> IndexName -> IOE KengineError [Document]
readDocs' dir idxName =
  M.fromMaybe [] <$> readForIdxFile jsonLDeCodec (pathToIdx dir idxName </> walFile)

readSnapshots' ::
  FilePath ->
  IndexName ->
  IOE KengineError [(Header, FieldStats, FieldTrigrams, Segment)]
readSnapshots' dir idxName = do
  snap <- readAllSnapshots dir idxName
  pure $
    (\(h, _, _, fieldStats, fieldTris, seg) -> (h, fieldStats, fieldTris, seg)) <$> snap

readFullSnapshots' ::
  FilePath ->
  IndexName ->
  IOE KengineError [(DocStore, FieldStats, FieldIndex, FieldTrigrams)]
readFullSnapshots' dir idxName = do
  snap <- readAllSnapshots dir idxName
  pure $
    (\(_, docs, fieldIdx, fieldStats, fieldTris, _) -> (docs, fieldStats, fieldIdx, fieldTris))
      <$> snap

readAllSnapshots ::
  FilePath ->
  IndexName ->
  IOE KengineError [(Header, DocStore, FieldIndex, FieldStats, FieldTrigrams, Segment)]
readAllSnapshots dir idxName = do
  let idxPath = pathToIdx dir idxName
  dirContent <- listDirs idxPath
  let segs = M.mapMaybe (\f -> (f,) <$> fileNameToNum f) dirContent
  M.catMaybes <$> traverse readOne segs
  where
    readOne (fname, num) =
      fmap (toTuple num) <$> readForIdxFile snapshotDeCodec (pathToIdx dir idxName </> fname)
    toTuple num (header, docStore, fieldIdx, sparse, doc, fieldStats, fieldTris) =
      ( header
      , docStore
      , fieldIdx
      , fieldStats
      , fieldTris
      , Segment sparse header.fieldNames doc num
      )
    fileNameToNum fileName = do
      rest <- stripPrefix segmentFilePrefix fileName
      readMaybe (takeWhile isDigit rest)

readDiskFieldIndex' ::
  FilePath -> IndexName -> Segment -> BlockLocation -> IOE KengineError FieldIndex
readDiskFieldIndex' dir idxName (Segment{fieldNames, segNum}) location = do
  let fieldNameLookup = Map.fromList $ zip [0 :: Word16 ..] fieldNames
  blockOfTokens <-
    readAtBlockLocation
      blockDecode
      (pathToIdx dir idxName </> fileNameFromSegNum segNum)
      location
  let indexPerEntry :: [FieldIndex] =
        ( \entry ->
            Map.singleton
              (fieldNameLookup Map.! entry.fieldId)
              (Map.singleton entry.token (Map.fromList entry.docs))
        )
          <$> blockOfTokens
  pure (foldl' mergeFieldIndex Map.empty indexPerEntry)

readDiskDoc' ::
  FilePath -> IndexName -> Segment -> BlockLocation -> IOE KengineError DocStore
readDiskDoc' dir idxName Segment{segNum} location = do
  documentBlock <-
    readAtBlockLocation
      docDecode
      (pathToIdx dir idxName </> fileNameFromSegNum segNum)
      location
  pure $ Map.fromList ((\doc -> (doc.docId, doc)) <$> documentBlock)

writePendingSnapshot' ::
  FilePath ->
  IndexName ->
  SegmentId ->
  (DocStore, FieldIndex, FieldStats, FieldTrigrams) ->
  IOE KengineError ()
writePendingSnapshot' dir idxName segId =
  writeFullFile snapshotEnCodec dir idxName (pendingFileName segId)

readPendingSnapshotSegment' ::
  FilePath -> IndexName -> SegmentId -> IOE KengineError Segment
readPendingSnapshotSegment' dir idxName segId = do
  segment <-
    readForIdxFile snapshotDeCodec (pathToIdx dir idxName </> pendingFileName segId)
  maybe
    (throwE (FileError "flushed segment not found"))
    (\(h, _, _, b, dss, _, _) -> pure $ Segment b h.fieldNames dss segId)
    segment

commitPendingSnapshot' ::
  FilePath -> IndexName -> SegmentId -> IOE KengineError ()
commitPendingSnapshot' dir idxName segId =
  liftIOE FileError "commitPendingSnapshot/renameFile" $
    renameFile
      (pathToIdx dir idxName </> pendingFileName segId)
      (pathToIdx dir idxName </> fileNameFromSegNum segId)

pendingFileName :: SegmentId -> FilePath
pendingFileName segId = fileNameFromSegNum segId ++ ".new"

truncateWAL' :: FilePath -> IndexName -> DocId -> IOE KengineError ()
truncateWAL' dir idxName upTo = do
  walDocs <- readDocs' dir idxName
  let kept = filter (\d -> d.docId > upTo) walDocs
  writeFullFile jsonLEnCodec dir idxName walFile kept

deleteSegments' :: FilePath -> IndexName -> [SegmentId] -> IOE KengineError ()
deleteSegments' dir idxName segmentIds = do
  let fileNames = fileNameFromSegNum <$> segmentIds
  traverse_ (deleteFile dir idxName) fileNames

newline :: Word8
newline = 10

appendToFile :: EnCodec a -> FilePath -> IndexName -> FilePath -> a -> IOE KengineError ()
appendToFile EnCodec{encode} dir idx file a =
  liftIOE FileError ("appendToFile " <> T.pack file) $ do
    createDirectoryIfMissing True (pathToIdx dir idx)
    BS.appendFile (pathToIdx dir idx </> file) (encode a <> "\n")

writeFullFile ::
  EnCodec a -> FilePath -> IndexName -> FilePath -> a -> IOE KengineError ()
writeFullFile EnCodec{encode} dir idx file a =
  liftIOE FileError ("writeFullFile " <> T.pack file) $ do
    createDirectoryIfMissing True (pathToIdx dir idx)
    BS.writeFile (pathToIdx dir idx </> file) (encode a)

deleteFile ::
  FilePath -> IndexName -> FilePath -> IOE KengineError ()
deleteFile dir idx file =
  liftIOE FileError ("delete file " <> T.pack file) $ do
    removeFile (pathToIdx dir idx </> file)

listDirs :: FilePath -> IOE KengineError [FilePath]
listDirs dir =
  liftIOE FileError ("listDirs " <> T.pack dir) $ do
    createDirectoryIfMissing True dir
    listDirectory dir

readForIdxFile :: DeCodec a -> FilePath -> IOE KengineError (Maybe a)
readForIdxFile DeCodec{decode} path = do
  f <- liftIOE FileError ("readForIdxFile " <> T.pack path) $ do
    exists <- doesFileExist path
    if exists
      then Just <$> BS.readFile path
      else pure Nothing
  except $ traverse decode f

readAtBlockLocation :: DeCodec a -> FilePath -> BlockLocation -> IOE KengineError a
readAtBlockLocation DeCodec{decode} file BlockLocation{firstByte, size} =
  liftIOE
    FileError
    ("readAtBlockLocation " <> T.pack file)
    ( withFile
        file
        ReadMode
        ( \handle -> do
            hSeek handle AbsoluteSeek (fromIntegral firstByte)
            bs <- BS.hGet handle size
            pure $ decode bs
        )
    )
    >>= except

pathToIdx :: FilePath -> IndexName -> FilePath
pathToIdx dir idxName = dir </> "indexes" </> T.unpack (unrefine idxName)

newtype EnCodec a = EnCodec {encode :: a -> BS.ByteString}
newtype DeCodec a = DeCodec {decode :: BS.ByteString -> Either KengineError a}

jsonEnCodec :: (AE.ToJSON a) => EnCodec a
jsonEnCodec = EnCodec{encode = L.toStrict . AE.encode}

jsonDeCodec :: (AE.FromJSON a) => DeCodec a
jsonDeCodec = DeCodec{decode = first (FileError . T.pack) . AE.eitherDecodeStrict}

jsonLEnCodec :: (AE.ToJSON a) => EnCodec [a]
jsonLEnCodec = EnCodec{encode = \a -> BS.intercalate "\n" (L.toStrict . AE.encode <$> a)}

jsonLDeCodec :: (AE.FromJSON a) => DeCodec [a]
jsonLDeCodec =
  DeCodec
    { decode =
        first (FileError . T.pack)
          . traverse
            AE.eitherDecode
          . filter (not . L.null)
          . L.split newline
          . L.fromStrict
    }

snapshotEnCodec :: EnCodec (DocStore, FieldIndex, FieldStats, FieldTrigrams)
snapshotEnCodec = EnCodec{encode = \(a, b, c, d) -> encodeState a b c d}

-- todo smarter reading, skip reading docstore
snapshotDeCodec ::
  DeCodec
    (Header, DocStore, FieldIndex, SparseIndex, DocSparseIndex, FieldStats, FieldTrigrams)
snapshotDeCodec =
  DeCodec
    { decode =
        first (FileError . T.pack)
          . fmap (\(h, ds, fi, si, dsi, fs, tris) -> (h, ds, fi, si, dsi, fs, tris))
          . decodeSnapshot
    }

blockDecode :: DeCodec [TokenEntry]
blockDecode = DeCodec{decode = first (FileError . T.pack) . decodeTokenEntry}

docDecode :: DeCodec [Document]
docDecode = DeCodec{decode = first (FileError . T.pack) . decodeDocument}
