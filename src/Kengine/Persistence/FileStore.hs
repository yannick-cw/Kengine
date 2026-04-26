module Kengine.Persistence.FileStore (FileStore (..), mkFileStore, mkFileStore') where

import Control.Monad.Trans.Except (ExceptT (ExceptT), except)
import Data.Aeson qualified as AE
import Data.Bifunctor (Bifunctor (first))
import Data.ByteString qualified as BS
import Data.ByteString.Lazy qualified as L
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
  IndexName,
  Mapping,
  Segment (..),
  SparseIndex,
 )
import Refined (refineFail, unrefine)
import System.Directory (
  XdgDirectory (XdgData),
  createDirectoryIfMissing,
  doesFileExist,
  getXdgDirectory,
  listDirectory,
  renameFile,
 )
import System.FilePath ((</>))
import System.IO (IOMode (ReadMode), SeekMode (AbsoluteSeek), hSeek, withFile)

dataDir :: IO FilePath
dataDir = getXdgDirectory XdgData "kengine"

data FileStore = FileStore
  { storeMapping :: IndexName -> Mapping -> IOE KengineError ()
  , readMapping :: IndexName -> IOE KengineError (Maybe Mapping)
  , readIdxs :: IOE KengineError [IndexName]
  , storeDoc :: IndexName -> Document -> IOE KengineError ()
  , readDocs :: IndexName -> IOE KengineError [Document]
  , readSnapshot ::
      IndexName -> IOE KengineError (Maybe (Header, SparseIndex, DocSparseIndex, FieldStats))
  , indexDir :: IndexName -> FilePath
  , readDiskFieldIndex :: IndexName -> Segment -> BlockLocation -> IOE KengineError FieldIndex
  , readDiskDoc :: IndexName -> BlockLocation -> IOE KengineError DocStore
  , readSnapshotFieldIndex :: IndexName -> IOE KengineError (Maybe FieldIndex)
  , readSnapshotDocs :: IndexName -> IOE KengineError (Maybe DocStore)
  , writePendingSnapshot ::
      IndexName -> (DocStore, FieldIndex, FieldStats) -> IOE KengineError ()
  , readPendingSnapshotSegment ::
      IndexName -> IOE KengineError Segment
  , commitPendingSnapshot :: IndexName -> IOE KengineError ()
  , truncateWAL :: IndexName -> DocId -> IOE KengineError ()
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
    , readSnapshot = readSnapshot' dir
    , indexDir = pathToIdx dir
    , readDiskFieldIndex = readDiskFieldIndex' dir
    , readDiskDoc = readDiskDoc' dir
    , readSnapshotFieldIndex = readSnapshotFieldIndex' dir
    , readSnapshotDocs = readSnapshotDocs' dir
    , writePendingSnapshot = writePendingSnapshot' dir
    , readPendingSnapshotSegment = readPendingSnapshotSegment' dir
    , commitPendingSnapshot = commitPendingSnapshot' dir
    , truncateWAL = truncateWAL' dir
    }

mappingFile, walFile, snapshotFile, pendingSnapshotFile :: FilePath
mappingFile = "mapping.json"
walFile = "log.jsonl"
snapshotFile = "snapshot.bin"
pendingSnapshotFile = "snapshot.new.bin"

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

readSnapshot' ::
  FilePath ->
  IndexName ->
  IOE KengineError (Maybe (Header, SparseIndex, DocSparseIndex, FieldStats))
readSnapshot' dir idxName =
  readForIdxFile snapshotDeCodec (pathToIdx dir idxName </> snapshotFile)

readDiskFieldIndex' ::
  FilePath -> IndexName -> Segment -> BlockLocation -> IOE KengineError FieldIndex
readDiskFieldIndex' dir idxName (Segment{fieldNames}) location = do
  let fieldNameLookup = Map.fromList $ zip [0 :: Word16 ..] fieldNames
  blockOfTokens <-
    readAtBlockLocation blockDecode (pathToIdx dir idxName </> snapshotFile) location
  let indexPerEntry :: [FieldIndex] =
        ( \entry ->
            Map.singleton
              (fieldNameLookup Map.! entry.fieldId)
              (Map.singleton entry.token (Map.fromList entry.docs))
        )
          <$> blockOfTokens
  pure (foldl' (Map.unionWith (Map.unionWith Map.union)) Map.empty indexPerEntry)

readDiskDoc' ::
  FilePath -> IndexName -> BlockLocation -> IOE KengineError DocStore
readDiskDoc' dir idxName location = do
  documentBlock <-
    readAtBlockLocation docDecode (pathToIdx dir idxName </> snapshotFile) location
  pure $ Map.fromList ((\doc -> (doc.docId, doc)) <$> documentBlock)

-- pure (foldl' (Map.unionWith (Map.unionWith Map.union)) Map.empty indexPerEntry)

readSnapshotFieldIndex' :: FilePath -> IndexName -> IOE KengineError (Maybe FieldIndex)
readSnapshotFieldIndex' dir idxName =
  readForIdxFile fieldIndexDecode (pathToIdx dir idxName </> snapshotFile)

readSnapshotDocs' :: FilePath -> IndexName -> IOE KengineError (Maybe DocStore)
readSnapshotDocs' dir idxName = readForIdxFile docsDecode (pathToIdx dir idxName </> snapshotFile)

writePendingSnapshot' ::
  FilePath ->
  IndexName ->
  (DocStore, FieldIndex, FieldStats) ->
  IOE KengineError ()
writePendingSnapshot' dir idxName = writeFullFile snapshotEnCodec dir idxName pendingSnapshotFile

readPendingSnapshotSegment' ::
  FilePath -> IndexName -> IOE KengineError Segment
readPendingSnapshotSegment' dir idxName =
  M.maybe (Segment Map.empty [] Map.empty) (\(h, b, dss, _) -> Segment b h.fieldNames dss)
    <$> readForIdxFile snapshotDeCodec (pathToIdx dir idxName </> pendingSnapshotFile)

commitPendingSnapshot' :: FilePath -> IndexName -> IOE KengineError ()
commitPendingSnapshot' dir idxName =
  liftIOE FileError $
    renameFile
      (pathToIdx dir idxName </> pendingSnapshotFile)
      (pathToIdx dir idxName </> snapshotFile)

truncateWAL' :: FilePath -> IndexName -> DocId -> IOE KengineError ()
truncateWAL' dir idxName upTo = do
  walDocs <- readDocs' dir idxName
  let kept = filter (\d -> d.docId > upTo) walDocs
  writeFullFile jsonLEnCodec dir idxName walFile kept

newline :: Word8
newline = 10

appendToFile :: EnCodec a -> FilePath -> IndexName -> FilePath -> a -> IOE KengineError ()
appendToFile EnCodec{encode} dir idx file a =
  liftIOE FileError $ do
    createDirectoryIfMissing True (pathToIdx dir idx)
    BS.appendFile (pathToIdx dir idx </> file) (encode a <> "\n")

writeFullFile ::
  EnCodec a -> FilePath -> IndexName -> FilePath -> a -> IOE KengineError ()
writeFullFile EnCodec{encode} dir idx file a =
  liftIOE FileError $ do
    createDirectoryIfMissing True (pathToIdx dir idx)
    BS.writeFile (pathToIdx dir idx </> file) (encode a)

listDirs :: FilePath -> IOE KengineError [FilePath]
listDirs dir =
  liftIOE FileError $ do
    createDirectoryIfMissing True dir
    listDirectory dir

readForIdxFile :: DeCodec a -> FilePath -> IOE KengineError (Maybe a)
readForIdxFile DeCodec{decode} path = do
  f <- liftIOE FileError $ do
    exists <- doesFileExist path
    if exists
      then Just <$> BS.readFile path
      else pure Nothing
  except $ traverse decode f

readAtBlockLocation :: DeCodec a -> FilePath -> BlockLocation -> IOE KengineError a
readAtBlockLocation DeCodec{decode} file BlockLocation{firstByte, size} =
  ExceptT $
    withFile
      file
      ReadMode
      ( \handle -> do
          hSeek handle AbsoluteSeek (fromIntegral firstByte)
          bs <- BS.hGet handle size
          pure $ decode bs
      )

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

snapshotEnCodec :: EnCodec (DocStore, FieldIndex, FieldStats)
snapshotEnCodec = EnCodec{encode = \(a, b, c) -> encodeState a b c}

-- todo smarter reading, skip eading docstore
snapshotDeCodec :: DeCodec (Header, SparseIndex, DocSparseIndex, FieldStats)
snapshotDeCodec =
  DeCodec
    { decode =
        first (FileError . T.pack)
          . fmap (\(h, _ds, _fi, si, dsi, fs) -> (h, si, dsi, fs))
          . decodeSnapshot
    }

fieldIndexDecode :: DeCodec FieldIndex
fieldIndexDecode =
  DeCodec
    { decode = first (FileError . T.pack) . fmap (\(_, _, f, _, _, _) -> f) . decodeSnapshot
    }

docsDecode :: DeCodec DocStore
docsDecode =
  DeCodec
    { decode = first (FileError . T.pack) . fmap (\(_, ds, _, _, _, _) -> ds) . decodeSnapshot
    }

blockDecode :: DeCodec [TokenEntry]
blockDecode = DeCodec{decode = first (FileError . T.pack) . decodeTokenEntry}

docDecode :: DeCodec [Document]
docDecode = DeCodec{decode = first (FileError . T.pack) . decodeDocument}
