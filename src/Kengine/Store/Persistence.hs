{-# LANGUAGE UndecidableInstances #-}

module Kengine.Store.Persistence (FileStore (..), mkFileStore, mkFileStore') where

import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Except (ExceptT (ExceptT), except)
import Data.Aeson qualified as AE
import Data.Bifunctor (Bifunctor (first))
import Data.ByteString qualified as BS
import Data.ByteString.Lazy qualified as L
import Data.Map qualified as Map
import Data.Maybe qualified as M
import Data.Text qualified as T
import Data.Word (Word16, Word8)
import GHC.Conc qualified as TVar
import Kengine.Errors (IOE, KengineError (FileError), liftIOE)
import Kengine.Store.Binary (
  Header (..),
  TokenEntry (..),
  decodeSnapshot,
  decodeState,
  decodeTokenEntry,
  encodeState,
 )
import Kengine.Types (
  DocStore,
  Document (..),
  FieldIndex,
  FieldMetadata,
  FieldName,
  IndexData (..),
  IndexName,
  Mapping,
  Offset (..),
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
  , unsafeFlushState :: IndexName -> IndexData -> IOE KengineError () -- if fails midway data is lost
  , readSnapshot ::
      IndexName -> IOE KengineError (DocStore, SparseIndex, FieldMetadata, [FieldName])
  , readDiskFieldIndex :: IndexName -> [FieldName] -> Offset -> IOE KengineError FieldIndex
  }

mkFileStore :: IO FileStore
mkFileStore = mkFileStore' <$> dataDir

mkFileStore' :: FilePath -> FileStore
mkFileStore' dir =
  FileStore
    { storeMapping = \idx m -> writeFullFile jsonEnCodec dir idx "mapping.json" m
    , readIdxs = M.mapMaybe (refineFail . T.pack) <$> listDirs (dir </> "indexes")
    , readMapping = \idx -> readForIdxFile jsonDeCodec (pathToIdx dir idx </> "mapping.json")
    , storeDoc = \idx doc -> appendToFile jsonEnCodec dir idx "log.jsonl" doc
    , readDocs = readDocs dir
    , -- TODO parts of this should be a higher level fn, just flush in here
      unsafeFlushState = \idxName (IndexData _ docStoreVar fieldIndexVar metadataVar sparseIdxVar fieldNamesVar) -> do
        diskFieldIdx <- readForIdxFile fieldIndexDecode (pathToIdx dir idxName </> "snapshot.bin")
        (docStore, fieldIndex, metadata) <- ExceptT $ TVar.atomically $ do
          docStore <- TVar.readTVar docStoreVar
          fieldIndex <- TVar.readTVar fieldIndexVar
          metadata <- TVar.readTVar metadataVar
          pure $ Right (docStore, fieldIndex, metadata)
        let maxPersistedDocId = fst <$> Map.lookupMax docStore
        let mergedFieldIdxs =
              Map.unionWith (Map.unionWith Map.union) fieldIndex (M.fromMaybe Map.empty diskFieldIdx)
        -- unsafe version, overwrites old snapshot
        writeFullFile
          snapshotEnCodec
          dir
          idxName
          "snapshot.new.bin"
          (docStore, mergedFieldIdxs, metadata)
        -- unsafe version, deletion of log entries
        walDocs <- readDocs dir idxName
        let truncatedDocs = filter (\d -> d.docId > M.fromMaybe 0 maxPersistedDocId) walDocs
        writeFullFile jsonLEnCodec dir idxName "log.jsonl" truncatedDocs
        -- read new sparse index + fieldnames
        (sparseIdx, fieldNames) <-
          M.maybe (Map.empty, []) (\(h, _, b, _) -> (b, h.fieldNames))
            <$> readForIdxFile snapshotDeCodec (pathToIdx dir idxName </> "snapshot.new.bin")
        liftIOE FileError $
          -- rename file,
          renameFile
            (pathToIdx dir idxName </> "snapshot.new.bin")
            (pathToIdx dir idxName </> "snapshot.bin")
        -- atomically:  replace sparse idx + fieldnames, cleanup in mem fieldIdx with exisiting entries (only keep newer maxdocid)
        liftIO $ TVar.atomically $ do
          currentFieldIdx <- TVar.readTVar fieldIndexVar
          let prunedFieldIdx =
                Map.filter (not . Map.null) $
                  Map.map
                    ( Map.filter (not . Map.null)
                        . Map.map (Map.filterWithKey (\d _ -> d > M.fromMaybe 0 maxPersistedDocId))
                    )
                    currentFieldIdx
          TVar.writeTVar sparseIdxVar sparseIdx
          TVar.writeTVar fieldNamesVar fieldNames
          TVar.writeTVar fieldIndexVar prunedFieldIdx
    , readSnapshot = \idxName -> do
        s <- readForIdxFile snapshotDeCodec (pathToIdx dir idxName </> "snapshot.bin")
        pure
          (M.maybe (Map.empty, Map.empty, Map.empty, []) (\(h, a, b, c) -> (a, b, c, h.fieldNames)) s)
    , readDiskFieldIndex = \idxName fieldNames offset -> do
        let fieldNameLookup = Map.fromList $ zip [0 :: Word16 ..] fieldNames
        blockOfTokens <-
          readAtOffset blockDecode (pathToIdx dir idxName </> "snapshot.bin") offset
        let indexPerEntry :: [FieldIndex] =
              ( \entry ->
                  Map.singleton
                    (fieldNameLookup Map.! entry.fieldId)
                    (Map.singleton entry.token (Map.fromList entry.docs))
              )
                <$> blockOfTokens
        pure (foldl' (Map.unionWith (Map.unionWith Map.union)) Map.empty indexPerEntry)
    }

newline :: Word8
newline = 10

readDocs :: FilePath -> IndexName -> IOE KengineError [Document]
readDocs dir idxName =
  M.fromMaybe [] <$> readForIdxFile jsonLDeCodec (pathToIdx dir idxName </> "log.jsonl")

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

readAtOffset :: DeCodec a -> FilePath -> Offset -> IOE KengineError a
readAtOffset DeCodec{decode} file Offset{firstByte, size} =
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

snapshotEnCodec :: EnCodec (DocStore, FieldIndex, FieldMetadata)
snapshotEnCodec = EnCodec{encode = \(a, b, c) -> encodeState a b c}

snapshotDeCodec :: DeCodec (Header, DocStore, SparseIndex, FieldMetadata)
snapshotDeCodec = DeCodec{decode = first (FileError . T.pack) . decodeState}

fieldIndexDecode :: DeCodec FieldIndex
fieldIndexDecode =
  DeCodec
    { decode = first (FileError . T.pack) . fmap (\(_, _, f, _, _) -> f) . decodeSnapshot
    }

blockDecode :: DeCodec [TokenEntry]
blockDecode = DeCodec{decode = first (FileError . T.pack) . decodeTokenEntry}
