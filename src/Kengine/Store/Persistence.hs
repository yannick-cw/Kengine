{-# LANGUAGE UndecidableInstances #-}

module Kengine.Store.Persistence (FileStore (..), mkFileStore, mkFileStore') where

import Control.Monad.Trans.Except (ExceptT (ExceptT), except)
import Data.Aeson qualified as AE
import Data.Bifunctor (Bifunctor (first))
import Data.Binary (Binary)
import Data.Binary qualified as B
import Data.ByteString qualified as BS
import Data.ByteString.Lazy qualified as L
import Data.Map qualified as Map
import Data.Maybe qualified as M
import Data.Text qualified as T
import Data.Word (Word8)
import GHC.Conc qualified as TVar
import Kengine.Errors (IOE, KengineError (FileError), liftIOE)
import Kengine.Types (
  DocStore,
  Document (..),
  FieldIndex,
  FieldMetadata,
  IndexData (..),
  IndexName,
  Mapping,
 )
import Refined (refineFail, unrefine)
import System.Directory (
  XdgDirectory (XdgData),
  createDirectoryIfMissing,
  doesFileExist,
  getXdgDirectory,
  listDirectory,
 )
import System.FilePath ((</>))

dataDir :: IO FilePath
dataDir = getXdgDirectory XdgData "kengine"

data FileStore = FileStore
  { storeMapping :: IndexName -> Mapping -> IOE KengineError ()
  , readMapping :: IndexName -> IOE KengineError (Maybe Mapping)
  , readIdxs :: IOE KengineError [IndexName]
  , storeDoc :: IndexName -> Document -> IOE KengineError ()
  , readDocs :: IndexName -> IOE KengineError [Document]
  , unsafeFlushState :: IndexName -> IndexData -> IOE KengineError () -- if fails midway data is lost
  , readSnapshot :: IndexName -> IOE KengineError (DocStore, FieldIndex, FieldMetadata)
  }

mkFileStore :: IO FileStore
mkFileStore = mkFileStore' <$> dataDir

mkFileStore' :: FilePath -> FileStore
mkFileStore' dir =
  FileStore
    { storeMapping = \idx m -> writeFullFile jsonCodec dir idx "mapping.json" m
    , readIdxs = M.mapMaybe (refineFail . T.pack) <$> listDirs (dir </> "indexes")
    , readMapping = \idx -> readForIdxFile jsonCodec (pathToIdx dir idx </> "mapping.json")
    , storeDoc = \idx doc -> appendToFile jsonCodec dir idx "log.jsonl" doc
    , readDocs = readDocs dir
    , unsafeFlushState = \idxName (IndexData _ docStoreVar fieldIndexVar metadataVar) -> do
        (docStore, fieldIndex, metadata) <- ExceptT $ TVar.atomically $ do
          docStore <- TVar.readTVar docStoreVar
          fieldIndex <- TVar.readTVar fieldIndexVar
          metadata <- TVar.readTVar metadataVar
          pure $ Right (docStore, fieldIndex, metadata)
        let maxDocId = fst <$> Map.lookupMax docStore
        -- unsafe version, overwrites old snapshot
        writeFullFile binaryCodec dir idxName "snapshot.bin" (docStore, fieldIndex, metadata)
        -- unsafe version, deletion of log entries
        allDocs <- readDocs dir idxName
        let truncatedDocs = filter (\d -> d.docId > M.fromMaybe 0 maxDocId) allDocs
        writeFullFile jsonLCodec dir idxName "log.jsonl" truncatedDocs
    , readSnapshot = \idxName ->
        M.fromMaybe (Map.empty, Map.empty, Map.empty)
          <$> readForIdxFile binaryCodec (pathToIdx dir idxName </> "snapshot.bin")
    }

-- TODO whole file needs refactoring

newline :: Word8
newline = 10

readDocs :: FilePath -> IndexName -> IOE KengineError [Document]
readDocs dir idxName =
  M.fromMaybe [] <$> readForIdxFile jsonLCodec (pathToIdx dir idxName </> "log.jsonl")

appendToFile :: Codec a -> FilePath -> IndexName -> FilePath -> a -> IOE KengineError ()
appendToFile Codec{encode} dir idx file a =
  liftIOE FileError $ do
    createDirectoryIfMissing True (pathToIdx dir idx)
    BS.appendFile (pathToIdx dir idx </> file) (encode a <> "\n")

writeFullFile :: Codec a -> FilePath -> IndexName -> FilePath -> a -> IOE KengineError ()
writeFullFile Codec{encode} dir idx file a =
  liftIOE FileError $ do
    createDirectoryIfMissing True (pathToIdx dir idx)
    BS.writeFile (pathToIdx dir idx </> file) (encode a)

listDirs :: FilePath -> IOE KengineError [FilePath]
listDirs dir =
  liftIOE FileError $ do
    createDirectoryIfMissing True dir
    listDirectory dir

readForIdxFile :: Codec a -> FilePath -> IOE KengineError (Maybe a)
readForIdxFile Codec{decode} path = do
  f <- liftIOE FileError $ do
    exists <- doesFileExist path
    if exists
      then Just <$> BS.readFile path
      else pure Nothing
  except $ traverse decode f

pathToIdx :: FilePath -> IndexName -> FilePath
pathToIdx dir idxName = dir </> "indexes" </> T.unpack (unrefine idxName)

data Codec a = Codec
  { encode :: a -> BS.ByteString
  , decode :: BS.ByteString -> Either KengineError a
  }

jsonCodec :: (AE.ToJSON a, AE.FromJSON a) => Codec a
jsonCodec =
  Codec
    { encode = L.toStrict . AE.encode
    , decode = first (FileError . T.pack) . AE.eitherDecodeStrict
    }

binaryCodec :: (Binary a) => Codec a
binaryCodec =
  Codec
    { encode = L.toStrict . B.encode
    , decode = Right . B.decode . L.fromStrict
    }

jsonLCodec :: (AE.ToJSON a, AE.FromJSON a) => Codec [a]
jsonLCodec =
  Codec
    { encode = \a -> BS.intercalate "\n" (L.toStrict . AE.encode <$> a)
    , decode =
        first (FileError . T.pack)
          . traverse
            AE.eitherDecode
          . filter (not . L.null)
          . L.split newline
          . L.fromStrict
    }
