module Kengine.Store.Persistence (FileStore (..), mkFileStore, mkFileStore') where

import Control.Monad.Trans.Except (except)
import Data.Aeson qualified as AE
import Data.Bifunctor (Bifunctor (first))
import Data.ByteString qualified as BS
import Data.ByteString.Lazy qualified as L
import Data.Map qualified as Map
import Data.Maybe qualified as M
import Data.Text qualified as T
import Data.Word (Word8)
import Kengine.Errors (IOE, KengineError (FileError), liftIOE)
import Kengine.Types (Document, IndexName, Mapping)
import Refined (refineFail, unrefine)
import System.Directory (
  XdgDirectory (XdgData),
  createDirectoryIfMissing,
  getXdgDirectory,
  listDirectory,
 )
import System.FilePath ((</>))

dataDir :: IO FilePath
dataDir = getXdgDirectory XdgData "kengine"

data FileStore = FileStore
  { storeMapping :: IndexName -> Mapping -> IOE KengineError ()
  , readMappings :: IOE KengineError (Map.Map IndexName Mapping)
  , storeDoc :: IndexName -> Document -> IOE KengineError ()
  , readDocs :: IOE KengineError (Map.Map IndexName [Document])
  }

mkFileStore :: IO FileStore
mkFileStore = mkFileStore' <$> dataDir

mkFileStore' :: FilePath -> FileStore
mkFileStore' dir =
  FileStore
    { storeMapping = \idx m -> do
        appendForIdxFile dir idx "mapping.json" (AE.encode m)
        appendForIdxFile dir idx "log.jsonl" ""
    , readMappings = do
        idxs <- M.mapMaybe (refineFail . T.pack) <$> listDirs (dir </> "indexes")
        rawMappings <- traverse (readForIdxFile dir "mapping.json") idxs
        let mappings :: Either String [Mapping] = traverse (AE.eitherDecode . L.fromStrict) rawMappings
        let idxMapping = Map.fromList . zip idxs <$> mappings
        except $ first (FileError . T.pack) idxMapping
    , storeDoc = \idx doc -> appendForIdxFile dir idx "log.jsonl" (AE.encode doc)
    , readDocs = do
        idxs <- M.mapMaybe (refineFail . T.pack) <$> listDirs (dir </> "indexes")
        rawDocs <- traverse (readForIdxFile dir "log.jsonl") idxs
        let docs :: Either String [[Document]] =
              mapM
                (traverse AE.eitherDecode . filter (not . L.null) . L.split newline . L.fromStrict)
                rawDocs
        let idxDocs = Map.fromList . zip idxs <$> docs
        except $ first (FileError . T.pack) idxDocs
    }
  where
    newline :: Word8
    newline = 10

appendForIdxFile ::
  FilePath -> IndexName -> FilePath -> L.ByteString -> IOE KengineError ()
appendForIdxFile dir idxName fileName content =
  liftIOE FileError $
    do
      createDirectoryIfMissing True (dir </> "indexes" </> T.unpack (unrefine idxName))
      L.appendFile
        (dir </> "indexes" </> T.unpack (unrefine idxName) </> fileName)
        (content <> "\n")

listDirs :: FilePath -> IOE KengineError [FilePath]
listDirs dir =
  liftIOE FileError $ do
    createDirectoryIfMissing True dir
    listDirectory dir

readForIdxFile :: FilePath -> FilePath -> IndexName -> IOE KengineError BS.ByteString
readForIdxFile dir fileName idxName =
  liftIOE FileError $ do
    BS.readFile ((dir </> "indexes" </> T.unpack (unrefine idxName)) </> fileName)
