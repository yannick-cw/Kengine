module Kengine.Store.Persistence (FileStore (..), mkFileStore, mkFileStore') where

import Control.Monad.Trans.Except (except)
import Data.Aeson qualified as AE
import Data.Bifunctor (Bifunctor (first))
import Data.ByteString.Lazy qualified as L
import Data.Maybe qualified as M
import Data.Text qualified as T
import Kengine.Errors (FileError (..), IOE, liftIOE)
import Kengine.Types (IndexName, Mapping)
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
  { storeMapping :: IndexName -> Mapping -> IOE FileError ()
  , readMappings :: IOE FileError [Mapping]
  }

mkFileStore :: IO FileStore
mkFileStore = mkFileStore' <$> dataDir

mkFileStore' :: FilePath -> FileStore
mkFileStore' dir =
  FileStore
    { storeMapping = \idx m -> writeForIdxFile dir idx "mapping.json" (AE.encode m)
    , readMappings = do
        idxs <- M.mapMaybe (refineFail . T.pack) <$> listDirs (dir </> "indexes")
        rawMappings <- traverse (readForIdxFile dir "mapping.json") idxs
        let mappings :: Either String [Mapping] = traverse AE.eitherDecode rawMappings
        except $ first (FileError . T.pack) mappings
    }

writeForIdxFile :: FilePath -> IndexName -> FilePath -> L.ByteString -> IOE FileError ()
writeForIdxFile dir idxName fileName content =
  liftIOE FileError $
    do
      createDirectoryIfMissing True (dir </> "indexes" </> T.unpack (unrefine idxName))
      L.writeFile (dir </> "indexes" </> T.unpack (unrefine idxName) </> fileName) content

listDirs :: FilePath -> IOE FileError [FilePath]
listDirs dir =
  liftIOE FileError $ listDirectory dir

readForIdxFile :: FilePath -> FilePath -> IndexName -> IOE FileError L.ByteString
readForIdxFile dir fileName idxName =
  liftIOE FileError $
    L.readFile ((dir </> "indexes" </> T.unpack (unrefine idxName)) </> fileName)
