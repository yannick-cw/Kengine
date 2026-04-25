module Kengine.Persistence.Flush (flushSegment) where

import Control.Monad.IO.Class (liftIO)
import Data.Map qualified as Map
import Data.Maybe qualified as M
import GHC.Conc qualified as TVar
import Kengine.Errors (IOE, KengineError)
import Kengine.Persistence.FileStore (FileStore (..))
import Kengine.Types (
  IndexData (..),
  IndexName,
  Memtable (..),
  Segment (..),
 )

-- Flush the in-memory state of one index to disk and atomically swap the
-- in-memory segment view to the new on-disk snapshot. Unsafe!
flushSegment :: FileStore -> IndexName -> TVar.TVar IndexData -> IOE KengineError ()
flushSegment fs idxName idxDataVar = do
  idxData <- liftIO $ TVar.readTVarIO idxDataVar
  diskFieldIdx <- fs.readSnapshotFieldIndex idxName
  let maxPersistedDocId = M.maybe 0 fst (Map.lookupMax idxData.memtable.docStore)
  let mergedFieldIdx =
        Map.unionWith
          (Map.unionWith Map.union)
          idxData.memtable.fieldIdx
          (M.fromMaybe Map.empty diskFieldIdx)
  -- unsafe version, overwrites old snapshot
  fs.writePendingSnapshot
    idxName
    (idxData.memtable.docStore, mergedFieldIdx, idxData.memtable.fieldMeta)
  -- unsafe version, deletion of log entries
  fs.truncateWAL idxName maxPersistedDocId
  -- read new sparse index + fieldnames
  (sparseIdx, fieldNames) <- fs.readPendingSnapshotSegment idxName
  -- rename file,
  fs.commitPendingSnapshot idxName
  -- atomically:  replace sparse idx + fieldnames, cleanup in mem fieldIdx with exisiting entries (only keep newer maxdocid)
  liftIO $ TVar.atomically $ do
    freshIdxData <- TVar.readTVar idxDataVar
    let prunedFieldIdx =
          Map.filter (not . Map.null) $
            Map.map
              ( Map.filter (not . Map.null)
                  . Map.map (Map.filterWithKey (\d _ -> d > maxPersistedDocId))
              )
              freshIdxData.memtable.fieldIdx
    TVar.writeTVar
      idxDataVar
      freshIdxData
        { memtable = freshIdxData.memtable{fieldIdx = prunedFieldIdx}
        , segment = Segment sparseIdx fieldNames
        }
