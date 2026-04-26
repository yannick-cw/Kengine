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
flushSegment
  FileStore
    { readSnapshotFieldIndex
    , writePendingSnapshot
    , truncateWAL
    , readPendingSnapshotSegment
    , readSnapshotDocs
    , commitPendingSnapshot
    }
  idxName
  idxDataVar = do
    IndexData{memtable = Memtable{docStore, fieldIdx, fieldMeta}, maxDocId} <-
      liftIO $ TVar.readTVarIO idxDataVar
    diskFieldIdx <- readSnapshotFieldIndex idxName
    diskDocs <- readSnapshotDocs idxName
    let completeDocStore = maybe docStore (Map.union docStore) diskDocs
    let mergedFieldIdx =
          Map.unionWith
            (Map.unionWith Map.union)
            fieldIdx
            (M.fromMaybe Map.empty diskFieldIdx)
    -- unsafe version, overwrites old snapshot
    writePendingSnapshot idxName (completeDocStore, mergedFieldIdx, fieldMeta)
    -- read new sparse index + fieldnames
    Segment{sparseIndex, docsSparseIndex, fieldNames} <- readPendingSnapshotSegment idxName
    -- rename file,
    commitPendingSnapshot idxName
    -- atomically:  replace sparse idx + fieldnames, cleanup in mem fieldIdx with exisiting entries (only keep newer maxdocid)
    -- unsafe version, deletion of log entries
    truncateWAL idxName maxDocId
    liftIO $ TVar.atomically $ do
      idxData@IndexData{memtable = freshMemtable} <-
        TVar.readTVar idxDataVar
      let prunedFieldIdx =
            Map.filter (not . Map.null) $
              Map.map
                ( Map.filter (not . Map.null)
                    . Map.map (Map.filterWithKey (\d _ -> d > maxDocId))
                )
                freshMemtable.fieldIdx
      let prunedDocStore = Map.filterWithKey (\d _ -> d > maxDocId) freshMemtable.docStore
      TVar.writeTVar
        idxDataVar
        idxData
          { memtable = freshMemtable{fieldIdx = prunedFieldIdx, docStore = prunedDocStore}
          , segment = Segment sparseIndex fieldNames docsSparseIndex
          }
