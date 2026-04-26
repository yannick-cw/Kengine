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
    , commitPendingSnapshot
    }
  idxName
  idxDataVar = do
    IndexData{memtable = Memtable{docStore, fieldIdx, fieldMeta}} <-
      liftIO $ TVar.readTVarIO idxDataVar
    diskFieldIdx <- readSnapshotFieldIndex idxName
    let maxPersistedDocId = M.maybe 0 fst (Map.lookupMax docStore)
    let mergedFieldIdx =
          Map.unionWith
            (Map.unionWith Map.union)
            fieldIdx
            (M.fromMaybe Map.empty diskFieldIdx)
    -- unsafe version, overwrites old snapshot
    writePendingSnapshot idxName (docStore, mergedFieldIdx, fieldMeta)
    -- unsafe version, deletion of log entries
    truncateWAL idxName maxPersistedDocId
    -- read new sparse index + fieldnames
    (sparseIdx, fieldNames) <- readPendingSnapshotSegment idxName
    -- rename file,
    commitPendingSnapshot idxName
    -- atomically:  replace sparse idx + fieldnames, cleanup in mem fieldIdx with exisiting entries (only keep newer maxdocid)
    liftIO $ TVar.atomically $ do
      IndexData{mapping, memtable = freshMemtable@Memtable{fieldIdx = freshFieldIdx}} <-
        TVar.readTVar idxDataVar
      let prunedFieldIdx =
            Map.filter (not . Map.null) $
              Map.map
                ( Map.filter (not . Map.null)
                    . Map.map (Map.filterWithKey (\d _ -> d > maxPersistedDocId))
                )
                freshFieldIdx
      TVar.writeTVar
        idxDataVar
        IndexData
          { mapping
          , memtable = freshMemtable{fieldIdx = prunedFieldIdx}
          , segment = Segment sparseIdx fieldNames Map.empty
          }
