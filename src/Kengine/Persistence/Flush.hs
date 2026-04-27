module Kengine.Persistence.Flush (flushSegment) where

import Control.Monad.IO.Class (liftIO)
import Data.Map qualified as Map
import GHC.Conc qualified as TVar
import Kengine.Errors (IOE, KengineError)
import Kengine.Persistence.FileStore (FileStore (..))
import Kengine.Types (
  IndexData (..),
  IndexName,
  Memtable (..),
  Segment (..),
  SegmentId (SegmentId),
  newestSegment,
 )

-- Flush the in-memory state of one index to disk and atomically swap the
-- in-memory segment view to the new on-disk snapshot. Unsafe!
flushSegment :: FileStore -> IndexName -> TVar.TVar IndexData -> IOE KengineError ()
flushSegment
  FileStore
    { writePendingSnapshot
    , truncateWAL
    , readPendingSnapshotSegment
    , commitPendingSnapshot
    }
  idxName
  idxDataVar = do
    IndexData{memtable = Memtable{docStore, fieldIdx, fieldMeta}, maxDocId, segments} <-
      liftIO $ TVar.readTVarIO idxDataVar
    -- unsafe version, overwrites old snapshot
    let nextSegmentId = maybe (SegmentId 1) ((+) 1 . (.segNum)) (newestSegment segments)
    -- write, rename flow, if during write crash -> no rename -> no corrupted file
    writePendingSnapshot idxName nextSegmentId (docStore, fieldIdx, fieldMeta)
    -- read new sparse index + fieldnames
    newSeg <- readPendingSnapshotSegment idxName nextSegmentId
    -- rename file, removes .new basically
    commitPendingSnapshot idxName nextSegmentId
    -- atomically:  replace sparse idx + fieldnames, cleanup in mem fieldIdx with exisiting entries (only keep newer maxdocid)
    -- unsafe version, deletion of log entries
    truncateWAL idxName maxDocId
    liftIO $ TVar.atomically $ do
      freshData <- TVar.readTVar idxDataVar
      let prunedFieldIdx =
            Map.filter (not . Map.null) $
              Map.map
                ( Map.filter (not . Map.null)
                    . Map.map (Map.filterWithKey (\d _ -> d > maxDocId))
                )
                freshData.memtable.fieldIdx
      let prunedDocStore = Map.filterWithKey (\d _ -> d > maxDocId) freshData.memtable.docStore
      TVar.writeTVar
        idxDataVar
        freshData
          { memtable = freshData.memtable{fieldIdx = prunedFieldIdx, docStore = prunedDocStore}
          , segments = newSeg : freshData.segments
          }
