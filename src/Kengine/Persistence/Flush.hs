module Kengine.Persistence.Flush (flushSegment) where

import Control.Concurrent (threadDelay)
import Control.Monad.IO.Class (liftIO)
import Data.Map qualified as Map
import GHC.Conc qualified as TVar
import Kengine.Errors (IOE, KengineError, Result)
import Kengine.Persistence.FileStore (FileStore (..))
import Kengine.Types (
  DocId,
  DocStore,
  FieldIndex,
  FieldStats,
  FieldTrigrams,
  IndexData (..),
  IndexName,
  Memtable (..),
  Segment (..),
  SegmentId (SegmentId),
  mergeDocStore,
  mergeFieldIndex,
  mergeFieldStats,
  mergeTrigrams,
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
    , readFullSnapshots
    , deleteSegments
    }
  idxName
  idxDataVar = do
    IndexData
      { memtable = Memtable{docStore, fieldIdx, fieldMeta, trigrams}
      , maxDocId
      , segments
      } <-
      liftIO $ TVar.readTVarIO idxDataVar
    -- unsafe version, overwrites old snapshot
    let nextSegmentId = maybe (SegmentId 1) ((+) 1 . (.segNum)) (newestSegment segments)
    if length segments < 5
      then appendSegment nextSegmentId docStore fieldIdx fieldMeta trigrams maxDocId
      else mergeSegments segments nextSegmentId docStore fieldIdx fieldMeta trigrams maxDocId
    where
      appendSegment ::
        SegmentId -> DocStore -> FieldIndex -> FieldStats -> FieldTrigrams -> DocId -> Result ()
      appendSegment nextSegmentId docStore fieldIdx fieldMeta fieldTris maxDocId = do
        let thisSegFieldMeta = (`Map.restrictKeys` Map.keysSet docStore) <$> fieldMeta
        let thisSegTrigrams = fieldTris `Map.restrictKeys` Map.keysSet fieldIdx
        newSeg <-
          multiStepSegmentUpdate
            nextSegmentId
            docStore
            fieldIdx
            thisSegFieldMeta
            thisSegTrigrams
            maxDocId
        updateIndexVar maxDocId (newSeg :)

      mergeSegments ::
        [Segment] ->
        SegmentId ->
        DocStore ->
        FieldIndex ->
        FieldStats ->
        FieldTrigrams ->
        DocId ->
        Result ()
      mergeSegments segments nextSegmentId docStore fieldIdx fieldMeta fieldTris maxDocId = do
        snaps <- readFullSnapshots idxName
        let (totalDocStore, totalFieldIdx, totalStats, totalTrigrams) =
              foldl'
                ( \(ds, fdx, fm, ft) (segDocStore, segStas, segFdx, tri) ->
                    ( mergeDocStore ds segDocStore
                    , mergeFieldIndex fdx segFdx
                    , mergeFieldStats fm segStas
                    , mergeTrigrams ft tri
                    )
                )
                (docStore, fieldIdx, fieldMeta, fieldTris)
                snaps
        newSeg <-
          multiStepSegmentUpdate
            nextSegmentId
            totalDocStore
            totalFieldIdx
            totalStats
            totalTrigrams
            maxDocId
        -- IF crash here -> startup reads existing segs + this huge seg, not ideal
        updateIndexVar maxDocId (const [newSeg])
        liftIO $ threadDelay 1000000 -- 1 sec - we wait so readers still reading the old file are safe
        deleteSegments idxName ((.segNum) <$> segments)

      multiStepSegmentUpdate ::
        SegmentId ->
        DocStore ->
        FieldIndex ->
        FieldStats ->
        FieldTrigrams ->
        DocId ->
        Result Segment
      multiStepSegmentUpdate nextSeg docStore fieldIdx stats tris maxDocId = do
        -- write, rename flow, if during write crash -> no rename -> no corrupted file
        writePendingSnapshot idxName nextSeg (docStore, fieldIdx, stats, tris)
        -- read new sparse index + fieldnames
        newSeg <- readPendingSnapshotSegment idxName nextSeg
        -- rename file, removes .new basically
        commitPendingSnapshot idxName nextSeg
        -- atomically:  replace sparse idx + fieldnames, cleanup in mem fieldIdx with exisiting entries (only keep newer maxdocid)
        -- unsafe version, deletion of log entries
        truncateWAL idxName maxDocId
        pure newSeg

      updateIndexVar :: DocId -> ([Segment] -> [Segment]) -> Result ()
      updateIndexVar maxDocId updateSegs = liftIO $ TVar.atomically $ do
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
            , segments = updateSegs freshData.segments
            }
