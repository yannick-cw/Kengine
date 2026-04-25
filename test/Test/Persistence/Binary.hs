module Test.Persistence.Binary (spec) where

import Data.Serialize qualified as C
import Hedgehog (diff, forAll)
import Kengine.Persistence.Binary (
  decodeSnapshot,
  encodeState,
  getDocument,
  getFieldMeta,
  getHeader,
  getSparseIndexEntry,
  getTokenEntry,
  putDocument,
  putFieldMeta,
  putHeader,
  putSparseIndexEntry,
  putTokenEntry,
 )
import Kengine.Types (DocId (..), Document (..))

import Test.Helpers.Generators (
  genDocForMapping,
  genFieldMeta,
  genHeader,
  genSparseIndexEntry,
  genState,
  genTokenEntry,
  genValidMapping,
 )
import Test.Hspec (Spec, describe, it)
import Test.Hspec.Hedgehog (hedgehog)

spec :: Spec
spec = do
  describe "Binary segment format" $ do
    it "roundtrips the header" $ hedgehog $ do
      header <- forAll genHeader
      diff (C.runGet getHeader (C.runPut $ putHeader header)) (==) (Right header)
    it "roundtrips the token entry" $ hedgehog $ do
      tknEntry <- forAll genTokenEntry
      diff (C.runGet getTokenEntry (C.runPut $ putTokenEntry tknEntry)) (==) (Right tknEntry)
    it "roundtrips the sparse index" $ hedgehog $ do
      sparseIndex <- forAll genSparseIndexEntry
      diff
        (C.runGet getSparseIndexEntry (C.runPut $ putSparseIndexEntry sparseIndex))
        (==)
        (Right sparseIndex)
    it "roundtrips document" $ hedgehog $ do
      mapping <- forAll genValidMapping
      doc <- forAll $ (\b -> Document{docId = DocId 1, body = b}) <$> genDocForMapping mapping
      diff (C.runGet getDocument (C.runPut $ putDocument doc)) (==) (Right doc)
    it "roundtrips field meta" $ hedgehog $ do
      fieldMeta <- forAll genFieldMeta
      diff (C.runGet getFieldMeta (C.runPut $ putFieldMeta fieldMeta)) (==) (Right fieldMeta)
    it "roundtrips whole state" $ hedgehog $ do
      (docStore, fieldIndex, metadata) <- forAll genState
      diff
        -- ignoring sparse index + header, derived struture
        ( (\(_, a, b, _, d) -> (a, b, d))
            <$> decodeSnapshot (encodeState docStore fieldIndex metadata)
        )
        (==)
        (Right (docStore, fieldIndex, metadata))
