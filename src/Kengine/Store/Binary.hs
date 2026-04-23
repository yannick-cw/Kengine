module Kengine.Store.Binary (Header (..), putHeader, getHeader) where

import Control.Monad (replicateM, unless)
import Data.ByteString qualified as BS
import Data.Foldable (traverse_)
import Data.Serialize qualified as C
import Data.Text.Encoding (decodeUtf8, encodeUtf8)
import Data.Word (Word32, Word64, Word8)
import Kengine.Types (FieldName)
import Refined (refineFail, unrefine)

data Header = Header
  { version :: Word8
  , mappingVersion :: Word32
  , docCount :: Word32
  , fieldNames :: [FieldName]
  , termSparseOffset :: Word64
  , storedFieldsOffset :: Word64
  , docMetadataOffset :: Word64
  }
  deriving stock (Eq, Show)

putHeader :: Header -> C.Put
putHeader
  Header
    { version
    , mappingVersion
    , docCount
    , fieldNames
    , termSparseOffset
    , storedFieldsOffset
    , docMetadataOffset
    } = do
    C.putByteString "KENG"
    C.putWord8 version
    C.putWord32be mappingVersion
    C.putWord32be docCount
    C.putWord16be (fromIntegral $ length fieldNames)
    traverse_ putFieldName fieldNames
    C.putWord64be termSparseOffset
    C.putWord64be storedFieldsOffset
    C.putWord64be docMetadataOffset
    where
      putFieldName :: FieldName -> C.Put
      putFieldName fn = do
        let bytes = encodeUtf8 (unrefine fn)
        C.putWord16be (fromIntegral $ BS.length bytes)
        C.putByteString bytes

getHeader :: C.Get Header
getHeader = do
  keng <- C.getBytes 4
  unless (keng == "KENG") (fail "Not a kengine header.")
  version <- C.getWord8
  mappingVersion <- C.getWord32be
  docCount <- C.getWord32be
  fieldCount <- C.getWord16be
  fieldNames <- replicateM (fromIntegral fieldCount) getFieldName
  termSparseOffset <- C.getWord64be
  storedFieldsOffset <- C.getWord64be
  docMetadataOffset <- C.getWord64be
  pure
    Header
      { version
      , mappingVersion
      , docCount
      , fieldNames
      , termSparseOffset
      , storedFieldsOffset
      , docMetadataOffset
      }
  where
    getFieldName :: C.Get FieldName
    getFieldName = do
      fieldNameLength <- C.getWord16be
      fnNameBytes <- C.getBytes (fromIntegral fieldNameLength)
      refineFail $ decodeUtf8 fnNameBytes
