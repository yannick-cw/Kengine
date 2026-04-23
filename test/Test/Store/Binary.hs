module Test.Store.Binary (spec) where

import Data.Serialize qualified as C
import Hedgehog (diff, forAll)
import Kengine.Store.Binary (getHeader, putHeader)
import Test.Helpers.Generators (genHeader)
import Test.Hspec (Spec, describe, it)
import Test.Hspec.Hedgehog (hedgehog)

spec :: Spec
spec = do
  describe "Binary segment format" $ do
    it "roundtrips the header" $ hedgehog $ do
      header <- forAll genHeader
      let binHeader = C.runPut $ putHeader header
      let readHeader = C.runGet getHeader binHeader
      diff readHeader (==) (Right header)
