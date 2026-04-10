module Test.Kengine (spec) where

import Control.Monad.IO.Class (liftIO)
import Data.Aeson qualified as Aeson
import Data.Aeson.Key (fromText)
import Hedgehog (Gen, annotate, diff, forAll)
import Hedgehog.Gen qualified as Gen
import Hedgehog.Range qualified as Range
import Kengine qualified
import Test.Hspec (Spec, describe, it)
import Test.Hspec.Hedgehog (hedgehog)

spec :: Spec
spec = describe "Kengine" $ do
  it "always finds what has been stored" $ hedgehog $ do
    key <- forAll keyGen
    value <- forAll genJson
    db <- liftIO Kengine.open
    liftIO $ Kengine.put db key value
    readVal <- liftIO $ Kengine.get db key
    annotate "Store -> Read roundtrip"
    case readVal of
      Just (Right v) -> diff v (==) value
      Just (Left err) -> fail err
      Nothing -> fail "Not found"

keyGen :: Gen Kengine.Key
keyGen = Kengine.Key <$> Gen.text (Range.linear 0 1000) Gen.alphaNum

genJson :: Gen Aeson.Value
genJson =
  Gen.recursive
    Gen.choice
    [ -- non recursive gens
      Aeson.String <$> Gen.text (Range.linear 0 100) Gen.unicode
    , Aeson.Number . fromIntegral <$> Gen.int (Range.linear (-1000) 1000)
    , Aeson.Bool <$> Gen.bool
    , pure Aeson.Null
    ]
    [ -- recursive gen
      Aeson.object
        <$> Gen.list
          (Range.linear 0 5)
          ((,) . fromText <$> Gen.text (Range.linear 1 10) Gen.alphaNum <*> genJson)
    ]
