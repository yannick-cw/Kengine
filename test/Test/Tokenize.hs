module Test.Tokenize (spec) where

import Data.Char qualified as C
import Data.Foldable (for_)
import Data.List qualified as L
import Data.Text qualified as T
import Hedgehog (annotate, annotateShow, assert, diff, forAll)
import Kengine.Tokenize (tokenize)
import Kengine.Types (Token (..))
import Test.Helpers.Generators (
  genNonAlphaText,
  genText,
  genTokenizableText,
 )
import Test.Hspec (Spec, describe, it, shouldBe)
import Test.Hspec.Hedgehog (hedgehog)

spec :: Spec
spec = do
  describe "tokenize" $ do
    it "lowercases all tokens" $ hedgehog $ do
      term <- forAll genText
      let tokenized = tokenize term
      let characters = tokenized >>= (\(Token tkn) -> T.unpack tkn)
      for_ characters $ \c -> do
        annotate ("found non lower char: " <> [c])
        assert (C.toLower c == c)
    it "splits on non alphanum chars" $ hedgehog $ do
      termsWithSep <- forAll genTokenizableText
      let term = T.intercalate "" termsWithSep
      let tokenized = tokenize term
      annotate "Tokenized must have same length as input"
      annotateShow tokenized
      diff (L.length tokenized) (==) (L.length termsWithSep)
    it "pure non alpha is discarded" $ hedgehog $ do
      nonAlphaTxt <- forAll genNonAlphaText
      let tokenized = tokenize nonAlphaTxt
      annotateShow tokenized
      diff tokenized (==) []
    it "tokenizes" $ do
      let tokenized = tokenize "Bio-Milch 3,5%"
      tokenized `shouldBe` (Token <$> ["bio", "milch", "3", "5"])
