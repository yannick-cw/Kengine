module Test.Engine (spec) where

import Data.Char qualified as C
import Data.Foldable (for_)
import Data.List qualified as L
import Data.Text qualified as T
import Hedgehog (annotate, assert, diff, forAll)
import Kengine.Engine (Token (..), tokenize)
import Kengine.Types (Term (..))
import Test.Helpers.Generators (genText, genTokenizableText)
import Test.Hspec (Spec, describe, it)
import Test.Hspec.Hedgehog (hedgehog)

spec :: Spec
spec =
  describe "tokenize" $ do
    it "lowercases all tokens" $ hedgehog $ do
      term <- forAll genText
      let tokenized = tokenize (Term term)
      let characters = tokenized >>= (\(Token tkn) -> T.unpack tkn)
      for_ characters $ \c -> do
        annotate ("found non lower char: " <> [c])
        assert (not (C.isAlpha c) || C.isLower c)
    it "splits on non alphanum chars" $ hedgehog $ do
      termsWithSep <- forAll genTokenizableText
      let term = Term $ T.intercalate "" termsWithSep
      let tokenized = tokenize term
      annotate "Tokenized must have same length as input"
      diff (L.length tokenized) (==) (L.length termsWithSep)
