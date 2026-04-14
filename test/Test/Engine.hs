module Test.Engine (tokenizeSpec, docSpec) where

import Data.Aeson qualified as AE
import Data.Aeson.Key qualified as AE.Key
import Data.Aeson.Types qualified as AE
import Data.Char qualified as C
import Data.Foldable (for_)
import Data.List qualified as L
import Data.Map qualified as Map
import Data.Text qualified as T
import Hedgehog (Opaque (unOpaque), annotate, annotateShow, assert, diff, forAll)
import Kengine.Engine (Token (..), parseDocument, tokenize)
import Kengine.Types (Document (Document), FieldValue, Term (..))
import Refined (unrefine)
import Test.Helpers.Generators (
  genDocForMapping,
  genNonAlphaText,
  genText,
  genTokenizableText,
  genValidMapping,
 )
import Test.Hspec (Spec, describe, it, shouldBe)
import Test.Hspec.Hedgehog (hedgehog)

tokenizeSpec :: Spec
tokenizeSpec = do
  describe "tokenize" $ do
    it "lowercases all tokens" $ hedgehog $ do
      term <- forAll genText
      let tokenized = tokenize (Term term)
      let characters = tokenized >>= (\(Token tkn) -> T.unpack tkn)
      for_ characters $ \c -> do
        annotate ("found non lower char: " <> [c])
        assert (C.toLower c == c)
    it "splits on non alphanum chars" $ hedgehog $ do
      termsWithSep <- forAll genTokenizableText
      let term = Term $ T.intercalate "" termsWithSep
      let tokenized = tokenize term
      annotate "Tokenized must have same length as input"
      annotateShow tokenized
      diff (L.length tokenized) (==) (L.length termsWithSep)
    it "pure non alpha is discarded" $ hedgehog $ do
      nonAlphaTxt <- forAll $ genNonAlphaText
      let tokenized = tokenize $ Term nonAlphaTxt
      annotateShow tokenized
      diff tokenized (==) []
    it "tokenizes" $ do
      let tokenized = tokenize $ Term "Bio-Milch 3,5%"
      tokenized `shouldBe` (Token <$> ["bio", "milch", "3", "5"])

docSpec :: Spec
docSpec = do
  describe "validate a document" $ do
    it "accepts valid documents for a mapping" $ hedgehog $ do
      mapping <- forAll genValidMapping
      d@(Document doc) <- forAll $ genDocForMapping mapping
      let jsonObj =
            AE.object $
              (\(name, fieldValue) -> (AE.Key.fromText $ unrefine name, fieldValToJSON fieldValue))
                <$> Map.toList doc
      let validatedDoc = parseDocument jsonObj mapping
      annotateShow validatedDoc
      diff validatedDoc (==) (Right d)

fieldValToJSON :: FieldValue -> AE.Value
fieldValToJSON = undefined
