module Kengine.Errors (KengineError (..), liftIOE, IOE, Result) where

import Control.Exception (IOException, try)
import Control.Monad.Trans.Except (ExceptT (..))
import Data.Bifunctor (first)
import Data.Text (Text, pack)

data KengineError
  = ParseError Text
  | ValidationError Text
  | SearchError Text
  | FileError Text
  deriving stock (Show, Eq)

liftIOE :: (Text -> e) -> IO a -> IOE e a
liftIOE mkError action = ExceptT $ first (mkError . pack . show) <$> try @IOException action

type IOE e a = ExceptT e IO a
type Result a = IOE KengineError a
