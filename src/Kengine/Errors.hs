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

liftIOE :: (Text -> e) -> Text -> IO a -> IOE e a
liftIOE mkError context action =
  ExceptT $
    first (\ex -> mkError (context <> ": " <> pack (show ex)))
      <$> try @IOException action

type IOE e a = ExceptT e IO a
type Result a = IOE KengineError a
