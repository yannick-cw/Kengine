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

-- | Run an IO action and convert any 'IOException' into a typed error.
-- The 'context' is prepended to the exception message so the resulting error
-- says where in the codebase it happened. Each call site picks its own label.
liftIOE :: (Text -> e) -> Text -> IO a -> IOE e a
liftIOE mkError context action =
  ExceptT $
    first (\ex -> mkError (context <> ": " <> pack (show ex)))
      <$> try @IOException action

type IOE e a = ExceptT e IO a
type Result a = IOE KengineError a
