module Kengine.Errors (KengineError (..), liftIOE, IOE) where

import Control.Exception (IOException, try)
import Control.Monad.Trans.Except (ExceptT (..))
import Data.Bifunctor (first)
import Data.Text (Text, pack)

data KengineError = SearchError Text | FileError Text deriving stock (Show, Eq)

liftIOE :: (Text -> e) -> IO a -> IOE e a
liftIOE mkError action = ExceptT $ first (mkError . pack . show) <$> try @IOException action

type IOE e a = ExceptT e IO a
