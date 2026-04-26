module Kengine.Server (runServer, routes) where

import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Except (ExceptT, runExceptT)
import Data.Aeson (ToJSON)
import Data.Text.Lazy qualified as TL
import Kengine.Persistence.FileStore (mkFileStore)
import Kengine.Store (Store (..), mkStore)
import Network.HTTP.Types (status400)
import System.Exit (exitFailure)
import System.IO (hPutStrLn, stderr)
import Web.Scotty (
  ActionM,
  get,
  html,
  json,
  jsonData,
  pathParam,
  post,
  put,
  queryParam,
  scotty,
  status,
  text,
 )
import Web.Scotty.Internal.Types (ScottyT)

runServer :: IO ()
runServer = do
  fileStore <- mkFileStore
  dbStore <- runExceptT $ mkStore fileStore
  case dbStore of
    Right store -> scotty 3333 (routes store)
    Left err -> do
      hPutStrLn stderr ("Failed to start kengine: " <> show err)
      exitFailure

routes :: Store -> ScottyT IO ()
routes Store{createIndex, indexDoc, search, flushState, debugLayout} = do
  get "/indexes/:name/search" $ do
    n <- pathParam "name"
    q <- queryParam "q"
    resOrErr (search n q)
  put "/indexes/:name" $ do
    n <- pathParam "name"
    m <- jsonData
    resOrErr (createIndex n m)
  post "/indexes/:name/documents" $ do
    n <- pathParam "name"
    r <- jsonData
    resOrErr (indexDoc n r)
  post "/flush-state" $ do
    resOrErr flushState
  get "/indexes/:name/_layout" $ do
    n <- pathParam "name"
    result <- liftIO $ runExceptT (debugLayout n)
    case result of
      Right h -> html h
      Left err -> do
        status status400
        text $ TL.show err

resOrErr :: (ToJSON a, Show e) => ExceptT e IO a -> ActionM ()
resOrErr action = do
  result <- liftIO $ runExceptT action
  case result of
    Right res -> json res
    Left err -> do
      status status400
      text $ TL.show err
