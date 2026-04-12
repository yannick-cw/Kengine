module Server (runServer, routes) where

import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Except (ExceptT, runExceptT)
import Data.Aeson (ToJSON)
import Data.Text.Lazy qualified as TL
import Kengine.Store.InMemory (Store (..), mkStore)
import Network.HTTP.Types (status400)
import Web.Scotty (
  ActionM,
  get,
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
runServer =
  mkStore >>= scotty 3333 . routes

routes :: Store -> ScottyT IO ()
routes Store{createIndex, indexDoc, search} = do
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

resOrErr :: (ToJSON a, Show e) => ExceptT e IO a -> ActionM ()
resOrErr action = do
  result <- liftIO $ runExceptT action
  case result of
    Right res -> json res
    Left err -> do
      status status400
      text $ TL.show err
