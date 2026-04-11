module Server (runServer, routes) where

import Control.Monad.IO.Class (liftIO)
import Kengine.Store.InMemory (Store (..), mkStore)
import Web.Scotty (get, json, jsonData, pathParam, post, put, queryParam, scotty)
import Web.Scotty.Internal.Types (ScottyT)

runServer :: IO ()
runServer =
  mkStore >>= scotty 3000 . routes

routes :: Store -> ScottyT IO ()
routes Store{createIndex, indexDoc, search} = do
  get "/indexes/:name/search" $ do
    n <- pathParam "name"
    q <- queryParam "q"
    searchRes <- liftIO $ search n q
    json searchRes
  put "/indexes/:name" $ do
    n <- pathParam "name"
    m <- jsonData
    indexCreation <- liftIO $ createIndex n m
    json indexCreation
  post "/indexes/:name/documents" $ do
    n <- pathParam "name"
    r <- jsonData
    indexRes <- liftIO $ indexDoc n r
    json indexRes
