-- !!! THIS IS FULLY CLAUDE CODE GENERATED !!! --
-- Just as a nice additional tools

{- | Debug-only HTML overview of an index's on-disk layout and in-memory state.

Goes through FileStore for path + snapshot reads so this stays in sync with
the production layout. Read-only and only wired into one debug endpoint.
-}
module Kengine.Debug.Layout (renderLayout) where

import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Except (throwE)
import Data.ByteString qualified as BS
import Data.ByteString.Char8 qualified as BSC
import Data.List qualified as L
import Data.List.NonEmpty qualified as Nel
import Data.Map qualified as Map
import Data.Maybe (fromMaybe)
import Data.Ord (Down (Down))
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding (decodeUtf8With)
import Data.Text.Encoding.Error (lenientDecode)
import Data.Text.Lazy qualified as LT
import Data.Text.Lazy.Builder qualified as B
import Data.Text.Lazy.Builder.Int qualified as B
import GHC.Conc qualified as TVar
import Kengine.Errors (IOE, KengineError (SearchError))
import Kengine.Persistence.Binary (Header (..))
import Kengine.Persistence.FileStore (FileStore (..))
import Kengine.Types (
  BlockLocation (..),
  DocId (..),
  DocSparseIndex,
  Field (..),
  FieldIndex,
  IndexData (..),
  IndexName,
  IndexView,
  Mapping (..),
  Memtable (..),
  SparseIndex,
  Token (..),
 )
import Numeric (showFFloat)
import Refined (unrefine)
import System.Directory (doesFileExist, getFileSize)
import System.FilePath ((</>))

renderLayout ::
  FileStore -> TVar.TVar IndexView -> IndexName -> IOE KengineError LT.Text
renderLayout fs viewVar name = do
  view <- liftIO (TVar.readTVarIO viewVar)
  idxVar <- case Map.lookup name view of
    Just v -> pure v
    Nothing -> throwE (SearchError ("No index found: " <> unrefine name))
  idxData <- liftIO (TVar.readTVarIO idxVar)

  let dir = fs.indexDir name
      snapPath = dir </> "snapshot.bin"
      walPath = dir </> "log.jsonl"

  snap <- readSnapshotInfo fs name snapPath
  wal <- liftIO (readWalInfo walPath)
  diskFieldIdx <- fs.readSnapshotFieldIndex name
  let mergedFi =
        Map.unionWith
          (Map.unionWith Map.union)
          idxData.memtable.fieldIdx
          (fromMaybe Map.empty diskFieldIdx)

  pure (B.toLazyText (renderPage name idxData mergedFi snapPath snap walPath wal))

-- on-disk reads -------------------------------------------------------------

data SnapshotInfo
  = NoSnapshot
  | GoodSnapshot Integer Header SparseIndex DocSparseIndex

data WalInfo = NoWal | HasWal Integer Int [Text]

readSnapshotInfo :: FileStore -> IndexName -> FilePath -> IOE KengineError SnapshotInfo
readSnapshotInfo fs name path = do
  parsed <- fs.readSnapshot name
  case parsed of
    Nothing -> pure NoSnapshot
    Just (h, sparse, docSparse, _meta) -> do
      sz <- liftIO (getFileSize path)
      pure (GoodSnapshot sz h sparse docSparse)

readWalInfo :: FilePath -> IO WalInfo
readWalInfo path = do
  exists <- doesFileExist path
  if not exists
    then pure NoWal
    else do
      sz <- getFileSize path
      bs <- BS.readFile path
      let ls = filter (not . BS.null) (BSC.lines bs)
          n = length ls
          decode = decodeUtf8With lenientDecode
          tlast = decode <$> drop (max 0 (n - 3)) ls
      pure (HasWal sz n tlast)

-- rendering -----------------------------------------------------------------

renderPage ::
  IndexName ->
  IndexData ->
  FieldIndex ->
  FilePath ->
  SnapshotInfo ->
  FilePath ->
  WalInfo ->
  B.Builder
renderPage name idxData mergedFi snapPath snap walPath wal =
  pageStart name
    <> renderMapping idxData.mapping
    <> renderMemtable idxData.memtable
    <> renderInvertedIndex mergedFi
    <> renderSnapshot snapPath snap
    <> renderSparseIndex snap
    <> renderWal walPath wal
    <> pageEnd

pageStart :: IndexName -> B.Builder
pageStart name =
  raw "<!DOCTYPE html>\n<html><head><meta charset=\"utf-8\"><title>kengine layout: "
    <> escT (unrefine name)
    <> raw "</title>"
    <> raw css
    <> raw "</head><body><h1>"
    <> escT (unrefine name)
    <> raw "</h1>"

pageEnd :: B.Builder
pageEnd = raw "</body></html>"

css :: Text
css =
  T.concat
    [ "<style>"
    , "body{font:13px/1.45 ui-monospace,SFMono-Regular,Menlo,monospace;max-width:980px;margin:1.5em auto;padding:0 1em;color:#222}"
    , "h1{font-size:18px;margin:0 0 .25em}"
    , "h2{font-size:13px;margin:1.4em 0 .35em;border-bottom:1px solid #ddd;padding-bottom:2px;font-weight:700;text-transform:uppercase;letter-spacing:.04em}"
    , "table{border-collapse:collapse;width:100%}"
    , "td,th{padding:2px 12px 2px 0;vertical-align:top;text-align:left}"
    , "th{font-weight:600;color:#555}"
    , ".num{font-variant-numeric:tabular-nums;text-align:right;padding-right:0;white-space:nowrap}"
    , ".muted{color:#888;font-weight:400}"
    , ".path{color:#666;font-size:11px;margin-bottom:.4em;word-break:break-all}"
    , ".bar{display:flex;height:20px;border:1px solid #bbb;border-radius:2px;overflow:hidden;margin:.4em 0}"
    , ".bar>div{display:flex;align-items:center;justify-content:center;color:#fff;font-size:10px;white-space:nowrap;overflow:hidden;text-shadow:0 1px 0 rgba(0,0,0,.3)}"
    , ".sw{display:inline-block;width:10px;height:10px;border-radius:1px;margin-right:.3em;vertical-align:middle}"
    , "code{font-family:ui-monospace,monospace;background:#f5f5f5;padding:0 4px;border-radius:2px}"
    , "pre.tail{background:#f5f5f5;padding:6px;font-size:11px;white-space:pre-wrap;word-break:break-all;margin:.3em 0;max-height:9em;overflow:hidden}"
    , "</style>"
    ]

renderMapping :: Mapping -> B.Builder
renderMapping (Mapping fields) =
  raw "<h2>mapping</h2><table><tr><th>field</th><th>type</th><th>required</th></tr>"
    <> mconcat (row <$> Nel.toList fields)
    <> raw "</table>"
  where
    row Field{fieldName, sType, required} =
      raw "<tr><td><code>"
        <> escT (unrefine fieldName)
        <> raw "</code></td><td>"
        <> B.fromString (show sType)
        <> raw "</td><td>"
        <> raw (if required then "yes" else "no")
        <> raw "</td></tr>"

renderMemtable :: Memtable -> B.Builder
renderMemtable Memtable{docStore, fieldIdx, fieldMeta} =
  raw "<h2>memtable <span class=\"muted\">in-memory, since last flush</span></h2><table>"
    <> kv "documents" (formatNum (Map.size docStore))
    <> kv "fields with postings" (formatNum (Map.size fieldIdx))
    <> kv "distinct (field, term) pairs" (formatNum totalTerms)
    <> kv "postings entries (term, doc)" (formatNum totalPostings)
    <> kv "fields with stats" (formatNum (Map.size fieldMeta))
    <> raw "</table>"
  where
    totalTerms = sum (Map.size <$> Map.elems fieldIdx)
    totalPostings =
      sum (sum . fmap Map.size . Map.elems <$> Map.elems fieldIdx)

renderInvertedIndex :: FieldIndex -> B.Builder
renderInvertedIndex fi
  | Map.null fi = mempty
  | otherwise = renderFieldSummary fi <> renderTopTokens fi

renderFieldSummary :: FieldIndex -> B.Builder
renderFieldSummary fi =
  raw "<h2>inverted index <span class=\"muted\">memtable + on-disk, merged</span></h2>"
    <> raw
      "<table><tr><th>field</th><th class=\"num\">tokens</th><th class=\"num\">postings</th><th class=\"num\">max df</th><th>most common token</th></tr>"
    <> mconcat (renderRow <$> Map.toAscList fi)
    <> raw "</table>"
  where
    renderRow (fn, tm) =
      let
        dfs = Map.size <$> Map.elems tm
        tokenCount = Map.size tm
        postingsTotal = sum dfs
        maxDf = if null dfs then 0 else maximum dfs
        ranked = L.sortOn (Down . snd) [(tk, Map.size pl) | (tk, pl) <- Map.toList tm]
       in
        raw "<tr><td><code>"
          <> escT (unrefine fn)
          <> raw "</code></td><td class=\"num\">"
          <> formatNum tokenCount
          <> raw "</td><td class=\"num\">"
          <> formatNum postingsTotal
          <> raw "</td><td class=\"num\">"
          <> formatNum maxDf
          <> raw "</td><td>"
          <> renderMost ranked
          <> raw "</td></tr>"
    renderMost [] = raw "-"
    renderMost ((Token t, df) : _) =
      raw "<code>"
        <> escT t
        <> raw "</code> <span class=\"muted\">("
        <> formatNum df
        <> raw ")</span>"

renderTopTokens :: FieldIndex -> B.Builder
renderTopTokens fi
  | null flat = mempty
  | otherwise =
      raw "<h2>top tokens by document frequency <span class=\"muted\">top "
        <> B.decimal (length top)
        <> raw " of "
        <> B.decimal (length flat)
        <> raw "</span></h2>"
        <> raw "<table><tr><th>field</th><th>token</th><th class=\"num\">df</th></tr>"
        <> mconcat
          [ raw "<tr><td><code>"
              <> escT (unrefine fn)
              <> raw "</code></td><td><code>"
              <> escT t
              <> raw "</code></td><td class=\"num\">"
              <> formatNum df
              <> raw "</td></tr>"
          | (fn, Token t, df) <- top
          ]
        <> raw "</table>"
  where
    flat = [(fn, tk, Map.size pl) | (fn, tm) <- Map.toList fi, (tk, pl) <- Map.toList tm]
    top = take 10 (L.sortOn (\(_, _, df) -> Down df) flat)

renderSnapshot :: FilePath -> SnapshotInfo -> B.Builder
renderSnapshot path NoSnapshot =
  raw "<h2>snapshot</h2>"
    <> pathLine path
    <> raw "<p class=\"muted\">no snapshot yet. POST /flush-state to write one.</p>"
renderSnapshot path (GoodSnapshot sz h sparse _docSparse) =
  raw "<h2>snapshot</h2>"
    <> pathLine path
    <> raw "<table>"
    <> kv "size" (formatBytes sz)
    <> kv "version" (formatNum (fromIntegral h.version :: Int))
    <> kv "mappingVersion" (formatNum (fromIntegral h.mappingVersion :: Int))
    <> kv "docCount" (formatNum (fromIntegral h.docCount :: Int))
    <> kv "fieldNames" (escT (T.intercalate ", " (unrefine <$> h.fieldNames)))
    <> kv "sparse index entries" (formatNum (Map.size sparse))
    <> raw "</table>"
    <> renderLayoutBar sections
    <> renderSectionTable sections
  where
    sections = snapshotSections sz h

snapshotSections :: Integer -> Header -> [(Text, Text, Int, Int)]
snapshotSections sz h =
  [ ("header + token blocks", "#ed7d31", 0, fromIntegral h.termSparseOffset)
  ,
    ( "sparse index"
    , "#a5a5a5"
    , fromIntegral h.termSparseOffset
    , fromIntegral h.storedFieldsOffset
    )
  ,
    ( "stored docs"
    , "#70ad47"
    , fromIntegral h.storedFieldsOffset
    , fromIntegral h.docMetadataOffset
    )
  , ("field metadata", "#ffc000", fromIntegral h.docMetadataOffset, fromIntegral sz)
  ]

renderLayoutBar :: [(Text, Text, Int, Int)] -> B.Builder
renderLayoutBar secs =
  raw "<div class=\"bar\">"
    <> mconcat
      [ raw "<div title=\""
          <> escT name
          <> raw "  "
          <> formatBytes (fromIntegral (b - a))
          <> raw "\" style=\"flex:"
          <> B.decimal (max 1 (b - a))
          <> raw " 0 0;background:"
          <> raw color
          <> raw ";\">"
          <> (if (b - a) * 8 > total then escT name else mempty)
          <> raw "</div>"
      | (name, color, a, b) <- secs
      ]
    <> raw "</div>"
  where
    total = max 1 (sum [b - a | (_, _, a, b) <- secs])

renderSectionTable :: [(Text, Text, Int, Int)] -> B.Builder
renderSectionTable secs =
  raw
    "<table><tr><th>section</th><th class=\"num\">start</th><th class=\"num\">end</th><th class=\"num\">size</th></tr>"
    <> mconcat
      [ raw "<tr><td><span class=\"sw\" style=\"background:"
          <> raw color
          <> raw "\"></span>"
          <> escT name
          <> raw "</td><td class=\"num\">"
          <> formatNum a
          <> raw "</td><td class=\"num\">"
          <> formatNum b
          <> raw "</td><td class=\"num\">"
          <> formatBytes (fromIntegral (b - a))
          <> raw "</td></tr>"
      | (name, color, a, b) <- secs
      ]
    <> raw "</table>"

renderSparseIndex :: SnapshotInfo -> B.Builder
renderSparseIndex (GoodSnapshot _ _ sparse docSparse) =
  renderTermSparse sparse <> renderDocSparse docSparse
renderSparseIndex _ = mempty

renderTermSparse :: SparseIndex -> B.Builder
renderTermSparse sparse
  | Map.null sparse = mempty
  | otherwise =
      let entries = take 10 (Map.toList sparse)
       in raw "<h2>term sparse index <span class=\"muted\">first "
            <> B.decimal (length entries)
            <> raw " of "
            <> B.decimal (Map.size sparse)
            <> raw " block anchors</span></h2>"
            <> raw
              "<table><tr><th>field</th><th>token (block start)</th><th class=\"num\">byte offset</th><th class=\"num\">block size</th></tr>"
            <> mconcat
              [ raw "<tr><td><code>"
                  <> escT (unrefine fn)
                  <> raw "</code></td><td><code>"
                  <> escT tk
                  <> raw "</code></td><td class=\"num\">"
                  <> formatNum firstByte
                  <> raw "</td><td class=\"num\">"
                  <> formatBytes (fromIntegral size)
                  <> raw "</td></tr>"
              | ((fn, Token tk), BlockLocation{firstByte, size}) <- entries
              ]
            <> raw "</table>"

renderDocSparse :: DocSparseIndex -> B.Builder
renderDocSparse docSparse
  | Map.null docSparse = mempty
  | otherwise =
      let entries = take 10 (Map.toList docSparse)
       in raw "<h2>doc sparse index <span class=\"muted\">first "
            <> B.decimal (length entries)
            <> raw " of "
            <> B.decimal (Map.size docSparse)
            <> raw " block anchors</span></h2>"
            <> raw
              "<table><tr><th>first docId in block</th><th class=\"num\">byte offset</th><th class=\"num\">block size</th></tr>"
            <> mconcat
              [ raw "<tr><td><code>"
                  <> B.decimal dId
                  <> raw "</code></td><td class=\"num\">"
                  <> formatNum firstByte
                  <> raw "</td><td class=\"num\">"
                  <> formatBytes (fromIntegral size)
                  <> raw "</td></tr>"
              | (DocId dId, BlockLocation{firstByte, size}) <- entries
              ]
            <> raw "</table>"

renderWal :: FilePath -> WalInfo -> B.Builder
renderWal path NoWal =
  raw "<h2>write-ahead log</h2>"
    <> pathLine path
    <> raw "<p class=\"muted\">no log file.</p>"
renderWal path (HasWal sz entryCount tailLines) =
  raw "<h2>write-ahead log</h2>"
    <> pathLine path
    <> raw "<table>"
    <> kv "size" (formatBytes sz)
    <> kv "entries" (formatNum entryCount)
    <> raw "</table>"
    <> if null tailLines
      then mempty
      else
        raw "<p class=\"muted\">last entries:</p><pre class=\"tail\">"
          <> mconcat [escT (truncateLine line) <> raw "\n" | line <- tailLines]
          <> raw "</pre>"
  where
    truncateLine t = if T.length t > 220 then T.take 217 t <> "..." else t

-- helpers -------------------------------------------------------------------

raw :: Text -> B.Builder
raw = B.fromText

kv :: Text -> B.Builder -> B.Builder
kv k v = raw "<tr><th>" <> raw k <> raw "</th><td>" <> v <> raw "</td></tr>"

pathLine :: FilePath -> B.Builder
pathLine p = raw "<div class=\"path\">" <> escT (T.pack p) <> raw "</div>"

formatBytes :: Integer -> B.Builder
formatBytes n
  | n < 1024 = B.decimal n <> raw " B"
  | n < 1024 * 1024 = decimal2 (fromIntegral n / 1024.0) <> raw " KB"
  | n < 1024 * 1024 * 1024 = decimal2 (fromIntegral n / 1048576.0) <> raw " MB"
  | otherwise = decimal2 (fromIntegral n / 1073741824.0) <> raw " GB"
  where
    decimal2 :: Double -> B.Builder
    decimal2 d = B.fromString (showFFloat (Just 2) d "")

formatNum :: Int -> B.Builder
formatNum = B.fromString . groupThousands . show
  where
    groupThousands s = case s of
      ('-' : rest) -> '-' : group rest
      _ -> group s
    group = reverse . L.intercalate "," . chunksOf 3 . reverse
    chunksOf k xs = case splitAt k xs of
      ([], _) -> []
      (h, t) -> h : chunksOf k t

escT :: Text -> B.Builder
escT = B.fromText . T.concatMap esc
  where
    esc '<' = "&lt;"
    esc '>' = "&gt;"
    esc '&' = "&amp;"
    esc '"' = "&quot;"
    esc '\'' = "&#39;"
    esc c = T.singleton c
