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
import Kengine.Persistence.FileStore (FileStore (..), fileNameFromSegNum)
import Kengine.Types (
  BlockLocation (..),
  DocFieldStats (..),
  DocId (..),
  DocSparseIndex,
  Field (..),
  FieldIndex,
  FieldStats,
  IndexData (..),
  IndexName,
  IndexView,
  Mapping (..),
  Memtable (..),
  Segment (..),
  SegmentId (..),
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
      walPath = dir </> "log.jsonl"

  diskSegments <- fs.readSnapshots name
  segInfos <- liftIO (traverse (toSegmentInfo dir) diskSegments)
  wal <- liftIO (readWalInfo walPath)

  pure (B.toLazyText (renderPage name idxData segInfos walPath wal))

-- segment info --------------------------------------------------------------

data SegmentInfo = SegmentInfo
  { segId :: SegmentId
  , filePath :: FilePath
  , fileSize :: Integer
  , header :: Header
  , sparseIndex :: SparseIndex
  , docsSparseIndex :: DocSparseIndex
  }

toSegmentInfo :: FilePath -> (Header, FieldStats, Segment) -> IO SegmentInfo
toSegmentInfo dir (h, _, Segment{sparseIndex, docsSparseIndex, segNum}) = do
  let path = dir </> fileNameFromSegNum segNum
  sz <- getFileSize path
  pure
    SegmentInfo
      { segId = segNum
      , filePath = path
      , fileSize = sz
      , header = h
      , sparseIndex
      , docsSparseIndex
      }

-- WAL -----------------------------------------------------------------------

data WalInfo = NoWal | HasWal Integer Int [Text]

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
  [SegmentInfo] ->
  FilePath ->
  WalInfo ->
  B.Builder
renderPage name idxData segs walPath wal =
  pageStart name
    <> renderSummary idxData segs wal
    <> renderMapping idxData.mapping
    <> renderFieldStats idxData.memtable.fieldMeta
    <> renderMemtable idxData.memtable
    <> renderMemtableIndex idxData.memtable.fieldIdx
    <> renderTopTokens idxData.memtable.fieldIdx
    <> renderSegments segs
    <> renderWal walPath wal
    <> pageEnd

-- summary -------------------------------------------------------------------

renderSummary :: IndexData -> [SegmentInfo] -> WalInfo -> B.Builder
renderSummary idxData segs wal =
  raw "<section class=\"summary\">"
    <> card "memtable docs" (formatNum (Map.size idxData.memtable.docStore))
    <> card "segments" (formatNum (length segs))
    <> card "snapshot docs" (formatNum totalSnapDocs)
    <> card "snapshot size" (formatBytes totalSnapSize)
    <> card "wal entries" walEntries
    <> card "max doc id" (let DocId d = idxData.maxDocId in formatNum d)
    <> card "fields tracked" (formatNum (Map.size idxData.memtable.fieldMeta))
    <> raw "</section>"
  where
    totalSnapDocs = sum [fromIntegral s.header.docCount :: Int | s <- segs]
    totalSnapSize = sum ((.fileSize) <$> segs)
    walEntries = case wal of
      NoWal -> raw "—"
      HasWal _ n _ -> formatNum n
    card label val =
      raw "<div class=\"card\"><div class=\"card-label\">"
        <> raw label
        <> raw "</div><div class=\"card-value\">"
        <> val
        <> raw "</div></div>"

-- mapping -------------------------------------------------------------------

renderMapping :: Mapping -> B.Builder
renderMapping (Mapping fields) =
  section "mapping" Nothing
    <> raw "<table><tr><th>field</th><th>type</th><th>required</th></tr>"
    <> mconcat (row <$> Nel.toList fields)
    <> raw "</table>"
  where
    row Field{fieldName, sType, required} =
      raw "<tr><td><code>"
        <> escT (unrefine fieldName)
        <> raw "</code></td><td>"
        <> B.fromString (show sType)
        <> raw "</td><td>"
        <> raw (if required then "yes" else "—")
        <> raw "</td></tr>"

-- field stats (BM25 corpus) -------------------------------------------------

renderFieldStats :: FieldStats -> B.Builder
renderFieldStats fs
  | Map.null fs = mempty
  | otherwise =
      section
        "field stats"
        (Just "in-memory, full corpus (BM25 inputs); never pruned")
        <> raw "<table><tr><th>field</th>"
        <> raw "<th class=\"num\">docs</th>"
        <> raw "<th class=\"num\">total tokens</th>"
        <> raw "<th class=\"num\">avgdl</th>"
        <> raw "<th class=\"num\">memory ≈</th>"
        <> raw "</tr>"
        <> mconcat (renderRow <$> Map.toAscList fs)
        <> totalsRow
        <> raw "</table>"
  where
    renderRow (fn, perDoc) =
      let
        n = Map.size perDoc
        total = sum [t | DocFieldStats t <- Map.elems perDoc]
        avgdl :: Double
        avgdl = if n == 0 then 0 else fromIntegral total / fromIntegral n
        bytes = approxFieldStatsBytes n
       in
        raw "<tr><td><code>"
          <> escT (unrefine fn)
          <> raw "</code></td><td class=\"num\">"
          <> formatNum n
          <> raw "</td><td class=\"num\">"
          <> formatNum total
          <> raw "</td><td class=\"num\">"
          <> formatFloat 1 avgdl
          <> raw "</td><td class=\"num muted\">"
          <> formatBytes bytes
          <> raw "</td></tr>"
    totalEntries = sum (Map.size <$> Map.elems fs)
    totalBytes = approxFieldStatsBytes totalEntries
    totalsRow =
      raw "<tr class=\"totals\"><td><strong>total</strong></td>"
        <> raw "<td class=\"num\">"
        <> formatNum totalEntries
        <> raw "</td><td class=\"num muted\" colspan=\"2\">—</td>"
        <> raw "<td class=\"num\">"
        <> formatBytes totalBytes
        <> raw "</td></tr>"

approxFieldStatsBytes :: Int -> Integer
approxFieldStatsBytes n = fromIntegral n * 56

-- memtable ------------------------------------------------------------------

renderMemtable :: Memtable -> B.Builder
renderMemtable Memtable{docStore, fieldIdx} =
  section
    "memtable"
    (Just "in-memory writes since last flush; pruned on flush")
    <> raw "<table>"
    <> kv "documents" (formatNum (Map.size docStore))
    <> kv "fields with postings" (formatNum (Map.size fieldIdx))
    <> kv "distinct (field, term) pairs" (formatNum totalTerms)
    <> kv "postings entries (term, doc)" (formatNum totalPostings)
    <> raw "</table>"
  where
    totalTerms = sum (Map.size <$> Map.elems fieldIdx)
    totalPostings =
      sum (sum . fmap Map.size . Map.elems <$> Map.elems fieldIdx)

renderMemtableIndex :: FieldIndex -> B.Builder
renderMemtableIndex fi
  | Map.null fi = mempty
  | otherwise =
      section
        "memtable inverted index"
        (Just "per-field token + posting counts")
        <> raw "<table><tr><th>field</th>"
        <> raw "<th class=\"num\">tokens</th>"
        <> raw "<th class=\"num\">postings</th>"
        <> raw "<th class=\"num\">max df</th>"
        <> raw "<th>most common token</th></tr>"
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
    renderMost [] = raw "—"
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
      section
        "memtable top tokens by document frequency"
        (Just (T.pack ("top " <> show (length top) <> " of " <> show (length flat))))
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
    top = take 15 (L.sortOn (\(_, _, df) -> Down df) flat)

-- segments ------------------------------------------------------------------

renderSegments :: [SegmentInfo] -> B.Builder
renderSegments [] =
  section "segments" Nothing
    <> raw "<p class=\"muted\">no segments on disk yet. POST /flush-state to write one.</p>"
renderSegments segs =
  mconcat (renderSegment <$> L.sortOn (Down . (.segId)) segs)

renderSegment :: SegmentInfo -> B.Builder
renderSegment si@SegmentInfo{segId = SegmentId n, filePath, fileSize = sz, header = h, sparseIndex, docsSparseIndex} =
  section
    (T.pack ("segment " <> show n))
    (Just (T.pack ("seg_" <> show n <> ".bin")))
    <> pathLine filePath
    <> raw "<table>"
    <> kv "size" (formatBytes sz)
    <> kv "version" (formatNum (fromIntegral h.version :: Int))
    <> kv "mappingVersion" (formatNum (fromIntegral h.mappingVersion :: Int))
    <> kv "docCount" (formatNum (fromIntegral h.docCount :: Int))
    <> kv "fieldNames" (escT (T.intercalate ", " (unrefine <$> h.fieldNames)))
    <> kv "term sparse anchors" (formatNum (Map.size sparseIndex))
    <> kv "doc sparse anchors" (formatNum (Map.size docsSparseIndex))
    <> raw "</table>"
    <> renderLayoutBar (segmentSections si)
    <> renderSectionTable (segmentSections si)
    <> termSparseTable sparseIndex
    <> docSparseTable docsSparseIndex

segmentSections :: SegmentInfo -> [(Text, Text, Int, Int)]
segmentSections SegmentInfo{fileSize = sz, header = h} =
  [ ("header + token blocks", "#ed7d31", 0, fromIntegral h.termSparseOffset)
  ,
    ( "term sparse index"
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
  ,
    ( "field metadata"
    , "#ffc000"
    , fromIntegral h.docMetadataOffset
    , fromIntegral h.docSparseOffset
    )
  , ("doc sparse index", "#5b9bd5", fromIntegral h.docSparseOffset, fromIntegral sz)
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

termSparseTable :: SparseIndex -> B.Builder
termSparseTable sparse
  | Map.null sparse = mempty
  | otherwise =
      let entries = take 10 (Map.toList sparse)
       in raw "<h3 class=\"sub\">term sparse index "
            <> raw "<span class=\"muted\">"
            <> escT
              ( T.pack
                  ("first " <> show (length entries) <> " of " <> show (Map.size sparse) <> " block anchors")
              )
            <> raw "</span></h3>"
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

docSparseTable :: DocSparseIndex -> B.Builder
docSparseTable docSparse
  | Map.null docSparse = mempty
  | otherwise =
      let entries = take 10 (Map.toList docSparse)
       in raw "<h3 class=\"sub\">doc sparse index "
            <> raw "<span class=\"muted\">"
            <> escT
              ( T.pack
                  ( "first "
                      <> show (length entries)
                      <> " of "
                      <> show (Map.size docSparse)
                      <> " block anchors"
                  )
              )
            <> raw "</span></h3>"
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

-- WAL -----------------------------------------------------------------------

renderWal :: FilePath -> WalInfo -> B.Builder
renderWal path NoWal =
  section "write-ahead log" Nothing
    <> pathLine path
    <> raw "<p class=\"muted\">no log file.</p>"
renderWal path (HasWal sz entryCount tailLines) =
  section "write-ahead log" Nothing
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

-- shell ---------------------------------------------------------------------

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

section :: Text -> Maybe Text -> B.Builder
section title Nothing = raw "<h2>" <> escT title <> raw "</h2>"
section title (Just sub) =
  raw "<h2>"
    <> escT title
    <> raw " <span class=\"muted\">"
    <> escT sub
    <> raw "</span></h2>"

css :: Text
css =
  T.concat
    [ "<style>"
    , ":root{--fg:#222;--muted:#888;--accent:#5b9bd5;--rule:#e5e5e5;--card-bg:#f5f5f5}"
    , "*{box-sizing:border-box}"
    , "body{font:13px/1.45 ui-monospace,SFMono-Regular,Menlo,monospace;max-width:1100px;margin:1.5em auto;padding:0 1em;color:var(--fg)}"
    , "h1{font-size:18px;margin:0 0 .25em}"
    , "h2{font-size:13px;margin:1.6em 0 .5em;border-bottom:1px solid var(--rule);padding-bottom:3px;font-weight:700;text-transform:uppercase;letter-spacing:.04em}"
    , "h3.sub{font-size:11px;margin:1.1em 0 .35em;font-weight:600;text-transform:uppercase;letter-spacing:.04em;color:#555}"
    , "table{border-collapse:collapse;width:100%}"
    , "td,th{padding:3px 14px 3px 0;vertical-align:top;text-align:left}"
    , "th{font-weight:600;color:#555;font-size:11px;text-transform:uppercase;letter-spacing:.03em}"
    , "tr.totals td{border-top:1px solid var(--rule);padding-top:6px;color:#444}"
    , ".num{font-variant-numeric:tabular-nums;text-align:right;padding-right:0;white-space:nowrap}"
    , ".muted{color:var(--muted);font-weight:400}"
    , ".path{color:#666;font-size:11px;margin:.2em 0 .5em;word-break:break-all}"
    , ".bar{display:flex;height:22px;border:1px solid #bbb;border-radius:3px;overflow:hidden;margin:.6em 0}"
    , ".bar>div{display:flex;align-items:center;justify-content:center;color:#fff;font-size:10px;white-space:nowrap;overflow:hidden;text-shadow:0 1px 0 rgba(0,0,0,.3)}"
    , ".sw{display:inline-block;width:10px;height:10px;border-radius:1px;margin-right:.4em;vertical-align:middle}"
    , "code{font-family:ui-monospace,monospace;background:var(--card-bg);padding:0 4px;border-radius:2px}"
    , "pre.tail{background:var(--card-bg);padding:8px;font-size:11px;white-space:pre-wrap;word-break:break-all;margin:.3em 0;max-height:9em;overflow:auto}"
    , ".summary{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:.6em;margin:.6em 0 1.2em}"
    , ".card{background:var(--card-bg);border-radius:4px;padding:.6em .8em;border-left:3px solid var(--accent)}"
    , ".card-label{font-size:10px;text-transform:uppercase;letter-spacing:.05em;color:var(--muted);font-weight:600}"
    , ".card-value{font-size:18px;font-weight:600;font-variant-numeric:tabular-nums;margin-top:.15em}"
    , "</style>"
    ]

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

formatFloat :: Int -> Double -> B.Builder
formatFloat digits d = B.fromString (showFFloat (Just digits) d "")

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
