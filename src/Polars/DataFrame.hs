{-# LANGUAGE OverloadedStrings #-}

{- |
Module      : Polars.DataFrame
Description : Safe eager DataFrame operations backed by Rust Polars handles.

A DataFrame wraps a Rust-owned Polars DataFrame handle in a ForeignPtr finalizer.
The module supports eager readers and writers, metadata queries, text rendering, and construction from owned Series handles.
Functions return Either so Polars and FFI failures stay explicit.
-}
module Polars.DataFrame
    ( DataFrame
    , dataFrame
    , dataFrameDropColumns
    , dataFrameDropNulls
    , dataFrameNullCount
    , dataFrameRename
    , dataFrameReverse
    , dataFrameSelect
    , dataFrameSlice
    , head
    , height
    , readCsv
    , readParquet
    , schema
    , shape
    , tail
    , toText
    , width
    , writeCsv
    , writeParquet
    ) where

import Prelude hiding (head, tail)

import qualified Data.ByteString as BS
import Data.Text (Text)
import qualified Data.Text.Encoding as TE
import Foreign.C.String (CString)
import Foreign.C.Types (CBool (..), CInt, CSize)
import Data.Word (Word64)
import Foreign.Marshal.Alloc (alloca)
import Foreign.Marshal.Array (withArray)
import Foreign.Ptr (Ptr, nullPtr)
import Foreign.Storable (peek, poke)

import Polars.Error (PolarsError (..), PolarsErrorCode (InvalidArgument))
import Polars.Internal.Bytes (copyAndFreeBytes)
import Polars.Internal.CString (withFilePathCString, withTextCString)
import Polars.Internal.Managed (DataFrame, Series, mkDataFrame, withDataFrame, withSeries)
import Polars.Internal.Raw
    ( RawBytes
    , RawDataFrame
    , RawError
    , RawSeries
    , phs_dataframe_drop
    , phs_dataframe_drop_nulls
    , phs_dataframe_head
    , phs_dataframe_new
    , phs_dataframe_height
    , phs_dataframe_null_count
    , phs_dataframe_rename
    , phs_dataframe_reverse
    , phs_dataframe_schema
    , phs_dataframe_select
    , phs_dataframe_shape
    , phs_dataframe_slice
    , phs_dataframe_tail
    , phs_dataframe_to_text
    , phs_dataframe_width
    , phs_read_csv
    , phs_read_parquet
    , phs_write_csv
    , phs_write_parquet
    )
import Polars.Internal.Result (consumeError, nullPointerError)
import Polars.Schema (Field (..), parseDataType)

readCsv :: FilePath -> IO (Either PolarsError DataFrame)
readCsv path = withFilePathCString path $ \cPath -> dataframeOut (phs_read_csv cPath)

readParquet :: FilePath -> IO (Either PolarsError DataFrame)
readParquet path = withFilePathCString path $ \cPath -> dataframeOut (phs_read_parquet cPath)

writeCsv :: FilePath -> DataFrame -> IO (Either PolarsError ())
writeCsv path df =
    withFilePathCString path $ \cPath ->
        withDataFrame df $ \ptr ->
            unitOut (phs_write_csv cPath ptr)

writeParquet :: FilePath -> DataFrame -> IO (Either PolarsError ())
writeParquet path df =
    withFilePathCString path $ \cPath ->
        withDataFrame df $ \ptr ->
            unitOut (phs_write_parquet cPath ptr)

dataFrame :: [Series] -> IO (Either PolarsError DataFrame)
dataFrame values = withSeriesArray values $ \ptr len -> dataframeOut (phs_dataframe_new ptr len)

dataFrameSelect :: [Text] -> DataFrame -> IO (Either PolarsError DataFrame)
dataFrameSelect [] _ = pure (Left (invalidArgument "dataFrameSelect requires at least one column name"))
dataFrameSelect names df = withDataFrame df $ \ptr -> withCStringList names $ \nameArray len ->
    dataframeOut (phs_dataframe_select ptr nameArray len)

dataFrameDropColumns :: [Text] -> DataFrame -> IO (Either PolarsError DataFrame)
dataFrameDropColumns [] _ = pure (Left (invalidArgument "dataFrameDropColumns requires at least one column name"))
dataFrameDropColumns names df = withDataFrame df $ \ptr -> withCStringList names $ \nameArray len ->
    dataframeOut (phs_dataframe_drop ptr nameArray len)

dataFrameRename :: [(Text, Text)] -> DataFrame -> IO (Either PolarsError DataFrame)
dataFrameRename [] _ = pure (Left (invalidArgument "dataFrameRename requires at least one column pair"))
dataFrameRename pairs df = withDataFrame df $ \ptr -> withRenamePairs pairs $ \existingArray newArray len ->
    dataframeOut (phs_dataframe_rename ptr existingArray newArray len)

dataFrameSlice :: Int -> Int -> DataFrame -> IO (Either PolarsError DataFrame)
dataFrameSlice offset len df = case nonNegativeWord64 "dataFrameSlice length" len of
    Left err -> pure (Left err)
    Right lenWord -> withDataFrame df $ \ptr ->
        dataframeOut (phs_dataframe_slice ptr (fromIntegral offset) lenWord)

dataFrameReverse :: DataFrame -> IO (Either PolarsError DataFrame)
dataFrameReverse df = withDataFrame df $ \ptr -> dataframeOut (phs_dataframe_reverse ptr)

dataFrameDropNulls :: Maybe [Text] -> DataFrame -> IO (Either PolarsError DataFrame)
dataFrameDropNulls (Just []) _ = pure (Left (invalidArgument "dataFrameDropNulls subset requires at least one column name"))
dataFrameDropNulls subset df = withDataFrame df $ \ptr -> withMaybeCStringList subset $ \nameArray len hasSubset ->
    dataframeOut (phs_dataframe_drop_nulls ptr nameArray len (toCBool hasSubset))

dataFrameNullCount :: DataFrame -> IO (Either PolarsError DataFrame)
dataFrameNullCount df = withDataFrame df $ \ptr -> dataframeOut (phs_dataframe_null_count ptr)

height :: DataFrame -> IO (Either PolarsError Int)
height df = withDataFrame df $ \ptr -> word64Out (phs_dataframe_height ptr)

width :: DataFrame -> IO (Either PolarsError Int)
width df = withDataFrame df $ \ptr -> word64Out (phs_dataframe_width ptr)

shape :: DataFrame -> IO (Either PolarsError (Int, Int))
shape df = withDataFrame df $ \ptr ->
    alloca $ \heightPtr ->
        alloca $ \widthPtr ->
            alloca $ \errPtr -> do
                poke errPtr nullPtr
                status <- phs_dataframe_shape ptr heightPtr widthPtr errPtr
                if status == 0
                    then do
                        h <- word64ToInt <$> peek heightPtr
                        w <- word64ToInt <$> peek widthPtr
                        pure ((,) <$> h <*> w)
                    else Left <$> (consumeError status =<< peek errPtr)

schema :: DataFrame -> IO (Either PolarsError [Field])
schema df = bytesOut df phs_dataframe_schema (parseSchemaBytes . BS.split 0)

head :: Int -> DataFrame -> IO (Either PolarsError DataFrame)
head n df
    | n < 0 = pure (Left (nullPointerError "head count"))
    | otherwise = withDataFrame df $ \ptr -> dataframeOut (phs_dataframe_head ptr (fromIntegral n))

tail :: Int -> DataFrame -> IO (Either PolarsError DataFrame)
tail n df
    | n < 0 = pure (Left (nullPointerError "tail count"))
    | otherwise = withDataFrame df $ \ptr -> dataframeOut (phs_dataframe_tail ptr (fromIntegral n))

toText :: DataFrame -> IO (Either PolarsError Text)
toText df = bytesOut df phs_dataframe_to_text TE.decodeUtf8

dataframeOut :: (Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt) -> IO (Either PolarsError DataFrame)
dataframeOut action =
    alloca $ \outPtr ->
        alloca $ \errPtr -> do
            poke outPtr nullPtr
            poke errPtr nullPtr
            status <- action outPtr errPtr
            if fromIntegralStatus status == 0
                then do
                    ptr <- peek outPtr
                    if ptr == nullPtr
                        then pure (Left (nullPointerError "dataframe output"))
                        else Right <$> mkDataFrame ptr
                else Left <$> (consumeError (fromIntegralStatus status) =<< peek errPtr)

word64Out :: (Ptr Word64 -> Ptr (Ptr RawError) -> IO CInt) -> IO (Either PolarsError Int)
word64Out action =
    alloca $ \outPtr ->
        alloca $ \errPtr -> do
            poke errPtr nullPtr
            status <- action outPtr errPtr
            if fromIntegralStatus status == 0
                then word64ToInt <$> peek outPtr
                else Left <$> (consumeError (fromIntegralStatus status) =<< peek errPtr)

unitOut :: (Ptr (Ptr RawError) -> IO CInt) -> IO (Either PolarsError ())
unitOut action =
    alloca $ \errPtr -> do
        poke errPtr nullPtr
        status <- action errPtr
        if fromIntegralStatus status == 0
            then pure (Right ())
            else Left <$> (consumeError (fromIntegralStatus status) =<< peek errPtr)

withSeriesArray :: [Series] -> (Ptr (Ptr RawSeries) -> CSize -> IO a) -> IO a
withSeriesArray values action = go values []
  where
    go [] acc = withArray (reverse acc) $ \ptr -> action ptr (fromIntegral (length acc))
    go (value : rest) acc = withSeries value $ \ptr -> go rest (ptr : acc)

withCStringList :: [Text] -> (Ptr CString -> CSize -> IO a) -> IO a
withCStringList values action = go values []
  where
    go [] acc = withArray (reverse acc) $ \ptr -> action ptr (fromIntegral (length acc))
    go (value : rest) acc = withTextCString value $ \ptr -> go rest (ptr : acc)

withMaybeCStringList :: Maybe [Text] -> (Ptr CString -> CSize -> Bool -> IO a) -> IO a
withMaybeCStringList Nothing action = action nullPtr 0 False
withMaybeCStringList (Just values) action = withCStringList values $ \ptr len -> action ptr len True

withRenamePairs :: [(Text, Text)] -> (Ptr CString -> Ptr CString -> CSize -> IO a) -> IO a
withRenamePairs values action = go values [] []
  where
    go [] existing new =
        withArray (reverse existing) $ \existingPtr ->
            withArray (reverse new) $ \newPtr ->
                action existingPtr newPtr (fromIntegral (length existing))
    go ((existingName, newName) : rest) existing new =
        withTextCString existingName $ \existingPtr ->
            withTextCString newName $ \newPtr ->
                go rest (existingPtr : existing) (newPtr : new)

bytesOut :: DataFrame -> (Ptr RawDataFrame -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt) -> (BS.ByteString -> a) -> IO (Either PolarsError a)
bytesOut df action decode = withDataFrame df $ \ptr ->
    alloca $ \outPtr ->
        alloca $ \errPtr -> do
            poke outPtr nullPtr
            poke errPtr nullPtr
            status <- action ptr outPtr errPtr
            if fromIntegralStatus status == 0
                then do
                    bytes <- copyAndFreeBytes =<< peek outPtr
                    pure (Right (decode bytes))
                else Left <$> (consumeError (fromIntegralStatus status) =<< peek errPtr)

parseSchemaBytes :: [BS.ByteString] -> [Field]
parseSchemaBytes chunks = go (dropTrailingEmpty chunks)
  where
    go (nameBytes : dtypeBytes : rest) =
        let name = TE.decodeUtf8 nameBytes
            dtype = parseDataType (TE.decodeUtf8 dtypeBytes)
         in Field name dtype : go rest
    go _ = []

    dropTrailingEmpty xs = case reverse xs of
        (empty : rest) | BS.null empty -> reverse rest
        _ -> xs

word64ToInt :: Word64 -> Either PolarsError Int
word64ToInt value
    | value <= fromIntegral (maxBound :: Int) = Right (fromIntegral value)
    | otherwise = Left (nullPointerError "integer conversion")

nonNegativeWord64 :: Text -> Int -> Either PolarsError Word64
nonNegativeWord64 label value
    | value < 0 = Left (invalidArgument (label <> " must be non-negative"))
    | otherwise = Right (fromIntegral value)

invalidArgument :: Text -> PolarsError
invalidArgument = PolarsError InvalidArgument

toCBool :: Bool -> CBool
toCBool False = CBool 0
toCBool True = CBool 1

fromIntegralStatus :: (Integral a) => a -> CInt
fromIntegralStatus = fromIntegral
