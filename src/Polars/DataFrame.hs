{-# LANGUAGE OverloadedStrings #-}

{- |
Module      : Polars.DataFrame
Description : Safe eager DataFrame operations backed by Rust Polars handles.

A DataFrame wraps a Rust-owned Polars DataFrame handle in a ForeignPtr finalizer.
Functions return Either so Polars and FFI failures stay explicit.
-}
module Polars.DataFrame
    ( DataFrame
    , head
    , height
    , readCsv
    , readParquet
    , schema
    , shape
    , tail
    , toText
    , width
    ) where

import Prelude hiding (head, tail)

import qualified Data.ByteString as BS
import Data.Text (Text)
import qualified Data.Text.Encoding as TE
import Data.Word (Word64)
import Foreign.C.Types (CInt)
import Foreign.Marshal.Alloc (alloca)
import Foreign.Ptr (Ptr, nullPtr)
import Foreign.Storable (peek, poke)

import Polars.Error (PolarsError)
import Polars.Internal.Bytes (copyAndFreeBytes)
import Polars.Internal.CString (withFilePathCString)
import Polars.Internal.Managed (DataFrame, mkDataFrame, withDataFrame)
import Polars.Internal.Raw
    ( RawBytes
    , RawDataFrame
    , RawError
    , phs_dataframe_head
    , phs_dataframe_height
    , phs_dataframe_schema
    , phs_dataframe_shape
    , phs_dataframe_tail
    , phs_dataframe_to_text
    , phs_dataframe_width
    , phs_read_csv
    , phs_read_parquet
    )
import Polars.Internal.Result (consumeError, nullPointerError)
import Polars.Schema (Field (..), parseDataType)

readCsv :: FilePath -> IO (Either PolarsError DataFrame)
readCsv path = withFilePathCString path $ \cPath -> dataframeOut (phs_read_csv cPath)

readParquet :: FilePath -> IO (Either PolarsError DataFrame)
readParquet path = withFilePathCString path $ \cPath -> dataframeOut (phs_read_parquet cPath)

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
toText df = bytesOut df phs_dataframe_to_text (TE.decodeUtf8)

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

fromIntegralStatus :: (Integral a) => a -> CInt
fromIntegralStatus = fromIntegral
