{-# LANGUAGE OverloadedStrings #-}

{- |
Module      : Polars.Internal.Series
Description : Shared helpers for safe Series FFI result handling.

This module centralizes the repetitive pointer-out patterns used by public
Series APIs. It converts Rust status codes into typed Haskell errors and wraps
successful Rust-owned handles in managed ForeignPtr values.
-}
module Polars.Internal.Series
    ( seriesBytesOut
    , seriesDataFrameOut
    , seriesOut
    , seriesWord64Out
    ) where

import qualified Data.ByteString as BS
import Data.Word (Word64)
import Foreign.C.Types (CInt)
import Foreign.Marshal.Alloc (alloca)
import Foreign.Ptr (Ptr, nullPtr)
import Foreign.Storable (peek, poke)

import Polars.DataFrame (DataFrame)
import Polars.Error (PolarsError)
import Polars.Internal.Bytes (copyAndFreeBytes)
import Polars.Internal.Managed (Series, mkDataFrame, mkSeries, withSeries)
import Polars.Internal.Raw (RawBytes, RawDataFrame, RawError, RawSeries)
import Polars.Internal.Result (consumeError, nullPointerError)

seriesOut :: (Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt) -> IO (Either PolarsError Series)
seriesOut action =
    alloca $ \outPtr ->
        alloca $ \errPtr -> do
            poke outPtr nullPtr
            poke errPtr nullPtr
            status <- action outPtr errPtr
            if status == 0
                then do
                    ptr <- peek outPtr
                    if ptr == nullPtr
                        then pure (Left (nullPointerError "series output"))
                        else Right <$> mkSeries ptr
                else Left <$> (consumeError status =<< peek errPtr)

seriesBytesOut :: Series -> (Ptr RawSeries -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt) -> (BS.ByteString -> Either PolarsError a) -> IO (Either PolarsError a)
seriesBytesOut series action decode = withSeries series $ \ptr ->
    alloca $ \outPtr ->
        alloca $ \errPtr -> do
            poke outPtr nullPtr
            poke errPtr nullPtr
            status <- action ptr outPtr errPtr
            if status == 0
                then do
                    bytes <- copyAndFreeBytes =<< peek outPtr
                    pure (decode bytes)
                else Left <$> (consumeError status =<< peek errPtr)

seriesWord64Out :: Series -> (Ptr RawSeries -> Ptr Word64 -> Ptr (Ptr RawError) -> IO CInt) -> IO (Either PolarsError Int)
seriesWord64Out series action = withSeries series $ \ptr ->
    alloca $ \outPtr ->
        alloca $ \errPtr -> do
            poke errPtr nullPtr
            status <- action ptr outPtr errPtr
            if status == 0
                then word64ToInt <$> peek outPtr
                else Left <$> (consumeError status =<< peek errPtr)

seriesDataFrameOut :: Series -> (Ptr RawSeries -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt) -> IO (Either PolarsError DataFrame)
seriesDataFrameOut series action = withSeries series $ \ptr ->
    alloca $ \outPtr ->
        alloca $ \errPtr -> do
            poke outPtr nullPtr
            poke errPtr nullPtr
            status <- action ptr outPtr errPtr
            if status == 0
                then do
                    out <- peek outPtr
                    if out == nullPtr
                        then pure (Left (nullPointerError "dataframe output"))
                        else Right <$> mkDataFrame out
                else Left <$> (consumeError status =<< peek errPtr)

word64ToInt :: Word64 -> Either PolarsError Int
word64ToInt value
    | value <= fromIntegral (maxBound :: Int) = Right (fromIntegral value)
    | otherwise = Left (nullPointerError "integer conversion")
