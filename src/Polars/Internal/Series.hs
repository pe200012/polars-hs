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
    , seriesBoolOut
    , seriesDataFrameOut
    , seriesMaybeDoubleOut
    , seriesOut
    , seriesPairOut
    , seriesWord64Out
    ) where

import qualified Data.ByteString as BS
import Data.Word (Word64)
import Foreign.C.Types (CBool (..), CDouble (..), CInt)
import Foreign.Marshal.Alloc (alloca)
import Foreign.Ptr (Ptr, nullPtr)
import Foreign.Storable (peek, poke)

import Polars.DataFrame (DataFrame)
import Polars.Error (PolarsError (..), PolarsErrorCode (InvalidArgument))
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

seriesBoolOut :: Series -> (Ptr RawSeries -> Ptr CBool -> Ptr (Ptr RawError) -> IO CInt) -> IO (Either PolarsError Bool)
seriesBoolOut series action = withSeries series $ \ptr ->
    alloca $ \outPtr ->
        alloca $ \errPtr -> do
            poke errPtr nullPtr
            status <- action ptr outPtr errPtr
            if status == 0
                then do
                    CBool out <- peek outPtr
                    pure (Right (out /= 0))
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

seriesPairOut ::
    Series ->
    (Ptr RawSeries -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt) ->
    IO (Either PolarsError (Series, Series))
seriesPairOut series action = withSeries series $ \ptr ->
    alloca $ \leftPtr ->
        alloca $ \rightPtr ->
            alloca $ \errPtr -> do
                poke leftPtr nullPtr
                poke rightPtr nullPtr
                poke errPtr nullPtr
                status <- action ptr leftPtr rightPtr errPtr
                if status == 0
                    then do
                        left <- peek leftPtr
                        right <- peek rightPtr
                        if left == nullPtr
                            then pure (Left (nullPointerError "left series output"))
                            else
                                if right == nullPtr
                                    then pure (Left (nullPointerError "right series output"))
                                    else do
                                        leftSeries <- mkSeries left
                                        rightSeries <- mkSeries right
                                        pure (Right (leftSeries, rightSeries))
                    else Left <$> (consumeError status =<< peek errPtr)

seriesMaybeDoubleOut :: Series -> (Ptr RawSeries -> Ptr CBool -> Ptr CDouble -> Ptr (Ptr RawError) -> IO CInt) -> IO (Either PolarsError (Maybe Double))
seriesMaybeDoubleOut series action = withSeries series $ \ptr ->
    alloca $ \hasValuePtr ->
        alloca $ \valuePtr ->
            alloca $ \errPtr -> do
                poke errPtr nullPtr
                status <- action ptr hasValuePtr valuePtr errPtr
                if status == 0
                    then do
                        CBool hasValue <- peek hasValuePtr
                        if hasValue == 0
                            then pure (Right Nothing)
                            else do
                                CDouble value <- peek valuePtr
                                pure (Right (Just value))
                    else Left <$> (consumeError status =<< peek errPtr)

word64ToInt :: Word64 -> Either PolarsError Int
word64ToInt value
    | value <= fromIntegral (maxBound :: Int) = Right (fromIntegral value)
    | otherwise = Left (PolarsError InvalidArgument "integer conversion exceeds Haskell Int range")
