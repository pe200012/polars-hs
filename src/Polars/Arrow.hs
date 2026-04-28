{-# LANGUAGE OverloadedStrings #-}

{- |
Module      : Polars.Arrow
Description : Arrow C Data Interface import helpers.

This module imports standard Arrow C Data Interface RecordBatches into managed
Polars DataFrames. The pointer wrapper is explicitly unsafe because pointer
validity and release callbacks come from the Arrow producer.
-}
module Polars.Arrow
    ( ArrowRecordBatch
    , fromArrowRecordBatch
    , unsafeArrowRecordBatch
    , withArrowRecordBatch
    ) where

import Control.Exception (bracket)
import Foreign.C.Types (CInt)
import Foreign.ForeignPtr (ForeignPtr, finalizeForeignPtr, newForeignPtr, withForeignPtr)
import Foreign.Marshal.Alloc (alloca)
import Foreign.Ptr (Ptr, castPtr, nullPtr)
import Foreign.Storable (peek, poke)

import Polars.DataFrame (DataFrame)
import Polars.Error (PolarsError)
import Polars.Internal.Managed (mkDataFrame, withDataFrame)
import Polars.Internal.Raw
    ( RawArrowRecordBatch
    , RawDataFrame
    , RawError
    , phs_arrow_record_batch_array
    , phs_arrow_record_batch_free_finalizer
    , phs_arrow_record_batch_schema
    , phs_dataframe_from_arrow_record_batch
    , phs_dataframe_to_arrow_record_batch
    )
import Polars.Internal.Result (consumeError, nullPointerError)

data ArrowRecordBatch = ArrowRecordBatch !(Ptr ()) !(Ptr ())

unsafeArrowRecordBatch :: Ptr schema -> Ptr array -> ArrowRecordBatch
unsafeArrowRecordBatch schema array = ArrowRecordBatch (castPtr schema) (castPtr array)

fromArrowRecordBatch :: ArrowRecordBatch -> IO (Either PolarsError DataFrame)
fromArrowRecordBatch (ArrowRecordBatch schema array) = dataframeOut (phs_dataframe_from_arrow_record_batch schema array)

withArrowRecordBatch :: DataFrame -> (Ptr schema -> Ptr array -> IO a) -> IO (Either PolarsError a)
withArrowRecordBatch df action = withDataFrame df $ \dfPtr ->
    arrowRecordBatchOut (phs_dataframe_to_arrow_record_batch dfPtr) $ \batch ->
        withForeignPtr batch $ \batchPtr -> do
            schema <- phs_arrow_record_batch_schema batchPtr
            array <- phs_arrow_record_batch_array batchPtr
            if schema == nullPtr || array == nullPtr
                then pure (Left (nullPointerError "arrow record batch output"))
                else Right <$> action (castPtr schema) (castPtr array)

arrowRecordBatchOut :: (Ptr (Ptr RawArrowRecordBatch) -> Ptr (Ptr RawError) -> IO CInt) -> (ForeignPtr RawArrowRecordBatch -> IO (Either PolarsError a)) -> IO (Either PolarsError a)
arrowRecordBatchOut action useBatch =
    alloca $ \outPtr ->
        alloca $ \errPtr -> do
            poke outPtr nullPtr
            poke errPtr nullPtr
            status <- action outPtr errPtr
            if status == 0
                then do
                    ptr <- peek outPtr
                    if ptr == nullPtr
                        then pure (Left (nullPointerError "arrow record batch output"))
                        else bracket (newForeignPtr phs_arrow_record_batch_free_finalizer ptr) finalizeForeignPtr useBatch
                else Left <$> (consumeError status =<< peek errPtr)

dataframeOut :: (Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt) -> IO (Either PolarsError DataFrame)
dataframeOut action =
    alloca $ \outPtr ->
        alloca $ \errPtr -> do
            poke outPtr nullPtr
            poke errPtr nullPtr
            status <- action outPtr errPtr
            if status == 0
                then do
                    ptr <- peek outPtr
                    if ptr == nullPtr
                        then pure (Left (nullPointerError "dataframe output"))
                        else Right <$> mkDataFrame ptr
                else Left <$> (consumeError status =<< peek errPtr)
