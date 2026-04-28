{-# LANGUAGE OverloadedStrings #-}

{- |
Module      : Polars.Arrow
Description : Arrow C Data Interface import and export helpers.

This module imports and exports standard Arrow C Data Interface RecordBatches
for managed Polars DataFrames and single Arrow Field/Array pairs for managed
Polars Series. Pointer wrappers are explicitly unsafe because pointer validity
and release callbacks come from the Arrow producer.
-}
module Polars.Arrow
    ( ArrowRecordBatch
    , ArrowSeries
    , fromArrowRecordBatch
    , fromArrowSeries
    , unsafeArrowRecordBatch
    , unsafeArrowSeries
    , withArrowRecordBatch
    , withArrowSeries
    ) where

import Control.Exception (bracket)
import Foreign.C.Types (CInt)
import Foreign.ForeignPtr (ForeignPtr, finalizeForeignPtr, newForeignPtr, withForeignPtr)
import Foreign.Marshal.Alloc (alloca)
import Foreign.Ptr (Ptr, castPtr, nullPtr)
import Foreign.Storable (peek, poke)

import Polars.DataFrame (DataFrame)
import Polars.Error (PolarsError)
import Polars.Internal.Managed (mkDataFrame, mkSeries, withDataFrame, withSeries)
import Polars.Series (Series)
import Polars.Internal.Raw
    ( RawArrowRecordBatch
    , RawArrowSeries
    , RawDataFrame
    , RawError
    , RawSeries
    , phs_arrow_record_batch_array
    , phs_arrow_record_batch_free_finalizer
    , phs_arrow_record_batch_schema
    , phs_arrow_series_array
    , phs_arrow_series_free_finalizer
    , phs_arrow_series_schema
    , phs_dataframe_from_arrow_record_batch
    , phs_dataframe_to_arrow_record_batch
    , phs_series_from_arrow_array
    , phs_series_to_arrow_array
    )
import Polars.Internal.Result (consumeError, nullPointerError)

data ArrowRecordBatch = ArrowRecordBatch !(Ptr ()) !(Ptr ())

data ArrowSeries = ArrowSeries !(Ptr ()) !(Ptr ())

unsafeArrowRecordBatch :: Ptr schema -> Ptr array -> ArrowRecordBatch
unsafeArrowRecordBatch schema array = ArrowRecordBatch (castPtr schema) (castPtr array)

unsafeArrowSeries :: Ptr schema -> Ptr array -> ArrowSeries
unsafeArrowSeries schema array = ArrowSeries (castPtr schema) (castPtr array)

fromArrowRecordBatch :: ArrowRecordBatch -> IO (Either PolarsError DataFrame)
fromArrowRecordBatch (ArrowRecordBatch schema array) = dataframeOut (phs_dataframe_from_arrow_record_batch schema array)

fromArrowSeries :: ArrowSeries -> IO (Either PolarsError Series)
fromArrowSeries (ArrowSeries schema array) = seriesOut (phs_series_from_arrow_array schema array)

withArrowRecordBatch :: DataFrame -> (Ptr schema -> Ptr array -> IO a) -> IO (Either PolarsError a)
withArrowRecordBatch df action = withDataFrame df $ \dfPtr ->
    arrowRecordBatchOut (phs_dataframe_to_arrow_record_batch dfPtr) $ \batch ->
        withForeignPtr batch $ \batchPtr -> do
            schema <- phs_arrow_record_batch_schema batchPtr
            array <- phs_arrow_record_batch_array batchPtr
            if schema == nullPtr || array == nullPtr
                then pure (Left (nullPointerError "arrow record batch output"))
                else Right <$> action (castPtr schema) (castPtr array)

withArrowSeries :: Series -> (Ptr schema -> Ptr array -> IO a) -> IO (Either PolarsError a)
withArrowSeries series action = withSeries series $ \seriesPtr ->
    arrowSeriesOut (phs_series_to_arrow_array seriesPtr) $ \exported ->
        withForeignPtr exported $ \exportedPtr -> do
            schema <- phs_arrow_series_schema exportedPtr
            array <- phs_arrow_series_array exportedPtr
            if schema == nullPtr || array == nullPtr
                then pure (Left (nullPointerError "arrow series output"))
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

arrowSeriesOut :: (Ptr (Ptr RawArrowSeries) -> Ptr (Ptr RawError) -> IO CInt) -> (ForeignPtr RawArrowSeries -> IO (Either PolarsError a)) -> IO (Either PolarsError a)
arrowSeriesOut action useSeries =
    alloca $ \outPtr ->
        alloca $ \errPtr -> do
            poke outPtr nullPtr
            poke errPtr nullPtr
            status <- action outPtr errPtr
            if status == 0
                then do
                    ptr <- peek outPtr
                    if ptr == nullPtr
                        then pure (Left (nullPointerError "arrow series output"))
                        else bracket (newForeignPtr phs_arrow_series_free_finalizer ptr) finalizeForeignPtr useSeries
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
