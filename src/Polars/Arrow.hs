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
    ) where

import Foreign.C.Types (CInt)
import Foreign.Marshal.Alloc (alloca)
import Foreign.Ptr (Ptr, castPtr, nullPtr)
import Foreign.Storable (peek, poke)

import Polars.DataFrame (DataFrame)
import Polars.Error (PolarsError)
import Polars.Internal.Managed (mkDataFrame)
import Polars.Internal.Raw (RawDataFrame, RawError, phs_dataframe_from_arrow_record_batch)
import Polars.Internal.Result (consumeError, nullPointerError)

data ArrowRecordBatch = ArrowRecordBatch !(Ptr ()) !(Ptr ())

unsafeArrowRecordBatch :: Ptr schema -> Ptr array -> ArrowRecordBatch
unsafeArrowRecordBatch schema array = ArrowRecordBatch (castPtr schema) (castPtr array)

fromArrowRecordBatch :: ArrowRecordBatch -> IO (Either PolarsError DataFrame)
fromArrowRecordBatch (ArrowRecordBatch schema array) = dataframeOut (phs_dataframe_from_arrow_record_batch schema array)

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
