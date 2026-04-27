{-# LANGUAGE OverloadedStrings #-}

{- |
Module      : Polars.Series
Description : Safe Series operations backed by Rust Polars handles.

A Series wraps a Rust-owned Polars Series handle in a ForeignPtr finalizer.
This module exposes metadata, slicing, conversion to a one-column DataFrame,
and typed value extraction with null preservation.
-}
module Polars.Series
    ( Series
    , seriesBool
    , seriesDataType
    , seriesDouble
    , seriesHead
    , seriesInt64
    , seriesLength
    , seriesName
    , seriesNullCount
    , seriesTail
    , seriesText
    , seriesToFrame
    ) where

import qualified Data.ByteString as BS
import Data.Int (Int64)
import Data.Text (Text)
import qualified Data.Text.Encoding as TE
import Data.Vector (Vector)

import Polars.DataFrame (DataFrame)
import Polars.Error (PolarsError (..), PolarsErrorCode (InvalidArgument))
import Polars.Internal.ColumnDecode
    ( decodeBoolColumn
    , decodeDoubleColumn
    , decodeInt64Column
    , decodeTextColumn
    )
import Polars.Internal.Managed (Series, withSeries)
import Polars.Internal.Series (seriesBytesOut, seriesDataFrameOut, seriesOut, seriesWord64Out)
import Polars.Internal.Raw
    ( phs_series_dtype
    , phs_series_head
    , phs_series_len
    , phs_series_name
    , phs_series_null_count
    , phs_series_tail
    , phs_series_to_frame
    , phs_series_values_bool
    , phs_series_values_f64
    , phs_series_values_i64
    , phs_series_values_text
    )
import Polars.Internal.Result (nullPointerError)
import Polars.Schema (DataType, parseDataType)

seriesName :: Series -> IO (Either PolarsError Text)
seriesName series = seriesBytesOut series phs_series_name decodeUtf8Bytes

seriesDataType :: Series -> IO (Either PolarsError DataType)
seriesDataType series = seriesBytesOut series phs_series_dtype (fmap parseDataType . decodeUtf8Bytes)

seriesLength :: Series -> IO (Either PolarsError Int)
seriesLength series = seriesWord64Out series phs_series_len

seriesNullCount :: Series -> IO (Either PolarsError Int)
seriesNullCount series = seriesWord64Out series phs_series_null_count

seriesHead :: Int -> Series -> IO (Either PolarsError Series)
seriesHead n series
    | n < 0 = pure (Left (nullPointerError "series head count"))
    | otherwise = withSeries series $ \ptr -> seriesOut (phs_series_head ptr (fromIntegral n))

seriesTail :: Int -> Series -> IO (Either PolarsError Series)
seriesTail n series
    | n < 0 = pure (Left (nullPointerError "series tail count"))
    | otherwise = withSeries series $ \ptr -> seriesOut (phs_series_tail ptr (fromIntegral n))

seriesToFrame :: Series -> IO (Either PolarsError DataFrame)
seriesToFrame series = seriesDataFrameOut series phs_series_to_frame

seriesBool :: Series -> IO (Either PolarsError (Vector (Maybe Bool)))
seriesBool series = seriesBytesOut series phs_series_values_bool decodeBoolColumn

seriesInt64 :: Series -> IO (Either PolarsError (Vector (Maybe Int64)))
seriesInt64 series = seriesBytesOut series phs_series_values_i64 decodeInt64Column

seriesDouble :: Series -> IO (Either PolarsError (Vector (Maybe Double)))
seriesDouble series = seriesBytesOut series phs_series_values_f64 decodeDoubleColumn

seriesText :: Series -> IO (Either PolarsError (Vector (Maybe Text)))
seriesText series = seriesBytesOut series phs_series_values_text decodeTextColumn

decodeUtf8Bytes :: BS.ByteString -> Either PolarsError Text
decodeUtf8Bytes bytes = case TE.decodeUtf8' bytes of
    Left _ -> Left (PolarsError InvalidArgument "series payload contained invalid UTF-8")
    Right text -> Right text
