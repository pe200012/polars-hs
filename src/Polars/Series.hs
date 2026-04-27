{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleInstances #-}
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
    , SeriesCast (..)
    , SeriesSortOptions (..)
    , defaultSeriesSortOptions
    , seriesAppend
    , seriesBool
    , seriesDataType
    , seriesDouble
    , seriesDropNulls
    , seriesHead
    , seriesInt64
    , seriesLength
    , seriesName
    , seriesRename
    , seriesReverse
    , seriesShift
    , seriesSort
    , seriesNullCount
    , seriesTail
    , seriesText
    , seriesToFrame
    , seriesUnique
    , seriesUniqueStable
    ) where

import qualified Data.ByteString as BS
import Data.Int (Int64)
import Data.Text (Text)
import qualified Data.Text.Encoding as TE
import Data.Word (Word64)
import Data.Vector (Vector)
import Foreign.C.Types (CBool (..), CInt)
import Foreign.Ptr (Ptr)

import Polars.DataFrame (DataFrame)
import Polars.Error (PolarsError (..), PolarsErrorCode (InvalidArgument))
import Polars.Internal.ColumnDecode
    ( decodeBoolColumn
    , decodeDoubleColumn
    , decodeInt64Column
    , decodeTextColumn
    )
import Polars.Internal.CString (withTextCString)
import Polars.Internal.Managed (Series, withSeries)
import Polars.Internal.Series (seriesBytesOut, seriesDataFrameOut, seriesOut, seriesWord64Out)
import Polars.Internal.Raw
    ( RawError
    , RawSeries
    , phs_series_append
    , phs_series_cast
    , phs_series_drop_nulls
    , phs_series_dtype
    , phs_series_head
    , phs_series_len
    , phs_series_name
    , phs_series_rename
    , phs_series_reverse
    , phs_series_shift
    , phs_series_sort
    , phs_series_null_count
    , phs_series_tail
    , phs_series_to_frame
    , phs_series_unique
    , phs_series_unique_stable
    , phs_series_values_bool
    , phs_series_values_f64
    , phs_series_values_i64
    , phs_series_values_text
    )
import Polars.Internal.Result (nullPointerError)
import Polars.Schema (DataType, parseDataType)

data SeriesSortOptions = SeriesSortOptions
    { seriesSortDescending :: !Bool
    , seriesSortNullsLast :: !Bool
    , seriesSortMultithreaded :: !Bool
    , seriesSortMaintainOrder :: !Bool
    , seriesSortLimit :: !(Maybe Int)
    }
    deriving stock (Eq, Show)

defaultSeriesSortOptions :: SeriesSortOptions
defaultSeriesSortOptions =
    SeriesSortOptions
        { seriesSortDescending = False
        , seriesSortNullsLast = False
        , seriesSortMultithreaded = True
        , seriesSortMaintainOrder = False
        , seriesSortLimit = Nothing
        }

class SeriesCast a where
    seriesCast :: Series -> IO (Either PolarsError Series)

instance SeriesCast Bool where
    seriesCast = seriesCastWithCode 0

instance SeriesCast Int64 where
    seriesCast = seriesCastWithCode 1

instance SeriesCast Double where
    seriesCast = seriesCastWithCode 2

instance SeriesCast Text where
    seriesCast = seriesCastWithCode 3

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

seriesRename :: Text -> Series -> IO (Either PolarsError Series)
seriesRename name series = withSeries series $ \ptr ->
    withTextCString name $ \cName -> seriesOut (phs_series_rename ptr cName)

seriesSort :: SeriesSortOptions -> Series -> IO (Either PolarsError Series)
seriesSort options series = case sortLimitWord64 (seriesSortLimit options) of
    Left err -> pure (Left err)
    Right (hasLimit, limitValue) -> withSeries series $ \ptr ->
        seriesOut
            ( phs_series_sort
                ptr
                (toCBool (seriesSortDescending options))
                (toCBool (seriesSortNullsLast options))
                (toCBool (seriesSortMultithreaded options))
                (toCBool (seriesSortMaintainOrder options))
                (toCBool hasLimit)
                limitValue
            )

seriesUnique :: Series -> IO (Either PolarsError Series)
seriesUnique series = seriesUnaryOut series phs_series_unique

seriesUniqueStable :: Series -> IO (Either PolarsError Series)
seriesUniqueStable series = seriesUnaryOut series phs_series_unique_stable

seriesReverse :: Series -> IO (Either PolarsError Series)
seriesReverse series = seriesUnaryOut series phs_series_reverse

seriesDropNulls :: Series -> IO (Either PolarsError Series)
seriesDropNulls series = seriesUnaryOut series phs_series_drop_nulls

seriesShift :: Int -> Series -> IO (Either PolarsError Series)
seriesShift periods series = withSeries series $ \ptr ->
    seriesOut (phs_series_shift ptr (fromIntegral periods))

seriesAppend :: Series -> Series -> IO (Either PolarsError Series)
seriesAppend left right =
    withSeries left $ \leftPtr ->
        withSeries right $ \rightPtr ->
            seriesOut (phs_series_append leftPtr rightPtr)

seriesBool :: Series -> IO (Either PolarsError (Vector (Maybe Bool)))
seriesBool series = seriesBytesOut series phs_series_values_bool decodeBoolColumn

seriesInt64 :: Series -> IO (Either PolarsError (Vector (Maybe Int64)))
seriesInt64 series = seriesBytesOut series phs_series_values_i64 decodeInt64Column

seriesDouble :: Series -> IO (Either PolarsError (Vector (Maybe Double)))
seriesDouble series = seriesBytesOut series phs_series_values_f64 decodeDoubleColumn

seriesText :: Series -> IO (Either PolarsError (Vector (Maybe Text)))
seriesText series = seriesBytesOut series phs_series_values_text decodeTextColumn

seriesCastWithCode :: CInt -> Series -> IO (Either PolarsError Series)
seriesCastWithCode code series = withSeries series $ \ptr -> seriesOut (phs_series_cast ptr code)

seriesUnaryOut :: Series -> (Ptr RawSeries -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt) -> IO (Either PolarsError Series)
seriesUnaryOut series action = withSeries series $ \ptr -> seriesOut (action ptr)

sortLimitWord64 :: Maybe Int -> Either PolarsError (Bool, Word64)
sortLimitWord64 Nothing = Right (False, 0)
sortLimitWord64 (Just value)
    | value < 0 = Left (nullPointerError "series sort limit")
    | otherwise = Right (True, fromIntegral value)

toCBool :: Bool -> CBool
toCBool False = CBool 0
toCBool True = CBool 1

decodeUtf8Bytes :: BS.ByteString -> Either PolarsError Text
decodeUtf8Bytes bytes = case TE.decodeUtf8' bytes of
    Left _ -> Left (PolarsError InvalidArgument "series payload contained invalid UTF-8")
    Right text -> Right text
