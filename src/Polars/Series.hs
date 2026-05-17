{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}

{- |
Module      : Polars.Series
Description : Safe Series operations backed by Rust Polars handles.

A Series wraps a Rust-owned Polars Series handle in a ForeignPtr finalizer.
This module exposes construction from Haskell vectors, metadata, slicing,
conversion to a one-column DataFrame, transforms, and typed value extraction with null preservation.
-}
module Polars.Series
    ( Series
    , FillNullStrategy (..)
    , SeriesCast (..)
    , SeriesDiffNullBehavior (..)
    , SeriesFrom (..)
    , SeriesInterpolationMethod (..)
    , SeriesModeOptions (..)
    , SeriesRoundMode (..)
    , SeriesSortOptions (..)
    , SeriesValueCountsOptions (..)
    , defaultSeriesModeOptions
    , defaultSeriesSortOptions
    , defaultSeriesValueCountsOptions
    , seriesAbs
    , seriesAdd
    , seriesAppend
    , seriesArgSort
    , seriesArgUnique
    , seriesBool
    , seriesCeil
    , seriesDataType
    , seriesDiff
    , seriesDiv
    , seriesDouble
    , seriesDropNulls
    , seriesFloat
    , seriesFloor
    , seriesHead
    , seriesInt8
    , seriesInt16
    , seriesInt32
    , seriesInt64
    , seriesInterpolate
    , seriesIsDuplicated
    , seriesIsFinite
    , seriesIsFirstDistinct
    , seriesIsInfinite
    , seriesIsLastDistinct
    , seriesIsNan
    , seriesIsNotNan
    , seriesFilter
    , seriesFillNull
    , seriesIsNotNull
    , seriesIsNull
    , seriesIsUnique
    , seriesLength
    , seriesMax
    , seriesMean
    , seriesMedian
    , seriesMin
    , seriesMode
    , seriesMul
    , seriesName
    , seriesNUnique
    , seriesRank
    , seriesRename
    , seriesReverse
    , seriesRem
    , seriesRound
    , seriesShift
    , seriesSlice
    , seriesSort
    , seriesStd
    , seriesSub
    , seriesSum
    , seriesNullCount
    , seriesTail
    , seriesTake
    , seriesText
    , seriesToFrame
    , seriesUnique
    , seriesUniqueCounts
    , seriesUniqueStable
    , seriesValueCounts
    , seriesVar
    , seriesWord8
    , seriesWord16
    , seriesWord32
    , seriesWord64
    ) where

import qualified Data.ByteString as BS
import Data.Int (Int16, Int32, Int64, Int8)
import Data.Text (Text)
import qualified Data.Text.Encoding as TE
import Data.Word (Word16, Word32, Word64, Word8)
import Data.Vector (Vector)
import qualified Data.Vector as V
import Foreign.C.String (CString)
import Foreign.C.Types (CBool (..), CInt, CSize, CUChar (..))
import Foreign.Marshal.Array (withArray)
import Foreign.Ptr (Ptr, castPtr)

import Polars.DataFrame (DataFrame, FillNullStrategy (..))
import Polars.Error (PolarsError (..), PolarsErrorCode (InvalidArgument))
import Polars.Expr (RankMethod (..), RankOptions (..))
import Polars.Internal.ColumnEncode
    ( encodeBoolColumn
    , encodeDoubleColumn
    , encodeFloatColumn
    , encodeInt8Column
    , encodeInt16Column
    , encodeInt32Column
    , encodeInt64Column
    , encodeTextColumn
    , encodeWord8Column
    , encodeWord16Column
    , encodeWord32Column
    , encodeWord64Column
    )
import Polars.Internal.ColumnDecode
    ( decodeBoolColumn
    , decodeDoubleColumn
    , decodeFloatColumn
    , decodeInt8Column
    , decodeInt16Column
    , decodeInt32Column
    , decodeInt64Column
    , decodeTextColumn
    , decodeWord8Column
    , decodeWord16Column
    , decodeWord32Column
    , decodeWord64Column
    )
import Polars.Internal.CString (withTextCString)
import Polars.Internal.Managed (Series, withSeries)
import Polars.Internal.Series (seriesBytesOut, seriesDataFrameOut, seriesMaybeDoubleOut, seriesOut, seriesWord64Out)
import Polars.Internal.Raw
    ( RawError
    , RawSeries
    , phs_series_abs
    , phs_series_append
    , phs_series_arg_sort
    , phs_series_arg_unique
    , phs_series_binary_op
    , phs_series_cast
    , phs_series_ceil
    , phs_series_diff
    , phs_series_drop_nulls
    , phs_series_dtype
    , phs_series_filter
    , phs_series_fill_null
    , phs_series_floor
    , phs_series_head
    , phs_series_interpolate
    , phs_series_is_duplicated
    , phs_series_is_finite
    , phs_series_is_first_distinct
    , phs_series_is_infinite
    , phs_series_is_last_distinct
    , phs_series_is_nan
    , phs_series_is_not_nan
    , phs_series_is_not_null
    , phs_series_is_null
    , phs_series_is_unique
    , phs_series_len
    , phs_series_mode
    , phs_series_name
    , phs_series_n_unique
    , phs_series_new_bool
    , phs_series_new_f32
    , phs_series_new_f64
    , phs_series_new_i8
    , phs_series_new_i16
    , phs_series_new_i32
    , phs_series_new_i64
    , phs_series_new_text
    , phs_series_new_u8
    , phs_series_new_u16
    , phs_series_new_u32
    , phs_series_new_u64
    , phs_series_rank
    , phs_series_rename
    , phs_series_reverse
    , phs_series_round
    , phs_series_shift
    , phs_series_slice
    , phs_series_sort
    , phs_series_stat
    , phs_series_null_count
    , phs_series_take
    , phs_series_tail
    , phs_series_to_frame
    , phs_series_unique
    , phs_series_unique_counts
    , phs_series_unique_stable
    , phs_series_value_counts
    , phs_series_values_bool
    , phs_series_values_f32
    , phs_series_values_f64
    , phs_series_values_i8
    , phs_series_values_i16
    , phs_series_values_i32
    , phs_series_values_i64
    , phs_series_values_text
    , phs_series_values_u8
    , phs_series_values_u16
    , phs_series_values_u32
    , phs_series_values_u64
    )
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

-- | Controls eager Series mode calculation.
newtype SeriesModeOptions = SeriesModeOptions
    { seriesModeMaintainOrder :: Bool
    }
    deriving stock (Eq, Show)

defaultSeriesModeOptions :: SeriesModeOptions
defaultSeriesModeOptions =
    SeriesModeOptions
        { seriesModeMaintainOrder = False
        }

-- | Controls eager Series frequency-table construction.
data SeriesValueCountsOptions = SeriesValueCountsOptions
    { seriesValueCountsSort :: !Bool
    , seriesValueCountsParallel :: !Bool
    , seriesValueCountsName :: !Text
    , seriesValueCountsNormalize :: !Bool
    }
    deriving stock (Eq, Show)

-- | Default value-count settings: unsorted counts in a column named @"count"@.
defaultSeriesValueCountsOptions :: SeriesValueCountsOptions
defaultSeriesValueCountsOptions =
    SeriesValueCountsOptions
        { seriesValueCountsSort = False
        , seriesValueCountsParallel = False
        , seriesValueCountsName = "count"
        , seriesValueCountsNormalize = False
        }

data SeriesRoundMode
    = RoundHalfToEven
    | RoundHalfAwayFromZero
    deriving stock (Eq, Show)

data SeriesDiffNullBehavior
    = SeriesDiffIgnore
    | SeriesDiffDrop
    deriving stock (Eq, Show)

-- | Interpolation algorithm for filling interior nulls in a 'Series'.
data SeriesInterpolationMethod
    = SeriesInterpolateLinear
    | SeriesInterpolateNearest
    deriving stock (Eq, Show)

class SeriesCast a where
    seriesCast :: Series -> IO (Either PolarsError Series)

instance SeriesCast Bool where
    seriesCast = seriesCastWithCode 0

instance SeriesCast Int8 where
    seriesCast = seriesCastWithCode 1

instance SeriesCast Int16 where
    seriesCast = seriesCastWithCode 2

instance SeriesCast Int32 where
    seriesCast = seriesCastWithCode 3

instance SeriesCast Int64 where
    seriesCast = seriesCastWithCode 4

instance SeriesCast Word8 where
    seriesCast = seriesCastWithCode 5

instance SeriesCast Word16 where
    seriesCast = seriesCastWithCode 6

instance SeriesCast Word32 where
    seriesCast = seriesCastWithCode 7

instance SeriesCast Word64 where
    seriesCast = seriesCastWithCode 8

instance SeriesCast Float where
    seriesCast = seriesCastWithCode 9

instance SeriesCast Double where
    seriesCast = seriesCastWithCode 10

instance SeriesCast Text where
    seriesCast = seriesCastWithCode 11

class SeriesFrom a where
    series :: Text -> Vector (Maybe a) -> IO (Either PolarsError Series)

instance SeriesFrom Bool where
    series name values = seriesFromBytes phs_series_new_bool name (encodeBoolColumn values)

instance SeriesFrom Int64 where
    series name values = seriesFromBytes phs_series_new_i64 name (encodeInt64Column values)

instance SeriesFrom Int8 where
    series name values = seriesFromBytes phs_series_new_i8 name (encodeInt8Column values)

instance SeriesFrom Int16 where
    series name values = seriesFromBytes phs_series_new_i16 name (encodeInt16Column values)

instance SeriesFrom Int32 where
    series name values = seriesFromBytes phs_series_new_i32 name (encodeInt32Column values)

instance SeriesFrom Word8 where
    series name values = seriesFromBytes phs_series_new_u8 name (encodeWord8Column values)

instance SeriesFrom Word16 where
    series name values = seriesFromBytes phs_series_new_u16 name (encodeWord16Column values)

instance SeriesFrom Word32 where
    series name values = seriesFromBytes phs_series_new_u32 name (encodeWord32Column values)

instance SeriesFrom Word64 where
    series name values = seriesFromBytes phs_series_new_u64 name (encodeWord64Column values)

instance SeriesFrom Float where
    series name values = seriesFromBytes phs_series_new_f32 name (encodeFloatColumn values)

instance SeriesFrom Double where
    series name values = seriesFromBytes phs_series_new_f64 name (encodeDoubleColumn values)

instance SeriesFrom Text where
    series name values = seriesFromBytes phs_series_new_text name (encodeTextColumn values)

seriesName :: Series -> IO (Either PolarsError Text)
seriesName input = seriesBytesOut input phs_series_name decodeUtf8Bytes

seriesDataType :: Series -> IO (Either PolarsError DataType)
seriesDataType input = seriesBytesOut input phs_series_dtype (fmap parseDataType . decodeUtf8Bytes)

seriesLength :: Series -> IO (Either PolarsError Int)
seriesLength input = seriesWord64Out input phs_series_len

seriesNullCount :: Series -> IO (Either PolarsError Int)
seriesNullCount input = seriesWord64Out input phs_series_null_count

seriesHead :: Int -> Series -> IO (Either PolarsError Series)
seriesHead n input
    | n < 0 = pure (Left (invalidArgument "series head count must be non-negative"))
    | otherwise = withSeries input $ \ptr -> seriesOut (phs_series_head ptr (fromIntegral n))

seriesTail :: Int -> Series -> IO (Either PolarsError Series)
seriesTail n input
    | n < 0 = pure (Left (invalidArgument "series tail count must be non-negative"))
    | otherwise = withSeries input $ \ptr -> seriesOut (phs_series_tail ptr (fromIntegral n))

seriesSlice :: Int -> Int -> Series -> IO (Either PolarsError Series)
seriesSlice offset len input
    | len < 0 = pure (Left (invalidArgument "series slice length must be non-negative"))
    | otherwise = withSeries input $ \ptr -> seriesOut (phs_series_slice ptr (fromIntegral offset) (fromIntegral len))

seriesToFrame :: Series -> IO (Either PolarsError DataFrame)
seriesToFrame input = seriesDataFrameOut input phs_series_to_frame

seriesRename :: Text -> Series -> IO (Either PolarsError Series)
seriesRename name input = withSeries input $ \ptr ->
    withTextCString name $ \cName -> seriesOut (phs_series_rename ptr cName)

seriesSort :: SeriesSortOptions -> Series -> IO (Either PolarsError Series)
seriesSort options input = case sortLimitWord64 (seriesSortLimit options) of
    Left err -> pure (Left err)
    Right (hasLimit, limitValue) -> withSeries input $ \ptr ->
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

seriesArgSort :: SeriesSortOptions -> Series -> IO (Either PolarsError Series)
seriesArgSort options input = case sortLimitWord64WithLabel "series arg sort limit" (seriesSortLimit options) of
    Left err -> pure (Left err)
    Right (hasLimit, limitValue) -> withSeries input $ \ptr ->
        seriesOut
            ( phs_series_arg_sort
                ptr
                (toCBool (seriesSortDescending options))
                (toCBool (seriesSortNullsLast options))
                (toCBool (seriesSortMultithreaded options))
                (toCBool (seriesSortMaintainOrder options))
                (toCBool hasLimit)
                limitValue
            )

seriesUnique :: Series -> IO (Either PolarsError Series)
seriesUnique input = seriesUnaryOut input phs_series_unique

seriesUniqueCounts :: Series -> IO (Either PolarsError Series)
seriesUniqueCounts input = seriesUnaryOut input phs_series_unique_counts

seriesUniqueStable :: Series -> IO (Either PolarsError Series)
seriesUniqueStable input = seriesUnaryOut input phs_series_unique_stable

seriesArgUnique :: Series -> IO (Either PolarsError Series)
seriesArgUnique input = seriesUnaryOut input phs_series_arg_unique

seriesRank :: RankOptions -> Series -> IO (Either PolarsError Series)
seriesRank options input =
    withSeries input $ \ptr ->
        seriesOut (phs_series_rank ptr (rankMethodCode (rankMethod options)) (toCBool (rankDescending options)))

seriesMode :: SeriesModeOptions -> Series -> IO (Either PolarsError Series)
seriesMode options input =
    withSeries input $ \ptr ->
        seriesOut (phs_series_mode ptr (toCBool (seriesModeMaintainOrder options)))

-- | Count unique Series values into a two-column DataFrame.
seriesValueCounts :: SeriesValueCountsOptions -> Series -> IO (Either PolarsError DataFrame)
seriesValueCounts options input =
    withTextCString (seriesValueCountsName options) $ \cName ->
        seriesDataFrameOut input $ \ptr out err ->
            phs_series_value_counts
                ptr
                (toCBool (seriesValueCountsSort options))
                (toCBool (seriesValueCountsParallel options))
                cName
                (toCBool (seriesValueCountsNormalize options))
                out
                err

seriesReverse :: Series -> IO (Either PolarsError Series)
seriesReverse input = seriesUnaryOut input phs_series_reverse

seriesDropNulls :: Series -> IO (Either PolarsError Series)
seriesDropNulls input = seriesUnaryOut input phs_series_drop_nulls

seriesAbs :: Series -> IO (Either PolarsError Series)
seriesAbs input = seriesUnaryOut input phs_series_abs

seriesRound :: Int -> SeriesRoundMode -> Series -> IO (Either PolarsError Series)
seriesRound decimals mode input = case nonNegativeWord32 "seriesRound decimals" decimals of
    Left err -> pure (Left err)
    Right decimalCount ->
        withSeries input $ \ptr ->
            seriesOut (phs_series_round ptr decimalCount (seriesRoundModeCode mode))

seriesFloor :: Series -> IO (Either PolarsError Series)
seriesFloor input = seriesUnaryOut input phs_series_floor

seriesCeil :: Series -> IO (Either PolarsError Series)
seriesCeil input = seriesUnaryOut input phs_series_ceil

seriesDiff :: Int64 -> SeriesDiffNullBehavior -> Series -> IO (Either PolarsError Series)
seriesDiff periods behavior input =
    withSeries input $ \ptr ->
        seriesOut (phs_series_diff ptr (fromIntegral periods) (seriesDiffNullBehaviorCode behavior))

-- | Fill interior null values using Polars Series interpolation.
seriesInterpolate :: SeriesInterpolationMethod -> Series -> IO (Either PolarsError Series)
seriesInterpolate method input =
    withSeries input $ \ptr ->
        seriesOut (phs_series_interpolate ptr (seriesInterpolationMethodCode method))

seriesIsNull :: Series -> IO (Either PolarsError Series)
seriesIsNull input = seriesUnaryOut input phs_series_is_null

seriesIsNotNull :: Series -> IO (Either PolarsError Series)
seriesIsNotNull input = seriesUnaryOut input phs_series_is_not_null

seriesIsNan :: Series -> IO (Either PolarsError Series)
seriesIsNan input = seriesUnaryOut input phs_series_is_nan

seriesIsNotNan :: Series -> IO (Either PolarsError Series)
seriesIsNotNan input = seriesUnaryOut input phs_series_is_not_nan

seriesIsFinite :: Series -> IO (Either PolarsError Series)
seriesIsFinite input = seriesUnaryOut input phs_series_is_finite

seriesIsInfinite :: Series -> IO (Either PolarsError Series)
seriesIsInfinite input = seriesUnaryOut input phs_series_is_infinite

seriesIsDuplicated :: Series -> IO (Either PolarsError Series)
seriesIsDuplicated input = seriesUnaryOut input phs_series_is_duplicated

seriesIsUnique :: Series -> IO (Either PolarsError Series)
seriesIsUnique input = seriesUnaryOut input phs_series_is_unique

seriesIsFirstDistinct :: Series -> IO (Either PolarsError Series)
seriesIsFirstDistinct input = seriesUnaryOut input phs_series_is_first_distinct

seriesIsLastDistinct :: Series -> IO (Either PolarsError Series)
seriesIsLastDistinct input = seriesUnaryOut input phs_series_is_last_distinct

seriesFilter :: Series -> Series -> IO (Either PolarsError Series)
seriesFilter mask input =
    withSeries input $ \seriesPtr ->
        withSeries mask $ \maskPtr ->
            seriesOut (phs_series_filter seriesPtr maskPtr)

seriesTake :: Vector Word64 -> Series -> IO (Either PolarsError Series)
seriesTake indices input =
    withSeries input $ \seriesPtr ->
        withArray (V.toList indices) $ \indicesPtr ->
            seriesOut (phs_series_take seriesPtr indicesPtr (fromIntegral (V.length indices)))

seriesFillNull :: FillNullStrategy -> Series -> IO (Either PolarsError Series)
seriesFillNull strategy input = case fillNullStrategyCode strategy of
    Left err -> pure (Left err)
    Right (strategyCode, hasLimit, limitValue) ->
        withSeries input $ \ptr ->
            seriesOut
                ( phs_series_fill_null
                    ptr
                    strategyCode
                    (toCBool hasLimit)
                    limitValue
                )

seriesShift :: Int -> Series -> IO (Either PolarsError Series)
seriesShift periods input = withSeries input $ \ptr ->
    seriesOut (phs_series_shift ptr (fromIntegral periods))

seriesAppend :: Series -> Series -> IO (Either PolarsError Series)
seriesAppend left right =
    withSeries left $ \leftPtr ->
        withSeries right $ \rightPtr ->
            seriesOut (phs_series_append leftPtr rightPtr)

seriesAdd :: Series -> Series -> IO (Either PolarsError Series)
seriesAdd = seriesBinaryOp 0

seriesSub :: Series -> Series -> IO (Either PolarsError Series)
seriesSub = seriesBinaryOp 1

seriesMul :: Series -> Series -> IO (Either PolarsError Series)
seriesMul = seriesBinaryOp 2

seriesDiv :: Series -> Series -> IO (Either PolarsError Series)
seriesDiv = seriesBinaryOp 3

seriesRem :: Series -> Series -> IO (Either PolarsError Series)
seriesRem = seriesBinaryOp 4

seriesMean :: Series -> IO (Either PolarsError (Maybe Double))
seriesMean = seriesStat 0 (CUChar 0)

seriesStd :: Int -> Series -> IO (Either PolarsError (Maybe Double))
seriesStd ddof input = case ddofCUChar "series std ddof" ddof of
    Left err -> pure (Left err)
    Right value -> seriesStat 1 value input

seriesVar :: Int -> Series -> IO (Either PolarsError (Maybe Double))
seriesVar ddof input = case ddofCUChar "series var ddof" ddof of
    Left err -> pure (Left err)
    Right value -> seriesStat 2 value input

seriesSum :: Series -> IO (Either PolarsError (Maybe Double))
seriesSum = seriesStat 3 (CUChar 0)

seriesMin :: Series -> IO (Either PolarsError (Maybe Double))
seriesMin = seriesStat 4 (CUChar 0)

seriesMax :: Series -> IO (Either PolarsError (Maybe Double))
seriesMax = seriesStat 5 (CUChar 0)

seriesMedian :: Series -> IO (Either PolarsError (Maybe Double))
seriesMedian = seriesStat 6 (CUChar 0)

seriesNUnique :: Series -> IO (Either PolarsError Int)
seriesNUnique input = seriesWord64Out input phs_series_n_unique

seriesBool :: Series -> IO (Either PolarsError (Vector (Maybe Bool)))
seriesBool input = seriesBytesOut input phs_series_values_bool decodeBoolColumn

seriesInt64 :: Series -> IO (Either PolarsError (Vector (Maybe Int64)))
seriesInt64 input = seriesBytesOut input phs_series_values_i64 decodeInt64Column

seriesInt8 :: Series -> IO (Either PolarsError (Vector (Maybe Int8)))
seriesInt8 input = seriesBytesOut input phs_series_values_i8 decodeInt8Column

seriesInt16 :: Series -> IO (Either PolarsError (Vector (Maybe Int16)))
seriesInt16 input = seriesBytesOut input phs_series_values_i16 decodeInt16Column

seriesInt32 :: Series -> IO (Either PolarsError (Vector (Maybe Int32)))
seriesInt32 input = seriesBytesOut input phs_series_values_i32 decodeInt32Column

seriesWord8 :: Series -> IO (Either PolarsError (Vector (Maybe Word8)))
seriesWord8 input = seriesBytesOut input phs_series_values_u8 decodeWord8Column

seriesWord16 :: Series -> IO (Either PolarsError (Vector (Maybe Word16)))
seriesWord16 input = seriesBytesOut input phs_series_values_u16 decodeWord16Column

seriesWord32 :: Series -> IO (Either PolarsError (Vector (Maybe Word32)))
seriesWord32 input = seriesBytesOut input phs_series_values_u32 decodeWord32Column

seriesWord64 :: Series -> IO (Either PolarsError (Vector (Maybe Word64)))
seriesWord64 input = seriesBytesOut input phs_series_values_u64 decodeWord64Column

seriesDouble :: Series -> IO (Either PolarsError (Vector (Maybe Double)))
seriesDouble input = seriesBytesOut input phs_series_values_f64 decodeDoubleColumn

seriesFloat :: Series -> IO (Either PolarsError (Vector (Maybe Float)))
seriesFloat input = seriesBytesOut input phs_series_values_f32 decodeFloatColumn

seriesText :: Series -> IO (Either PolarsError (Vector (Maybe Text)))
seriesText input = seriesBytesOut input phs_series_values_text decodeTextColumn

type SeriesNewAction = CString -> Ptr Word8 -> CSize -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

seriesFromBytes :: SeriesNewAction -> Text -> BS.ByteString -> IO (Either PolarsError Series)
seriesFromBytes action name bytes =
    withTextCString name $ \cName ->
        BS.useAsCStringLen bytes $ \(bytesPtr, len) ->
            seriesOut (action cName (castPtr bytesPtr) (fromIntegral len))

seriesCastWithCode :: CInt -> Series -> IO (Either PolarsError Series)
seriesCastWithCode code input = withSeries input $ \ptr -> seriesOut (phs_series_cast ptr code)

seriesUnaryOut :: Series -> (Ptr RawSeries -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt) -> IO (Either PolarsError Series)
seriesUnaryOut input action = withSeries input $ \ptr -> seriesOut (action ptr)

seriesBinaryOp :: CInt -> Series -> Series -> IO (Either PolarsError Series)
seriesBinaryOp op left right =
    withSeries left $ \leftPtr ->
        withSeries right $ \rightPtr ->
            seriesOut (phs_series_binary_op leftPtr rightPtr op)

seriesStat :: CInt -> CUChar -> Series -> IO (Either PolarsError (Maybe Double))
seriesStat op ddof input =
    seriesMaybeDoubleOut input $ \ptr hasValuePtr valuePtr errPtr ->
        phs_series_stat ptr op ddof hasValuePtr valuePtr errPtr

sortLimitWord64 :: Maybe Int -> Either PolarsError (Bool, Word64)
sortLimitWord64 = sortLimitWord64WithLabel "series sort limit"

sortLimitWord64WithLabel :: Text -> Maybe Int -> Either PolarsError (Bool, Word64)
sortLimitWord64WithLabel _ Nothing = Right (False, 0)
sortLimitWord64WithLabel label (Just value)
    | value < 0 = Left (invalidArgument (label <> " must be non-negative"))
    | otherwise = Right (True, fromIntegral value)

ddofCUChar :: Text -> Int -> Either PolarsError CUChar
ddofCUChar label value
    | value < 0 || value > fromIntegral (maxBound :: Word8) =
        Left (invalidArgument (label <> " must be between 0 and 255"))
    | otherwise = Right (CUChar (fromIntegral value))

nonNegativeWord32 :: Text -> Int -> Either PolarsError Word32
nonNegativeWord32 label value
    | value < 0 = Left (invalidArgument (label <> " must be non-negative"))
    | value > fromIntegral (maxBound :: Word32) =
        Left (invalidArgument (label <> " exceeds Word32 range"))
    | otherwise = Right (fromIntegral value)

seriesRoundModeCode :: SeriesRoundMode -> CInt
seriesRoundModeCode RoundHalfToEven = 0
seriesRoundModeCode RoundHalfAwayFromZero = 1

seriesDiffNullBehaviorCode :: SeriesDiffNullBehavior -> CInt
seriesDiffNullBehaviorCode SeriesDiffIgnore = 0
seriesDiffNullBehaviorCode SeriesDiffDrop = 1

seriesInterpolationMethodCode :: SeriesInterpolationMethod -> CInt
seriesInterpolationMethodCode SeriesInterpolateLinear = 0
seriesInterpolationMethodCode SeriesInterpolateNearest = 1

rankMethodCode :: RankMethod -> CInt
rankMethodCode RankAverage = 0
rankMethodCode RankMin = 1
rankMethodCode RankMax = 2
rankMethodCode RankDense = 3
rankMethodCode RankOrdinal = 4

fillNullStrategyCode :: FillNullStrategy -> Either PolarsError (CInt, Bool, Word64)
fillNullStrategyCode (FillForward limit) = fillNullLimitedStrategy 0 limit
fillNullStrategyCode (FillBackward limit) = fillNullLimitedStrategy 1 limit
fillNullStrategyCode FillMean = Right (2, False, 0)
fillNullStrategyCode FillMin = Right (3, False, 0)
fillNullStrategyCode FillMax = Right (4, False, 0)
fillNullStrategyCode FillZero = Right (5, False, 0)
fillNullStrategyCode FillOne = Right (6, False, 0)

fillNullLimitedStrategy :: CInt -> Maybe Int -> Either PolarsError (CInt, Bool, Word64)
fillNullLimitedStrategy strategy Nothing = Right (strategy, False, 0)
fillNullLimitedStrategy strategy (Just value)
    | value < 0 = Left (invalidArgument "fill null limit must be non-negative")
    | otherwise = Right (strategy, True, fromIntegral value)

invalidArgument :: Text -> PolarsError
invalidArgument = PolarsError InvalidArgument

toCBool :: Bool -> CBool
toCBool False = CBool 0
toCBool True = CBool 1

decodeUtf8Bytes :: BS.ByteString -> Either PolarsError Text
decodeUtf8Bytes bytes = case TE.decodeUtf8' bytes of
    Left _ -> Left (PolarsError InvalidArgument "series payload contained invalid UTF-8")
    Right text -> Right text
