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
    , SeriesCast (..)
    , SeriesFrom (..)
    , SeriesSortOptions (..)
    , defaultSeriesSortOptions
    , seriesAppend
    , seriesBool
    , seriesDataType
    , seriesDouble
    , seriesDropNulls
    , seriesFloat
    , seriesHead
    , seriesInt8
    , seriesInt16
    , seriesInt32
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
import Foreign.C.String (CString)
import Foreign.C.Types (CBool (..), CInt, CSize)
import Foreign.Ptr (Ptr, castPtr)

import Polars.DataFrame (DataFrame)
import Polars.Error (PolarsError (..), PolarsErrorCode (InvalidArgument))
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

seriesUnique :: Series -> IO (Either PolarsError Series)
seriesUnique input = seriesUnaryOut input phs_series_unique

seriesUniqueStable :: Series -> IO (Either PolarsError Series)
seriesUniqueStable input = seriesUnaryOut input phs_series_unique_stable

seriesReverse :: Series -> IO (Either PolarsError Series)
seriesReverse input = seriesUnaryOut input phs_series_reverse

seriesDropNulls :: Series -> IO (Either PolarsError Series)
seriesDropNulls input = seriesUnaryOut input phs_series_drop_nulls

seriesShift :: Int -> Series -> IO (Either PolarsError Series)
seriesShift periods input = withSeries input $ \ptr ->
    seriesOut (phs_series_shift ptr (fromIntegral periods))

seriesAppend :: Series -> Series -> IO (Either PolarsError Series)
seriesAppend left right =
    withSeries left $ \leftPtr ->
        withSeries right $ \rightPtr ->
            seriesOut (phs_series_append leftPtr rightPtr)

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

sortLimitWord64 :: Maybe Int -> Either PolarsError (Bool, Word64)
sortLimitWord64 Nothing = Right (False, 0)
sortLimitWord64 (Just value)
    | value < 0 = Left (invalidArgument "series sort limit must be non-negative")
    | otherwise = Right (True, fromIntegral value)

invalidArgument :: Text -> PolarsError
invalidArgument = PolarsError InvalidArgument

toCBool :: Bool -> CBool
toCBool False = CBool 0
toCBool True = CBool 1

decodeUtf8Bytes :: BS.ByteString -> Either PolarsError Text
decodeUtf8Bytes bytes = case TE.decodeUtf8' bytes of
    Left _ -> Left (PolarsError InvalidArgument "series payload contained invalid UTF-8")
    Right text -> Right text
