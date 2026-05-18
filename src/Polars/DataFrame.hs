{-# LANGUAGE OverloadedStrings #-}

{- |
Module      : Polars.DataFrame
Description : Safe eager DataFrame operations backed by Rust Polars handles.

A DataFrame wraps a Rust-owned Polars DataFrame handle in a ForeignPtr finalizer.
The module supports eager readers and writers, metadata queries, text rendering, and construction from owned Series handles.
Functions return Either so Polars and FFI failures stay explicit.
-}
module Polars.DataFrame
    ( CsvReadOptions (..)
    , CsvWriteOptions (..)
    , DataFrame
    , DataFrameExplodeOptions (..)
    , DataFrameJoinOptions (..)
    , DataFrameJoinType (..)
    , DataFramePartitionOptions (..)
    , DataFrameSampleOptions (..)
    , DataFrameSortOptions (..)
    , DataFrameUniqueKeepStrategy (..)
    , DataFrameUniqueOptions (..)
    , FillNullStrategy (..)
    , ParquetCompression (..)
    , ParquetParallelStrategy (..)
    , ParquetReadOptions (..)
    , ParquetStatisticsOptions (..)
    , ParquetWriteOptions (..)
    , dataFrame
    , dataFrameAlignChunks
    , dataFrameClear
    , dataFrameDropColumns
    , dataFrameDropNulls
    , dataFrameEstimatedSize
    , dataFrameExplode
    , dataFrameFilter
    , dataFrameFillNull
    , dataFrameFirstColNChunks
    , dataFrameHStack
    , dataFrameInsertColumn
    , dataFrameIsDuplicated
    , dataFrameIsEmpty
    , dataFrameIsUnique
    , dataFrameJoin
    , dataFrameMaxNChunks
    , dataFrameNewFromIndex
    , dataFrameNullCount
    , dataFramePartitionBy
    , dataFrameRechunk
    , dataFrameReplaceColumn
    , dataFrameRename
    , dataFrameReverse
    , dataFrameSampleFrac
    , dataFrameSampleN
    , dataFrameSelect
    , dataFrameShift
    , dataFrameShouldRechunk
    , dataFrameSlice
    , dataFrameSplitAt
    , dataFrameSort
    , dataFrameTake
    , dataFrameUnique
    , dataFrameVStack
    , dataFrameWithColumns
    , dataFrameWithRowIndex
    , head
    , height
    , defaultCsvReadOptions
    , defaultCsvWriteOptions
    , defaultDataFrameExplodeOptions
    , defaultDataFrameJoinOptions
    , defaultDataFramePartitionOptions
    , defaultDataFrameSampleOptions
    , defaultDataFrameSortOptions
    , defaultDataFrameUniqueOptions
    , defaultParquetReadOptions
    , defaultParquetStatisticsOptions
    , defaultParquetWriteOptions
    , readCsv
    , readCsvWith
    , readParquet
    , readParquetWith
    , schema
    , shape
    , tail
    , toText
    , width
    , writeCsv
    , writeCsvWith
    , writeParquet
    , writeParquetWith
    ) where

import Prelude hiding (head, tail)

import Control.Exception (bracket)
import Control.Monad (when)
import qualified Data.ByteString as BS
import Data.Bits ((.|.), shiftL)
import Data.Text (Text)
import qualified Data.Text.Encoding as TE
import Data.Vector (Vector)
import qualified Data.Vector as V
import Foreign.C.String (CString)
import Data.Word (Word8, Word64)
import Foreign.C.Types (CBool (..), CDouble (..), CInt, CSize, CUChar (..))
import Foreign.Marshal.Alloc (alloca)
import Foreign.Marshal.Array (withArray)
import Foreign.Ptr (Ptr, nullPtr)
import Foreign.Storable (peek, poke)

import Polars.Error (PolarsError (..), PolarsErrorCode (InvalidArgument))
import Polars.Internal.Bytes (copyAndFreeBytes)
import Polars.Internal.CString (withFilePathCString, withMaybeTextCString, withTextCString)
import Polars.Internal.Managed (DataFrame, Series, mkDataFrame, mkSeries, withDataFrame, withSeries)
import Polars.Internal.Raw
    ( RawBytes
    , RawDataFrame
    , RawDataFrameArray
    , RawError
    , RawSeries
    , phs_dataframe_align_chunks
    , phs_dataframe_array_free
    , phs_dataframe_array_get
    , phs_dataframe_array_len
    , phs_dataframe_clear
    , phs_dataframe_drop
    , phs_dataframe_drop_nulls
    , phs_dataframe_explode
    , phs_dataframe_estimated_size
    , phs_dataframe_filter
    , phs_dataframe_fill_null
    , phs_dataframe_first_col_n_chunks
    , phs_dataframe_free
    , phs_dataframe_hstack
    , phs_dataframe_head
    , phs_dataframe_height
    , phs_dataframe_is_duplicated
    , phs_dataframe_is_empty
    , phs_dataframe_is_unique
    , phs_dataframe_insert_column
    , phs_dataframe_join
    , phs_dataframe_max_n_chunks
    , phs_dataframe_new
    , phs_dataframe_new_from_index
    , phs_dataframe_null_count
    , phs_dataframe_partition_by
    , phs_dataframe_rechunk
    , phs_dataframe_replace_column
    , phs_dataframe_rename
    , phs_dataframe_reverse
    , phs_dataframe_sample_frac
    , phs_dataframe_sample_n
    , phs_dataframe_schema
    , phs_dataframe_select
    , phs_dataframe_shape
    , phs_dataframe_should_rechunk
    , phs_dataframe_shift
    , phs_dataframe_slice
    , phs_dataframe_split_at
    , phs_dataframe_sort
    , phs_dataframe_tail
    , phs_dataframe_take
    , phs_dataframe_to_text
    , phs_dataframe_unique
    , phs_dataframe_vstack
    , phs_dataframe_with_columns
    , phs_dataframe_with_row_index
    , phs_dataframe_width
    , phs_read_csv_options
    , phs_read_parquet_options
    , phs_write_csv_options
    , phs_write_parquet_options
    )
import Polars.IO
    ( CsvReadOptions (..)
    , CsvWriteOptions (..)
    , ParquetCompression (..)
    , ParquetParallelStrategy (..)
    , ParquetReadOptions (..)
    , ParquetStatisticsOptions (..)
    , ParquetWriteOptions (..)
    , defaultCsvReadOptions
    , defaultCsvWriteOptions
    , defaultParquetReadOptions
    , defaultParquetStatisticsOptions
    , defaultParquetWriteOptions
    )
import Polars.Internal.Result (consumeError, nullPointerError)
import Polars.Schema (Field (..), dataTypeFromSchemaTag)

schemaMagic :: BS.ByteString
schemaMagic = "PHS1SCH\0"

data DataFrameSortOptions = DataFrameSortOptions
    { dataFrameSortDescending :: ![Bool]
    , dataFrameSortNullsLast :: ![Bool]
    , dataFrameSortMultithreaded :: !Bool
    , dataFrameSortMaintainOrder :: !Bool
    , dataFrameSortLimit :: !(Maybe Int)
    }
    deriving (Eq, Show)

defaultDataFrameSortOptions :: DataFrameSortOptions
defaultDataFrameSortOptions =
    DataFrameSortOptions
        { dataFrameSortDescending = [False]
        , dataFrameSortNullsLast = [False]
        , dataFrameSortMultithreaded = True
        , dataFrameSortMaintainOrder = False
        , dataFrameSortLimit = Nothing
        }

data DataFrameJoinType
    = DataFrameJoinInner
    | DataFrameJoinLeft
    | DataFrameJoinRight
    | DataFrameJoinFull
    | DataFrameJoinSemi
    | DataFrameJoinAnti
    | DataFrameJoinCross
    deriving (Eq, Show)

data DataFrameJoinOptions = DataFrameJoinOptions
    { dataFrameJoinType :: !DataFrameJoinType
    , dataFrameJoinLeftOn :: ![Text]
    , dataFrameJoinRightOn :: ![Text]
    , dataFrameJoinSuffix :: !(Maybe Text)
    }
    deriving (Eq, Show)

defaultDataFrameJoinOptions :: DataFrameJoinOptions
defaultDataFrameJoinOptions =
    DataFrameJoinOptions
        { dataFrameJoinType = DataFrameJoinInner
        , dataFrameJoinLeftOn = []
        , dataFrameJoinRightOn = []
        , dataFrameJoinSuffix = Nothing
        }

data DataFrameUniqueKeepStrategy
    = DataFrameKeepFirst
    | DataFrameKeepLast
    | DataFrameKeepNone
    | DataFrameKeepAny
    deriving (Eq, Show)

data DataFrameUniqueOptions = DataFrameUniqueOptions
    { dataFrameUniqueSubset :: !(Maybe [Text])
    , dataFrameUniqueKeepStrategy :: !DataFrameUniqueKeepStrategy
    , dataFrameUniqueMaintainOrder :: !Bool
    }
    deriving (Eq, Show)

defaultDataFrameUniqueOptions :: DataFrameUniqueOptions
defaultDataFrameUniqueOptions =
    DataFrameUniqueOptions
        { dataFrameUniqueSubset = Nothing
        , dataFrameUniqueKeepStrategy = DataFrameKeepAny
        , dataFrameUniqueMaintainOrder = False
        }

data DataFrameSampleOptions = DataFrameSampleOptions
    { dataFrameSampleWithReplacement :: !Bool
    , dataFrameSampleShuffle :: !Bool
    , dataFrameSampleSeed :: !(Maybe Word64)
    }
    deriving (Eq, Show)

defaultDataFrameSampleOptions :: DataFrameSampleOptions
defaultDataFrameSampleOptions =
    DataFrameSampleOptions
        { dataFrameSampleWithReplacement = False
        , dataFrameSampleShuffle = False
        , dataFrameSampleSeed = Nothing
        }

data DataFramePartitionOptions = DataFramePartitionOptions
    { dataFramePartitionColumns :: ![Text]
    , dataFramePartitionIncludeKey :: !Bool
    , dataFramePartitionMaintainOrder :: !Bool
    }
    deriving (Eq, Show)

defaultDataFramePartitionOptions :: DataFramePartitionOptions
defaultDataFramePartitionOptions =
    DataFramePartitionOptions
        { dataFramePartitionColumns = []
        , dataFramePartitionIncludeKey = True
        , dataFramePartitionMaintainOrder = False
        }

data DataFrameExplodeOptions = DataFrameExplodeOptions
    { dataFrameExplodeColumns :: ![Text]
    , dataFrameExplodeEmptyAsNull :: !Bool
    , dataFrameExplodeKeepNulls :: !Bool
    }
    deriving (Eq, Show)

defaultDataFrameExplodeOptions :: DataFrameExplodeOptions
defaultDataFrameExplodeOptions =
    DataFrameExplodeOptions
        { dataFrameExplodeColumns = []
        , dataFrameExplodeEmptyAsNull = True
        , dataFrameExplodeKeepNulls = True
        }

data FillNullStrategy
    = FillForward !(Maybe Int)
    | FillBackward !(Maybe Int)
    | FillMean
    | FillMin
    | FillMax
    | FillZero
    | FillOne
    deriving (Eq, Show)

readCsv :: FilePath -> IO (Either PolarsError DataFrame)
readCsv = readCsvWith defaultCsvReadOptions

readCsvWith :: CsvReadOptions -> FilePath -> IO (Either PolarsError DataFrame)
readCsvWith options path =
    case csvReadWordOptions options of
        Left err -> pure (Left err)
        Right (hasNRows, nRows, skipRows, skipRowsAfterHeader, hasInferSchemaLength, inferSchemaLength) ->
            withFilePathCString path $ \cPath ->
                withMaybeTextCString (csvReadNullValue options) $ \cNullValue hasNullValue ->
                    dataframeOut
                        ( phs_read_csv_options
                            cPath
                            (toCBool (csvReadHasHeader options))
                            (CUChar (csvReadSeparator options))
                            (toCBool hasNullValue)
                            cNullValue
                            (toCBool hasNRows)
                            nRows
                            skipRows
                            skipRowsAfterHeader
                            (toCBool hasInferSchemaLength)
                            inferSchemaLength
                            (toCBool (csvReadIgnoreErrors options))
                            (toCBool (csvReadTruncateRaggedLines options))
                            (toCBool (csvReadMissingIsNull options))
                            (toCBool (csvReadLowMemory options))
                            (toCBool (csvReadRechunk options))
                        )

readParquet :: FilePath -> IO (Either PolarsError DataFrame)
readParquet = readParquetWith defaultParquetReadOptions

readParquetWith :: ParquetReadOptions -> FilePath -> IO (Either PolarsError DataFrame)
readParquetWith options path =
    case optionalNonNegativeWord64 "parquetReadNRows" (parquetReadNRows options) of
        Left err -> pure (Left err)
        Right (hasNRows, nRows) ->
            withFilePathCString path $ \cPath ->
                dataframeOut
                    ( phs_read_parquet_options
                        cPath
                        (toCBool hasNRows)
                        nRows
                        (parquetParallelCode (parquetReadParallel options))
                        (toCBool (parquetReadLowMemory options))
                        (toCBool (parquetReadRechunk options))
                    )

writeCsv :: FilePath -> DataFrame -> IO (Either PolarsError ())
writeCsv = writeCsvWith defaultCsvWriteOptions

writeCsvWith :: CsvWriteOptions -> FilePath -> DataFrame -> IO (Either PolarsError ())
writeCsvWith options path df =
    withFilePathCString path $ \cPath ->
        withTextCString (csvWriteNullValue options) $ \cNullValue ->
            withDataFrame df $ \ptr ->
                unitOut
                    ( phs_write_csv_options
                        cPath
                        ptr
                        (toCBool (csvWriteIncludeHeader options))
                        (CUChar (csvWriteSeparator options))
                        cNullValue
                    )

writeParquet :: FilePath -> DataFrame -> IO (Either PolarsError ())
writeParquet = writeParquetWith defaultParquetWriteOptions

writeParquetWith :: ParquetWriteOptions -> FilePath -> DataFrame -> IO (Either PolarsError ())
writeParquetWith options path df =
    case parquetWriteWordOptions options of
        Left err -> pure (Left err)
        Right (hasRowGroupSize, rowGroupSize, hasDataPageSize, dataPageSize) ->
            withFilePathCString path $ \cPath ->
                withDataFrame df $ \ptr ->
                    let statistics = parquetWriteStatistics options
                     in
                    unitOut
                        ( phs_write_parquet_options
                            cPath
                            ptr
                            (parquetCompressionCode (parquetWriteCompression options))
                            (toCBool hasRowGroupSize)
                            rowGroupSize
                            (toCBool hasDataPageSize)
                            dataPageSize
                            (toCBool (parquetStatisticsMinValue statistics))
                            (toCBool (parquetStatisticsMaxValue statistics))
                            (toCBool (parquetStatisticsDistinctCount statistics))
                            (toCBool (parquetStatisticsNullCount statistics))
                            (toCBool (parquetWriteParallel options))
                        )

dataFrame :: [Series] -> IO (Either PolarsError DataFrame)
dataFrame values = withSeriesArray values $ \ptr len -> dataframeOut (phs_dataframe_new ptr len)

dataFrameSelect :: [Text] -> DataFrame -> IO (Either PolarsError DataFrame)
dataFrameSelect [] _ = pure (Left (invalidArgument "dataFrameSelect requires at least one column name"))
dataFrameSelect names df = withDataFrame df $ \ptr -> withCStringList names $ \nameArray len ->
    dataframeOut (phs_dataframe_select ptr nameArray len)

dataFrameDropColumns :: [Text] -> DataFrame -> IO (Either PolarsError DataFrame)
dataFrameDropColumns [] _ = pure (Left (invalidArgument "dataFrameDropColumns requires at least one column name"))
dataFrameDropColumns names df = withDataFrame df $ \ptr -> withCStringList names $ \nameArray len ->
    dataframeOut (phs_dataframe_drop ptr nameArray len)

dataFrameFilter :: Series -> DataFrame -> IO (Either PolarsError DataFrame)
dataFrameFilter mask df =
    withDataFrame df $ \dfPtr ->
        withSeries mask $ \maskPtr ->
            dataframeOut (phs_dataframe_filter dfPtr maskPtr)

dataFrameTake :: Vector Word64 -> DataFrame -> IO (Either PolarsError DataFrame)
dataFrameTake indices df =
    withDataFrame df $ \dfPtr ->
        withArray (V.toList indices) $ \indicesPtr ->
            dataframeOut (phs_dataframe_take dfPtr indicesPtr (fromIntegral (V.length indices)))

dataFrameJoin :: DataFrameJoinOptions -> DataFrame -> DataFrame -> IO (Either PolarsError DataFrame)
dataFrameJoin options left right = case validateDataFrameJoinOptions options of
    Left err -> pure (Left err)
    Right () ->
        withDataFrame left $ \leftPtr ->
            withDataFrame right $ \rightPtr ->
                withCStringList (dataFrameJoinLeftOn options) $ \leftArray leftLen ->
                    withCStringList (dataFrameJoinRightOn options) $ \rightArray rightLen ->
                        withMaybeTextCString (dataFrameJoinSuffix options) $ \suffixPtr _ ->
                            dataframeOut
                                ( phs_dataframe_join
                                    leftPtr
                                    rightPtr
                                    leftArray
                                    leftLen
                                    rightArray
                                    rightLen
                                    (dataFrameJoinTypeCode (dataFrameJoinType options))
                                    suffixPtr
                                )

dataFrameVStack :: DataFrame -> DataFrame -> IO (Either PolarsError DataFrame)
dataFrameVStack left right =
    withDataFrame left $ \leftPtr ->
        withDataFrame right $ \rightPtr ->
            dataframeOut (phs_dataframe_vstack leftPtr rightPtr)

dataFrameHStack :: [Series] -> DataFrame -> IO (Either PolarsError DataFrame)
dataFrameHStack [] _ = pure (Left (invalidArgument "dataFrameHStack requires at least one Series"))
dataFrameHStack columns df =
    withDataFrame df $ \dfPtr ->
        withSeriesArray columns $ \seriesPtr len ->
            dataframeOut (phs_dataframe_hstack dfPtr seriesPtr len)

dataFrameWithColumns :: [Series] -> DataFrame -> IO (Either PolarsError DataFrame)
dataFrameWithColumns [] _ = pure (Left (invalidArgument "dataFrameWithColumns requires at least one Series"))
dataFrameWithColumns columns df =
    withDataFrame df $ \dfPtr ->
        withSeriesArray columns $ \seriesPtr len ->
            dataframeOut (phs_dataframe_with_columns dfPtr seriesPtr len)

dataFrameInsertColumn :: Int -> Series -> DataFrame -> IO (Either PolarsError DataFrame)
dataFrameInsertColumn index column df = case nonNegativeWord64 "dataFrameInsertColumn index" index of
    Left err -> pure (Left err)
    Right indexValue ->
        withDataFrame df $ \dfPtr ->
            withSeries column $ \seriesPtr ->
                dataframeOut (phs_dataframe_insert_column dfPtr indexValue seriesPtr)

dataFrameReplaceColumn :: Int -> Series -> DataFrame -> IO (Either PolarsError DataFrame)
dataFrameReplaceColumn index column df = case nonNegativeWord64 "dataFrameReplaceColumn index" index of
    Left err -> pure (Left err)
    Right indexValue ->
        withDataFrame df $ \dfPtr ->
            withSeries column $ \seriesPtr ->
                dataframeOut (phs_dataframe_replace_column dfPtr indexValue seriesPtr)

dataFramePartitionBy :: DataFramePartitionOptions -> DataFrame -> IO (Either PolarsError [DataFrame])
dataFramePartitionBy options df
    | null (dataFramePartitionColumns options) =
        pure (Left (invalidArgument "dataFramePartitionBy requires at least one column name"))
    | otherwise =
        withDataFrame df $ \dfPtr ->
            withCStringList (dataFramePartitionColumns options) $ \nameArray nameLen ->
                dataframeArrayOut
                    ( phs_dataframe_partition_by
                        dfPtr
                        nameArray
                        nameLen
                        (toCBool (dataFramePartitionIncludeKey options))
                        (toCBool (dataFramePartitionMaintainOrder options))
                    )

dataFrameExplode :: DataFrameExplodeOptions -> DataFrame -> IO (Either PolarsError DataFrame)
dataFrameExplode options df
    | null (dataFrameExplodeColumns options) =
        pure (Left (invalidArgument "dataFrameExplode requires at least one column name"))
    | otherwise =
        withDataFrame df $ \dfPtr ->
            withCStringList (dataFrameExplodeColumns options) $ \nameArray nameLen ->
                dataframeOut
                    ( phs_dataframe_explode
                        dfPtr
                        nameArray
                        nameLen
                        (toCBool (dataFrameExplodeEmptyAsNull options))
                        (toCBool (dataFrameExplodeKeepNulls options))
                    )

dataFrameRename :: [(Text, Text)] -> DataFrame -> IO (Either PolarsError DataFrame)
dataFrameRename [] _ = pure (Left (invalidArgument "dataFrameRename requires at least one column pair"))
dataFrameRename pairs df = withDataFrame df $ \ptr -> withRenamePairs pairs $ \existingArray newArray len ->
    dataframeOut (phs_dataframe_rename ptr existingArray newArray len)

dataFrameSlice :: Int -> Int -> DataFrame -> IO (Either PolarsError DataFrame)
dataFrameSlice offset len df = case nonNegativeWord64 "dataFrameSlice length" len of
    Left err -> pure (Left err)
    Right lenWord -> withDataFrame df $ \ptr ->
        dataframeOut (phs_dataframe_slice ptr (fromIntegral offset) lenWord)

dataFrameSort :: DataFrameSortOptions -> [Text] -> DataFrame -> IO (Either PolarsError DataFrame)
dataFrameSort options names df = case dataFrameSortValidatedOptions options names of
    Left err -> pure (Left err)
    Right (descending, nullsLast, hasLimit, limitValue) ->
        withDataFrame df $ \dfPtr ->
            withCStringList names $ \nameArray nameLen ->
                withWord8List descending $ \descendingPtr descendingLen ->
                    withWord8List nullsLast $ \nullsLastPtr nullsLastLen ->
                        dataframeOut
                            ( phs_dataframe_sort
                                dfPtr
                                nameArray
                                nameLen
                                descendingPtr
                                descendingLen
                                nullsLastPtr
                                nullsLastLen
                                (toCBool (dataFrameSortMultithreaded options))
                                (toCBool (dataFrameSortMaintainOrder options))
                                (toCBool hasLimit)
                                limitValue
                            )

dataFrameUnique :: DataFrameUniqueOptions -> DataFrame -> IO (Either PolarsError DataFrame)
dataFrameUnique options df = case dataFrameUniqueSubset options of
    Just [] -> pure (Left (invalidArgument "dataFrameUnique subset requires at least one column name"))
    subset ->
        withDataFrame df $ \dfPtr ->
            withMaybeCStringList subset $ \subsetArray subsetLen hasSubset ->
                dataframeOut
                    ( phs_dataframe_unique
                        dfPtr
                        subsetArray
                        subsetLen
                        (toCBool hasSubset)
                        (dataFrameUniqueKeepStrategyCode (dataFrameUniqueKeepStrategy options))
                        (toCBool (dataFrameUniqueMaintainOrder options))
                    )

dataFrameIsUnique :: DataFrame -> IO (Either PolarsError Series)
dataFrameIsUnique df = withDataFrame df $ \ptr -> seriesOut (phs_dataframe_is_unique ptr)

dataFrameIsDuplicated :: DataFrame -> IO (Either PolarsError Series)
dataFrameIsDuplicated df = withDataFrame df $ \ptr -> seriesOut (phs_dataframe_is_duplicated ptr)

dataFrameSampleN :: DataFrameSampleOptions -> Int -> DataFrame -> IO (Either PolarsError DataFrame)
dataFrameSampleN options n df = case nonNegativeWord64 "dataFrameSampleN size" n of
    Left err -> pure (Left err)
    Right sampleSize ->
        withDataFrame df $ \ptr ->
            dataframeOut
                ( phs_dataframe_sample_n
                    ptr
                    sampleSize
                    (toCBool (dataFrameSampleWithReplacement options))
                    (toCBool (dataFrameSampleShuffle options))
                    (toCBool hasSeed)
                    seed
                )
  where
    (hasSeed, seed) = seedWord64 (dataFrameSampleSeed options)

dataFrameSampleFrac :: DataFrameSampleOptions -> Double -> DataFrame -> IO (Either PolarsError DataFrame)
dataFrameSampleFrac options frac df = case validDataFrameSampleFraction options frac of
    Left err -> pure (Left err)
    Right () ->
        withDataFrame df $ \ptr ->
            dataframeOut
                ( phs_dataframe_sample_frac
                    ptr
                    (CDouble frac)
                    (toCBool (dataFrameSampleWithReplacement options))
                    (toCBool (dataFrameSampleShuffle options))
                    (toCBool hasSeed)
                    seed
                )
  where
    (hasSeed, seed) = seedWord64 (dataFrameSampleSeed options)

dataFrameFillNull :: FillNullStrategy -> DataFrame -> IO (Either PolarsError DataFrame)
dataFrameFillNull strategy df = case fillNullStrategyCode strategy of
    Left err -> pure (Left err)
    Right (strategyCode, hasLimit, limitValue) ->
        withDataFrame df $ \ptr ->
            dataframeOut
                ( phs_dataframe_fill_null
                    ptr
                    strategyCode
                    (toCBool hasLimit)
                    limitValue
                )

dataFrameReverse :: DataFrame -> IO (Either PolarsError DataFrame)
dataFrameReverse df = withDataFrame df $ \ptr -> dataframeOut (phs_dataframe_reverse ptr)

dataFrameDropNulls :: Maybe [Text] -> DataFrame -> IO (Either PolarsError DataFrame)
dataFrameDropNulls (Just []) _ = pure (Left (invalidArgument "dataFrameDropNulls subset requires at least one column name"))
dataFrameDropNulls subset df = withDataFrame df $ \ptr -> withMaybeCStringList subset $ \nameArray len hasSubset ->
    dataframeOut (phs_dataframe_drop_nulls ptr nameArray len (toCBool hasSubset))

dataFrameNullCount :: DataFrame -> IO (Either PolarsError DataFrame)
dataFrameNullCount df = withDataFrame df $ \ptr -> dataframeOut (phs_dataframe_null_count ptr)

dataFrameEstimatedSize :: DataFrame -> IO (Either PolarsError Int)
dataFrameEstimatedSize df = withDataFrame df $ \ptr -> word64Out (phs_dataframe_estimated_size ptr)

dataFrameFirstColNChunks :: DataFrame -> IO (Either PolarsError Int)
dataFrameFirstColNChunks df = withDataFrame df $ \ptr -> word64Out (phs_dataframe_first_col_n_chunks ptr)

dataFrameMaxNChunks :: DataFrame -> IO (Either PolarsError Int)
dataFrameMaxNChunks df = withDataFrame df $ \ptr -> word64Out (phs_dataframe_max_n_chunks ptr)

dataFrameIsEmpty :: DataFrame -> IO (Either PolarsError Bool)
dataFrameIsEmpty df = withDataFrame df $ \ptr -> boolOut (phs_dataframe_is_empty ptr)

dataFrameClear :: DataFrame -> IO (Either PolarsError DataFrame)
dataFrameClear df = withDataFrame df $ \ptr -> dataframeOut (phs_dataframe_clear ptr)

dataFrameSplitAt :: Int -> DataFrame -> IO (Either PolarsError (DataFrame, DataFrame))
dataFrameSplitAt offset df = withDataFrame df $ \ptr -> dataframePairOut (phs_dataframe_split_at ptr (fromIntegral offset))

dataFrameNewFromIndex :: Int -> Int -> DataFrame -> IO (Either PolarsError DataFrame)
dataFrameNewFromIndex index len df =
    case (nonNegativeWord64 "dataFrameNewFromIndex index" index, nonNegativeWord64 "dataFrameNewFromIndex length" len) of
        (Left err, _) -> pure (Left err)
        (_, Left err) -> pure (Left err)
        (Right indexValue, Right lenValue) ->
            withDataFrame df $ \ptr ->
                dataframeOut (phs_dataframe_new_from_index ptr indexValue lenValue)

dataFrameWithRowIndex :: Text -> Maybe Int -> DataFrame -> IO (Either PolarsError DataFrame)
dataFrameWithRowIndex name offset df = case optionalNonNegativeWord64 "dataFrameWithRowIndex offset" offset of
    Left err -> pure (Left err)
    Right (hasOffset, offsetValue) ->
        withTextCString name $ \cName ->
            withDataFrame df $ \ptr ->
                dataframeOut (phs_dataframe_with_row_index ptr cName (toCBool hasOffset) offsetValue)

dataFrameRechunk :: DataFrame -> IO (Either PolarsError DataFrame)
dataFrameRechunk df = withDataFrame df $ \ptr -> dataframeOut (phs_dataframe_rechunk ptr)

dataFrameAlignChunks :: DataFrame -> IO (Either PolarsError DataFrame)
dataFrameAlignChunks df = withDataFrame df $ \ptr -> dataframeOut (phs_dataframe_align_chunks ptr)

dataFrameShouldRechunk :: DataFrame -> IO (Either PolarsError Bool)
dataFrameShouldRechunk df = withDataFrame df $ \ptr -> boolOut (phs_dataframe_should_rechunk ptr)

dataFrameShift :: Int -> DataFrame -> IO (Either PolarsError DataFrame)
dataFrameShift periods df = withDataFrame df $ \ptr -> dataframeOut (phs_dataframe_shift ptr (fromIntegral periods))

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
schema df = bytesEitherOut df phs_dataframe_schema parseSchemaBytes

head :: Int -> DataFrame -> IO (Either PolarsError DataFrame)
head n df
    | n < 0 = pure (Left (invalidArgument "head count must be non-negative"))
    | otherwise = withDataFrame df $ \ptr -> dataframeOut (phs_dataframe_head ptr (fromIntegral n))

tail :: Int -> DataFrame -> IO (Either PolarsError DataFrame)
tail n df
    | n < 0 = pure (Left (invalidArgument "tail count must be non-negative"))
    | otherwise = withDataFrame df $ \ptr -> dataframeOut (phs_dataframe_tail ptr (fromIntegral n))

toText :: DataFrame -> IO (Either PolarsError Text)
toText df = bytesOut df phs_dataframe_to_text TE.decodeUtf8

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

seriesOut :: (Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt) -> IO (Either PolarsError Series)
seriesOut action =
    alloca $ \outPtr ->
        alloca $ \errPtr -> do
            poke outPtr nullPtr
            poke errPtr nullPtr
            status <- action outPtr errPtr
            if fromIntegralStatus status == 0
                then do
                    ptr <- peek outPtr
                    if ptr == nullPtr
                        then pure (Left (nullPointerError "series output"))
                        else Right <$> mkSeries ptr
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

boolOut :: (Ptr CBool -> Ptr (Ptr RawError) -> IO CInt) -> IO (Either PolarsError Bool)
boolOut action =
    alloca $ \outPtr ->
        alloca $ \errPtr -> do
            poke errPtr nullPtr
            status <- action outPtr errPtr
            if fromIntegralStatus status == 0
                then do
                    CBool out <- peek outPtr
                    pure (Right (out /= 0))
                else Left <$> (consumeError (fromIntegralStatus status) =<< peek errPtr)

dataframePairOut :: (Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt) -> IO (Either PolarsError (DataFrame, DataFrame))
dataframePairOut action =
    alloca $ \leftPtr ->
        alloca $ \rightPtr ->
            alloca $ \errPtr -> do
                poke leftPtr nullPtr
                poke rightPtr nullPtr
                poke errPtr nullPtr
                status <- action leftPtr rightPtr errPtr
                if fromIntegralStatus status == 0
                    then do
                        left <- peek leftPtr
                        right <- peek rightPtr
                        if left == nullPtr
                            then do
                                when (right /= nullPtr) (phs_dataframe_free right)
                                pure (Left (nullPointerError "dataframe left output"))
                            else
                                if right == nullPtr
                                    then do
                                        phs_dataframe_free left
                                        pure (Left (nullPointerError "dataframe right output"))
                                    else do
                                        leftDf <- mkDataFrame left
                                        rightDf <- mkDataFrame right
                                        pure (Right (leftDf, rightDf))
                    else Left <$> (consumeError (fromIntegralStatus status) =<< peek errPtr)

dataframeArrayOut :: (Ptr (Ptr RawDataFrameArray) -> Ptr (Ptr RawError) -> IO CInt) -> IO (Either PolarsError [DataFrame])
dataframeArrayOut action =
    alloca $ \outPtr ->
        alloca $ \errPtr -> do
            poke outPtr nullPtr
            poke errPtr nullPtr
            status <- action outPtr errPtr
            if fromIntegralStatus status == 0
                then do
                    array <- peek outPtr
                    if array == nullPtr
                        then pure (Left (nullPointerError "dataframe array output"))
                        else bracket (pure array) phs_dataframe_array_free dataframeArrayToList
                else Left <$> (consumeError (fromIntegralStatus status) =<< peek errPtr)

dataframeArrayToList :: Ptr RawDataFrameArray -> IO (Either PolarsError [DataFrame])
dataframeArrayToList array = do
    len <- phs_dataframe_array_len array
    let go index acc
            | index >= len = pure (Right (reverse acc))
            | otherwise =
                alloca $ \outPtr ->
                    alloca $ \errPtr -> do
                        poke outPtr nullPtr
                        poke errPtr nullPtr
                        status <- phs_dataframe_array_get array index outPtr errPtr
                        if fromIntegralStatus status == 0
                            then do
                                ptr <- peek outPtr
                                if ptr == nullPtr
                                    then pure (Left (nullPointerError "dataframe array item output"))
                                    else do
                                        df <- mkDataFrame ptr
                                        go (index + 1) (df : acc)
                            else Left <$> (consumeError (fromIntegralStatus status) =<< peek errPtr)
    go 0 []

unitOut :: (Ptr (Ptr RawError) -> IO CInt) -> IO (Either PolarsError ())
unitOut action =
    alloca $ \errPtr -> do
        poke errPtr nullPtr
        status <- action errPtr
        if fromIntegralStatus status == 0
            then pure (Right ())
            else Left <$> (consumeError (fromIntegralStatus status) =<< peek errPtr)

withSeriesArray :: [Series] -> (Ptr (Ptr RawSeries) -> CSize -> IO a) -> IO a
withSeriesArray values action = go values []
  where
    go [] acc = withArray (reverse acc) $ \ptr -> action ptr (fromIntegral (length acc))
    go (value : rest) acc = withSeries value $ \ptr -> go rest (ptr : acc)

withCStringList :: [Text] -> (Ptr CString -> CSize -> IO a) -> IO a
withCStringList values action = go values []
  where
    go [] acc = withArray (reverse acc) $ \ptr -> action ptr (fromIntegral (length acc))
    go (value : rest) acc = withTextCString value $ \ptr -> go rest (ptr : acc)

withMaybeCStringList :: Maybe [Text] -> (Ptr CString -> CSize -> Bool -> IO a) -> IO a
withMaybeCStringList Nothing action = action nullPtr 0 False
withMaybeCStringList (Just values) action = withCStringList values $ \ptr len -> action ptr len True

withWord8List :: [Word8] -> (Ptr Word8 -> CSize -> IO a) -> IO a
withWord8List values action = withArray values $ \ptr -> action ptr (fromIntegral (length values))

withRenamePairs :: [(Text, Text)] -> (Ptr CString -> Ptr CString -> CSize -> IO a) -> IO a
withRenamePairs values action = go values [] []
  where
    go [] existing new =
        withArray (reverse existing) $ \existingPtr ->
            withArray (reverse new) $ \newPtr ->
                action existingPtr newPtr (fromIntegral (length existing))
    go ((existingName, newName) : rest) existing new =
        withTextCString existingName $ \existingPtr ->
            withTextCString newName $ \newPtr ->
                go rest (existingPtr : existing) (newPtr : new)

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

bytesEitherOut :: DataFrame -> (Ptr RawDataFrame -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt) -> (BS.ByteString -> Either PolarsError a) -> IO (Either PolarsError a)
bytesEitherOut df action decode = withDataFrame df $ \ptr ->
    alloca $ \outPtr ->
        alloca $ \errPtr -> do
            poke outPtr nullPtr
            poke errPtr nullPtr
            status <- action ptr outPtr errPtr
            if fromIntegralStatus status == 0
                then do
                    bytes <- copyAndFreeBytes =<< peek outPtr
                    pure (decode bytes)
                else Left <$> (consumeError (fromIntegralStatus status) =<< peek errPtr)

parseSchemaBytes :: BS.ByteString -> Either PolarsError [Field]
parseSchemaBytes bytes0 = do
    bytes1 <- stripSchemaMagic bytes0
    (fieldCount, bytes2) <- takeWord64 "schema field count" bytes1
    parseFields fieldCount bytes2 []
  where
    parseFields 0 rest acc
        | BS.null rest = Right (reverse acc)
        | otherwise = Left (invalidArgument "schema payload contained trailing bytes")
    parseFields remaining bytes acc = do
        (nameLen, bytes1) <- takeWord64 "schema field name length" bytes
        (nameBytes, bytes2) <- takeBytes "schema field name" nameLen bytes1
        name <- decodeUtf8Schema "schema field name" nameBytes
        (dtypeTag, bytes3) <- takeWord16 "schema dtype tag" bytes2
        (detailLen, bytes4) <- takeWord64 "schema dtype detail length" bytes3
        (detailBytes, bytes5) <- takeBytes "schema dtype detail" detailLen bytes4
        detail <- decodeUtf8Schema "schema dtype detail" detailBytes
        let dtype = dataTypeFromSchemaTag dtypeTag detail
        parseFields (remaining - 1) bytes5 (Field name dtype : acc)

stripSchemaMagic :: BS.ByteString -> Either PolarsError BS.ByteString
stripSchemaMagic bytes
    | schemaMagic `BS.isPrefixOf` bytes = Right (BS.drop (BS.length schemaMagic) bytes)
    | otherwise = Left (invalidArgument "schema payload has unsupported format")

takeBytes :: Text -> Int -> BS.ByteString -> Either PolarsError (BS.ByteString, BS.ByteString)
takeBytes label len bytes
    | len < 0 = Left (invalidArgument (label <> " length exceeds Haskell Int range"))
    | BS.length bytes < len = Left (invalidArgument (label <> " ended early"))
    | otherwise = Right (BS.splitAt len bytes)

takeWord64 :: Text -> BS.ByteString -> Either PolarsError (Int, BS.ByteString)
takeWord64 label bytes = do
    (wordBytes, rest) <- takeBytes label 8 bytes
    value <- word64ToInt (foldWordLE wordBytes)
    Right (value, rest)
  where
    foldWordLE :: BS.ByteString -> Word64
    foldWordLE = BS.foldr' (\byte acc -> (acc `shiftL` 8) .|. fromIntegral byte) 0

takeWord16 :: Text -> BS.ByteString -> Either PolarsError (Int, BS.ByteString)
takeWord16 label bytes = do
    (wordBytes, rest) <- takeBytes label 2 bytes
    Right (fromIntegral (foldWordLE wordBytes), rest)
  where
    foldWordLE :: BS.ByteString -> Word64
    foldWordLE = BS.foldr' (\byte acc -> (acc `shiftL` 8) .|. fromIntegral byte) 0

decodeUtf8Schema :: Text -> BS.ByteString -> Either PolarsError Text
decodeUtf8Schema label bytes = case TE.decodeUtf8' bytes of
    Left _ -> Left (invalidArgument (label <> " contained invalid UTF-8"))
    Right text -> Right text

word64ToInt :: Word64 -> Either PolarsError Int
word64ToInt value
    | value <= fromIntegral (maxBound :: Int) = Right (fromIntegral value)
    | otherwise = Left (invalidArgument "integer conversion exceeds Haskell Int range")

nonNegativeWord64 :: Text -> Int -> Either PolarsError Word64
nonNegativeWord64 label value
    | value < 0 = Left (invalidArgument (label <> " must be non-negative"))
    | otherwise = Right (fromIntegral value)

optionalNonNegativeWord64 :: Text -> Maybe Int -> Either PolarsError (Bool, Word64)
optionalNonNegativeWord64 _ Nothing = Right (False, 0)
optionalNonNegativeWord64 label (Just value) = do
    word <- nonNegativeWord64 label value
    Right (True, word)

validDataFrameSampleFraction :: DataFrameSampleOptions -> Double -> Either PolarsError ()
validDataFrameSampleFraction options frac
    | isNaN frac || isInfinite frac =
        Left (invalidArgument "dataFrameSampleFrac fraction must be finite")
    | frac < 0 =
        Left (invalidArgument "dataFrameSampleFrac fraction must be non-negative")
    | not (dataFrameSampleWithReplacement options) && frac > 1.0 =
        Left (invalidArgument "dataFrameSampleFrac fraction must be at most 1.0 without replacement")
    | otherwise = Right ()

seedWord64 :: Maybe Word64 -> (Bool, Word64)
seedWord64 Nothing = (False, 0)
seedWord64 (Just seed) = (True, seed)

csvReadWordOptions :: CsvReadOptions -> Either PolarsError (Bool, Word64, Word64, Word64, Bool, Word64)
csvReadWordOptions options = do
    (hasNRows, nRows) <- optionalNonNegativeWord64 "csvReadNRows" (csvReadNRows options)
    skipRows <- nonNegativeWord64 "csvReadSkipRows" (csvReadSkipRows options)
    skipRowsAfterHeader <- nonNegativeWord64 "csvReadSkipRowsAfterHeader" (csvReadSkipRowsAfterHeader options)
    (hasInferSchemaLength, inferSchemaLength) <- optionalNonNegativeWord64 "csvReadInferSchemaLength" (csvReadInferSchemaLength options)
    Right (hasNRows, nRows, skipRows, skipRowsAfterHeader, hasInferSchemaLength, inferSchemaLength)

parquetCompressionCode :: ParquetCompression -> CInt
parquetCompressionCode ParquetDefaultCompression = 0
parquetCompressionCode ParquetUncompressed = 1
parquetCompressionCode ParquetSnappy = 2
parquetCompressionCode ParquetZstd = 3

parquetParallelCode :: ParquetParallelStrategy -> CInt
parquetParallelCode ParquetParallelAuto = 0
parquetParallelCode ParquetParallelNone = 1
parquetParallelCode ParquetParallelColumns = 2
parquetParallelCode ParquetParallelRowGroups = 3
parquetParallelCode ParquetParallelPrefiltered = 4

parquetWriteWordOptions :: ParquetWriteOptions -> Either PolarsError (Bool, Word64, Bool, Word64)
parquetWriteWordOptions options = do
    (hasRowGroupSize, rowGroupSize) <- optionalNonNegativeWord64 "parquetWriteRowGroupSize" (parquetWriteRowGroupSize options)
    (hasDataPageSize, dataPageSize) <- optionalNonNegativeWord64 "parquetWriteDataPageSize" (parquetWriteDataPageSize options)
    Right (hasRowGroupSize, rowGroupSize, hasDataPageSize, dataPageSize)

dataFrameSortValidatedOptions :: DataFrameSortOptions -> [Text] -> Either PolarsError ([Word8], [Word8], Bool, Word64)
dataFrameSortValidatedOptions options names = do
    let nameCount = length names
    if nameCount == 0
        then Left (invalidArgument "dataFrameSort requires at least one column name")
        else Right ()
    descending <- boolOptionWords "dataFrameSortDescending" nameCount (dataFrameSortDescending options)
    nullsLast <- boolOptionWords "dataFrameSortNullsLast" nameCount (dataFrameSortNullsLast options)
    (hasLimit, limitValue) <- optionalNonNegativeWord64 "dataFrameSort limit" (dataFrameSortLimit options)
    Right (descending, nullsLast, hasLimit, limitValue)

boolOptionWords :: Text -> Int -> [Bool] -> Either PolarsError [Word8]
boolOptionWords label nameCount values
    | optionCount == 1 || optionCount == nameCount = Right (map boolWord8 values)
    | otherwise = Left (invalidArgument (label <> " must contain one value or one value per sort column"))
  where
    optionCount = length values

boolWord8 :: Bool -> Word8
boolWord8 False = 0
boolWord8 True = 1

dataFrameUniqueKeepStrategyCode :: DataFrameUniqueKeepStrategy -> CInt
dataFrameUniqueKeepStrategyCode DataFrameKeepFirst = 0
dataFrameUniqueKeepStrategyCode DataFrameKeepLast = 1
dataFrameUniqueKeepStrategyCode DataFrameKeepNone = 2
dataFrameUniqueKeepStrategyCode DataFrameKeepAny = 3

validateDataFrameJoinOptions :: DataFrameJoinOptions -> Either PolarsError ()
validateDataFrameJoinOptions options
    | dataFrameJoinType options == DataFrameJoinCross && (not (null leftKeys) || not (null rightKeys)) =
        Left (invalidArgument "dataFrameJoin cross join requires empty join key lists")
    | dataFrameJoinType options == DataFrameJoinCross = Right ()
    | null leftKeys = Left (invalidArgument "dataFrameJoin left keys must contain at least one column name")
    | null rightKeys = Left (invalidArgument "dataFrameJoin right keys must contain at least one column name")
    | length leftKeys /= length rightKeys = Left (invalidArgument "dataFrameJoin left and right key counts must match")
    | otherwise = Right ()
  where
    leftKeys = dataFrameJoinLeftOn options
    rightKeys = dataFrameJoinRightOn options

dataFrameJoinTypeCode :: DataFrameJoinType -> CInt
dataFrameJoinTypeCode DataFrameJoinInner = 0
dataFrameJoinTypeCode DataFrameJoinLeft = 1
dataFrameJoinTypeCode DataFrameJoinRight = 2
dataFrameJoinTypeCode DataFrameJoinFull = 3
dataFrameJoinTypeCode DataFrameJoinSemi = 4
dataFrameJoinTypeCode DataFrameJoinAnti = 5
dataFrameJoinTypeCode DataFrameJoinCross = 6

fillNullStrategyCode :: FillNullStrategy -> Either PolarsError (CInt, Bool, Word64)
fillNullStrategyCode (FillForward limit) = fillNullLimitedStrategy 0 limit
fillNullStrategyCode (FillBackward limit) = fillNullLimitedStrategy 1 limit
fillNullStrategyCode FillMean = Right (2, False, 0)
fillNullStrategyCode FillMin = Right (3, False, 0)
fillNullStrategyCode FillMax = Right (4, False, 0)
fillNullStrategyCode FillZero = Right (5, False, 0)
fillNullStrategyCode FillOne = Right (6, False, 0)

fillNullLimitedStrategy :: CInt -> Maybe Int -> Either PolarsError (CInt, Bool, Word64)
fillNullLimitedStrategy strategy limit = do
    (hasLimit, limitValue) <- optionalNonNegativeWord64 "fill null limit" limit
    Right (strategy, hasLimit, limitValue)

invalidArgument :: Text -> PolarsError
invalidArgument = PolarsError InvalidArgument

toCBool :: Bool -> CBool
toCBool False = CBool 0
toCBool True = CBool 1

fromIntegralStatus :: (Integral a) => a -> CInt
fromIntegralStatus = fromIntegral
