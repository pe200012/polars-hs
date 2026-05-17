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
    , ParquetCompression (..)
    , ParquetParallelStrategy (..)
    , ParquetReadOptions (..)
    , ParquetStatisticsOptions (..)
    , ParquetWriteOptions (..)
    , dataFrame
    , dataFrameDropColumns
    , dataFrameDropNulls
    , dataFrameFilter
    , dataFrameNullCount
    , dataFrameRename
    , dataFrameReverse
    , dataFrameSelect
    , dataFrameSlice
    , head
    , height
    , defaultCsvReadOptions
    , defaultCsvWriteOptions
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

import qualified Data.ByteString as BS
import Data.Bits ((.|.), shiftL)
import Data.Text (Text)
import qualified Data.Text.Encoding as TE
import Foreign.C.String (CString)
import Data.Word (Word64)
import Foreign.C.Types (CBool (..), CInt, CSize, CUChar (..))
import Foreign.Marshal.Alloc (alloca)
import Foreign.Marshal.Array (withArray)
import Foreign.Ptr (Ptr, nullPtr)
import Foreign.Storable (peek, poke)

import Polars.Error (PolarsError (..), PolarsErrorCode (InvalidArgument))
import Polars.Internal.Bytes (copyAndFreeBytes)
import Polars.Internal.CString (withFilePathCString, withMaybeTextCString, withTextCString)
import Polars.Internal.Managed (DataFrame, Series, mkDataFrame, withDataFrame, withSeries)
import Polars.Internal.Raw
    ( RawBytes
    , RawDataFrame
    , RawError
    , RawSeries
    , phs_dataframe_drop
    , phs_dataframe_drop_nulls
    , phs_dataframe_filter
    , phs_dataframe_head
    , phs_dataframe_new
    , phs_dataframe_height
    , phs_dataframe_null_count
    , phs_dataframe_rename
    , phs_dataframe_reverse
    , phs_dataframe_schema
    , phs_dataframe_select
    , phs_dataframe_shape
    , phs_dataframe_slice
    , phs_dataframe_tail
    , phs_dataframe_to_text
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

dataFrameRename :: [(Text, Text)] -> DataFrame -> IO (Either PolarsError DataFrame)
dataFrameRename [] _ = pure (Left (invalidArgument "dataFrameRename requires at least one column pair"))
dataFrameRename pairs df = withDataFrame df $ \ptr -> withRenamePairs pairs $ \existingArray newArray len ->
    dataframeOut (phs_dataframe_rename ptr existingArray newArray len)

dataFrameSlice :: Int -> Int -> DataFrame -> IO (Either PolarsError DataFrame)
dataFrameSlice offset len df = case nonNegativeWord64 "dataFrameSlice length" len of
    Left err -> pure (Left err)
    Right lenWord -> withDataFrame df $ \ptr ->
        dataframeOut (phs_dataframe_slice ptr (fromIntegral offset) lenWord)

dataFrameReverse :: DataFrame -> IO (Either PolarsError DataFrame)
dataFrameReverse df = withDataFrame df $ \ptr -> dataframeOut (phs_dataframe_reverse ptr)

dataFrameDropNulls :: Maybe [Text] -> DataFrame -> IO (Either PolarsError DataFrame)
dataFrameDropNulls (Just []) _ = pure (Left (invalidArgument "dataFrameDropNulls subset requires at least one column name"))
dataFrameDropNulls subset df = withDataFrame df $ \ptr -> withMaybeCStringList subset $ \nameArray len hasSubset ->
    dataframeOut (phs_dataframe_drop_nulls ptr nameArray len (toCBool hasSubset))

dataFrameNullCount :: DataFrame -> IO (Either PolarsError DataFrame)
dataFrameNullCount df = withDataFrame df $ \ptr -> dataframeOut (phs_dataframe_null_count ptr)

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

word64Out :: (Ptr Word64 -> Ptr (Ptr RawError) -> IO CInt) -> IO (Either PolarsError Int)
word64Out action =
    alloca $ \outPtr ->
        alloca $ \errPtr -> do
            poke errPtr nullPtr
            status <- action outPtr errPtr
            if fromIntegralStatus status == 0
                then word64ToInt <$> peek outPtr
                else Left <$> (consumeError (fromIntegralStatus status) =<< peek errPtr)

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

invalidArgument :: Text -> PolarsError
invalidArgument = PolarsError InvalidArgument

toCBool :: Bool -> CBool
toCBool False = CBool 0
toCBool True = CBool 1

fromIntegralStatus :: (Integral a) => a -> CInt
fromIntegralStatus = fromIntegral
