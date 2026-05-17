{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE OverloadedStrings #-}

{- |
Module      : Polars.IO
Description : Public option records for Polars IO helpers.

This module keeps file IO configuration in Haskell records while Rust performs
the final conversion to Polars 0.53 reader and writer option types.
-}
module Polars.IO
    ( CsvReadOptions (..)
    , CsvWriteOptions (..)
    , ParquetCompression (..)
    , ParquetReadOptions (..)
    , ParquetScanOptions (..)
    , ParquetWriteOptions (..)
    , defaultCsvReadOptions
    , defaultCsvWriteOptions
    , defaultParquetReadOptions
    , defaultParquetScanOptions
    , defaultParquetWriteOptions
    ) where

import Data.Text (Text)
import Data.Word (Word8)

data CsvReadOptions = CsvReadOptions
    { csvReadHasHeader :: !Bool
    , csvReadSeparator :: !Word8
    , csvReadNullValue :: !(Maybe Text)
    , csvReadNRows :: !(Maybe Int)
    , csvReadSkipRows :: !Int
    , csvReadSkipRowsAfterHeader :: !Int
    , csvReadInferSchemaLength :: !(Maybe Int)
    , csvReadIgnoreErrors :: !Bool
    , csvReadTruncateRaggedLines :: !Bool
    , csvReadMissingIsNull :: !Bool
    , csvReadLowMemory :: !Bool
    , csvReadRechunk :: !Bool
    }
    deriving stock (Eq, Show)

defaultCsvReadOptions :: CsvReadOptions
defaultCsvReadOptions =
    CsvReadOptions
        { csvReadHasHeader = True
        , csvReadSeparator = 44
        , csvReadNullValue = Nothing
        , csvReadNRows = Nothing
        , csvReadSkipRows = 0
        , csvReadSkipRowsAfterHeader = 0
        , csvReadInferSchemaLength = Just 100
        , csvReadIgnoreErrors = False
        , csvReadTruncateRaggedLines = False
        , csvReadMissingIsNull = True
        , csvReadLowMemory = False
        , csvReadRechunk = False
        }

data CsvWriteOptions = CsvWriteOptions
    { csvWriteIncludeHeader :: !Bool
    , csvWriteSeparator :: !Word8
    , csvWriteNullValue :: !Text
    }
    deriving stock (Eq, Show)

defaultCsvWriteOptions :: CsvWriteOptions
defaultCsvWriteOptions =
    CsvWriteOptions
        { csvWriteIncludeHeader = True
        , csvWriteSeparator = 44
        , csvWriteNullValue = ""
        }

newtype ParquetReadOptions = ParquetReadOptions
    { parquetReadNRows :: Maybe Int
    }
    deriving stock (Eq, Show)

defaultParquetReadOptions :: ParquetReadOptions
defaultParquetReadOptions =
    ParquetReadOptions
        { parquetReadNRows = Nothing
        }

data ParquetCompression
    = ParquetDefaultCompression
    | ParquetUncompressed
    | ParquetSnappy
    | ParquetZstd
    deriving stock (Eq, Show)

data ParquetWriteOptions = ParquetWriteOptions
    { parquetWriteCompression :: !ParquetCompression
    , parquetWriteRowGroupSize :: !(Maybe Int)
    }
    deriving stock (Eq, Show)

defaultParquetWriteOptions :: ParquetWriteOptions
defaultParquetWriteOptions =
    ParquetWriteOptions
        { parquetWriteCompression = ParquetDefaultCompression
        , parquetWriteRowGroupSize = Nothing
        }

data ParquetScanOptions = ParquetScanOptions
    { parquetScanNRows :: !(Maybe Int)
    , parquetScanUseStatistics :: !Bool
    , parquetScanLowMemory :: !Bool
    , parquetScanRechunk :: !Bool
    , parquetScanCache :: !Bool
    }
    deriving stock (Eq, Show)

defaultParquetScanOptions :: ParquetScanOptions
defaultParquetScanOptions =
    ParquetScanOptions
        { parquetScanNRows = Nothing
        , parquetScanUseStatistics = True
        , parquetScanLowMemory = False
        , parquetScanRechunk = False
        , parquetScanCache = True
        }
