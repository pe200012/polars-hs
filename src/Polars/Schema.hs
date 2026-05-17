{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE OverloadedStrings #-}

{- |
Module      : Polars.Schema
Description : Schema types returned by Polars DataFrames.

The MVP schema decoder maps common Polars debug datatype names into a small
Haskell datatype and preserves unknown names for forward compatibility.
-}
module Polars.Schema
    ( DataType (..)
    , Field (..)
    , dataTypeFromSchemaTag
    , parseDataType
    ) where

import Data.Text (Text)
import qualified Data.Text as T

-- | Haskell representation of common Polars data types.
data DataType
    = Boolean
    | Int8
    | Int16
    | Int32
    | Int64
    | UInt8
    | UInt16
    | UInt32
    | UInt64
    | Float32
    | Float64
    | Utf8
    | Date
    | Datetime
    | Duration
    | Time
    | Binary
    | Null
    | Categorical
    | UnknownType !Text
    deriving stock (Eq, Show)

-- | A named field in a Polars DataFrame schema.
data Field = Field
    { fieldName :: !Text
    , fieldType :: !DataType
    }
    deriving stock (Eq, Show)

parseDataType :: Text -> DataType
parseDataType "Boolean" = Boolean
parseDataType "Int8" = Int8
parseDataType "Int16" = Int16
parseDataType "Int32" = Int32
parseDataType "Int64" = Int64
parseDataType "UInt8" = UInt8
parseDataType "UInt16" = UInt16
parseDataType "UInt32" = UInt32
parseDataType "UInt64" = UInt64
parseDataType "Float32" = Float32
parseDataType "Float64" = Float64
parseDataType "String" = Utf8
parseDataType "Utf8" = Utf8
parseDataType "Date" = Date
parseDataType value | "Datetime" `T.isPrefixOf` value = Datetime
parseDataType value | "Duration" `T.isPrefixOf` value = Duration
parseDataType "Time" = Time
parseDataType "Binary" = Binary
parseDataType "Null" = Null
parseDataType value | "Categorical" `T.isPrefixOf` value = Categorical
parseDataType value = UnknownType value

dataTypeFromSchemaTag :: Int -> Text -> DataType
dataTypeFromSchemaTag 0 _ = Boolean
dataTypeFromSchemaTag 1 _ = Int8
dataTypeFromSchemaTag 2 _ = Int16
dataTypeFromSchemaTag 3 _ = Int32
dataTypeFromSchemaTag 4 _ = Int64
dataTypeFromSchemaTag 5 _ = UInt8
dataTypeFromSchemaTag 6 _ = UInt16
dataTypeFromSchemaTag 7 _ = UInt32
dataTypeFromSchemaTag 8 _ = UInt64
dataTypeFromSchemaTag 9 _ = Float32
dataTypeFromSchemaTag 10 _ = Float64
dataTypeFromSchemaTag 11 _ = Utf8
dataTypeFromSchemaTag 12 _ = Date
dataTypeFromSchemaTag 13 _ = Datetime
dataTypeFromSchemaTag 14 _ = Duration
dataTypeFromSchemaTag 15 _ = Time
dataTypeFromSchemaTag 16 _ = Binary
dataTypeFromSchemaTag 17 _ = Null
dataTypeFromSchemaTag 18 _ = Categorical
dataTypeFromSchemaTag _ detail
    | T.null detail = UnknownType "unknown schema datatype"
    | otherwise = UnknownType detail
