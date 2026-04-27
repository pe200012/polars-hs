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
