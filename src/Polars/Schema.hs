{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE OverloadedStrings #-}

{- |
Module      : Polars.Schema
Description : Schema types returned by Polars DataFrames.

The MVP schema decoder maps common Polars debug datatype names into a small
Haskell datatype and preserves unknown names for forward compatibility. Both
eager and lazy schema APIs decode the same Rust-owned structured byte format.
-}
module Polars.Schema
    ( DataType (..)
    , Field (..)
    , dataTypeFromSchemaTag
    , parseDataType
    , parseSchemaBytes
    ) where

import qualified Data.ByteString as BS
import Data.Bits ((.|.), shiftL)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Data.Word (Word64)

import Polars.Error (PolarsError (..), PolarsErrorCode (InvalidArgument))

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

schemaMagic :: BS.ByteString
schemaMagic = "PHS1SCH\0"

-- | Decode the structured schema byte payload produced by the Rust ABI.
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
    value <- schemaWord64ToInt (foldWordLE wordBytes)
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

schemaWord64ToInt :: Word64 -> Either PolarsError Int
schemaWord64ToInt value
    | value <= fromIntegral (maxBound :: Int) = Right (fromIntegral value)
    | otherwise = Left (invalidArgument "integer conversion exceeds Haskell Int range")

invalidArgument :: Text -> PolarsError
invalidArgument = PolarsError InvalidArgument
