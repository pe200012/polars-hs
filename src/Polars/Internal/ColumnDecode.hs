{-# LANGUAGE OverloadedStrings #-}

{- |
Module      : Polars.Internal.ColumnDecode
Description : Decode typed column payloads returned by the Rust Polars adapter.

Rust encodes DataFrame column values as compact tagged byte streams. This module
keeps decoding pure, validates every byte boundary, and reports malformed
payloads as typed binding errors.
-}
module Polars.Internal.ColumnDecode
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
    ) where

import Data.Bits ((.|.), shiftL)
import qualified Data.ByteString as BS
import Data.Int (Int16, Int32, Int64, Int8)
import Data.Text (Text)
import qualified Data.Text.Encoding as TE
import Data.Vector (Vector)
import qualified Data.Vector as V
import Data.Word (Word16, Word32, Word64, Word8)
import GHC.Float (castWord32ToFloat, castWord64ToDouble)

import Polars.Error (PolarsError (..), PolarsErrorCode (InvalidArgument))

decodeBoolColumn :: BS.ByteString -> Either PolarsError (Vector (Maybe Bool))
decodeBoolColumn = decodeTaggedColumn decodeBoolValue

decodeInt64Column :: BS.ByteString -> Either PolarsError (Vector (Maybe Int64))
decodeInt64Column = decodeTaggedColumn decodeInt64Value

decodeInt8Column :: BS.ByteString -> Either PolarsError (Vector (Maybe Int8))
decodeInt8Column = decodeTaggedColumn decodeInt8Value

decodeInt16Column :: BS.ByteString -> Either PolarsError (Vector (Maybe Int16))
decodeInt16Column = decodeTaggedColumn decodeInt16Value

decodeInt32Column :: BS.ByteString -> Either PolarsError (Vector (Maybe Int32))
decodeInt32Column = decodeTaggedColumn decodeInt32Value

decodeWord8Column :: BS.ByteString -> Either PolarsError (Vector (Maybe Word8))
decodeWord8Column = decodeTaggedColumn decodeWord8Value

decodeWord16Column :: BS.ByteString -> Either PolarsError (Vector (Maybe Word16))
decodeWord16Column = decodeTaggedColumn decodeWord16Value

decodeWord32Column :: BS.ByteString -> Either PolarsError (Vector (Maybe Word32))
decodeWord32Column = decodeTaggedColumn decodeWord32Value

decodeWord64Column :: BS.ByteString -> Either PolarsError (Vector (Maybe Word64))
decodeWord64Column = decodeTaggedColumn decodeWord64Value

decodeDoubleColumn :: BS.ByteString -> Either PolarsError (Vector (Maybe Double))
decodeDoubleColumn = decodeTaggedColumn decodeDoubleValue

decodeFloatColumn :: BS.ByteString -> Either PolarsError (Vector (Maybe Float))
decodeFloatColumn = decodeTaggedColumn decodeFloatValue

decodeTextColumn :: BS.ByteString -> Either PolarsError (Vector (Maybe Text))
decodeTextColumn = decodeTaggedColumn decodeTextValue

decodeTaggedColumn :: (BS.ByteString -> Either PolarsError (a, BS.ByteString)) -> BS.ByteString -> Either PolarsError (Vector (Maybe a))
decodeTaggedColumn decodeValue = go []
  where
    go acc bytes = case BS.uncons bytes of
        Nothing -> Right (V.fromList (reverse acc))
        Just (tag, rest)
            | tag == tagNull -> go (Nothing : acc) rest
            | tag == tagValue -> do
                (value, remaining) <- decodeValue rest
                go (Just value : acc) remaining
            | otherwise -> decodeError "column payload contained an unknown tag"

decodeBoolValue :: BS.ByteString -> Either PolarsError (Bool, BS.ByteString)
decodeBoolValue bytes = case BS.uncons bytes of
    Nothing -> decodeError "column bool payload ended early"
    Just (0, rest) -> Right (False, rest)
    Just (1, rest) -> Right (True, rest)
    Just _ -> decodeError "column bool payload contained an invalid boolean value"

decodeInt64Value :: BS.ByteString -> Either PolarsError (Int64, BS.ByteString)
decodeInt64Value bytes = do
    (word, rest) <- takeWord64 "column int64 payload ended early" bytes
    Right (fromIntegral word, rest)

decodeInt8Value :: BS.ByteString -> Either PolarsError (Int8, BS.ByteString)
decodeInt8Value bytes = do
    (word, rest) <- takeWord8 "column int8 payload ended early" bytes
    Right (fromIntegral word, rest)

decodeInt16Value :: BS.ByteString -> Either PolarsError (Int16, BS.ByteString)
decodeInt16Value bytes = do
    (word, rest) <- takeWord16 "column int16 payload ended early" bytes
    Right (fromIntegral word, rest)

decodeInt32Value :: BS.ByteString -> Either PolarsError (Int32, BS.ByteString)
decodeInt32Value bytes = do
    (word, rest) <- takeWord32 "column int32 payload ended early" bytes
    Right (fromIntegral word, rest)

decodeWord8Value :: BS.ByteString -> Either PolarsError (Word8, BS.ByteString)
decodeWord8Value = takeWord8 "column word8 payload ended early"

decodeWord16Value :: BS.ByteString -> Either PolarsError (Word16, BS.ByteString)
decodeWord16Value = takeWord16 "column word16 payload ended early"

decodeWord32Value :: BS.ByteString -> Either PolarsError (Word32, BS.ByteString)
decodeWord32Value = takeWord32 "column word32 payload ended early"

decodeWord64Value :: BS.ByteString -> Either PolarsError (Word64, BS.ByteString)
decodeWord64Value = takeWord64 "column word64 payload ended early"

decodeDoubleValue :: BS.ByteString -> Either PolarsError (Double, BS.ByteString)
decodeDoubleValue bytes = do
    (word, rest) <- takeWord64 "column double payload ended early" bytes
    Right (castWord64ToDouble word, rest)

decodeFloatValue :: BS.ByteString -> Either PolarsError (Float, BS.ByteString)
decodeFloatValue bytes = do
    (word, rest) <- takeWord32 "column float payload ended early" bytes
    Right (castWord32ToFloat word, rest)

decodeTextValue :: BS.ByteString -> Either PolarsError (Text, BS.ByteString)
decodeTextValue bytes = do
    (lenWord, afterLength) <- takeWord64 "column text payload ended early" bytes
    len <- word64ToInt lenWord
    let (textBytes, rest) = BS.splitAt len afterLength
    if BS.length textBytes == len
        then case TE.decodeUtf8' textBytes of
            Left _ -> decodeError "column text payload contained invalid UTF-8"
            Right text -> Right (text, rest)
        else decodeError "column text payload ended early"

takeWord64 :: Text -> BS.ByteString -> Either PolarsError (Word64, BS.ByteString)
takeWord64 message bytes =
    let (wordBytes, rest) = BS.splitAt 8 bytes
     in if BS.length wordBytes == 8
        then Right (word64LE wordBytes, rest)
        else decodeError message

takeWord32 :: Text -> BS.ByteString -> Either PolarsError (Word32, BS.ByteString)
takeWord32 message bytes =
    let (wordBytes, rest) = BS.splitAt 4 bytes
     in if BS.length wordBytes == 4
            then Right (word32LE wordBytes, rest)
            else decodeError message

takeWord16 :: Text -> BS.ByteString -> Either PolarsError (Word16, BS.ByteString)
takeWord16 message bytes =
    let (wordBytes, rest) = BS.splitAt 2 bytes
     in if BS.length wordBytes == 2
            then Right (word16LE wordBytes, rest)
            else decodeError message

takeWord8 :: Text -> BS.ByteString -> Either PolarsError (Word8, BS.ByteString)
takeWord8 message bytes = case BS.uncons bytes of
    Nothing -> decodeError message
    Just (word, rest) -> Right (word, rest)

word64LE :: BS.ByteString -> Word64
word64LE bytes = foldl' step 0 (zip [0, 8 .. 56] (BS.unpack bytes))
  where
    step :: Word64 -> (Int, Word8) -> Word64
    step acc (shiftBits, byte) = acc .|. shiftL (fromIntegral byte) shiftBits

word32LE :: BS.ByteString -> Word32
word32LE bytes = foldl' step 0 (zip [0, 8 .. 24] (BS.unpack bytes))
  where
    step :: Word32 -> (Int, Word8) -> Word32
    step acc (shiftBits, byte) = acc .|. shiftL (fromIntegral byte) shiftBits

word16LE :: BS.ByteString -> Word16
word16LE bytes = foldl' step 0 (zip [0, 8] (BS.unpack bytes))
  where
    step :: Word16 -> (Int, Word8) -> Word16
    step acc (shiftBits, byte) = acc .|. shiftL (fromIntegral byte) shiftBits

word64ToInt :: Word64 -> Either PolarsError Int
word64ToInt value
    | value <= fromIntegral (maxBound :: Int) = Right (fromIntegral value)
    | otherwise = decodeError "column text payload length exceeds Haskell Int"

decodeError :: Text -> Either PolarsError a
decodeError = Left . PolarsError InvalidArgument

tagNull :: Word8
tagNull = 0

tagValue :: Word8
tagValue = 1
