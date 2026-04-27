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
    , decodeInt64Column
    , decodeTextColumn
    ) where

import Data.Bits ((.|.), shiftL)
import qualified Data.ByteString as BS
import Data.Int (Int64)
import Data.Text (Text)
import qualified Data.Text.Encoding as TE
import Data.Vector (Vector)
import qualified Data.Vector as V
import Data.Word (Word8, Word64)
import GHC.Float (castWord64ToDouble)

import Polars.Error (PolarsError (..), PolarsErrorCode (InvalidArgument))

decodeBoolColumn :: BS.ByteString -> Either PolarsError (Vector (Maybe Bool))
decodeBoolColumn = decodeTaggedColumn decodeBoolValue

decodeInt64Column :: BS.ByteString -> Either PolarsError (Vector (Maybe Int64))
decodeInt64Column = decodeTaggedColumn decodeInt64Value

decodeDoubleColumn :: BS.ByteString -> Either PolarsError (Vector (Maybe Double))
decodeDoubleColumn = decodeTaggedColumn decodeDoubleValue

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

decodeDoubleValue :: BS.ByteString -> Either PolarsError (Double, BS.ByteString)
decodeDoubleValue bytes = do
    (word, rest) <- takeWord64 "column double payload ended early" bytes
    Right (castWord64ToDouble word, rest)

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

word64LE :: BS.ByteString -> Word64
word64LE bytes = foldl' step 0 (zip [0, 8 .. 56] (BS.unpack bytes))
  where
    step :: Word64 -> (Int, Word8) -> Word64
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
