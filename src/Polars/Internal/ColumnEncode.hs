{- |
Module      : Polars.Internal.ColumnEncode
Description : Encode Haskell vectors into tagged column payloads for Rust constructors.

This module mirrors the tagged byte format decoded by Polars.Internal.ColumnDecode.
It keeps constructor payload encoding pure and centralised.
-}
module Polars.Internal.ColumnEncode
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
    ) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Lazy as LBS
import Data.Int (Int16, Int32, Int64, Int8)
import Data.Text (Text)
import qualified Data.Text.Encoding as TE
import qualified Data.Vector as V
import Data.Word (Word16, Word32, Word64, Word8)
import GHC.Float (castDoubleToWord64, castFloatToWord32)

encodeBoolColumn :: V.Vector (Maybe Bool) -> BS.ByteString
encodeBoolColumn = builderToStrict . V.foldMap encodeValue
  where
    encodeValue Nothing = nullTag
    encodeValue (Just False) = valueTag <> BB.word8 0
    encodeValue (Just True) = valueTag <> BB.word8 1

encodeInt64Column :: V.Vector (Maybe Int64) -> BS.ByteString
encodeInt64Column = builderToStrict . V.foldMap encodeValue
  where
    encodeValue Nothing = nullTag
    encodeValue (Just value) = valueTag <> BB.int64LE value

encodeInt8Column :: V.Vector (Maybe Int8) -> BS.ByteString
encodeInt8Column = encodeFixedColumn (BB.word8 . fromIntegral)

encodeInt16Column :: V.Vector (Maybe Int16) -> BS.ByteString
encodeInt16Column = encodeFixedColumn (BB.word16LE . fromIntegral)

encodeInt32Column :: V.Vector (Maybe Int32) -> BS.ByteString
encodeInt32Column = encodeFixedColumn (BB.word32LE . fromIntegral)

encodeWord8Column :: V.Vector (Maybe Word8) -> BS.ByteString
encodeWord8Column = encodeFixedColumn BB.word8

encodeWord16Column :: V.Vector (Maybe Word16) -> BS.ByteString
encodeWord16Column = encodeFixedColumn BB.word16LE

encodeWord32Column :: V.Vector (Maybe Word32) -> BS.ByteString
encodeWord32Column = encodeFixedColumn BB.word32LE

encodeWord64Column :: V.Vector (Maybe Word64) -> BS.ByteString
encodeWord64Column = encodeFixedColumn BB.word64LE

encodeDoubleColumn :: V.Vector (Maybe Double) -> BS.ByteString
encodeDoubleColumn = builderToStrict . V.foldMap encodeValue
  where
    encodeValue Nothing = nullTag
    encodeValue (Just value) = valueTag <> BB.word64LE (castDoubleToWord64 value)

encodeFloatColumn :: V.Vector (Maybe Float) -> BS.ByteString
encodeFloatColumn = encodeFixedColumn (BB.word32LE . castFloatToWord32)

encodeTextColumn :: V.Vector (Maybe Text) -> BS.ByteString
encodeTextColumn = builderToStrict . V.foldMap encodeValue
  where
    encodeValue Nothing = nullTag
    encodeValue (Just value) =
        let bytes = TE.encodeUtf8 value
         in valueTag <> BB.word64LE (fromIntegral (BS.length bytes)) <> BB.byteString bytes

encodeFixedColumn :: (a -> BB.Builder) -> V.Vector (Maybe a) -> BS.ByteString
encodeFixedColumn encodeValue = builderToStrict . V.foldMap encodeMaybeValue
  where
    encodeMaybeValue Nothing = nullTag
    encodeMaybeValue (Just value) = valueTag <> encodeValue value

nullTag :: BB.Builder
nullTag = BB.word8 tagNull

valueTag :: BB.Builder
valueTag = BB.word8 tagValue

builderToStrict :: BB.Builder -> BS.ByteString
builderToStrict = LBS.toStrict . BB.toLazyByteString

tagNull :: Word8
tagNull = 0

tagValue :: Word8
tagValue = 1
