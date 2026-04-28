{- |
Module      : Polars.Internal.ColumnEncode
Description : Encode Haskell vectors into tagged column payloads for Rust constructors.

This module mirrors the tagged byte format decoded by Polars.Internal.ColumnDecode.
It keeps constructor payload encoding pure and centralised.
-}
module Polars.Internal.ColumnEncode
    ( encodeBoolColumn
    , encodeDoubleColumn
    , encodeInt64Column
    , encodeTextColumn
    ) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Lazy as LBS
import Data.Int (Int64)
import Data.Text (Text)
import qualified Data.Text.Encoding as TE
import qualified Data.Vector as V
import Data.Word (Word8)
import GHC.Float (castDoubleToWord64)

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

encodeDoubleColumn :: V.Vector (Maybe Double) -> BS.ByteString
encodeDoubleColumn = builderToStrict . V.foldMap encodeValue
  where
    encodeValue Nothing = nullTag
    encodeValue (Just value) = valueTag <> BB.word64LE (castDoubleToWord64 value)

encodeTextColumn :: V.Vector (Maybe Text) -> BS.ByteString
encodeTextColumn = builderToStrict . V.foldMap encodeValue
  where
    encodeValue Nothing = nullTag
    encodeValue (Just value) =
        let bytes = TE.encodeUtf8 value
         in valueTag <> BB.word64LE (fromIntegral (BS.length bytes)) <> BB.byteString bytes

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
