{- |
Module      : Polars.Internal.Bytes
Description : Copy and free Rust-owned byte buffers.

Rust returns phs_bytes handles for rendered text, schema bytes, and IPC payloads.
This module copies bytes into Haskell-owned ByteString values before releasing
Rust ownership.
-}
module Polars.Internal.Bytes
    ( copyAndFreeBytes
    ) where

import qualified Data.ByteString as BS
import Foreign.C.Types (CSize)
import Foreign.Ptr (Ptr, castPtr, nullPtr)

import Polars.Internal.Raw (RawBytes, phs_bytes_data, phs_bytes_free, phs_bytes_len)

copyAndFreeBytes :: Ptr RawBytes -> IO BS.ByteString
copyAndFreeBytes ptr
    | ptr == nullPtr = pure BS.empty
    | otherwise = do
        len <- phs_bytes_len ptr
        dataPtr <- phs_bytes_data ptr
        bytes <- if dataPtr == nullPtr
            then pure BS.empty
            else BS.packCStringLen (castPtr dataPtr, fromIntegral (len :: CSize))
        phs_bytes_free ptr
        pure bytes
