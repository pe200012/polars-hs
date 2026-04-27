{-# LANGUAGE OverloadedStrings #-}

{- |
Module      : Polars.Internal.Result
Description : Convert Rust status/error out-pointers into Haskell errors.

Rust owns phs_error values. This module copies the message into Text and frees
that Rust allocation immediately.
-}
module Polars.Internal.Result
    ( consumeError
    , nullPointerError
    ) where

import qualified Data.ByteString as BS
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Foreign.C.String (CString)
import Foreign.C.Types (CInt)
import Foreign.Ptr (Ptr, nullPtr)

import Polars.Error (PolarsError (..), decodePolarsErrorCode)
import Polars.Internal.Raw (RawError, phs_error_code, phs_error_free, phs_error_message)

consumeError :: CInt -> Ptr RawError -> IO PolarsError
consumeError status errPtr
    | errPtr == nullPtr = pure (PolarsError (decodePolarsErrorCode (fromIntegral status)) "foreign call failed without an error object")
    | otherwise = do
        code <- phs_error_code errPtr
        messagePtr <- phs_error_message errPtr
        message <- peekUtf8 messagePtr
        phs_error_free errPtr
        pure (PolarsError (decodePolarsErrorCode (fromIntegral code)) message)

nullPointerError :: T.Text -> PolarsError
nullPointerError name = PolarsError (decodePolarsErrorCode 2) (name <> " pointer was null")

peekUtf8 :: CString -> IO T.Text
peekUtf8 ptr
    | ptr == nullPtr = pure ""
    | otherwise = TE.decodeUtf8 <$> BS.packCString ptr
