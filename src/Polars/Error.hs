{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE OverloadedStrings #-}

{- |
Module      : Polars.Error
Description : Typed errors returned by the Polars Haskell binding.

This module defines Haskell-owned error values copied from Rust FFI error
handles. Rust error objects are freed immediately after their message is copied.
-}
module Polars.Error
    ( PolarsError (..)
    , PolarsErrorCode (..)
    , decodePolarsErrorCode
    ) where

import Data.Text (Text)

-- | Stable error categories used by the repository-owned C ABI.
data PolarsErrorCode
    = PolarsFailure
    | InvalidArgument
    | Utf8Error
    | PanicError
    | UnknownError !Int
    deriving stock (Eq, Show)

-- | A recoverable failure reported by Rust Polars or the FFI boundary.
data PolarsError = PolarsError
    { polarsErrorCode :: !PolarsErrorCode
    , polarsErrorMessage :: !Text
    }
    deriving stock (Eq, Show)

decodePolarsErrorCode :: Int -> PolarsErrorCode
decodePolarsErrorCode 1 = PolarsFailure
decodePolarsErrorCode 2 = InvalidArgument
decodePolarsErrorCode 3 = Utf8Error
decodePolarsErrorCode 4 = PanicError
decodePolarsErrorCode value = UnknownError value
