{- |
Module      : Polars.Internal.CString
Description : Scoped UTF-8 CString helpers for FFI calls.

All C strings created here are valid only for the dynamic extent of the callback.
Rust copies path and text data during each FFI call.
-}
module Polars.Internal.CString
    ( withFilePathCString
    , withTextCString
    ) where

import qualified Data.ByteString as BS
import qualified Data.Text as T
import Data.Text (Text)
import qualified Data.Text.Encoding as TE
import Foreign.C.String (CString)

withTextCString :: Text -> (CString -> IO a) -> IO a
withTextCString text = BS.useAsCString (TE.encodeUtf8 text)

withFilePathCString :: FilePath -> (CString -> IO a) -> IO a
withFilePathCString path = BS.useAsCString (TE.encodeUtf8 (T.pack path))
