{- |
Module      : Polars.Column
Description : Typed DataFrame column extraction helpers.

This module exposes safe column-by-name extraction from Rust-owned Polars
DataFrames. Each function copies a Rust-encoded column payload into Haskell and
returns one `Maybe` value per DataFrame row.
-}
module Polars.Column
    ( columnBool
    , columnDouble
    , columnInt64
    , columnText
    ) where

import qualified Data.ByteString as BS
import Data.Int (Int64)
import Data.Text (Text)
import Foreign.C.String (CString)
import Foreign.C.Types (CInt)
import Foreign.Marshal.Alloc (alloca)
import Foreign.Ptr (Ptr, nullPtr)
import Foreign.Storable (peek, poke)

import Polars.DataFrame (DataFrame)
import Polars.Error (PolarsError)
import Polars.Internal.Bytes (copyAndFreeBytes)
import Polars.Internal.ColumnDecode
    ( decodeBoolColumn
    , decodeDoubleColumn
    , decodeInt64Column
    , decodeTextColumn
    )
import Polars.Internal.CString (withTextCString)
import Polars.Internal.Managed (withDataFrame)
import Polars.Internal.Raw
    ( RawBytes
    , RawDataFrame
    , RawError
    , phs_dataframe_column_bool
    , phs_dataframe_column_f64
    , phs_dataframe_column_i64
    , phs_dataframe_column_text
    )
import Polars.Internal.Result (consumeError)

columnBool :: DataFrame -> Text -> IO (Either PolarsError [Maybe Bool])
columnBool df name = columnBytesOut df name phs_dataframe_column_bool decodeBoolColumn

columnInt64 :: DataFrame -> Text -> IO (Either PolarsError [Maybe Int64])
columnInt64 df name = columnBytesOut df name phs_dataframe_column_i64 decodeInt64Column

columnDouble :: DataFrame -> Text -> IO (Either PolarsError [Maybe Double])
columnDouble df name = columnBytesOut df name phs_dataframe_column_f64 decodeDoubleColumn

columnText :: DataFrame -> Text -> IO (Either PolarsError [Maybe Text])
columnText df name = columnBytesOut df name phs_dataframe_column_text decodeTextColumn

type ColumnBytesAction =
    Ptr RawDataFrame -> CString -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt

columnBytesOut :: DataFrame -> Text -> ColumnBytesAction -> (BS.ByteString -> Either PolarsError a) -> IO (Either PolarsError a)
columnBytesOut df name action decode = withDataFrame df $ \ptr ->
    withTextCString name $ \cName ->
        alloca $ \outPtr ->
            alloca $ \errPtr -> do
                poke outPtr nullPtr
                poke errPtr nullPtr
                status <- action ptr cName outPtr errPtr
                if status == 0
                    then do
                        bytes <- copyAndFreeBytes =<< peek outPtr
                        pure (decode bytes)
                    else Left <$> (consumeError status =<< peek errPtr)
