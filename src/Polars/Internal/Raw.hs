{-# LANGUAGE ForeignFunctionInterface #-}

{- |
Module      : Polars.Internal.Raw
Description : Raw C FFI imports for the Rust Polars adapter.

This module is the unsafe boundary. Public modules wrap these imports with
managed handles, scoped CString lifetimes, and typed error conversion.
-}
module Polars.Internal.Raw
    ( RawBytes
    , RawDataFrame
    , RawError
    , RawExpr
    , RawLazyFrame
    , phs_bytes_data
    , phs_bytes_free
    , phs_bytes_len
    , phs_dataframe_free_finalizer
    , phs_dataframe_from_ipc_bytes
    , phs_dataframe_head
    , phs_dataframe_height
    , phs_dataframe_schema
    , phs_dataframe_shape
    , phs_dataframe_tail
    , phs_dataframe_to_ipc_bytes
    , phs_dataframe_to_text
    , phs_dataframe_width
    , phs_error_code
    , phs_error_free
    , phs_error_message
    , phs_expr_agg
    , phs_expr_alias
    , phs_expr_binary
    , phs_expr_col
    , phs_expr_free_finalizer
    , phs_expr_lit_bool
    , phs_expr_lit_double
    , phs_expr_lit_int
    , phs_expr_lit_text
    , phs_expr_not
    , phs_lazyframe_collect
    , phs_lazyframe_filter
    , phs_lazyframe_free_finalizer
    , phs_lazyframe_group_by_agg
    , phs_lazyframe_join
    , phs_lazyframe_limit
    , phs_lazyframe_select
    , phs_lazyframe_sort
    , phs_lazyframe_with_columns
    , phs_read_csv
    , phs_read_ipc_file
    , phs_read_parquet
    , phs_scan_csv
    , phs_scan_parquet
    , phs_write_ipc_file
    ) where

import Data.Word (Word8, Word64)
import Foreign.C.String (CString)
import Foreign.C.Types (CBool (..), CDouble (..), CInt (..), CLLong (..), CSize (..))
import Foreign.ForeignPtr (FinalizerPtr)
import Foreign.Ptr (Ptr)

data RawBytes
data RawDataFrame
data RawError
data RawExpr
data RawLazyFrame

foreign import ccall unsafe "phs_error_code"
    phs_error_code :: Ptr RawError -> IO CInt

foreign import ccall unsafe "phs_error_message"
    phs_error_message :: Ptr RawError -> IO CString

foreign import ccall unsafe "phs_error_free"
    phs_error_free :: Ptr RawError -> IO ()

foreign import ccall unsafe "phs_bytes_len"
    phs_bytes_len :: Ptr RawBytes -> IO CSize

foreign import ccall unsafe "phs_bytes_data"
    phs_bytes_data :: Ptr RawBytes -> IO (Ptr Word8)

foreign import ccall unsafe "phs_bytes_free"
    phs_bytes_free :: Ptr RawBytes -> IO ()

foreign import ccall unsafe "&phs_dataframe_free"
    phs_dataframe_free_finalizer :: FinalizerPtr RawDataFrame

foreign import ccall unsafe "&phs_lazyframe_free"
    phs_lazyframe_free_finalizer :: FinalizerPtr RawLazyFrame

foreign import ccall unsafe "&phs_expr_free"
    phs_expr_free_finalizer :: FinalizerPtr RawExpr

foreign import ccall unsafe "phs_read_csv"
    phs_read_csv :: CString -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_read_parquet"
    phs_read_parquet :: CString -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_dataframe_shape"
    phs_dataframe_shape :: Ptr RawDataFrame -> Ptr Word64 -> Ptr Word64 -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_dataframe_height"
    phs_dataframe_height :: Ptr RawDataFrame -> Ptr Word64 -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_dataframe_width"
    phs_dataframe_width :: Ptr RawDataFrame -> Ptr Word64 -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_dataframe_schema"
    phs_dataframe_schema :: Ptr RawDataFrame -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_dataframe_head"
    phs_dataframe_head :: Ptr RawDataFrame -> Word64 -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_dataframe_tail"
    phs_dataframe_tail :: Ptr RawDataFrame -> Word64 -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_dataframe_to_text"
    phs_dataframe_to_text :: Ptr RawDataFrame -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_expr_col"
    phs_expr_col :: CString -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_expr_lit_bool"
    phs_expr_lit_bool :: CBool -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_expr_lit_int"
    phs_expr_lit_int :: CLLong -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_expr_lit_double"
    phs_expr_lit_double :: CDouble -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_expr_lit_text"
    phs_expr_lit_text :: CString -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_expr_alias"
    phs_expr_alias :: Ptr RawExpr -> CString -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_expr_not"
    phs_expr_not :: Ptr RawExpr -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_expr_binary"
    phs_expr_binary :: CInt -> Ptr RawExpr -> Ptr RawExpr -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_expr_agg"
    phs_expr_agg :: CInt -> Ptr RawExpr -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_scan_csv"
    phs_scan_csv :: CString -> Ptr (Ptr RawLazyFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_scan_parquet"
    phs_scan_parquet :: CString -> Ptr (Ptr RawLazyFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_lazyframe_collect"
    phs_lazyframe_collect :: Ptr RawLazyFrame -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_lazyframe_filter"
    phs_lazyframe_filter :: Ptr RawLazyFrame -> Ptr RawExpr -> Ptr (Ptr RawLazyFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_lazyframe_select"
    phs_lazyframe_select :: Ptr RawLazyFrame -> Ptr (Ptr RawExpr) -> CSize -> Ptr (Ptr RawLazyFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_lazyframe_with_columns"
    phs_lazyframe_with_columns :: Ptr RawLazyFrame -> Ptr (Ptr RawExpr) -> CSize -> Ptr (Ptr RawLazyFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_lazyframe_sort"
    phs_lazyframe_sort :: Ptr RawLazyFrame -> Ptr CString -> CSize -> Ptr (Ptr RawLazyFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_lazyframe_limit"
    phs_lazyframe_limit :: Ptr RawLazyFrame -> Word64 -> Ptr (Ptr RawLazyFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_lazyframe_group_by_agg"
    phs_lazyframe_group_by_agg :: Ptr RawLazyFrame -> Ptr (Ptr RawExpr) -> CSize -> Ptr (Ptr RawExpr) -> CSize -> CBool -> Ptr (Ptr RawLazyFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_lazyframe_join"
    phs_lazyframe_join :: Ptr RawLazyFrame -> Ptr RawLazyFrame -> Ptr (Ptr RawExpr) -> CSize -> Ptr (Ptr RawExpr) -> CSize -> CInt -> CString -> Ptr (Ptr RawLazyFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_dataframe_to_ipc_bytes"
    phs_dataframe_to_ipc_bytes :: Ptr RawDataFrame -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_dataframe_from_ipc_bytes"
    phs_dataframe_from_ipc_bytes :: Ptr Word8 -> CSize -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_read_ipc_file"
    phs_read_ipc_file :: CString -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_write_ipc_file"
    phs_write_ipc_file :: CString -> Ptr RawDataFrame -> Ptr (Ptr RawError) -> IO CInt
