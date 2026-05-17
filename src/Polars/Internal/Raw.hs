{-# LANGUAGE ForeignFunctionInterface #-}

{- |
Module      : Polars.Internal.Raw
Description : Raw C FFI imports for the Rust Polars adapter.

This module is the unsafe boundary. Public modules wrap these imports with
managed handles, scoped CString lifetimes, and typed error conversion.
-}
module Polars.Internal.Raw
    ( RawArrowRecordBatch
    , RawArrowSeries
    , RawBytes
    , RawDataFrame
    , RawError
    , RawExpr
    , RawLazyFrame
    , RawSeries
    , phs_bytes_data
    , phs_bytes_free
    , phs_arrow_record_batch_array
    , phs_arrow_record_batch_free_finalizer
    , phs_arrow_record_batch_schema
    , phs_arrow_series_array
    , phs_arrow_series_free_finalizer
    , phs_arrow_series_schema
    , phs_bytes_len
    , phs_dataframe_column
    , phs_dataframe_column_bool
    , phs_dataframe_drop
    , phs_dataframe_drop_nulls
    , phs_dataframe_filter
    , phs_dataframe_fill_null
    , phs_dataframe_from_arrow_record_batch
    , phs_dataframe_hstack
    , phs_dataframe_to_arrow_record_batch
    , phs_dataframe_new
    , phs_dataframe_column_f32
    , phs_dataframe_column_f64
    , phs_dataframe_column_i8
    , phs_dataframe_column_i16
    , phs_dataframe_column_i32
    , phs_dataframe_column_i64
    , phs_dataframe_column_text
    , phs_dataframe_column_u8
    , phs_dataframe_column_u16
    , phs_dataframe_column_u32
    , phs_dataframe_column_u64
    , phs_dataframe_free_finalizer
    , phs_dataframe_from_ipc_bytes
    , phs_dataframe_head
    , phs_dataframe_height
    , phs_dataframe_join
    , phs_dataframe_null_count
    , phs_dataframe_rename
    , phs_dataframe_reverse
    , phs_dataframe_schema
    , phs_dataframe_select
    , phs_dataframe_shape
    , phs_dataframe_slice
    , phs_dataframe_sort
    , phs_dataframe_tail
    , phs_dataframe_take
    , phs_dataframe_to_ipc_bytes
    , phs_dataframe_to_text
    , phs_dataframe_unique
    , phs_dataframe_vstack
    , phs_dataframe_with_columns
    , phs_dataframe_width
    , phs_error_code
    , phs_error_free
    , phs_error_message
    , phs_expr_agg
    , phs_expr_alias
    , phs_expr_binary
    , phs_expr_binary_function
    , phs_expr_boolean_unary
    , phs_expr_cast
    , phs_expr_clip
    , phs_expr_col
    , phs_expr_free_finalizer
    , phs_expr_horizontal_function
    , phs_expr_is_between
    , phs_expr_is_close
    , phs_expr_is_in
    , phs_expr_name_function
    , phs_expr_lit_bool
    , phs_expr_lit_double
    , phs_expr_lit_int
    , phs_expr_lit_text
    , phs_expr_not
    , phs_expr_over
    , phs_expr_quantile
    , phs_expr_rank
    , phs_expr_slice
    , phs_expr_sort_by
    , phs_expr_string_function
    , phs_expr_string_function_i64
    , phs_expr_string_nary_function
    , phs_expr_list_function
    , phs_expr_temporal_function
    , phs_expr_temporal_time_unit
    , phs_expr_temporal_string
    , phs_expr_ternary
    , phs_expr_unary
    , phs_expr_unary_i64
    , phs_lazyframe_collect
    , phs_lazyframe_drop
    , phs_lazyframe_drop_nulls
    , phs_lazyframe_explain
    , phs_lazyframe_fill_nan
    , phs_lazyframe_fill_null
    , phs_lazyframe_filter
    , phs_lazyframe_free_finalizer
    , phs_lazyframe_group_by_agg
    , phs_lazyframe_head
    , phs_lazyframe_join
    , phs_lazyframe_limit
    , phs_lazyframe_null_count
    , phs_lazyframe_profile
    , phs_lazyframe_rename
    , phs_lazyframe_select
    , phs_lazyframe_slice
    , phs_lazyframe_sort
    , phs_lazyframe_tail
    , phs_lazyframe_unique
    , phs_lazyframe_with_columns
    , phs_read_csv
    , phs_read_csv_options
    , phs_read_ipc_file
    , phs_read_parquet
    , phs_read_parquet_options
    , phs_scan_csv
    , phs_scan_csv_options
    , phs_scan_parquet
    , phs_scan_parquet_options
    , phs_series_abs
    , phs_series_append
    , phs_series_arg_sort
    , phs_series_arg_unique
    , phs_series_binary_op
    , phs_series_cast
    , phs_series_ceil
    , phs_series_compare_op
    , phs_series_diff
    , phs_series_drop_nulls
    , phs_series_dtype
    , phs_series_filter
    , phs_series_fill_null
    , phs_series_floor
    , phs_series_free_finalizer
    , phs_series_gather_every
    , phs_series_head
    , phs_series_interpolate
    , phs_series_is_duplicated
    , phs_series_is_finite
    , phs_series_is_first_distinct
    , phs_series_is_infinite
    , phs_series_is_between
    , phs_series_is_last_distinct
    , phs_series_is_nan
    , phs_series_is_not_nan
    , phs_series_is_not_null
    , phs_series_is_null
    , phs_series_is_unique
    , phs_series_len
    , phs_series_mode
    , phs_series_name
    , phs_series_n_unique
    , phs_series_new_bool
    , phs_series_new_f32
    , phs_series_new_f64
    , phs_series_new_i8
    , phs_series_new_i16
    , phs_series_new_i32
    , phs_series_new_i64
    , phs_series_new_text
    , phs_series_new_u8
    , phs_series_new_u16
    , phs_series_new_u32
    , phs_series_new_u64
    , phs_series_pct_change
    , phs_series_rank
    , phs_series_rename
    , phs_series_reverse
    , phs_series_null_count
    , phs_series_round
    , phs_series_search_sorted
    , phs_series_shift
    , phs_series_slice
    , phs_series_sort
    , phs_series_stat
    , phs_series_tail
    , phs_series_take
    , phs_series_to_arrow_array
    , phs_series_to_frame
    , phs_series_unique
    , phs_series_unique_counts
    , phs_series_unique_stable
    , phs_series_value_counts
    , phs_series_values_bool
    , phs_series_values_f32
    , phs_series_values_f64
    , phs_series_values_i8
    , phs_series_values_i16
    , phs_series_values_i32
    , phs_series_values_i64
    , phs_series_values_text
    , phs_series_values_u8
    , phs_series_values_u16
    , phs_series_values_u32
    , phs_series_values_u64
    , phs_series_from_arrow_array
    , phs_series_zip_with
    , phs_write_csv
    , phs_write_csv_options
    , phs_write_ipc_file
    , phs_write_parquet
    , phs_write_parquet_options
    ) where

import Data.Word (Word8, Word32, Word64)
import Foreign.C.String (CString)
import Foreign.C.Types (CBool (..), CDouble (..), CInt (..), CLLong (..), CSize (..), CUChar (..))
import Foreign.ForeignPtr (FinalizerPtr)
import Foreign.Ptr (Ptr)

data RawArrowRecordBatch
data RawArrowSeries
data RawBytes
data RawDataFrame
data RawError
data RawExpr
data RawLazyFrame
data RawSeries

foreign import ccall unsafe "phs_error_code"
    phs_error_code :: Ptr RawError -> IO CInt

foreign import ccall unsafe "phs_error_message"
    phs_error_message :: Ptr RawError -> IO CString

foreign import ccall unsafe "phs_error_free"
    phs_error_free :: Ptr RawError -> IO ()

foreign import ccall unsafe "phs_arrow_record_batch_schema"
    phs_arrow_record_batch_schema :: Ptr RawArrowRecordBatch -> IO (Ptr ())

foreign import ccall unsafe "phs_arrow_record_batch_array"
    phs_arrow_record_batch_array :: Ptr RawArrowRecordBatch -> IO (Ptr ())

foreign import ccall unsafe "&phs_arrow_record_batch_free"
    phs_arrow_record_batch_free_finalizer :: FinalizerPtr RawArrowRecordBatch

foreign import ccall unsafe "phs_arrow_series_schema"
    phs_arrow_series_schema :: Ptr RawArrowSeries -> IO (Ptr ())

foreign import ccall unsafe "phs_arrow_series_array"
    phs_arrow_series_array :: Ptr RawArrowSeries -> IO (Ptr ())

foreign import ccall unsafe "&phs_arrow_series_free"
    phs_arrow_series_free_finalizer :: FinalizerPtr RawArrowSeries

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

foreign import ccall unsafe "&phs_series_free"
    phs_series_free_finalizer :: FinalizerPtr RawSeries

foreign import ccall safe "phs_read_csv"
    phs_read_csv :: CString -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_read_csv_options"
    phs_read_csv_options ::
        CString ->
        CBool ->
        CUChar ->
        CBool ->
        CString ->
        CBool ->
        Word64 ->
        Word64 ->
        Word64 ->
        CBool ->
        Word64 ->
        CBool ->
        CBool ->
        CBool ->
        CBool ->
        CBool ->
        Ptr (Ptr RawDataFrame) ->
        Ptr (Ptr RawError) ->
        IO CInt

foreign import ccall safe "phs_read_parquet"
    phs_read_parquet :: CString -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_read_parquet_options"
    phs_read_parquet_options ::
        CString ->
        CBool ->
        Word64 ->
        CInt ->
        CBool ->
        CBool ->
        Ptr (Ptr RawDataFrame) ->
        Ptr (Ptr RawError) ->
        IO CInt

foreign import ccall safe "phs_write_csv"
    phs_write_csv :: CString -> Ptr RawDataFrame -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_write_csv_options"
    phs_write_csv_options ::
        CString ->
        Ptr RawDataFrame ->
        CBool ->
        CUChar ->
        CString ->
        Ptr (Ptr RawError) ->
        IO CInt

foreign import ccall safe "phs_write_parquet"
    phs_write_parquet :: CString -> Ptr RawDataFrame -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_write_parquet_options"
    phs_write_parquet_options ::
        CString ->
        Ptr RawDataFrame ->
        CInt ->
        CBool ->
        Word64 ->
        CBool ->
        Word64 ->
        CBool ->
        CBool ->
        CBool ->
        CBool ->
        CBool ->
        Ptr (Ptr RawError) ->
        IO CInt

foreign import ccall unsafe "phs_dataframe_new"
    phs_dataframe_new :: Ptr (Ptr RawSeries) -> CSize -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_dataframe_select"
    phs_dataframe_select :: Ptr RawDataFrame -> Ptr CString -> CSize -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_dataframe_drop"
    phs_dataframe_drop :: Ptr RawDataFrame -> Ptr CString -> CSize -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_dataframe_rename"
    phs_dataframe_rename :: Ptr RawDataFrame -> Ptr CString -> Ptr CString -> CSize -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_dataframe_slice"
    phs_dataframe_slice :: Ptr RawDataFrame -> CLLong -> Word64 -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_dataframe_filter"
    phs_dataframe_filter :: Ptr RawDataFrame -> Ptr RawSeries -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_dataframe_take"
    phs_dataframe_take :: Ptr RawDataFrame -> Ptr Word64 -> CSize -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_dataframe_fill_null"
    phs_dataframe_fill_null :: Ptr RawDataFrame -> CInt -> CBool -> Word64 -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_dataframe_join"
    phs_dataframe_join :: Ptr RawDataFrame -> Ptr RawDataFrame -> Ptr CString -> CSize -> Ptr CString -> CSize -> CInt -> CString -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_dataframe_vstack"
    phs_dataframe_vstack :: Ptr RawDataFrame -> Ptr RawDataFrame -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_dataframe_hstack"
    phs_dataframe_hstack :: Ptr RawDataFrame -> Ptr (Ptr RawSeries) -> CSize -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_dataframe_with_columns"
    phs_dataframe_with_columns :: Ptr RawDataFrame -> Ptr (Ptr RawSeries) -> CSize -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_dataframe_sort"
    phs_dataframe_sort ::
        Ptr RawDataFrame ->
        Ptr CString ->
        CSize ->
        Ptr Word8 ->
        CSize ->
        Ptr Word8 ->
        CSize ->
        CBool ->
        CBool ->
        CBool ->
        Word64 ->
        Ptr (Ptr RawDataFrame) ->
        Ptr (Ptr RawError) ->
        IO CInt

foreign import ccall unsafe "phs_dataframe_unique"
    phs_dataframe_unique :: Ptr RawDataFrame -> Ptr CString -> CSize -> CBool -> CInt -> CBool -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_dataframe_reverse"
    phs_dataframe_reverse :: Ptr RawDataFrame -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_dataframe_drop_nulls"
    phs_dataframe_drop_nulls :: Ptr RawDataFrame -> Ptr CString -> CSize -> CBool -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_dataframe_null_count"
    phs_dataframe_null_count :: Ptr RawDataFrame -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_dataframe_from_arrow_record_batch"
    phs_dataframe_from_arrow_record_batch :: Ptr () -> Ptr () -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_dataframe_to_arrow_record_batch"
    phs_dataframe_to_arrow_record_batch :: Ptr RawDataFrame -> Ptr (Ptr RawArrowRecordBatch) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_series_from_arrow_array"
    phs_series_from_arrow_array :: Ptr () -> Ptr () -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_series_to_arrow_array"
    phs_series_to_arrow_array :: Ptr RawSeries -> Ptr (Ptr RawArrowSeries) -> Ptr (Ptr RawError) -> IO CInt

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

foreign import ccall unsafe "phs_dataframe_column"
    phs_dataframe_column :: Ptr RawDataFrame -> CString -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_dataframe_column_bool"
    phs_dataframe_column_bool :: Ptr RawDataFrame -> CString -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_dataframe_column_i64"
    phs_dataframe_column_i64 :: Ptr RawDataFrame -> CString -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_dataframe_column_i8"
    phs_dataframe_column_i8 :: Ptr RawDataFrame -> CString -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_dataframe_column_i16"
    phs_dataframe_column_i16 :: Ptr RawDataFrame -> CString -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_dataframe_column_i32"
    phs_dataframe_column_i32 :: Ptr RawDataFrame -> CString -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_dataframe_column_u8"
    phs_dataframe_column_u8 :: Ptr RawDataFrame -> CString -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_dataframe_column_u16"
    phs_dataframe_column_u16 :: Ptr RawDataFrame -> CString -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_dataframe_column_u32"
    phs_dataframe_column_u32 :: Ptr RawDataFrame -> CString -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_dataframe_column_u64"
    phs_dataframe_column_u64 :: Ptr RawDataFrame -> CString -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_dataframe_column_f32"
    phs_dataframe_column_f32 :: Ptr RawDataFrame -> CString -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_dataframe_column_f64"
    phs_dataframe_column_f64 :: Ptr RawDataFrame -> CString -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_dataframe_column_text"
    phs_dataframe_column_text :: Ptr RawDataFrame -> CString -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt

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

foreign import ccall safe "phs_scan_csv"
    phs_scan_csv :: CString -> Ptr (Ptr RawLazyFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_scan_csv_options"
    phs_scan_csv_options ::
        CString ->
        CBool ->
        CUChar ->
        CBool ->
        CString ->
        CBool ->
        Word64 ->
        Word64 ->
        Word64 ->
        CBool ->
        Word64 ->
        CBool ->
        CBool ->
        CBool ->
        CBool ->
        CBool ->
        Ptr (Ptr RawLazyFrame) ->
        Ptr (Ptr RawError) ->
        IO CInt

foreign import ccall safe "phs_scan_parquet"
    phs_scan_parquet :: CString -> Ptr (Ptr RawLazyFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_scan_parquet_options"
    phs_scan_parquet_options ::
        CString ->
        CBool ->
        Word64 ->
        CInt ->
        CBool ->
        CBool ->
        CBool ->
        CBool ->
        Ptr (Ptr RawLazyFrame) ->
        Ptr (Ptr RawError) ->
        IO CInt

foreign import ccall safe "phs_lazyframe_collect"
    phs_lazyframe_collect :: Ptr RawLazyFrame -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_lazyframe_explain"
    phs_lazyframe_explain :: Ptr RawLazyFrame -> CBool -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_lazyframe_profile"
    phs_lazyframe_profile :: Ptr RawLazyFrame -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt

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

foreign import ccall unsafe "phs_lazyframe_drop"
    phs_lazyframe_drop :: Ptr RawLazyFrame -> Ptr CString -> CSize -> Ptr (Ptr RawLazyFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_lazyframe_rename"
    phs_lazyframe_rename :: Ptr RawLazyFrame -> Ptr CString -> Ptr CString -> CSize -> CBool -> Ptr (Ptr RawLazyFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_lazyframe_slice"
    phs_lazyframe_slice :: Ptr RawLazyFrame -> CLLong -> Word64 -> Ptr (Ptr RawLazyFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_lazyframe_head"
    phs_lazyframe_head :: Ptr RawLazyFrame -> Word64 -> Ptr (Ptr RawLazyFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_lazyframe_tail"
    phs_lazyframe_tail :: Ptr RawLazyFrame -> Word64 -> Ptr (Ptr RawLazyFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_lazyframe_drop_nulls"
    phs_lazyframe_drop_nulls :: Ptr RawLazyFrame -> Ptr CString -> CSize -> CBool -> Ptr (Ptr RawLazyFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_lazyframe_fill_null"
    phs_lazyframe_fill_null :: Ptr RawLazyFrame -> Ptr RawExpr -> Ptr (Ptr RawLazyFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_lazyframe_fill_nan"
    phs_lazyframe_fill_nan :: Ptr RawLazyFrame -> Ptr RawExpr -> Ptr (Ptr RawLazyFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_lazyframe_null_count"
    phs_lazyframe_null_count :: Ptr RawLazyFrame -> Ptr (Ptr RawLazyFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_lazyframe_unique"
    phs_lazyframe_unique :: Ptr RawLazyFrame -> Ptr CString -> CSize -> CBool -> CInt -> CBool -> Ptr (Ptr RawLazyFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_lazyframe_group_by_agg"
    phs_lazyframe_group_by_agg :: Ptr RawLazyFrame -> Ptr (Ptr RawExpr) -> CSize -> Ptr (Ptr RawExpr) -> CSize -> CBool -> Ptr (Ptr RawLazyFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_lazyframe_join"
    phs_lazyframe_join :: Ptr RawLazyFrame -> Ptr RawLazyFrame -> Ptr (Ptr RawExpr) -> CSize -> Ptr (Ptr RawExpr) -> CSize -> CInt -> CString -> Ptr (Ptr RawLazyFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_dataframe_to_ipc_bytes"
    phs_dataframe_to_ipc_bytes :: Ptr RawDataFrame -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_dataframe_from_ipc_bytes"
    phs_dataframe_from_ipc_bytes :: Ptr Word8 -> CSize -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_read_ipc_file"
    phs_read_ipc_file :: CString -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_write_ipc_file"
    phs_write_ipc_file :: CString -> Ptr RawDataFrame -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_new_bool"
    phs_series_new_bool :: CString -> Ptr Word8 -> CSize -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_new_i64"
    phs_series_new_i64 :: CString -> Ptr Word8 -> CSize -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_new_i8"
    phs_series_new_i8 :: CString -> Ptr Word8 -> CSize -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_new_i16"
    phs_series_new_i16 :: CString -> Ptr Word8 -> CSize -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_new_i32"
    phs_series_new_i32 :: CString -> Ptr Word8 -> CSize -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_new_u8"
    phs_series_new_u8 :: CString -> Ptr Word8 -> CSize -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_new_u16"
    phs_series_new_u16 :: CString -> Ptr Word8 -> CSize -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_new_u32"
    phs_series_new_u32 :: CString -> Ptr Word8 -> CSize -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_new_u64"
    phs_series_new_u64 :: CString -> Ptr Word8 -> CSize -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_new_f32"
    phs_series_new_f32 :: CString -> Ptr Word8 -> CSize -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_new_f64"
    phs_series_new_f64 :: CString -> Ptr Word8 -> CSize -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_new_text"
    phs_series_new_text :: CString -> Ptr Word8 -> CSize -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_name"
    phs_series_name :: Ptr RawSeries -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_dtype"
    phs_series_dtype :: Ptr RawSeries -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_series_gather_every"
    phs_series_gather_every :: Ptr RawSeries -> Word64 -> Word64 -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_rename"
    phs_series_rename :: Ptr RawSeries -> CString -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_cast"
    phs_series_cast :: Ptr RawSeries -> CInt -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_series_pct_change"
    phs_series_pct_change :: Ptr RawSeries -> CLLong -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_series_search_sorted"
    phs_series_search_sorted :: Ptr RawSeries -> Ptr RawSeries -> CInt -> CBool -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_shift"
    phs_series_shift :: Ptr RawSeries -> CLLong -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_append"
    phs_series_append :: Ptr RawSeries -> Ptr RawSeries -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_series_binary_op"
    phs_series_binary_op :: Ptr RawSeries -> Ptr RawSeries -> CInt -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_series_compare_op"
    phs_series_compare_op :: Ptr RawSeries -> Ptr RawSeries -> CInt -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_sort"
    phs_series_sort :: Ptr RawSeries -> CBool -> CBool -> CBool -> CBool -> CBool -> Word64 -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_series_arg_sort"
    phs_series_arg_sort :: Ptr RawSeries -> CBool -> CBool -> CBool -> CBool -> CBool -> Word64 -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_unique"
    phs_series_unique :: Ptr RawSeries -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_series_unique_counts"
    phs_series_unique_counts :: Ptr RawSeries -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_unique_stable"
    phs_series_unique_stable :: Ptr RawSeries -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_series_arg_unique"
    phs_series_arg_unique :: Ptr RawSeries -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_series_rank"
    phs_series_rank :: Ptr RawSeries -> CInt -> CBool -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_reverse"
    phs_series_reverse :: Ptr RawSeries -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_drop_nulls"
    phs_series_drop_nulls :: Ptr RawSeries -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_len"
    phs_series_len :: Ptr RawSeries -> Ptr Word64 -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_null_count"
    phs_series_null_count :: Ptr RawSeries -> Ptr Word64 -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_series_n_unique"
    phs_series_n_unique :: Ptr RawSeries -> Ptr Word64 -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_series_mode"
    phs_series_mode :: Ptr RawSeries -> CBool -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_series_zip_with"
    phs_series_zip_with :: Ptr RawSeries -> Ptr RawSeries -> Ptr RawSeries -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_series_value_counts"
    phs_series_value_counts :: Ptr RawSeries -> CBool -> CBool -> CString -> CBool -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_series_stat"
    phs_series_stat :: Ptr RawSeries -> CInt -> CUChar -> Ptr CBool -> Ptr CDouble -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_head"
    phs_series_head :: Ptr RawSeries -> Word64 -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_tail"
    phs_series_tail :: Ptr RawSeries -> Word64 -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_slice"
    phs_series_slice :: Ptr RawSeries -> CLLong -> Word64 -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_series_take"
    phs_series_take :: Ptr RawSeries -> Ptr Word64 -> CSize -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_series_abs"
    phs_series_abs :: Ptr RawSeries -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_series_round"
    phs_series_round :: Ptr RawSeries -> Word32 -> CInt -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_series_floor"
    phs_series_floor :: Ptr RawSeries -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_series_ceil"
    phs_series_ceil :: Ptr RawSeries -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_series_diff"
    phs_series_diff :: Ptr RawSeries -> CLLong -> CInt -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_series_interpolate"
    phs_series_interpolate :: Ptr RawSeries -> CInt -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_is_null"
    phs_series_is_null :: Ptr RawSeries -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_is_not_null"
    phs_series_is_not_null :: Ptr RawSeries -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_series_is_nan"
    phs_series_is_nan :: Ptr RawSeries -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_series_is_not_nan"
    phs_series_is_not_nan :: Ptr RawSeries -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_series_is_finite"
    phs_series_is_finite :: Ptr RawSeries -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_series_is_infinite"
    phs_series_is_infinite :: Ptr RawSeries -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_series_is_duplicated"
    phs_series_is_duplicated :: Ptr RawSeries -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_series_is_unique"
    phs_series_is_unique :: Ptr RawSeries -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_series_is_first_distinct"
    phs_series_is_first_distinct :: Ptr RawSeries -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_series_is_last_distinct"
    phs_series_is_last_distinct :: Ptr RawSeries -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_series_is_between"
    phs_series_is_between :: Ptr RawSeries -> Ptr RawSeries -> Ptr RawSeries -> CInt -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_filter"
    phs_series_filter :: Ptr RawSeries -> Ptr RawSeries -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall safe "phs_series_fill_null"
    phs_series_fill_null :: Ptr RawSeries -> CInt -> CBool -> Word64 -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_to_frame"
    phs_series_to_frame :: Ptr RawSeries -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_values_bool"
    phs_series_values_bool :: Ptr RawSeries -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_values_i64"
    phs_series_values_i64 :: Ptr RawSeries -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_values_i8"
    phs_series_values_i8 :: Ptr RawSeries -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_values_i16"
    phs_series_values_i16 :: Ptr RawSeries -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_values_i32"
    phs_series_values_i32 :: Ptr RawSeries -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_values_u8"
    phs_series_values_u8 :: Ptr RawSeries -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_values_u16"
    phs_series_values_u16 :: Ptr RawSeries -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_values_u32"
    phs_series_values_u32 :: Ptr RawSeries -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_values_u64"
    phs_series_values_u64 :: Ptr RawSeries -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_values_f32"
    phs_series_values_f32 :: Ptr RawSeries -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_values_f64"
    phs_series_values_f64 :: Ptr RawSeries -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_values_text"
    phs_series_values_text :: Ptr RawSeries -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_expr_cast"
    phs_expr_cast :: CBool -> CInt -> Ptr RawExpr -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_expr_unary"
    phs_expr_unary :: CInt -> Ptr RawExpr -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_expr_unary_i64"
    phs_expr_unary_i64 :: CInt -> CLLong -> Ptr RawExpr -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_expr_binary_function"
    phs_expr_binary_function :: CInt -> Ptr RawExpr -> Ptr RawExpr -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_expr_ternary"
    phs_expr_ternary :: Ptr RawExpr -> Ptr RawExpr -> Ptr RawExpr -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_expr_quantile"
    phs_expr_quantile :: CInt -> Ptr RawExpr -> Ptr RawExpr -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_expr_rank"
    phs_expr_rank :: CInt -> CBool -> Ptr RawExpr -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_expr_slice"
    phs_expr_slice :: Ptr RawExpr -> Ptr RawExpr -> Ptr RawExpr -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_expr_sort_by"
    phs_expr_sort_by :: Ptr RawExpr -> Ptr (Ptr RawExpr) -> CSize -> CBool -> CBool -> CBool -> CBool -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_expr_over"
    phs_expr_over :: Ptr RawExpr -> Ptr (Ptr RawExpr) -> CSize -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_expr_string_function"
    phs_expr_string_function :: CInt -> Ptr RawExpr -> Ptr (Ptr RawExpr) -> CSize -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_expr_string_function_i64"
    phs_expr_string_function_i64 :: CInt -> CLLong -> Ptr RawExpr -> Ptr (Ptr RawExpr) -> CSize -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_expr_string_nary_function"
    phs_expr_string_nary_function :: CInt -> CString -> CBool -> Ptr (Ptr RawExpr) -> CSize -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_expr_list_function"
    phs_expr_list_function :: CInt -> Ptr RawExpr -> Ptr (Ptr RawExpr) -> CSize -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_expr_temporal_function"
    phs_expr_temporal_function :: CInt -> Ptr RawExpr -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_expr_temporal_time_unit"
    phs_expr_temporal_time_unit :: CInt -> CInt -> Ptr RawExpr -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_expr_temporal_string"
    phs_expr_temporal_string :: CInt -> CString -> Ptr RawExpr -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_expr_boolean_unary"
    phs_expr_boolean_unary :: CInt -> Ptr RawExpr -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_expr_horizontal_function"
    phs_expr_horizontal_function :: CInt -> CBool -> Ptr (Ptr RawExpr) -> CSize -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_expr_is_between"
    phs_expr_is_between :: CInt -> Ptr RawExpr -> Ptr RawExpr -> Ptr RawExpr -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_expr_is_close"
    phs_expr_is_close :: CDouble -> CDouble -> CBool -> Ptr RawExpr -> Ptr RawExpr -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_expr_is_in"
    phs_expr_is_in :: CBool -> Ptr RawExpr -> Ptr RawExpr -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_expr_clip"
    phs_expr_clip :: CInt -> Ptr RawExpr -> Ptr (Ptr RawExpr) -> CSize -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_expr_name_function"
    phs_expr_name_function :: CInt -> CString -> CString -> CBool -> Ptr RawExpr -> Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt
