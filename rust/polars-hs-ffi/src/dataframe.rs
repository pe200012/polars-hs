use std::fs::File;
use std::os::raw::{c_char, c_int, c_uchar};
use std::path::PathBuf;
use std::ptr;

use polars::prelude::*;

use crate::bytes::{bytes_into_raw, phs_bytes};
use crate::error::{PhsError, PhsResult, c_str_to_str, ffi_boundary, phs_error, required_mut};
use crate::handles::{
    dataframe_into_raw, dataframe_ref, phs_dataframe, phs_series, series_into_raw, series_ref,
};
use crate::series::{
    encode_bool_series, encode_f32_series, encode_f64_series, encode_i8_series, encode_i16_series,
    encode_i32_series, encode_i64_series, encode_text_series, encode_u8_series, encode_u16_series,
    encode_u32_series, encode_u64_series,
};

const SCHEMA_MAGIC: &[u8; 8] = b"PHS1SCH\0";

unsafe fn c_path(path: *const c_char) -> PhsResult<PathBuf> {
    Ok(PathBuf::from(unsafe { c_str_to_str(path, "path") }?))
}

unsafe fn name_vec(names: *const *const c_char, len: usize, label: &str) -> PhsResult<Vec<String>> {
    if names.is_null() && len > 0 {
        return Err(PhsError::invalid_argument(format!("{label} pointer was null")));
    }
    let slice = if len == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(names, len) }
    };
    slice
        .iter()
        .map(|name| unsafe { c_str_to_str(*name, label) }.map(str::to_owned))
        .collect()
}

fn usize_from_u64(value: u64, label: &str) -> PhsResult<usize> {
    usize::try_from(value)
        .map_err(|_| PhsError::invalid_argument(format!("{label} exceeded usize")))
}

unsafe fn csv_read_options(
    has_header: bool,
    separator: c_uchar,
    has_null_value: bool,
    null_value: *const c_char,
    has_n_rows: bool,
    n_rows: u64,
    skip_rows: u64,
    skip_rows_after_header: u64,
    has_infer_schema_length: bool,
    infer_schema_length: u64,
    ignore_errors: bool,
    truncate_ragged_lines: bool,
    missing_is_null: bool,
    low_memory: bool,
    rechunk: bool,
) -> PhsResult<CsvReadOptions> {
    let n_rows = if has_n_rows { Some(usize_from_u64(n_rows, "csv n rows")?) } else { None };
    let infer_schema_length = if has_infer_schema_length {
        Some(usize_from_u64(infer_schema_length, "csv infer schema length")?)
    } else {
        None
    };
    let mut options = CsvReadOptions::default()
        .with_has_header(has_header)
        .with_n_rows(n_rows)
        .with_skip_rows(usize_from_u64(skip_rows, "csv skip rows")?)
        .with_skip_rows_after_header(usize_from_u64(skip_rows_after_header, "csv skip rows after header")?)
        .with_infer_schema_length(infer_schema_length)
        .with_ignore_errors(ignore_errors)
        .with_low_memory(low_memory)
        .with_rechunk(rechunk)
        .map_parse_options(|parse_options| {
            parse_options
                .with_separator(separator)
                .with_truncate_ragged_lines(truncate_ragged_lines)
                .with_missing_is_null(missing_is_null)
        });
    if has_null_value {
        let value = unsafe { c_str_to_str(null_value, "csv null value") }?;
        let null_values = Some(NullValues::AllColumnsSingle(value.into()));
        options = options
            .map_parse_options(|parse_options| parse_options.with_null_values(null_values.clone()));
    }
    Ok(options)
}

fn parquet_compression_from_code(code: c_int) -> PhsResult<ParquetCompression> {
    match code {
        0 => Ok(ParquetCompression::default()),
        1 => Ok(ParquetCompression::Uncompressed),
        2 => Ok(ParquetCompression::Snappy),
        3 => Ok(ParquetCompression::Zstd(None)),
        _ => Err(PhsError::invalid_argument(format!(
            "unknown parquet compression code {code}"
        ))),
    }
}

fn parquet_parallel_from_code(code: c_int) -> PhsResult<ParallelStrategy> {
    match code {
        0 => Ok(ParallelStrategy::Auto),
        1 => Ok(ParallelStrategy::None),
        2 => Ok(ParallelStrategy::Columns),
        3 => Ok(ParallelStrategy::RowGroups),
        4 => Ok(ParallelStrategy::Prefiltered),
        _ => Err(PhsError::invalid_argument(format!(
            "unknown parquet parallel strategy code {code}"
        ))),
    }
}

fn push_u64_le(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn push_u16_le(bytes: &mut Vec<u8>, value: u16) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn schema_dtype_tag(dtype: &DataType) -> u16 {
    match dtype {
        DataType::Boolean => 0,
        DataType::Int8 => 1,
        DataType::Int16 => 2,
        DataType::Int32 => 3,
        DataType::Int64 => 4,
        DataType::UInt8 => 5,
        DataType::UInt16 => 6,
        DataType::UInt32 => 7,
        DataType::UInt64 => 8,
        DataType::Float32 => 9,
        DataType::Float64 => 10,
        DataType::String => 11,
        DataType::Date => 12,
        DataType::Datetime(_, _) => 13,
        DataType::Duration(_) => 14,
        DataType::Time => 15,
        DataType::Binary | DataType::BinaryOffset => 16,
        DataType::Null => 17,
        dtype if dtype.is_categorical() || dtype.is_enum() => 18,
        _ => 255,
    }
}

fn encode_schema_bytes(dataframe: &DataFrame) -> Vec<u8> {
    let schema = dataframe.schema();
    let mut bytes = Vec::new();
    bytes.extend_from_slice(SCHEMA_MAGIC);
    push_u64_le(&mut bytes, schema.len() as u64);
    for field in schema.iter_fields() {
        let name = field.name().as_str().as_bytes();
        let dtype = field.dtype();
        let detail = format!("{dtype:?}");
        push_u64_le(&mut bytes, name.len() as u64);
        bytes.extend_from_slice(name);
        push_u16_le(&mut bytes, schema_dtype_tag(dtype));
        push_u64_le(&mut bytes, detail.len() as u64);
        bytes.extend_from_slice(detail.as_bytes());
    }
    bytes
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_read_csv(
    path: *const c_char,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    unsafe {
        phs_read_csv_options(
            path,
            true,
            b',',
            false,
            ptr::null(),
            false,
            0,
            0,
            0,
            true,
            100,
            false,
            false,
            true,
            false,
            false,
            out,
            err,
        )
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_read_csv_options(
    path: *const c_char,
    has_header: bool,
    separator: c_uchar,
    has_null_value: bool,
    null_value: *const c_char,
    has_n_rows: bool,
    n_rows: u64,
    skip_rows: u64,
    skip_rows_after_header: u64,
    has_infer_schema_length: bool,
    infer_schema_length: u64,
    ignore_errors: bool,
    truncate_ragged_lines: bool,
    missing_is_null: bool,
    low_memory: bool,
    rechunk: bool,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let path = unsafe { c_path(path) }?;
        let df = unsafe {
            csv_read_options(
                has_header,
                separator,
                has_null_value,
                null_value,
                has_n_rows,
                n_rows,
                skip_rows,
                skip_rows_after_header,
                has_infer_schema_length,
                infer_schema_length,
                ignore_errors,
                truncate_ragged_lines,
                missing_is_null,
                low_memory,
                rechunk,
            )
        }?
        .try_into_reader_with_file_path(Some(path))?
        .finish()?;
        *out = dataframe_into_raw(df);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_read_parquet(
    path: *const c_char,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    unsafe { phs_read_parquet_options(path, false, 0, 0, false, false, out, err) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_read_parquet_options(
    path: *const c_char,
    has_n_rows: bool,
    n_rows: u64,
    parallel: c_int,
    low_memory: bool,
    rechunk: bool,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let path = unsafe { c_path(path) }?;
        let file = File::open(path)?;
        let mut reader = ParquetReader::new(file)
            .read_parallel(parquet_parallel_from_code(parallel)?)
            .set_low_memory(low_memory)
            .set_rechunk(rechunk);
        if has_n_rows {
            reader = reader.with_slice(Some((0, usize_from_u64(n_rows, "parquet n rows")?)));
        }
        let df = reader.finish()?;
        *out = dataframe_into_raw(df);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_write_csv(
    path: *const c_char,
    dataframe: *const phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    unsafe { phs_write_csv_options(path, dataframe, true, b',', c"".as_ptr(), err) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_write_csv_options(
    path: *const c_char,
    dataframe: *const phs_dataframe,
    include_header: bool,
    separator: c_uchar,
    null_value: *const c_char,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let path = unsafe { c_path(path) }?;
        let handle = unsafe { dataframe_ref(dataframe) }?;
        let mut df = handle.value.clone();
        let mut file = File::create(path)?;
        let null_value = unsafe { c_str_to_str(null_value, "csv null value") }?;
        CsvWriter::new(&mut file)
            .include_header(include_header)
            .with_separator(separator)
            .with_null_value(null_value.into())
            .finish(&mut df)?;
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_write_parquet(
    path: *const c_char,
    dataframe: *const phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    unsafe {
        phs_write_parquet_options(
            path, dataframe, 0, false, 0, false, 0, true, true, false, true, true, err,
        )
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_write_parquet_options(
    path: *const c_char,
    dataframe: *const phs_dataframe,
    compression: c_int,
    has_row_group_size: bool,
    row_group_size: u64,
    has_data_page_size: bool,
    data_page_size: u64,
    statistics_min_value: bool,
    statistics_max_value: bool,
    statistics_distinct_count: bool,
    statistics_null_count: bool,
    parallel: bool,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let path = unsafe { c_path(path) }?;
        let handle = unsafe { dataframe_ref(dataframe) }?;
        let mut df = handle.value.clone();
        let file = File::create(path)?;
        let mut writer =
            ParquetWriter::new(file)
                .with_compression(parquet_compression_from_code(compression)?)
                .with_statistics(StatisticsOptions {
                    min_value: statistics_min_value,
                    max_value: statistics_max_value,
                    distinct_count: statistics_distinct_count,
                    null_count: statistics_null_count,
                })
                .set_parallel(parallel);
        if has_row_group_size {
            writer = writer.with_row_group_size(Some(usize_from_u64(
                row_group_size,
                "parquet row group size",
            )?));
        }
        if has_data_page_size {
            writer = writer.with_data_page_size(Some(usize_from_u64(
                data_page_size,
                "parquet data page size",
            )?));
        }
        let _bytes = writer.finish(&mut df)?;
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_new(
    series: *const *const phs_series,
    len: usize,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let ptrs = if len == 0 {
            &[]
        } else if series.is_null() {
            return Err(PhsError::invalid_argument("series array pointer was null"));
        } else {
            unsafe { std::slice::from_raw_parts(series, len) }
        };
        let mut columns = Vec::with_capacity(ptrs.len());
        for ptr in ptrs {
            let handle = unsafe { series_ref(*ptr) }?;
            columns.push(handle.value.clone().into());
        }
        *out = dataframe_into_raw(DataFrame::new_infer_height(columns)?);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_select(
    dataframe: *const phs_dataframe,
    names: *const *const c_char,
    len: usize,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { dataframe_ref(dataframe) }?;
        let names = unsafe { name_vec(names, len, "names") }?;
        if names.is_empty() {
            return Err(PhsError::invalid_argument("select requires at least one column name"));
        }
        *out = dataframe_into_raw(handle.value.select(names.iter().map(String::as_str))?);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_drop(
    dataframe: *const phs_dataframe,
    names: *const *const c_char,
    len: usize,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { dataframe_ref(dataframe) }?;
        let names = unsafe { name_vec(names, len, "names") }?;
        if names.is_empty() {
            return Err(PhsError::invalid_argument("drop requires at least one column name"));
        }
        let mut df = handle.value.clone();
        for name in names {
            df = df.drop(&name)?;
        }
        *out = dataframe_into_raw(df);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_rename(
    dataframe: *const phs_dataframe,
    existing: *const *const c_char,
    new_names: *const *const c_char,
    len: usize,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { dataframe_ref(dataframe) }?;
        let existing = unsafe { name_vec(existing, len, "existing") }?;
        let new_names = unsafe { name_vec(new_names, len, "new_names") }?;
        if existing.is_empty() {
            return Err(PhsError::invalid_argument("rename requires at least one column pair"));
        }
        let mut df = handle.value.clone();
        for (from, to) in existing.iter().zip(new_names.iter()) {
            df.rename(from, PlSmallStr::from_str(to))?;
        }
        *out = dataframe_into_raw(df);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_slice(
    dataframe: *const phs_dataframe,
    offset: i64,
    len: u64,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { dataframe_ref(dataframe) }?;
        let len = usize_from_u64(len, "slice length")?;
        *out = dataframe_into_raw(handle.value.slice(offset, len));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_reverse(
    dataframe: *const phs_dataframe,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { dataframe_ref(dataframe) }?;
        *out = dataframe_into_raw(handle.value.reverse());
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_drop_nulls(
    dataframe: *const phs_dataframe,
    names: *const *const c_char,
    len: usize,
    has_subset: bool,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { dataframe_ref(dataframe) }?;
        let df = if has_subset {
            let names = unsafe { name_vec(names, len, "names") }?;
            if names.is_empty() {
                return Err(PhsError::invalid_argument(
                    "drop_nulls subset requires at least one column name",
                ));
            }
            handle.value.drop_nulls(Some(names.as_slice()))?
        } else {
            handle.value.drop_nulls::<String>(None)?
        };
        *out = dataframe_into_raw(df);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_null_count(
    dataframe: *const phs_dataframe,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { dataframe_ref(dataframe) }?;
        *out = dataframe_into_raw(handle.value.null_count());
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_shape(
    dataframe: *const phs_dataframe,
    height_out: *mut u64,
    width_out: *mut u64,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let height_out = unsafe { required_mut(height_out, "height_out") }?;
        let width_out = unsafe { required_mut(width_out, "width_out") }?;
        let handle = unsafe { dataframe_ref(dataframe) }?;
        let (height, width) = handle.value.shape();
        *height_out = height as u64;
        *width_out = width as u64;
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_height(
    dataframe: *const phs_dataframe,
    height_out: *mut u64,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let height_out = unsafe { required_mut(height_out, "height_out") }?;
        let handle = unsafe { dataframe_ref(dataframe) }?;
        *height_out = handle.value.height() as u64;
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_width(
    dataframe: *const phs_dataframe,
    width_out: *mut u64,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let width_out = unsafe { required_mut(width_out, "width_out") }?;
        let handle = unsafe { dataframe_ref(dataframe) }?;
        *width_out = handle.value.width() as u64;
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_schema(
    dataframe: *const phs_dataframe,
    out: *mut *mut phs_bytes,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { dataframe_ref(dataframe) }?;
        *out = bytes_into_raw(encode_schema_bytes(&handle.value));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_head(
    dataframe: *const phs_dataframe,
    n: u64,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { dataframe_ref(dataframe) }?;
        let df = handle.value.head(Some(n as usize));
        *out = dataframe_into_raw(df);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_tail(
    dataframe: *const phs_dataframe,
    n: u64,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { dataframe_ref(dataframe) }?;
        let df = handle.value.tail(Some(n as usize));
        *out = dataframe_into_raw(df);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_to_text(
    dataframe: *const phs_dataframe,
    out: *mut *mut phs_bytes,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { dataframe_ref(dataframe) }?;
        *out = bytes_into_raw(handle.value.to_string().into_bytes());
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_column(
    dataframe: *const phs_dataframe,
    name: *const c_char,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { dataframe_ref(dataframe) }?;
        let name = unsafe { c_str_to_str(name, "name") }?;
        let series = handle.value.column(name)?.as_materialized_series().clone();
        *out = series_into_raw(series);
        Ok(())
    })
}

fn dataframe_column_bytes<F>(
    dataframe: *const phs_dataframe,
    name: *const c_char,
    out: *mut *mut phs_bytes,
    err: *mut *mut phs_error,
    encode: F,
) -> c_int
where
    F: FnOnce(&Series) -> PhsResult<Vec<u8>> + std::panic::UnwindSafe,
{
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { dataframe_ref(dataframe) }?;
        let name = unsafe { c_str_to_str(name, "name") }?;
        let series = handle.value.column(name)?.as_materialized_series();
        *out = bytes_into_raw(encode(series)?);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_column_bool(
    dataframe: *const phs_dataframe,
    name: *const c_char,
    out: *mut *mut phs_bytes,
    err: *mut *mut phs_error,
) -> c_int {
    dataframe_column_bytes(dataframe, name, out, err, encode_bool_series)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_column_i64(
    dataframe: *const phs_dataframe,
    name: *const c_char,
    out: *mut *mut phs_bytes,
    err: *mut *mut phs_error,
) -> c_int {
    dataframe_column_bytes(dataframe, name, out, err, encode_i64_series)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_column_i8(
    dataframe: *const phs_dataframe,
    name: *const c_char,
    out: *mut *mut phs_bytes,
    err: *mut *mut phs_error,
) -> c_int {
    dataframe_column_bytes(dataframe, name, out, err, encode_i8_series)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_column_i16(
    dataframe: *const phs_dataframe,
    name: *const c_char,
    out: *mut *mut phs_bytes,
    err: *mut *mut phs_error,
) -> c_int {
    dataframe_column_bytes(dataframe, name, out, err, encode_i16_series)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_column_i32(
    dataframe: *const phs_dataframe,
    name: *const c_char,
    out: *mut *mut phs_bytes,
    err: *mut *mut phs_error,
) -> c_int {
    dataframe_column_bytes(dataframe, name, out, err, encode_i32_series)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_column_u8(
    dataframe: *const phs_dataframe,
    name: *const c_char,
    out: *mut *mut phs_bytes,
    err: *mut *mut phs_error,
) -> c_int {
    dataframe_column_bytes(dataframe, name, out, err, encode_u8_series)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_column_u16(
    dataframe: *const phs_dataframe,
    name: *const c_char,
    out: *mut *mut phs_bytes,
    err: *mut *mut phs_error,
) -> c_int {
    dataframe_column_bytes(dataframe, name, out, err, encode_u16_series)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_column_u32(
    dataframe: *const phs_dataframe,
    name: *const c_char,
    out: *mut *mut phs_bytes,
    err: *mut *mut phs_error,
) -> c_int {
    dataframe_column_bytes(dataframe, name, out, err, encode_u32_series)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_column_u64(
    dataframe: *const phs_dataframe,
    name: *const c_char,
    out: *mut *mut phs_bytes,
    err: *mut *mut phs_error,
) -> c_int {
    dataframe_column_bytes(dataframe, name, out, err, encode_u64_series)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_column_f32(
    dataframe: *const phs_dataframe,
    name: *const c_char,
    out: *mut *mut phs_bytes,
    err: *mut *mut phs_error,
) -> c_int {
    dataframe_column_bytes(dataframe, name, out, err, encode_f32_series)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_column_f64(
    dataframe: *const phs_dataframe,
    name: *const c_char,
    out: *mut *mut phs_bytes,
    err: *mut *mut phs_error,
) -> c_int {
    dataframe_column_bytes(dataframe, name, out, err, encode_f64_series)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_column_text(
    dataframe: *const phs_dataframe,
    name: *const c_char,
    out: *mut *mut phs_bytes,
    err: *mut *mut phs_error,
) -> c_int {
    dataframe_column_bytes(dataframe, name, out, err, encode_text_series)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bytes::{phs_bytes_data, phs_bytes_free, phs_bytes_len};
    use crate::error::{PHS_OK, phs_error_free};

    fn fixture_path() -> std::ffi::CString {
        data_path("people.csv")
    }

    fn values_fixture_path() -> std::ffi::CString {
        data_path("values.csv")
    }

    fn data_path(file_name: &str) -> std::ffi::CString {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("test")
            .join("data")
            .join(file_name);
        std::ffi::CString::new(path.to_string_lossy().as_bytes()).unwrap()
    }

    fn read_values_dataframe() -> *mut phs_dataframe {
        let path = values_fixture_path();
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();
        let status = unsafe { phs_read_csv(path.as_ptr(), &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert!(err.is_null());
        assert!(!out.is_null());
        out
    }

    unsafe fn take_raw_bytes(raw: *mut phs_bytes) -> Vec<u8> {
        assert!(!raw.is_null());
        let len = unsafe { phs_bytes_len(raw) };
        let data = unsafe { phs_bytes_data(raw) };
        assert!(!data.is_null());
        let bytes = unsafe { std::slice::from_raw_parts(data, len) }.to_vec();
        unsafe { phs_bytes_free(raw) };
        bytes
    }

    fn call_column_bytes(
        dataframe: *const phs_dataframe,
        column_name: &str,
        action: unsafe extern "C" fn(
            *const phs_dataframe,
            *const c_char,
            *mut *mut phs_bytes,
            *mut *mut phs_error,
        ) -> c_int,
    ) -> Vec<u8> {
        let name = std::ffi::CString::new(column_name).unwrap();
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();
        let status = unsafe { action(dataframe, name.as_ptr(), &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert!(err.is_null());
        unsafe { take_raw_bytes(out) }
    }

    #[test]
    fn read_csv_success_reports_shape() {
        let path = fixture_path();
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();
        let status = unsafe { phs_read_csv(path.as_ptr(), &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert!(err.is_null());
        let mut height = 0;
        let mut width = 0;
        let status = unsafe { phs_dataframe_shape(out, &mut height, &mut width, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!((height, width), (3, 2));
        unsafe { crate::handles::phs_dataframe_free(out) };
    }

    #[test]
    fn read_csv_missing_file_returns_error() {
        let path = std::ffi::CString::new("missing-file.csv").unwrap();
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();
        let status = unsafe { phs_read_csv(path.as_ptr(), &mut out, &mut err) };
        assert_eq!(status, crate::error::PHS_POLARS_ERROR);
        assert!(out.is_null());
        assert!(!err.is_null());
        unsafe { phs_error_free(err) };
    }

    #[test]
    fn column_i64_encodes_values_and_nulls() {
        let df = read_values_dataframe();
        let bytes = call_column_bytes(df, "age", phs_dataframe_column_i64);
        let mut expected = vec![1];
        expected.extend_from_slice(&34_i64.to_le_bytes());
        expected.push(0);
        expected.push(1);
        expected.extend_from_slice(&29_i64.to_le_bytes());
        assert_eq!(bytes, expected);
        unsafe { crate::handles::phs_dataframe_free(df) };
    }

    #[test]
    fn column_f64_encodes_values_and_nulls() {
        let df = read_values_dataframe();
        let bytes = call_column_bytes(df, "score", phs_dataframe_column_f64);
        let mut expected = vec![1];
        expected.extend_from_slice(&9.5_f64.to_le_bytes());
        expected.push(1);
        expected.extend_from_slice(&8.25_f64.to_le_bytes());
        expected.push(0);
        assert_eq!(bytes, expected);
        unsafe { crate::handles::phs_dataframe_free(df) };
    }

    #[test]
    fn column_bool_encodes_values_and_nulls() {
        let df = read_values_dataframe();
        let bytes = call_column_bytes(df, "active", phs_dataframe_column_bool);
        assert_eq!(bytes, vec![1, 1, 1, 0, 0]);
        unsafe { crate::handles::phs_dataframe_free(df) };
    }

    #[test]
    fn column_text_encodes_values_and_nulls() {
        let df = read_values_dataframe();
        let bytes = call_column_bytes(df, "name", phs_dataframe_column_text);
        let mut expected = vec![1];
        expected.extend_from_slice(&5_u64.to_le_bytes());
        expected.extend_from_slice(b"Alice");
        expected.push(1);
        expected.extend_from_slice(&3_u64.to_le_bytes());
        expected.extend_from_slice(b"Bob");
        expected.push(1);
        expected.extend_from_slice(&5_u64.to_le_bytes());
        expected.extend_from_slice(b"Carol");
        assert_eq!(bytes, expected);
        unsafe { crate::handles::phs_dataframe_free(df) };
    }

    #[test]
    fn schema_encoding_preserves_nul_field_names() {
        let column = Series::new("a\0b".into(), [1_i64, 2]).into();
        let df = DataFrame::new_infer_height(vec![column]).unwrap();
        let raw = dataframe_into_raw(df);
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();

        let status = unsafe { phs_dataframe_schema(raw, &mut out, &mut err) };

        assert_eq!(status, PHS_OK);
        assert!(err.is_null());
        let bytes = unsafe { take_raw_bytes(out) };
        let mut expected = b"PHS1SCH\0".to_vec();
        expected.extend_from_slice(&1_u64.to_le_bytes());
        expected.extend_from_slice(&3_u64.to_le_bytes());
        expected.extend_from_slice(b"a\0b");
        expected.extend_from_slice(&4_u16.to_le_bytes());
        expected.extend_from_slice(&5_u64.to_le_bytes());
        expected.extend_from_slice(b"Int64");
        assert_eq!(bytes, expected);
        unsafe { crate::handles::phs_dataframe_free(raw) };
    }

    #[test]
    fn column_i64_reports_dtype_mismatch() {
        let df = read_values_dataframe();
        let name = std::ffi::CString::new("name").unwrap();
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();
        let status = unsafe { phs_dataframe_column_i64(df, name.as_ptr(), &mut out, &mut err) };
        assert_eq!(status, crate::error::PHS_POLARS_ERROR);
        assert!(out.is_null());
        assert!(!err.is_null());
        unsafe {
            phs_error_free(err);
            crate::handles::phs_dataframe_free(df);
        }
    }
}
