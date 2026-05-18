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

#[repr(C)]
pub struct phs_dataframe_array {
    _private: [u8; 0],
}

struct DataFrameArrayHandle {
    values: Vec<DataFrame>,
}

fn dataframe_array_into_raw(values: Vec<DataFrame>) -> *mut phs_dataframe_array {
    Box::into_raw(Box::new(DataFrameArrayHandle { values })) as *mut phs_dataframe_array
}

unsafe fn dataframe_array_ref<'a>(
    ptr: *const phs_dataframe_array,
) -> PhsResult<&'a DataFrameArrayHandle> {
    if ptr.is_null() {
        Err(PhsError::invalid_argument("dataframe array pointer was null"))
    } else {
        Ok(unsafe { &*(ptr as *const DataFrameArrayHandle) })
    }
}

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

unsafe fn bool_vec(values: *const u8, len: usize, label: &str) -> PhsResult<Vec<bool>> {
    if values.is_null() && len > 0 {
        return Err(PhsError::invalid_argument(format!("{label} pointer was null")));
    }
    let slice = if len == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(values, len) }
    };
    slice
        .iter()
        .map(|value| match *value {
            0 => Ok(false),
            1 => Ok(true),
            other => Err(PhsError::invalid_argument(format!(
                "{label} value must be 0 or 1, received {other}"
            ))),
        })
        .collect()
}

unsafe fn raw_u64_slice<'a>(data: *const u64, len: usize, name: &str) -> PhsResult<&'a [u64]> {
    if len == 0 {
        Ok(&[])
    } else if data.is_null() {
        Err(PhsError::invalid_argument(format!("{name} pointer was null")))
    } else {
        Ok(unsafe { std::slice::from_raw_parts(data, len) })
    }
}

fn usize_from_u64(value: u64, label: &str) -> PhsResult<usize> {
    usize::try_from(value)
        .map_err(|_| PhsError::invalid_argument(format!("{label} exceeded usize")))
}

fn idx_size_from_u64(value: u64, label: &str) -> PhsResult<IdxSize> {
    value
        .try_into()
        .map_err(|_| PhsError::invalid_argument(format!("{label} exceeds Polars index size")))
}

fn idx_ca_from_u64_slice(values: &[u64], label: &str) -> PhsResult<IdxCa> {
    let indices = values
        .iter()
        .map(|value| idx_size_from_u64(*value, label))
        .collect::<PhsResult<Vec<IdxSize>>>()?;
    Ok(IdxCa::from_vec(PlSmallStr::EMPTY, indices))
}

fn validate_sort_bool_options(label: &str, column_count: usize, values: &[bool]) -> PhsResult<()> {
    let option_count = values.len();
    if option_count == 1 || option_count == column_count {
        Ok(())
    } else {
        Err(PhsError::invalid_argument(format!(
            "{label} must contain one value or one value per sort column"
        )))
    }
}

fn unique_keep_strategy_from_code(code: c_int) -> PhsResult<UniqueKeepStrategy> {
    match code {
        0 => Ok(UniqueKeepStrategy::First),
        1 => Ok(UniqueKeepStrategy::Last),
        2 => Ok(UniqueKeepStrategy::None),
        3 => Ok(UniqueKeepStrategy::Any),
        _ => Err(PhsError::invalid_argument(format!(
            "unknown dataframe unique keep strategy code {code}"
        ))),
    }
}

fn join_type_from_code(code: c_int) -> PhsResult<JoinType> {
    match code {
        0 => Ok(JoinType::Inner),
        1 => Ok(JoinType::Left),
        2 => Ok(JoinType::Right),
        3 => Ok(JoinType::Full),
        4 => Ok(JoinType::Semi),
        5 => Ok(JoinType::Anti),
        6 => Ok(JoinType::Cross),
        _ => Err(PhsError::invalid_argument(format!("unknown dataframe join type code {code}"))),
    }
}

unsafe fn optional_suffix(suffix: *const c_char) -> PhsResult<Option<PlSmallStr>> {
    if suffix.is_null() {
        Ok(None)
    } else {
        let suffix = unsafe { c_str_to_str(suffix, "suffix") }?;
        Ok(Some(PlSmallStr::from_str(suffix)))
    }
}

fn fill_null_strategy_from_code(
    code: c_int,
    has_limit: bool,
    limit: u64,
) -> PhsResult<FillNullStrategy> {
    let limit = if has_limit {
        Some(idx_size_from_u64(limit, "fill null limit")?)
    } else {
        None
    };
    match code {
        0 => Ok(FillNullStrategy::Forward(limit)),
        1 => Ok(FillNullStrategy::Backward(limit)),
        2 => Ok(FillNullStrategy::Mean),
        3 => Ok(FillNullStrategy::Min),
        4 => Ok(FillNullStrategy::Max),
        5 => Ok(FillNullStrategy::Zero),
        6 => Ok(FillNullStrategy::One),
        _ => Err(PhsError::invalid_argument(format!(
            "unknown fill null strategy code {code}"
        ))),
    }
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
pub unsafe extern "C" fn phs_dataframe_filter(
    dataframe: *const phs_dataframe,
    mask: *const phs_series,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { dataframe_ref(dataframe) }?;
        let mask = unsafe { series_ref(mask) }?;
        let mask = mask.value.bool()?;
        *out = dataframe_into_raw(handle.value.filter(mask)?);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_take(
    dataframe: *const phs_dataframe,
    indices: *const u64,
    len: usize,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { dataframe_ref(dataframe) }?;
        let indices = unsafe { raw_u64_slice(indices, len, "indices") }?;
        let indices = idx_ca_from_u64_slice(indices, "dataframe take index")?;
        *out = dataframe_into_raw(handle.value.take(&indices)?);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_join(
    left: *const phs_dataframe,
    right: *const phs_dataframe,
    left_on: *const *const c_char,
    left_len: usize,
    right_on: *const *const c_char,
    right_len: usize,
    join_type: c_int,
    suffix: *const c_char,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let join_type = join_type_from_code(join_type)?;
        let left_df = unsafe { dataframe_ref(left) }?;
        let right_df = unsafe { dataframe_ref(right) }?;
        let left_on = unsafe { name_vec(left_on, left_len, "left_on") }?;
        let right_on = unsafe { name_vec(right_on, right_len, "right_on") }?;
        let suffix = unsafe { optional_suffix(suffix) }?;
        if matches!(join_type, JoinType::Cross) {
            if left_len != 0 || right_len != 0 {
                return Err(PhsError::invalid_argument("cross join requires empty join key lists"));
            }
        } else {
            if left_on.is_empty() {
                return Err(PhsError::invalid_argument("left join keys must contain at least one column name"));
            }
            if right_on.is_empty() {
                return Err(PhsError::invalid_argument("right join keys must contain at least one column name"));
            }
            if left_on.len() != right_on.len() {
                return Err(PhsError::invalid_argument("left and right join key counts must match"));
            }
        }
        let mut args = JoinArgs::new(join_type);
        if let Some(suffix) = suffix {
            args = args.with_suffix(Some(suffix));
        }
        *out = dataframe_into_raw(left_df.value.join(&right_df.value, &left_on, &right_on, args, None)?);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_vstack(
    left: *const phs_dataframe,
    right: *const phs_dataframe,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let left_df = unsafe { dataframe_ref(left) }?;
        let right_df = unsafe { dataframe_ref(right) }?;
        *out = dataframe_into_raw(left_df.value.vstack(&right_df.value)?);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_hstack(
    dataframe: *const phs_dataframe,
    series: *const *const phs_series,
    len: usize,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { dataframe_ref(dataframe) }?;
        let ptrs = if len == 0 {
            &[]
        } else if series.is_null() {
            return Err(PhsError::invalid_argument("series array pointer was null"));
        } else {
            unsafe { std::slice::from_raw_parts(series, len) }
        };
        if ptrs.is_empty() {
            return Err(PhsError::invalid_argument("hstack requires at least one series"));
        }
        let mut columns = Vec::with_capacity(ptrs.len());
        for ptr in ptrs {
            let handle = unsafe { series_ref(*ptr) }?;
            columns.push(handle.value.clone().into());
        }
        *out = dataframe_into_raw(handle.value.hstack(&columns)?);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_with_columns(
    dataframe: *const phs_dataframe,
    series: *const *const phs_series,
    len: usize,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { dataframe_ref(dataframe) }?;
        let ptrs = if len == 0 {
            &[]
        } else if series.is_null() {
            return Err(PhsError::invalid_argument("series array pointer was null"));
        } else {
            unsafe { std::slice::from_raw_parts(series, len) }
        };
        if ptrs.is_empty() {
            return Err(PhsError::invalid_argument("with_columns requires at least one series"));
        }
        let mut df = handle.value.clone();
        for ptr in ptrs {
            let handle = unsafe { series_ref(*ptr) }?;
            df.with_column(handle.value.clone().into())?;
        }
        *out = dataframe_into_raw(df);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_insert_column(
    dataframe: *const phs_dataframe,
    index: u64,
    series: *const phs_series,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { dataframe_ref(dataframe) }?;
        let series = unsafe { series_ref(series) }?;
        let index = usize_from_u64(index, "dataframe insert-column index")?;
        let mut df = handle.value.clone();
        if index > df.width() {
            return Err(PhsError::invalid_argument(format!(
                "dataframe insert-column index {index} exceeds width {}",
                df.width()
            )));
        }
        df.insert_column(index, series.value.clone().into())?;
        *out = dataframe_into_raw(df);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_replace_column(
    dataframe: *const phs_dataframe,
    index: u64,
    series: *const phs_series,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { dataframe_ref(dataframe) }?;
        let series = unsafe { series_ref(series) }?;
        let index = usize_from_u64(index, "dataframe replace-column index")?;
        let mut df = handle.value.clone();
        let column: Column = series.value.clone().into();
        let name = column.name();
        if let Some(existing_index) = df.get_column_index(name) {
            if existing_index != index {
                return Err(PhsError::invalid_argument(format!(
                    "dataframe replace-column name {:?} already exists at index {existing_index}",
                    name
                )));
            }
        }
        df.replace_column(index, column)?;
        *out = dataframe_into_raw(df);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_partition_by(
    dataframe: *const phs_dataframe,
    names: *const *const c_char,
    names_len: usize,
    include_key: bool,
    maintain_order: bool,
    out: *mut *mut phs_dataframe_array,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { dataframe_ref(dataframe) }?;
        let names = unsafe { name_vec(names, names_len, "names") }?;
        if names.is_empty() {
            return Err(PhsError::invalid_argument(
                "partition_by requires at least one column",
            ));
        }
        let names = names
            .iter()
            .map(|name| PlSmallStr::from_str(name))
            .collect::<Vec<_>>();
        let partitions = if maintain_order {
            handle.value.partition_by_stable(names, include_key)?
        } else {
            handle.value.partition_by(names, include_key)?
        };
        *out = dataframe_array_into_raw(partitions);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_explode(
    dataframe: *const phs_dataframe,
    names: *const *const c_char,
    names_len: usize,
    empty_as_null: bool,
    keep_nulls: bool,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { dataframe_ref(dataframe) }?;
        let names = unsafe { name_vec(names, names_len, "names") }?;
        if names.is_empty() {
            return Err(PhsError::invalid_argument(
                "explode requires at least one column",
            ));
        }
        let options = ExplodeOptions {
            empty_as_null,
            keep_nulls,
        };
        *out = dataframe_into_raw(handle.value.explode(names.iter().map(String::as_str), options)?);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_gather_every(
    dataframe: *const phs_dataframe,
    step: u64,
    offset: u64,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { dataframe_ref(dataframe) }?;
        let step = usize_from_u64(step, "dataframe gather-every step")?;
        if step == 0 {
            return Err(PhsError::invalid_argument(
                "dataframe gather-every step must be positive",
            ));
        }
        let height = handle.value.height();
        if offset >= height as u64 {
            *out = dataframe_into_raw(handle.value.clear());
            return Ok(());
        }
        let offset = usize_from_u64(offset, "dataframe gather-every offset")?;
        let indexes = (offset..height)
            .step_by(step)
            .map(|value| idx_size_from_u64(value as u64, "dataframe gather-every index"))
            .collect::<PhsResult<Vec<_>>>()?;
        let indexes = IdxCa::from_vec(PlSmallStr::EMPTY, indexes);
        *out = dataframe_into_raw(handle.value.take(&indexes)?);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_array_len(array: *const phs_dataframe_array) -> usize {
    if array.is_null() {
        0
    } else {
        unsafe { (*(array as *const DataFrameArrayHandle)).values.len() }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_array_get(
    array: *const phs_dataframe_array,
    index: usize,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let array = unsafe { dataframe_array_ref(array) }?;
        let value = array.values.get(index).ok_or_else(|| {
            PhsError::invalid_argument(format!("dataframe array index {index} out of bounds"))
        })?;
        *out = dataframe_into_raw(value.clone());
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_array_free(array: *mut phs_dataframe_array) {
    if !array.is_null() {
        unsafe {
            drop(Box::from_raw(array as *mut DataFrameArrayHandle));
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_fill_null(
    dataframe: *const phs_dataframe,
    strategy: c_int,
    has_limit: bool,
    limit: u64,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { dataframe_ref(dataframe) }?;
        let strategy = fill_null_strategy_from_code(strategy, has_limit, limit)?;
        *out = dataframe_into_raw(handle.value.fill_null(strategy)?);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_sort(
    dataframe: *const phs_dataframe,
    names: *const *const c_char,
    names_len: usize,
    descending: *const u8,
    descending_len: usize,
    nulls_last: *const u8,
    nulls_last_len: usize,
    multithreaded: bool,
    maintain_order: bool,
    has_limit: bool,
    limit: u64,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { dataframe_ref(dataframe) }?;
        let names = unsafe { name_vec(names, names_len, "names") }?;
        if names.is_empty() {
            return Err(PhsError::invalid_argument(
                "dataframe sort requires at least one column name",
            ));
        }
        let descending = unsafe { bool_vec(descending, descending_len, "descending") }?;
        validate_sort_bool_options("descending", names.len(), &descending)?;
        let nulls_last = unsafe { bool_vec(nulls_last, nulls_last_len, "nulls_last") }?;
        validate_sort_bool_options("nulls_last", names.len(), &nulls_last)?;
        let mut options = SortMultipleOptions::default()
            .with_order_descending_multi(descending)
            .with_nulls_last_multi(nulls_last)
            .with_multithreaded(multithreaded)
            .with_maintain_order(maintain_order);
        if has_limit {
            options.limit = Some(idx_size_from_u64(limit, "dataframe sort limit")?);
        }
        *out = dataframe_into_raw(handle.value.sort(names.iter().map(String::as_str), options)?);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_unique(
    dataframe: *const phs_dataframe,
    subset: *const *const c_char,
    subset_len: usize,
    has_subset: bool,
    keep_strategy: c_int,
    maintain_order: bool,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { dataframe_ref(dataframe) }?;
        let subset = if has_subset {
            let names = unsafe { name_vec(subset, subset_len, "subset") }?;
            if names.is_empty() {
                return Err(PhsError::invalid_argument(
                    "dataframe unique subset requires at least one column name",
                ));
            }
            Some(names)
        } else {
            None
        };
        let keep_strategy = unique_keep_strategy_from_code(keep_strategy)?;
        let output = if maintain_order {
            handle
                .value
                .unique_stable(subset.as_deref(), keep_strategy, None)?
        } else {
            handle
                .value
                .unique::<String, String>(subset.as_deref(), keep_strategy, None)?
        };
        *out = dataframe_into_raw(output);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_is_unique(
    dataframe: *const phs_dataframe,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { dataframe_ref(dataframe) }?;
        *out = series_into_raw(handle.value.is_unique()?.into_series());
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_is_duplicated(
    dataframe: *const phs_dataframe,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { dataframe_ref(dataframe) }?;
        *out = series_into_raw(handle.value.is_duplicated()?.into_series());
        Ok(())
    })
}

fn sample_seed(has_seed: bool, seed: u64) -> Option<u64> {
    has_seed.then_some(seed)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_sample_n(
    dataframe: *const phs_dataframe,
    n: u64,
    with_replacement: bool,
    shuffle: bool,
    has_seed: bool,
    seed: u64,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { dataframe_ref(dataframe) }?;
        let n = usize_from_u64(n, "dataframe sample size")?;
        let sampled =
            handle
                .value
                .sample_n_literal(n, with_replacement, shuffle, sample_seed(has_seed, seed))?;
        *out = dataframe_into_raw(sampled);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_sample_frac(
    dataframe: *const phs_dataframe,
    frac: f64,
    with_replacement: bool,
    shuffle: bool,
    has_seed: bool,
    seed: u64,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { dataframe_ref(dataframe) }?;
        let frac = Series::new(PlSmallStr::from_static("frac"), [frac]);
        let sampled = handle
            .value
            .sample_frac(&frac, with_replacement, shuffle, sample_seed(has_seed, seed))?;
        *out = dataframe_into_raw(sampled);
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
pub unsafe extern "C" fn phs_dataframe_estimated_size(
    dataframe: *const phs_dataframe,
    out: *mut u64,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        let handle = unsafe { dataframe_ref(dataframe) }?;
        *out = u64::try_from(handle.value.estimated_size())
            .map_err(|_| PhsError::invalid_argument("dataframe estimated size exceeded u64"))?;
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_first_col_n_chunks(
    dataframe: *const phs_dataframe,
    out: *mut u64,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        let handle = unsafe { dataframe_ref(dataframe) }?;
        *out = u64::try_from(handle.value.first_col_n_chunks())
            .map_err(|_| PhsError::invalid_argument("dataframe first-column chunk count exceeded u64"))?;
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_max_n_chunks(
    dataframe: *const phs_dataframe,
    out: *mut u64,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        let handle = unsafe { dataframe_ref(dataframe) }?;
        *out = u64::try_from(handle.value.max_n_chunks())
            .map_err(|_| PhsError::invalid_argument("dataframe max chunk count exceeded u64"))?;
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_is_empty(
    dataframe: *const phs_dataframe,
    out: *mut bool,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        let handle = unsafe { dataframe_ref(dataframe) }?;
        *out = handle.value.height() == 0;
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_equals(
    left: *const phs_dataframe,
    right: *const phs_dataframe,
    missing_equal: bool,
    out: *mut bool,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = false;
        let left = unsafe { dataframe_ref(left) }?;
        let right = unsafe { dataframe_ref(right) }?;
        *out = if missing_equal {
            left.value.equals_missing(&right.value)
        } else {
            left.value.equals(&right.value)
        };
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_clear(
    dataframe: *const phs_dataframe,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { dataframe_ref(dataframe) }?;
        *out = dataframe_into_raw(handle.value.clear());
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_split_at(
    dataframe: *const phs_dataframe,
    offset: i64,
    left_out: *mut *mut phs_dataframe,
    right_out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let left_out = unsafe { required_mut(left_out, "left_out") }?;
        let right_out = unsafe { required_mut(right_out, "right_out") }?;
        *left_out = ptr::null_mut();
        *right_out = ptr::null_mut();
        let handle = unsafe { dataframe_ref(dataframe) }?;
        let (left, right) = handle.value.split_at(offset);
        *left_out = dataframe_into_raw(left);
        *right_out = dataframe_into_raw(right);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_new_from_index(
    dataframe: *const phs_dataframe,
    index: u64,
    len: u64,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { dataframe_ref(dataframe) }?;
        let index = usize_from_u64(index, "dataframe new-from-index index")?;
        let len = usize_from_u64(len, "dataframe new-from-index length")?;
        *out = dataframe_into_raw(handle.value.new_from_index(index, len));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_with_row_index(
    dataframe: *const phs_dataframe,
    name: *const c_char,
    has_offset: bool,
    offset: u64,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { dataframe_ref(dataframe) }?;
        let name = unsafe { c_str_to_str(name, "name") }?;
        let offset = if has_offset {
            Some(idx_size_from_u64(offset, "dataframe row-index offset")?)
        } else {
            None
        };
        *out = dataframe_into_raw(handle.value.with_row_index(PlSmallStr::from_str(name), offset)?);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_rechunk(
    dataframe: *const phs_dataframe,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { dataframe_ref(dataframe) }?;
        let mut dataframe = handle.value.clone();
        dataframe.rechunk_mut();
        *out = dataframe_into_raw(dataframe);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_align_chunks(
    dataframe: *const phs_dataframe,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { dataframe_ref(dataframe) }?;
        let mut dataframe = handle.value.clone();
        dataframe.align_chunks();
        *out = dataframe_into_raw(dataframe);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_should_rechunk(
    dataframe: *const phs_dataframe,
    out: *mut bool,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        let handle = unsafe { dataframe_ref(dataframe) }?;
        *out = handle.value.should_rechunk();
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_shift(
    dataframe: *const phs_dataframe,
    periods: i64,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { dataframe_ref(dataframe) }?;
        *out = dataframe_into_raw(handle.value.shift(periods));
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
    use crate::error::{PHS_INVALID_ARGUMENT, PHS_OK, phs_error_free, phs_error_message};
    use std::ffi::CStr;

    fn fixture_path() -> std::ffi::CString {
        data_path("people.csv")
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
        read_fixture_dataframe("values.csv")
    }

    fn read_fixture_dataframe(file_name: &str) -> *mut phs_dataframe {
        let path = data_path(file_name);
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

    unsafe fn take_error_message(raw: *mut phs_error) -> String {
        assert!(!raw.is_null());
        let message = unsafe { CStr::from_ptr(phs_error_message(raw)) }
            .to_str()
            .unwrap()
            .to_owned();
        unsafe { phs_error_free(raw) };
        message
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
    fn dataframe_join_inner_returns_expected_shape() {
        let left = read_fixture_dataframe("employees.csv");
        let right = read_fixture_dataframe("departments.csv");
        let key = std::ffi::CString::new("department").unwrap();
        let left_on = [key.as_ptr()];
        let right_on = [key.as_ptr()];
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();

        let status = unsafe {
            phs_dataframe_join(
                left,
                right,
                left_on.as_ptr(),
                left_on.len(),
                right_on.as_ptr(),
                right_on.len(),
                0,
                ptr::null(),
                &mut out,
                &mut err,
            )
        };

        assert_eq!(status, PHS_OK);
        assert!(err.is_null());
        assert_eq!(unsafe { dataframe_ref(out) }.unwrap().value.shape(), (3, 6));
        unsafe {
            crate::handles::phs_dataframe_free(out);
            crate::handles::phs_dataframe_free(right);
            crate::handles::phs_dataframe_free(left);
        }
    }

    #[test]
    fn dataframe_join_rejects_invalid_options() {
        let left = read_fixture_dataframe("employees.csv");
        let right = read_fixture_dataframe("departments.csv");
        let key = std::ffi::CString::new("department").unwrap();
        let name = std::ffi::CString::new("name").unwrap();
        let left_on = [key.as_ptr(), name.as_ptr()];
        let right_on = [key.as_ptr()];
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();

        let status = unsafe {
            phs_dataframe_join(
                left,
                right,
                left_on.as_ptr(),
                left_on.len(),
                right_on.as_ptr(),
                right_on.len(),
                0,
                ptr::null(),
                &mut out,
                &mut err,
            )
        };

        assert_eq!(status, PHS_INVALID_ARGUMENT);
        assert!(out.is_null());
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "left and right join key counts must match");

        let status = unsafe {
            phs_dataframe_join(
                left,
                right,
                right_on.as_ptr(),
                right_on.len(),
                right_on.as_ptr(),
                right_on.len(),
                99,
                ptr::null(),
                &mut out,
                &mut err,
            )
        };

        assert_eq!(status, PHS_INVALID_ARGUMENT);
        assert!(out.is_null());
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "unknown dataframe join type code 99");
        unsafe {
            crate::handles::phs_dataframe_free(right);
            crate::handles::phs_dataframe_free(left);
        }
    }

    #[test]
    fn dataframe_stack_operations_return_expected_shapes() {
        let df = read_values_dataframe();
        let other = read_values_dataframe();
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();

        let status = unsafe { phs_dataframe_vstack(df, other, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert!(err.is_null());
        assert_eq!(unsafe { dataframe_ref(out) }.unwrap().value.shape(), (6, 4));
        unsafe { crate::handles::phs_dataframe_free(out) };

        let city = series_into_raw(Series::new("city".into(), ["Tokyo", "Paris", "Oslo"]));
        let rank = series_into_raw(Series::new("rank".into(), [1_i64, 2, 3]));
        let series = [city as *const phs_series, rank as *const phs_series];
        let status = unsafe { phs_dataframe_hstack(df, series.as_ptr(), series.len(), &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert!(err.is_null());
        assert_eq!(unsafe { dataframe_ref(out) }.unwrap().value.shape(), (3, 6));
        unsafe {
            crate::handles::phs_dataframe_free(out);
            crate::handles::phs_series_free(rank);
            crate::handles::phs_series_free(city);
            crate::handles::phs_dataframe_free(other);
            crate::handles::phs_dataframe_free(df);
        }
    }

    #[test]
    fn dataframe_equality_uses_polars_null_semantics() {
        let left = read_values_dataframe();
        let right = read_values_dataframe();
        let employees_left = read_fixture_dataframe("employees.csv");
        let employees_right = read_fixture_dataframe("employees.csv");
        let mut head = ptr::null_mut();
        let mut out = false;
        let mut err = ptr::null_mut();

        let status = unsafe { phs_dataframe_equals(left, right, true, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert!(out);

        let status = unsafe { phs_dataframe_equals(left, right, false, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert!(!out);

        let status = unsafe { phs_dataframe_equals(employees_left, employees_right, false, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert!(out);

        let status = unsafe { phs_dataframe_head(right, 1, &mut head, &mut err) };
        assert_eq!(status, PHS_OK);
        let status = unsafe { phs_dataframe_equals(left, head, true, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert!(!out);

        out = true;
        let status = unsafe { phs_dataframe_equals(ptr::null(), right, true, &mut out, &mut err) };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        assert!(!out);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "dataframe pointer was null");

        out = true;
        let status = unsafe { phs_dataframe_equals(left, ptr::null(), true, &mut out, &mut err) };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        assert!(!out);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "dataframe pointer was null");

        let status = unsafe { phs_dataframe_equals(left, right, true, ptr::null_mut(), &mut err) };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "out pointer was null");

        unsafe {
            crate::handles::phs_dataframe_free(head);
            crate::handles::phs_dataframe_free(employees_right);
            crate::handles::phs_dataframe_free(employees_left);
            crate::handles::phs_dataframe_free(right);
            crate::handles::phs_dataframe_free(left);
        }
    }

    #[test]
    fn dataframe_metadata_clear_and_split_at_work() {
        let df = read_values_dataframe();
        let mut out = ptr::null_mut();
        let mut left_out = ptr::null_mut();
        let mut right_out = ptr::null_mut();
        let mut word_out = 0_u64;
        let mut bool_out = false;
        let mut err = ptr::null_mut();

        let status = unsafe { phs_dataframe_estimated_size(df, &mut word_out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert!(word_out > 0);

        let status = unsafe { phs_dataframe_first_col_n_chunks(df, &mut word_out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(word_out, 1);

        let status = unsafe { phs_dataframe_max_n_chunks(df, &mut word_out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(word_out, 1);

        let status = unsafe { phs_dataframe_is_empty(df, &mut bool_out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert!(!bool_out);

        let status = unsafe { phs_dataframe_clear(df, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let cleared = out;
        let original_ref = unsafe { dataframe_ref(df) }.unwrap();
        let cleared_ref = unsafe { dataframe_ref(cleared) }.unwrap();
        assert_eq!(cleared_ref.value.shape(), (0, 4));
        assert_eq!(cleared_ref.value.dtypes(), original_ref.value.dtypes());
        let cleared_names: Vec<_> = cleared_ref
            .value
            .get_column_names()
            .into_iter()
            .map(|name| name.as_str())
            .collect();
        assert_eq!(cleared_names, vec!["name", "age", "score", "active"]);

        let status = unsafe { phs_dataframe_is_empty(cleared, &mut bool_out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert!(bool_out);

        let zero_width = dataframe_into_raw(DataFrame::empty_with_height(3));
        let status = unsafe { phs_dataframe_is_empty(zero_width, &mut bool_out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert!(!bool_out);
        unsafe { crate::handles::phs_dataframe_free(zero_width) };

        let status = unsafe { phs_dataframe_split_at(df, 2, &mut left_out, &mut right_out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { dataframe_ref(left_out) }.unwrap().value.shape(), (2, 4));
        assert_eq!(unsafe { dataframe_ref(right_out) }.unwrap().value.shape(), (1, 4));
        unsafe {
            crate::handles::phs_dataframe_free(left_out);
            crate::handles::phs_dataframe_free(right_out);
        }

        let status = unsafe { phs_dataframe_split_at(df, -1, &mut left_out, &mut right_out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { dataframe_ref(left_out) }.unwrap().value.shape(), (2, 4));
        assert_eq!(unsafe { dataframe_ref(right_out) }.unwrap().value.shape(), (1, 4));
        unsafe {
            crate::handles::phs_dataframe_free(left_out);
            crate::handles::phs_dataframe_free(right_out);
        }

        let status = unsafe { phs_dataframe_split_at(df, 99, &mut left_out, &mut right_out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { dataframe_ref(left_out) }.unwrap().value.shape(), (3, 4));
        assert_eq!(unsafe { dataframe_ref(right_out) }.unwrap().value.shape(), (0, 4));
        unsafe {
            crate::handles::phs_dataframe_free(left_out);
            crate::handles::phs_dataframe_free(right_out);
        }

        let status = unsafe { phs_dataframe_split_at(df, -99, &mut left_out, &mut right_out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { dataframe_ref(left_out) }.unwrap().value.shape(), (0, 4));
        assert_eq!(unsafe { dataframe_ref(right_out) }.unwrap().value.shape(), (3, 4));
        unsafe {
            crate::handles::phs_dataframe_free(right_out);
            crate::handles::phs_dataframe_free(left_out);
        }
        right_out = ptr::null_mut();

        let status = unsafe { phs_dataframe_estimated_size(df, ptr::null_mut(), &mut err) };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "out pointer was null");

        let status = unsafe { phs_dataframe_split_at(df, 0, ptr::null_mut(), &mut right_out, &mut err) };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        assert!(right_out.is_null());
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "left_out pointer was null");

        let status = unsafe { phs_dataframe_is_empty(ptr::null(), &mut bool_out, &mut err) };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "dataframe pointer was null");

        unsafe {
            crate::handles::phs_dataframe_free(cleared);
            crate::handles::phs_dataframe_free(df);
        }
    }

    #[test]
    fn dataframe_rechunk_align_and_new_from_index_work() {
        let df = read_values_dataframe();
        let mut out = ptr::null_mut();
        let mut word_out = 0_u64;
        let mut bool_out = false;
        let mut err = ptr::null_mut();

        let mut chunked = Series::new("value".into(), [1_i64, 2, 3]);
        let right = Series::new("value".into(), [4_i64, 5, 6]);
        chunked.append(&right).unwrap();
        let other = Series::new("other".into(), [10_i64, 11, 12, 13, 14, 15]);
        let misaligned = dataframe_into_raw(DataFrame::new_infer_height(vec![chunked.into(), other.into()]).unwrap());

        let status = unsafe { phs_dataframe_should_rechunk(misaligned, &mut bool_out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert!(bool_out);

        let status = unsafe { phs_dataframe_max_n_chunks(misaligned, &mut word_out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(word_out, 2);

        let mut aligned_left = Series::new("value".into(), [1_i64, 2, 3]);
        aligned_left.append(&Series::new("value".into(), [4_i64, 5])).unwrap();
        let mut aligned_right = Series::new("other".into(), [10_i64, 11, 12]);
        aligned_right.append(&Series::new("other".into(), [13_i64, 14])).unwrap();
        let aligned_multichunk = dataframe_into_raw(DataFrame::new_infer_height(vec![aligned_left.into(), aligned_right.into()]).unwrap());
        let status = unsafe { phs_dataframe_should_rechunk(aligned_multichunk, &mut bool_out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert!(!bool_out);
        let status = unsafe { phs_dataframe_max_n_chunks(aligned_multichunk, &mut word_out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(word_out, 2);

        let mut mismatch_left = Series::new("value".into(), [1_i64, 2, 3]);
        mismatch_left.append(&Series::new("value".into(), [4_i64, 5])).unwrap();
        let mut mismatch_right = Series::new("other".into(), [10_i64, 11]);
        mismatch_right.append(&Series::new("other".into(), [12_i64, 13, 14])).unwrap();
        let length_mismatch = dataframe_into_raw(DataFrame::new_infer_height(vec![mismatch_left.into(), mismatch_right.into()]).unwrap());
        let status = unsafe { phs_dataframe_should_rechunk(length_mismatch, &mut bool_out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert!(bool_out);

        let status = unsafe { phs_dataframe_align_chunks(misaligned, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let aligned = out;
        let status = unsafe { phs_dataframe_should_rechunk(aligned, &mut bool_out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert!(!bool_out);
        let status = unsafe { phs_dataframe_max_n_chunks(aligned, &mut word_out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(word_out, 1);

        let status = unsafe { phs_dataframe_rechunk(misaligned, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let rechunked = out;
        let rechunked_ref = unsafe { dataframe_ref(rechunked) }.unwrap();
        assert_eq!(rechunked_ref.value.shape(), (6, 2));
        let values: Vec<Option<i64>> = rechunked_ref.value.column("value").unwrap().as_materialized_series().i64().unwrap().into_iter().collect();
        assert_eq!(values, vec![Some(1), Some(2), Some(3), Some(4), Some(5), Some(6)]);

        let status = unsafe { phs_dataframe_new_from_index(df, 1, 4, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let repeated = out;
        let repeated_ref = unsafe { dataframe_ref(repeated) }.unwrap();
        assert_eq!(repeated_ref.value.shape(), (4, 4));
        let names: Vec<Option<&str>> = repeated_ref.value.column("name").unwrap().as_materialized_series().str().unwrap().into_iter().collect();
        assert_eq!(names, vec![Some("Bob"), Some("Bob"), Some("Bob"), Some("Bob")]);

        let status = unsafe { phs_dataframe_new_from_index(df, 99, 2, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let null_repeated = out;
        let null_repeated_ref = unsafe { dataframe_ref(null_repeated) }.unwrap();
        assert_eq!(null_repeated_ref.value.shape(), (2, 4));
        let active: Vec<Option<bool>> = null_repeated_ref.value.column("active").unwrap().as_materialized_series().bool().unwrap().into_iter().collect();
        assert_eq!(active, vec![None, None]);

        let status = unsafe { phs_dataframe_new_from_index(df, 0, 1, ptr::null_mut(), &mut err) };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "out pointer was null");

        unsafe {
            crate::handles::phs_dataframe_free(null_repeated);
            crate::handles::phs_dataframe_free(repeated);
            crate::handles::phs_dataframe_free(rechunked);
            crate::handles::phs_dataframe_free(aligned);
            crate::handles::phs_dataframe_free(length_mismatch);
            crate::handles::phs_dataframe_free(aligned_multichunk);
            crate::handles::phs_dataframe_free(misaligned);
            crate::handles::phs_dataframe_free(df);
        }
    }

    #[test]
    fn dataframe_row_distinct_masks_work() {
        let df = dataframe_into_raw(
            DataFrame::new_infer_height(vec![
                Series::new("name".into(), ["a", "b", "a", "c", "b"]).into(),
                Series::new("age".into(), [1_i64, 2, 1, 3, 2]).into(),
            ])
            .unwrap(),
        );
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();

        let status = unsafe { phs_dataframe_is_duplicated(df, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let duplicated = out;
        let duplicated_values: Vec<Option<bool>> = unsafe { series_ref(duplicated) }
            .unwrap()
            .value
            .bool()
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(duplicated_values, vec![Some(true), Some(true), Some(true), Some(false), Some(true)]);

        let status = unsafe { phs_dataframe_is_unique(df, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let unique = out;
        let unique_values: Vec<Option<bool>> = unsafe { series_ref(unique) }
            .unwrap()
            .value
            .bool()
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(unique_values, vec![Some(false), Some(false), Some(false), Some(true), Some(false)]);

        let status = unsafe { phs_dataframe_is_unique(df, ptr::null_mut(), &mut err) };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "out pointer was null");

        unsafe {
            crate::handles::phs_series_free(unique);
            crate::handles::phs_series_free(duplicated);
            crate::handles::phs_dataframe_free(df);
        }
    }

    #[test]
    fn dataframe_sampling_uses_seeded_options() {
        let df = dataframe_into_raw(
            DataFrame::new_infer_height(vec![
                Series::new("value".into(), [10_i64, 20, 30, 40, 50]).into(),
                Series::new("label".into(), [Some("a"), None, Some("c"), Some("d"), Some("e")]).into(),
            ])
            .unwrap(),
        );
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();

        let status = unsafe { phs_dataframe_sample_n(df, 2, false, false, true, 0, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let sampled = out;
        let sampled_ref = unsafe { dataframe_ref(sampled) }.unwrap();
        let values: Vec<Option<i64>> = sampled_ref.value.column("value").unwrap().as_materialized_series().i64().unwrap().into_iter().collect();
        assert_eq!(values, vec![Some(50), Some(20)]);

        let status = unsafe { phs_dataframe_sample_frac(df, 0.4, false, false, true, 0, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let frac_sampled = out;
        let values: Vec<Option<i64>> = unsafe { dataframe_ref(frac_sampled) }.unwrap().value.column("value").unwrap().as_materialized_series().i64().unwrap().into_iter().collect();
        assert_eq!(values, vec![Some(50), Some(20)]);

        let status = unsafe { phs_dataframe_sample_n(df, 7, true, false, true, 0, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let replacement = out;
        let values: Vec<Option<i64>> = unsafe { dataframe_ref(replacement) }.unwrap().value.column("value").unwrap().as_materialized_series().i64().unwrap().into_iter().collect();
        assert_eq!(values, vec![Some(20), Some(20), Some(20), Some(10), Some(30), Some(10), Some(50)]);

        let status = unsafe { phs_dataframe_sample_frac(df, 1.4, true, false, true, 0, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let frac_replacement = out;
        let values: Vec<Option<i64>> = unsafe { dataframe_ref(frac_replacement) }.unwrap().value.column("value").unwrap().as_materialized_series().i64().unwrap().into_iter().collect();
        assert_eq!(values, vec![Some(20), Some(20), Some(20), Some(10), Some(30), Some(10), Some(50)]);

        out = ptr::null_mut();
        let status = unsafe { phs_dataframe_sample_n(df, 6, false, false, true, 0, &mut out, &mut err) };
        assert_ne!(status, PHS_OK);
        assert!(out.is_null());
        unsafe { take_error_message(err) };

        let status = unsafe { phs_dataframe_sample_frac(df, 1.2, false, false, true, 0, &mut out, &mut err) };
        assert_ne!(status, PHS_OK);
        assert!(out.is_null());
        unsafe { take_error_message(err) };

        let status = unsafe { phs_dataframe_sample_n(df, 0, false, false, true, 0, ptr::null_mut(), &mut err) };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "out pointer was null");

        let status = unsafe { phs_dataframe_sample_frac(df, 0.0, false, false, true, 0, ptr::null_mut(), &mut err) };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "out pointer was null");

        unsafe {
            crate::handles::phs_dataframe_free(frac_replacement);
            crate::handles::phs_dataframe_free(replacement);
            crate::handles::phs_dataframe_free(frac_sampled);
            crate::handles::phs_dataframe_free(sampled);
            crate::handles::phs_dataframe_free(df);
        }
    }

    #[test]
    fn dataframe_row_index_and_shift_work() {
        let df = dataframe_into_raw(
            DataFrame::new_infer_height(vec![
                Series::new("value".into(), [10_i64, 20, 30]).into(),
                Series::new("label".into(), [Some("a"), None, Some("c")]).into(),
            ])
            .unwrap(),
        );
        let name = std::ffi::CString::new("row_nr").unwrap();
        let duplicate_name = std::ffi::CString::new("value").unwrap();
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();

        let status = unsafe { phs_dataframe_with_row_index(df, name.as_ptr(), true, 5, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let indexed = out;
        let row_index: Vec<Option<IdxSize>> = unsafe { dataframe_ref(indexed) }
            .unwrap()
            .value
            .column("row_nr")
            .unwrap()
            .as_materialized_series()
            .idx()
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(row_index, vec![Some(5 as IdxSize), Some(6 as IdxSize), Some(7 as IdxSize)]);

        let status = unsafe { phs_dataframe_shift(df, 1, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let shifted_down = out;
        let values: Vec<Option<i64>> = unsafe { dataframe_ref(shifted_down) }
            .unwrap()
            .value
            .column("value")
            .unwrap()
            .as_materialized_series()
            .i64()
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(values, vec![None, Some(10), Some(20)]);

        let status = unsafe { phs_dataframe_shift(df, -1, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let shifted_up = out;
        let values: Vec<Option<i64>> = unsafe { dataframe_ref(shifted_up) }
            .unwrap()
            .value
            .column("value")
            .unwrap()
            .as_materialized_series()
            .i64()
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(values, vec![Some(20), Some(30), None]);

        out = ptr::null_mut();
        let status = unsafe { phs_dataframe_with_row_index(df, duplicate_name.as_ptr(), false, 0, &mut out, &mut err) };
        assert_ne!(status, PHS_OK);
        assert!(out.is_null());
        unsafe { take_error_message(err) };

        let status = unsafe { phs_dataframe_with_row_index(df, name.as_ptr(), true, u64::MAX, &mut out, &mut err) };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        assert!(out.is_null());
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "dataframe row-index offset exceeds Polars index size");

        let status = unsafe {
            phs_dataframe_with_row_index(
                df,
                name.as_ptr(),
                true,
                IdxSize::MAX as u64,
                &mut out,
                &mut err,
            )
        };
        assert_ne!(status, PHS_OK);
        assert!(out.is_null());
        unsafe { take_error_message(err) };

        let status = unsafe { phs_dataframe_with_row_index(df, ptr::null(), false, 0, &mut out, &mut err) };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        assert!(out.is_null());
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "name pointer was null");

        let status = unsafe { phs_dataframe_with_row_index(df, name.as_ptr(), false, 0, ptr::null_mut(), &mut err) };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "out pointer was null");

        let status = unsafe { phs_dataframe_shift(df, 0, ptr::null_mut(), &mut err) };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "out pointer was null");

        unsafe {
            crate::handles::phs_dataframe_free(shifted_up);
            crate::handles::phs_dataframe_free(shifted_down);
            crate::handles::phs_dataframe_free(indexed);
            crate::handles::phs_dataframe_free(df);
        }
    }

    #[test]
    fn dataframe_hstack_rejects_invalid_series_array() {
        let df = read_values_dataframe();
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();

        let status = unsafe { phs_dataframe_hstack(df, ptr::null(), 1, &mut out, &mut err) };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        assert!(out.is_null());
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "series array pointer was null");

        let status = unsafe { phs_dataframe_hstack(df, ptr::null(), 0, &mut out, &mut err) };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        assert!(out.is_null());
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "hstack requires at least one series");
        unsafe { crate::handles::phs_dataframe_free(df) };
    }

    #[test]
    fn dataframe_with_columns_replaces_and_broadcasts() {
        let df = read_values_dataframe();
        let age = series_into_raw(Series::new("age".into(), [40_i64, 41, 42]));
        let score = series_into_raw(Series::new("score".into(), [10.0_f64]));
        let series = [age as *const phs_series, score as *const phs_series];
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();

        let status = unsafe { phs_dataframe_with_columns(df, series.as_ptr(), series.len(), &mut out, &mut err) };

        assert_eq!(status, PHS_OK);
        assert!(err.is_null());
        let result = unsafe { dataframe_ref(out) }.unwrap();
        assert_eq!(result.value.shape(), (3, 4));
        let age_values: Vec<Option<i64>> = result.value.column("age").unwrap().as_materialized_series().i64().unwrap().into_iter().collect();
        assert_eq!(age_values, vec![Some(40), Some(41), Some(42)]);
        let score_values: Vec<Option<f64>> = result.value.column("score").unwrap().as_materialized_series().f64().unwrap().into_iter().collect();
        assert_eq!(score_values, vec![Some(10.0), Some(10.0), Some(10.0)]);
        unsafe {
            crate::handles::phs_dataframe_free(out);
            crate::handles::phs_series_free(score);
            crate::handles::phs_series_free(age);
            crate::handles::phs_dataframe_free(df);
        }
    }

    #[test]
    fn dataframe_insert_and_replace_column_work() {
        let df = read_values_dataframe();
        let active = series_into_raw(Series::new("active_inserted".into(), [Some(true), Some(false), None]));
        let city = series_into_raw(Series::new("city".into(), ["Tokyo", "Paris", "Oslo"]));
        let score = series_into_raw(Series::new("score_replaced".into(), [9.5_f64, 8.0, 7.25]));
        let duplicate_age = series_into_raw(Series::new("age".into(), [40_i64, 41, 42]));
        let short = series_into_raw(Series::new("short".into(), [1_i64, 2]));
        let unit = series_into_raw(Series::new("unit".into(), [1.0_f64]));
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();

        let status = unsafe { phs_dataframe_insert_column(df, 1, active, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let inserted = out;
        let names: Vec<&str> = unsafe { dataframe_ref(inserted) }
            .unwrap()
            .value
            .get_column_names()
            .into_iter()
            .map(|name| name.as_str())
            .collect();
        assert_eq!(names, vec!["name", "active_inserted", "age", "score", "active"]);

        let status = unsafe { phs_dataframe_insert_column(df, 4, city, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let appended = out;
        let names: Vec<&str> = unsafe { dataframe_ref(appended) }
            .unwrap()
            .value
            .get_column_names()
            .into_iter()
            .map(|name| name.as_str())
            .collect();
        assert_eq!(names, vec!["name", "age", "score", "active", "city"]);

        out = ptr::null_mut();
        let status = unsafe { phs_dataframe_insert_column(df, 1, duplicate_age, &mut out, &mut err) };
        assert_ne!(status, PHS_OK);
        assert!(out.is_null());
        unsafe { take_error_message(err) };

        let status = unsafe { phs_dataframe_insert_column(df, 5, city, &mut out, &mut err) };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        assert!(out.is_null());
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "dataframe insert-column index 5 exceeds width 4");

        let status = unsafe { phs_dataframe_insert_column(df, 1, short, &mut out, &mut err) };
        assert_ne!(status, PHS_OK);
        assert!(out.is_null());
        unsafe { take_error_message(err) };

        let status = unsafe { phs_dataframe_insert_column(df, 1, unit, &mut out, &mut err) };
        assert_ne!(status, PHS_OK);
        assert!(out.is_null());
        unsafe { take_error_message(err) };

        let status = unsafe { phs_dataframe_insert_column(df, 0, active, ptr::null_mut(), &mut err) };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "out pointer was null");

        let status = unsafe { phs_dataframe_replace_column(df, 1, score, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let replaced = out;
        let names: Vec<&str> = unsafe { dataframe_ref(replaced) }
            .unwrap()
            .value
            .get_column_names()
            .into_iter()
            .map(|name| name.as_str())
            .collect();
        assert_eq!(names, vec!["name", "score_replaced", "score", "active"]);

        let status = unsafe { phs_dataframe_replace_column(df, 0, duplicate_age, &mut out, &mut err) };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "dataframe replace-column name \"age\" already exists at index 1");

        out = ptr::null_mut();
        let status = unsafe { phs_dataframe_replace_column(df, 4, score, &mut out, &mut err) };
        assert_ne!(status, PHS_OK);
        assert!(out.is_null());
        unsafe { take_error_message(err) };

        let status = unsafe { phs_dataframe_replace_column(df, 0, short, &mut out, &mut err) };
        assert_ne!(status, PHS_OK);
        assert!(out.is_null());
        unsafe { take_error_message(err) };

        let status = unsafe { phs_dataframe_replace_column(df, 0, unit, &mut out, &mut err) };
        assert_ne!(status, PHS_OK);
        assert!(out.is_null());
        unsafe { take_error_message(err) };

        let status = unsafe { phs_dataframe_replace_column(df, 0, score, ptr::null_mut(), &mut err) };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "out pointer was null");

        unsafe {
            crate::handles::phs_dataframe_free(replaced);
            crate::handles::phs_dataframe_free(appended);
            crate::handles::phs_dataframe_free(inserted);
            crate::handles::phs_series_free(unit);
            crate::handles::phs_series_free(short);
            crate::handles::phs_series_free(duplicate_age);
            crate::handles::phs_series_free(score);
            crate::handles::phs_series_free(city);
            crate::handles::phs_series_free(active);
            crate::handles::phs_dataframe_free(df);
        }
    }

    #[test]
    fn dataframe_partition_by_returns_group_frames() {
        let df = read_fixture_dataframe("employees.csv");
        let department = std::ffi::CString::new("department").unwrap();
        let missing = std::ffi::CString::new("missing").unwrap();
        let names = [department.as_ptr()];
        let missing_names = [missing.as_ptr()];
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();

        let status = unsafe { phs_dataframe_partition_by(df, names.as_ptr(), names.len(), true, true, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let partitions = out;
        assert_eq!(unsafe { phs_dataframe_array_len(partitions) }, 3);

        let mut frame_out = ptr::null_mut();
        let status = unsafe { phs_dataframe_array_get(partitions, 0, &mut frame_out, &mut err) };
        assert_eq!(status, PHS_OK);
        let engineering = frame_out;

        let status = unsafe { phs_dataframe_array_get(partitions, 0, ptr::null_mut(), &mut err) };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "out pointer was null");

        let status = unsafe { phs_dataframe_array_get(partitions, 99, &mut frame_out, &mut err) };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "dataframe array index 99 out of bounds");

        let status = unsafe { phs_dataframe_array_get(ptr::null(), 0, &mut frame_out, &mut err) };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "dataframe array pointer was null");

        unsafe {
            phs_dataframe_array_free(partitions);
            phs_dataframe_array_free(ptr::null_mut());
        }
        let values: Vec<Option<&str>> = unsafe { dataframe_ref(engineering) }
            .unwrap()
            .value
            .column("name")
            .unwrap()
            .as_materialized_series()
            .str()
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(values, vec![Some("Alice"), Some("Bob")]);

        unsafe { crate::handles::phs_dataframe_free(engineering) };

        let status = unsafe { phs_dataframe_partition_by(df, names.as_ptr(), names.len(), false, true, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let no_key_partitions = out;
        let status = unsafe { phs_dataframe_array_get(no_key_partitions, 0, &mut frame_out, &mut err) };
        assert_eq!(status, PHS_OK);
        let engineering_no_key = frame_out;
        let names_without_key: Vec<&str> = unsafe { dataframe_ref(engineering_no_key) }
            .unwrap()
            .value
            .get_column_names()
            .into_iter()
            .map(|name| name.as_str())
            .collect();
        assert_eq!(names_without_key, vec!["id", "name", "salary"]);
        unsafe {
            crate::handles::phs_dataframe_free(engineering_no_key);
            phs_dataframe_array_free(no_key_partitions);
        }

        let status = unsafe { phs_dataframe_partition_by(df, ptr::null(), 0, true, true, &mut out, &mut err) };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "partition_by requires at least one column");

        let status = unsafe { phs_dataframe_partition_by(df, missing_names.as_ptr(), missing_names.len(), true, true, &mut out, &mut err) };
        assert_ne!(status, PHS_OK);
        unsafe { take_error_message(err) };

        let status = unsafe { phs_dataframe_partition_by(df, names.as_ptr(), names.len(), true, true, ptr::null_mut(), &mut err) };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "out pointer was null");

        unsafe { crate::handles::phs_dataframe_free(df) };
    }

    #[test]
    fn dataframe_explode_list_columns_work() {
        let part0 = Series::new(PlSmallStr::from_static(""), ["red", "green", "blue"]);
        let part1 = Series::new(PlSmallStr::from_static(""), ["red", "red"]);
        let part2 = Series::new(PlSmallStr::from_static(""), ["日本", "語"]);
        let part3 = Series::new(PlSmallStr::from_static(""), ["solo"]);
        let parts = Column::new(PlSmallStr::from_static("parts"), [part0, part1, part2, part3]);
        let id = Column::new(PlSmallStr::from_static("id"), [1i32, 2, 3, 4]);
        let df = dataframe_into_raw(DataFrame::new_infer_height(vec![id, parts]).unwrap());
        let parts_name = std::ffi::CString::new("parts").unwrap();
        let missing_name = std::ffi::CString::new("missing").unwrap();
        let id_name = std::ffi::CString::new("id").unwrap();
        let names = [parts_name.as_ptr()];
        let missing_names = [missing_name.as_ptr()];
        let id_names = [id_name.as_ptr()];
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();

        let status = unsafe { phs_dataframe_explode(df, names.as_ptr(), names.len(), true, true, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let exploded = out;
        let exploded_ref = unsafe { dataframe_ref(exploded) }.unwrap();
        assert_eq!(exploded_ref.value.shape(), (8, 2));
        let ids: Vec<Option<i32>> = exploded_ref
            .value
            .column("id")
            .unwrap()
            .as_materialized_series()
            .i32()
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(ids, vec![Some(1), Some(1), Some(1), Some(2), Some(2), Some(3), Some(3), Some(4)]);
        let values: Vec<Option<&str>> = exploded_ref
            .value
            .column("parts")
            .unwrap()
            .as_materialized_series()
            .str()
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(
            values,
            vec![Some("red"), Some("green"), Some("blue"), Some("red"), Some("red"), Some("日本"), Some("語"), Some("solo")]
        );

        let status = unsafe { phs_dataframe_explode(df, ptr::null(), 0, true, true, &mut out, &mut err) };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "explode requires at least one column");

        let status = unsafe { phs_dataframe_explode(df, ptr::null(), 1, true, true, &mut out, &mut err) };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "names pointer was null");

        let null_name = [ptr::null()];
        let status = unsafe { phs_dataframe_explode(df, null_name.as_ptr(), null_name.len(), true, true, &mut out, &mut err) };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "names pointer was null");

        let status = unsafe { phs_dataframe_explode(df, missing_names.as_ptr(), missing_names.len(), true, true, &mut out, &mut err) };
        assert_ne!(status, PHS_OK);
        unsafe { take_error_message(err) };

        let status = unsafe { phs_dataframe_explode(df, id_names.as_ptr(), id_names.len(), true, true, &mut out, &mut err) };
        assert_ne!(status, PHS_OK);
        unsafe { take_error_message(err) };

        let status = unsafe { phs_dataframe_explode(df, names.as_ptr(), names.len(), true, true, ptr::null_mut(), &mut err) };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "out pointer was null");

        unsafe {
            crate::handles::phs_dataframe_free(exploded);
            crate::handles::phs_dataframe_free(df);
        }
    }

    #[test]
    fn dataframe_gather_every_returns_strided_rows() {
        let df = read_values_dataframe();
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();

        let status = unsafe { phs_dataframe_gather_every(df, 2, 0, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let every_two = out;
        let values: Vec<Option<&str>> = unsafe { dataframe_ref(every_two) }
            .unwrap()
            .value
            .column("name")
            .unwrap()
            .as_materialized_series()
            .str()
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(values, vec![Some("Alice"), Some("Carol")]);

        let status = unsafe { phs_dataframe_gather_every(df, 2, 1, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let offset_rows = out;
        let values: Vec<Option<&str>> = unsafe { dataframe_ref(offset_rows) }
            .unwrap()
            .value
            .column("name")
            .unwrap()
            .as_materialized_series()
            .str()
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(values, vec![Some("Bob")]);

        let status = unsafe { phs_dataframe_gather_every(df, 2, 10, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let empty = out;
        assert_eq!(unsafe { dataframe_ref(empty) }.unwrap().value.shape(), (0, 4));

        let status = unsafe { phs_dataframe_gather_every(df, 2, u64::MAX, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let huge_offset = out;
        assert_eq!(unsafe { dataframe_ref(huge_offset) }.unwrap().value.shape(), (0, 4));

        let status = unsafe { phs_dataframe_gather_every(df, 0, 0, &mut out, &mut err) };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "dataframe gather-every step must be positive");

        let status = unsafe { phs_dataframe_gather_every(df, 2, 0, ptr::null_mut(), &mut err) };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "out pointer was null");

        unsafe {
            crate::handles::phs_dataframe_free(huge_offset);
            crate::handles::phs_dataframe_free(empty);
            crate::handles::phs_dataframe_free(offset_rows);
            crate::handles::phs_dataframe_free(every_two);
            crate::handles::phs_dataframe_free(df);
        }
    }

    #[test]
    fn dataframe_with_columns_rejects_invalid_series_array() {
        let df = read_values_dataframe();
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();

        let status = unsafe { phs_dataframe_with_columns(df, ptr::null(), 1, &mut out, &mut err) };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        assert!(out.is_null());
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "series array pointer was null");

        let status = unsafe { phs_dataframe_with_columns(df, ptr::null(), 0, &mut out, &mut err) };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        assert!(out.is_null());
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "with_columns requires at least one series");
        unsafe { crate::handles::phs_dataframe_free(df) };
    }

    #[test]
    fn dataframe_take_handles_empty_and_reordered_indices() {
        let df = read_values_dataframe();
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();
        let indices = [2_u64, 0, 1, 1];

        let status = unsafe { phs_dataframe_take(df, indices.as_ptr(), indices.len(), &mut out, &mut err) };

        assert_eq!(status, PHS_OK);
        assert!(err.is_null());
        assert_eq!(unsafe { dataframe_ref(out) }.unwrap().value.shape(), (4, 4));
        unsafe { crate::handles::phs_dataframe_free(out) };

        let status = unsafe { phs_dataframe_take(df, ptr::null(), 0, &mut out, &mut err) };

        assert_eq!(status, PHS_OK);
        assert!(err.is_null());
        assert_eq!(unsafe { dataframe_ref(out) }.unwrap().value.shape(), (0, 4));
        unsafe {
            crate::handles::phs_dataframe_free(out);
            crate::handles::phs_dataframe_free(df);
        }
    }

    #[test]
    fn dataframe_take_rejects_null_indices_pointer_with_positive_length() {
        let df = read_values_dataframe();
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();

        let status = unsafe { phs_dataframe_take(df, ptr::null(), 1, &mut out, &mut err) };

        assert_eq!(status, PHS_INVALID_ARGUMENT);
        assert!(out.is_null());
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "indices pointer was null");
        unsafe { crate::handles::phs_dataframe_free(df) };
    }

    #[test]
    fn dataframe_take_rejects_index_overflow() {
        let df = read_values_dataframe();
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();
        let indices = [u64::from(u32::MAX) + 1];

        let status = unsafe { phs_dataframe_take(df, indices.as_ptr(), indices.len(), &mut out, &mut err) };

        assert_eq!(status, PHS_INVALID_ARGUMENT);
        assert!(out.is_null());
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "dataframe take index exceeds Polars index size");
        unsafe { crate::handles::phs_dataframe_free(df) };
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
