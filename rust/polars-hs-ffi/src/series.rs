use std::os::raw::{c_char, c_int};
use std::ptr;

use polars::prelude::*;
use polars::series::ops::NullBehavior;
use polars_ops::series::{
    abs as polars_series_abs,
    diff as polars_series_diff,
    interpolate as polars_series_interpolate,
    InterpolationMethod,
    is_duplicated as polars_series_is_duplicated,
    is_first_distinct as polars_series_is_first_distinct,
    is_last_distinct as polars_series_is_last_distinct,
    is_unique as polars_series_is_unique,
    pct_change as polars_series_pct_change,
    RoundMode,
    RoundSeries,
    SeriesMethods,
    SeriesRank,
    unique_counts as polars_series_unique_counts,
};
use polars_ops::chunked_array::mode::mode as polars_series_mode;

use crate::bytes::{bytes_into_raw, phs_bytes};
use crate::error::{PhsError, PhsResult, c_str_to_str, ffi_boundary, phs_error, required_mut};
use crate::handles::{dataframe_into_raw, phs_dataframe, phs_series, series_into_raw, series_ref};

const COLUMN_TAG_NULL: u8 = 0;
const COLUMN_TAG_VALUE: u8 = 1;

pub(crate) fn encode_bool_series(series: &Series) -> PhsResult<Vec<u8>> {
    let values = series.bool()?;
    let mut bytes = Vec::with_capacity(values.len() * 2);
    for value in values {
        match value {
            None => bytes.push(COLUMN_TAG_NULL),
            Some(value) => {
                bytes.push(COLUMN_TAG_VALUE);
                bytes.push(u8::from(value));
            },
        }
    }
    Ok(bytes)
}

pub(crate) fn encode_i64_series(series: &Series) -> PhsResult<Vec<u8>> {
    let values = series.i64()?;
    let mut bytes = Vec::with_capacity(values.len() * 9);
    for value in values {
        match value {
            None => bytes.push(COLUMN_TAG_NULL),
            Some(value) => {
                bytes.push(COLUMN_TAG_VALUE);
                bytes.extend_from_slice(&value.to_le_bytes());
            },
        }
    }
    Ok(bytes)
}

macro_rules! encode_le_series {
    ($name:ident, $accessor:ident, $width:expr) => {
        pub(crate) fn $name(series: &Series) -> PhsResult<Vec<u8>> {
            let values = series.$accessor()?;
            let mut bytes = Vec::with_capacity(values.len() * (1 + $width));
            for value in values {
                match value {
                    None => bytes.push(COLUMN_TAG_NULL),
                    Some(value) => {
                        bytes.push(COLUMN_TAG_VALUE);
                        bytes.extend_from_slice(&value.to_le_bytes());
                    },
                }
            }
            Ok(bytes)
        }
    };
}

encode_le_series!(encode_i8_series, i8, 1);
encode_le_series!(encode_i16_series, i16, 2);
encode_le_series!(encode_i32_series, i32, 4);
encode_le_series!(encode_u8_series, u8, 1);
encode_le_series!(encode_u16_series, u16, 2);
encode_le_series!(encode_u32_series, u32, 4);
encode_le_series!(encode_u64_series, u64, 8);
encode_le_series!(encode_f32_series, f32, 4);

pub(crate) fn encode_f64_series(series: &Series) -> PhsResult<Vec<u8>> {
    let values = series.f64()?;
    let mut bytes = Vec::with_capacity(values.len() * 9);
    for value in values {
        match value {
            None => bytes.push(COLUMN_TAG_NULL),
            Some(value) => {
                bytes.push(COLUMN_TAG_VALUE);
                bytes.extend_from_slice(&value.to_le_bytes());
            },
        }
    }
    Ok(bytes)
}

pub(crate) fn encode_text_series(series: &Series) -> PhsResult<Vec<u8>> {
    let values = series.str()?;
    let mut bytes = Vec::with_capacity(values.len() * 9);
    for value in values {
        match value {
            None => bytes.push(COLUMN_TAG_NULL),
            Some(value) => {
                bytes.push(COLUMN_TAG_VALUE);
                bytes.extend_from_slice(&(value.len() as u64).to_le_bytes());
                bytes.extend_from_slice(value.as_bytes());
            },
        }
    }
    Ok(bytes)
}

fn series_values_bytes<F>(
    series: *const phs_series,
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
        let handle = unsafe { series_ref(series) }?;
        *out = bytes_into_raw(encode(&handle.value)?);
        Ok(())
    })
}

fn series_transform<F>(
    series: *const phs_series,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
    transform: F,
) -> c_int
where
    F: FnOnce(&Series) -> PhsResult<Series> + std::panic::UnwindSafe,
{
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { series_ref(series) }?;
        *out = series_into_raw(transform(&handle.value)?);
        Ok(())
    })
}

fn dtype_from_code(code: c_int) -> PhsResult<DataType> {
    match code {
        0 => Ok(DataType::Boolean),
        1 => Ok(DataType::Int8),
        2 => Ok(DataType::Int16),
        3 => Ok(DataType::Int32),
        4 => Ok(DataType::Int64),
        5 => Ok(DataType::UInt8),
        6 => Ok(DataType::UInt16),
        7 => Ok(DataType::UInt32),
        8 => Ok(DataType::UInt64),
        9 => Ok(DataType::Float32),
        10 => Ok(DataType::Float64),
        11 => Ok(DataType::String),
        value => Err(PhsError::invalid_argument(format!("unknown series cast dtype code {value}"))),
    }
}

fn idx_size_from_u64(value: u64, label: &str) -> PhsResult<IdxSize> {
    value
        .try_into()
        .map_err(|_| PhsError::invalid_argument(format!("{label} exceeds Polars index size")))
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

fn round_mode_from_code(code: c_int) -> PhsResult<RoundMode> {
    match code {
        0 => Ok(RoundMode::HalfToEven),
        1 => Ok(RoundMode::HalfAwayFromZero),
        _ => Err(PhsError::invalid_argument(format!("unknown round mode code {code}"))),
    }
}

fn null_behavior_from_code(code: c_int) -> PhsResult<NullBehavior> {
    match code {
        0 => Ok(NullBehavior::Ignore),
        1 => Ok(NullBehavior::Drop),
        _ => Err(PhsError::invalid_argument(format!(
            "unknown null behavior code {code}"
        ))),
    }
}

fn interpolation_method_from_code(code: c_int) -> PhsResult<InterpolationMethod> {
    match code {
        0 => Ok(InterpolationMethod::Linear),
        1 => Ok(InterpolationMethod::Nearest),
        _ => Err(PhsError::invalid_argument(format!(
            "unknown interpolation method code {code}"
        ))),
    }
}

fn rank_method_from_code(code: c_int) -> PhsResult<RankMethod> {
    match code {
        0 => Ok(RankMethod::Average),
        1 => Ok(RankMethod::Min),
        2 => Ok(RankMethod::Max),
        3 => Ok(RankMethod::Dense),
        4 => Ok(RankMethod::Ordinal),
        value => Err(PhsError::invalid_argument(format!(
            "unknown rank method code {value}"
        ))),
    }
}

fn ensure_interpolation_dtype(series: &Series, method: InterpolationMethod) -> PhsResult<()> {
    if method == InterpolationMethod::Nearest {
        match series.dtype() {
            DataType::Boolean
            | DataType::String
            | DataType::BinaryOffset
            | DataType::Null
            | DataType::Unknown(_) => {
                return Err(PhsError::invalid_argument(format!(
                    "series interpolate nearest is unsupported for dtype {:?}",
                    series.dtype()
                )));
            },
            _ => {},
        }
    }
    Ok(())
}

fn ensure_diff_period_in_bounds(series: &Series, n: i64, null_behavior: NullBehavior) -> PhsResult<()> {
    if null_behavior == NullBehavior::Drop && n.unsigned_abs() > series.len() as u64 {
        Err(PhsError::invalid_argument(format!(
            "series diff period {} exceeds series length {}",
            n.unsigned_abs(),
            series.len()
        )))
    } else {
        Ok(())
    }
}

fn series_binary_op_from_code(left: &Series, right: &Series, op: c_int) -> PhsResult<Series> {
    match op {
        0 => Ok(std::ops::Add::add(left, right)?),
        1 => Ok(std::ops::Sub::sub(left, right)?),
        2 => Ok(std::ops::Mul::mul(left, right)?),
        3 => Ok(std::ops::Div::div(left, right)?),
        4 => Ok(std::ops::Rem::rem(left, right)?),
        _ => Err(PhsError::invalid_argument(format!(
            "unknown series binary op code {op}"
        ))),
    }
}

fn series_stat_from_code(series: &Series, op: c_int, ddof: u8) -> PhsResult<Option<f64>> {
    match op {
        0 => Ok(series.mean()),
        1 => Ok(series.std(ddof)),
        2 => Ok(series.var(ddof)),
        3 => {
            ensure_numeric_stat_dtype(series, "series sum")?;
            Ok(Some(series.sum::<f64>()?))
        },
        4 => {
            ensure_numeric_stat_dtype(series, "series min")?;
            Ok(series.min::<f64>()?)
        },
        5 => {
            ensure_numeric_stat_dtype(series, "series max")?;
            Ok(series.max::<f64>()?)
        },
        6 => Ok(series.median()),
        _ => Err(PhsError::invalid_argument(format!(
            "unknown series stat op code {op}"
        ))),
    }
}

fn ensure_numeric_stat_dtype(series: &Series, label: &str) -> PhsResult<()> {
    if series.dtype().is_numeric() {
        Ok(())
    } else {
        Err(PhsError::invalid_argument(format!("{label} requires numeric dtype")))
    }
}

fn usize_from_u64(value: u64, label: &str) -> PhsResult<usize> {
    value
        .try_into()
        .map_err(|_| PhsError::invalid_argument(format!("{label} exceeded usize")))
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

fn idx_ca_from_u64_slice(values: &[u64], label: &str) -> PhsResult<IdxCa> {
    let indices = values
        .iter()
        .map(|value| idx_size_from_u64(*value, label))
        .collect::<PhsResult<Vec<IdxSize>>>()?;
    Ok(IdxCa::from_vec(PlSmallStr::EMPTY, indices))
}

fn raw_bytes<'a>(data: *const u8, len: usize, name: &str) -> PhsResult<&'a [u8]> {
    if len == 0 {
        Ok(&[])
    } else if data.is_null() {
        Err(PhsError::invalid_argument(format!("{name} pointer was null")))
    } else {
        Ok(unsafe { std::slice::from_raw_parts(data, len) })
    }
}

fn read_tag(bytes: &[u8], offset: &mut usize, context: &str) -> PhsResult<u8> {
    if *offset >= bytes.len() {
        return Err(PhsError::invalid_argument(format!("{context} ended early")));
    }
    let tag = bytes[*offset];
    *offset += 1;
    Ok(tag)
}

fn read_u64_le(bytes: &[u8], offset: &mut usize, context: &str) -> PhsResult<u64> {
    Ok(u64::from_le_bytes(read_fixed_le::<8>(bytes, offset, context)?))
}

fn read_fixed_le<const N: usize>(bytes: &[u8], offset: &mut usize, context: &str) -> PhsResult<[u8; N]> {
    let end = offset
        .checked_add(N)
        .ok_or_else(|| PhsError::invalid_argument(format!("{context} length overflow")))?;
    if end > bytes.len() {
        return Err(PhsError::invalid_argument(format!("{context} ended early")));
    }
    let value = bytes[*offset..end].try_into().expect("slice length checked");
    *offset = end;
    Ok(value)
}

fn decode_bool_values(bytes: &[u8]) -> PhsResult<Vec<Option<bool>>> {
    let mut values = Vec::new();
    let mut offset = 0;
    while offset < bytes.len() {
        match read_tag(bytes, &mut offset, "bool payload")? {
            COLUMN_TAG_NULL => values.push(None),
            COLUMN_TAG_VALUE => {
                let value = read_tag(bytes, &mut offset, "bool value")?;
                match value {
                    0 => values.push(Some(false)),
                    1 => values.push(Some(true)),
                    other => return Err(PhsError::invalid_argument(format!("invalid bool value {other}"))),
                }
            },
            other => return Err(PhsError::invalid_argument(format!("unknown bool tag {other}"))),
        }
    }
    Ok(values)
}

fn decode_i64_values(bytes: &[u8]) -> PhsResult<Vec<Option<i64>>> {
    let mut values = Vec::new();
    let mut offset = 0;
    while offset < bytes.len() {
        match read_tag(bytes, &mut offset, "i64 payload")? {
            COLUMN_TAG_NULL => values.push(None),
            COLUMN_TAG_VALUE => values.push(Some(read_u64_le(bytes, &mut offset, "i64 value")? as i64)),
            other => return Err(PhsError::invalid_argument(format!("unknown i64 tag {other}"))),
        }
    }
    Ok(values)
}

macro_rules! decode_le_values {
    ($name:ident, $type:ty, $label:literal, $width:literal) => {
        fn $name(bytes: &[u8]) -> PhsResult<Vec<Option<$type>>> {
            let mut values = Vec::new();
            let mut offset = 0;
            while offset < bytes.len() {
                match read_tag(bytes, &mut offset, concat!($label, " payload"))? {
                    COLUMN_TAG_NULL => values.push(None),
                    COLUMN_TAG_VALUE => {
                        let value = <$type>::from_le_bytes(read_fixed_le::<$width>(
                            bytes,
                            &mut offset,
                            concat!($label, " value"),
                        )?);
                        values.push(Some(value));
                    },
                    other => return Err(PhsError::invalid_argument(format!("unknown {} tag {other}", $label))),
                }
            }
            Ok(values)
        }
    };
}

decode_le_values!(decode_i8_values, i8, "i8", 1);
decode_le_values!(decode_i16_values, i16, "i16", 2);
decode_le_values!(decode_i32_values, i32, "i32", 4);
decode_le_values!(decode_u8_values, u8, "u8", 1);
decode_le_values!(decode_u16_values, u16, "u16", 2);
decode_le_values!(decode_u32_values, u32, "u32", 4);
decode_le_values!(decode_u64_values, u64, "u64", 8);
decode_le_values!(decode_f32_values, f32, "f32", 4);

fn decode_f64_values(bytes: &[u8]) -> PhsResult<Vec<Option<f64>>> {
    let mut values = Vec::new();
    let mut offset = 0;
    while offset < bytes.len() {
        match read_tag(bytes, &mut offset, "f64 payload")? {
            COLUMN_TAG_NULL => values.push(None),
            COLUMN_TAG_VALUE => values.push(Some(f64::from_bits(read_u64_le(bytes, &mut offset, "f64 value")?))),
            other => return Err(PhsError::invalid_argument(format!("unknown f64 tag {other}"))),
        }
    }
    Ok(values)
}

fn decode_text_values(bytes: &[u8]) -> PhsResult<Vec<Option<String>>> {
    let mut values = Vec::new();
    let mut offset = 0;
    while offset < bytes.len() {
        match read_tag(bytes, &mut offset, "text payload")? {
            COLUMN_TAG_NULL => values.push(None),
            COLUMN_TAG_VALUE => {
                let len: usize = read_u64_le(bytes, &mut offset, "text length")?
                    .try_into()
                    .map_err(|_| PhsError::invalid_argument("text length overflow"))?;
                let end = offset
                    .checked_add(len)
                    .ok_or_else(|| PhsError::invalid_argument("text length overflow"))?;
                if end > bytes.len() {
                    return Err(PhsError::invalid_argument("text value ended early"));
                }
                let value = std::str::from_utf8(&bytes[offset..end])?.to_owned();
                offset = end;
                values.push(Some(value));
            },
            other => return Err(PhsError::invalid_argument(format!("unknown text tag {other}"))),
        }
    }
    Ok(values)
}

fn series_new_from_payload<T, F>(
    name: *const c_char,
    data: *const u8,
    len: usize,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
    decode: F,
) -> c_int
where
    Series: NamedFrom<Vec<Option<T>>, [Option<T>]>,
    F: FnOnce(&[u8]) -> PhsResult<Vec<Option<T>>> + std::panic::UnwindSafe,
{
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let name = unsafe { c_str_to_str(name, "name") }?;
        let bytes = raw_bytes(data, len, "data")?;
        let values = decode(bytes)?;
        *out = series_into_raw(Series::new(name.into(), values));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_new_bool(
    name: *const c_char,
    data: *const u8,
    len: usize,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_new_from_payload(name, data, len, out, err, decode_bool_values)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_new_i64(
    name: *const c_char,
    data: *const u8,
    len: usize,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_new_from_payload(name, data, len, out, err, decode_i64_values)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_new_i8(
    name: *const c_char,
    data: *const u8,
    len: usize,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_new_from_payload(name, data, len, out, err, decode_i8_values)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_new_i16(
    name: *const c_char,
    data: *const u8,
    len: usize,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_new_from_payload(name, data, len, out, err, decode_i16_values)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_new_i32(
    name: *const c_char,
    data: *const u8,
    len: usize,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_new_from_payload(name, data, len, out, err, decode_i32_values)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_new_u8(
    name: *const c_char,
    data: *const u8,
    len: usize,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_new_from_payload(name, data, len, out, err, decode_u8_values)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_new_u16(
    name: *const c_char,
    data: *const u8,
    len: usize,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_new_from_payload(name, data, len, out, err, decode_u16_values)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_new_u32(
    name: *const c_char,
    data: *const u8,
    len: usize,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_new_from_payload(name, data, len, out, err, decode_u32_values)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_new_u64(
    name: *const c_char,
    data: *const u8,
    len: usize,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_new_from_payload(name, data, len, out, err, decode_u64_values)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_new_f32(
    name: *const c_char,
    data: *const u8,
    len: usize,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_new_from_payload(name, data, len, out, err, decode_f32_values)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_new_f64(
    name: *const c_char,
    data: *const u8,
    len: usize,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_new_from_payload(name, data, len, out, err, decode_f64_values)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_new_text(
    name: *const c_char,
    data: *const u8,
    len: usize,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_new_from_payload(name, data, len, out, err, decode_text_values)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_name(
    series: *const phs_series,
    out: *mut *mut phs_bytes,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { series_ref(series) }?;
        *out = bytes_into_raw(handle.value.name().as_str().as_bytes().to_vec());
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_dtype(
    series: *const phs_series,
    out: *mut *mut phs_bytes,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { series_ref(series) }?;
        *out = bytes_into_raw(format!("{:?}", handle.value.dtype()).into_bytes());
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_len(
    series: *const phs_series,
    out: *mut u64,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        let handle = unsafe { series_ref(series) }?;
        *out = handle.value.len() as u64;
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_null_count(
    series: *const phs_series,
    out: *mut u64,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        let handle = unsafe { series_ref(series) }?;
        *out = handle.value.null_count() as u64;
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_n_unique(
    series: *const phs_series,
    out: *mut u64,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        let handle = unsafe { series_ref(series) }?;
        *out = handle.value.n_unique()? as u64;
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_value_counts(
    series: *const phs_series,
    sort: bool,
    parallel: bool,
    name: *const c_char,
    normalize: bool,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { series_ref(series) }?;
        let name = unsafe { c_str_to_str(name, "name") }?;
        *out = dataframe_into_raw(handle.value.value_counts(sort, parallel, name.into(), normalize)?);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_mode(
    series: *const phs_series,
    maintain_order: bool,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_transform(series, out, err, |value| {
        Ok(polars_series_mode(value, maintain_order)?)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_zip_with(
    mask: *const phs_series,
    true_values: *const phs_series,
    false_values: *const phs_series,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let mask_handle = unsafe { series_ref(mask) }?;
        let true_handle = unsafe { series_ref(true_values) }?;
        let false_handle = unsafe { series_ref(false_values) }?;
        let mask_values = mask_handle.value.bool()?;
        *out = series_into_raw(true_handle.value.zip_with(mask_values, &false_handle.value)?);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_gather_every(
    series: *const phs_series,
    step: u64,
    offset: u64,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_transform(series, out, err, |value| {
        if step == 0 {
            return Err(PhsError::invalid_argument(
                "series gather every step must be positive",
            ));
        }
        let step = usize_from_u64(step, "series gather every step")?;
        let offset_idx = idx_size_from_u64(offset, "series gather every offset")?;
        let offset = usize::try_from(offset_idx)
            .map_err(|_| PhsError::invalid_argument("series gather every offset exceeded usize"))?;
        Ok(value.gather_every(step, offset)?)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_pct_change(
    series: *const phs_series,
    periods: i64,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_transform(series, out, err, |value| {
        let periods = Series::new(PlSmallStr::EMPTY, [periods]);
        Ok(polars_series_pct_change(value, &periods)?)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_head(
    series: *const phs_series,
    n: u64,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { series_ref(series) }?;
        *out = series_into_raw(handle.value.head(Some(n as usize)));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_tail(
    series: *const phs_series,
    n: u64,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { series_ref(series) }?;
        *out = series_into_raw(handle.value.tail(Some(n as usize)));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_slice(
    series: *const phs_series,
    offset: i64,
    len: u64,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_transform(series, out, err, |value| {
        Ok(value.slice(offset, usize_from_u64(len, "series slice length")?))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_abs(
    series: *const phs_series,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_transform(series, out, err, |value| Ok(polars_series_abs(value)?))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_round(
    series: *const phs_series,
    decimals: u32,
    mode: c_int,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_transform(series, out, err, |value| {
        Ok(value.round(decimals, round_mode_from_code(mode)?)?)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_floor(
    series: *const phs_series,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_transform(series, out, err, |value| Ok(value.floor()?))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_ceil(
    series: *const phs_series,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_transform(series, out, err, |value| Ok(value.ceil()?))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_diff(
    series: *const phs_series,
    n: i64,
    null_behavior: c_int,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_transform(series, out, err, |value| {
        let null_behavior = null_behavior_from_code(null_behavior)?;
        ensure_diff_period_in_bounds(value, n, null_behavior)?;
        Ok(polars_series_diff(value, n, null_behavior)?)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_interpolate(
    series: *const phs_series,
    method: c_int,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_transform(series, out, err, |value| {
        let method = interpolation_method_from_code(method)?;
        ensure_interpolation_dtype(value, method)?;
        Ok(polars_series_interpolate(value, method))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_is_null(
    series: *const phs_series,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_transform(series, out, err, |value| Ok(value.is_null().into_series()))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_is_not_null(
    series: *const phs_series,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_transform(series, out, err, |value| Ok(value.is_not_null().into_series()))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_is_nan(
    series: *const phs_series,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_transform(series, out, err, |value| Ok(value.is_nan()?.into_series()))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_is_not_nan(
    series: *const phs_series,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_transform(series, out, err, |value| Ok(value.is_not_nan()?.into_series()))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_is_finite(
    series: *const phs_series,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_transform(series, out, err, |value| Ok(value.is_finite()?.into_series()))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_is_infinite(
    series: *const phs_series,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_transform(series, out, err, |value| Ok(value.is_infinite()?.into_series()))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_is_duplicated(
    series: *const phs_series,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_transform(series, out, err, |value| {
        Ok(polars_series_is_duplicated(value)?.into_series())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_is_unique(
    series: *const phs_series,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_transform(series, out, err, |value| {
        Ok(polars_series_is_unique(value)?.into_series())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_is_first_distinct(
    series: *const phs_series,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_transform(series, out, err, |value| {
        Ok(polars_series_is_first_distinct(value)?.into_series())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_is_last_distinct(
    series: *const phs_series,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_transform(series, out, err, |value| {
        Ok(polars_series_is_last_distinct(value)?.into_series())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_filter(
    series: *const phs_series,
    mask: *const phs_series,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let series = unsafe { series_ref(series) }?;
        let mask = unsafe { series_ref(mask) }?;
        let mask = mask.value.bool()?;
        *out = series_into_raw(series.value.filter(mask)?);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_take(
    series: *const phs_series,
    indices: *const u64,
    len: usize,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let series = unsafe { series_ref(series) }?;
        let indices = unsafe { raw_u64_slice(indices, len, "indices") }?;
        let indices = idx_ca_from_u64_slice(indices, "series take index")?;
        *out = series_into_raw(series.value.take(&indices)?);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_fill_null(
    series: *const phs_series,
    strategy: c_int,
    has_limit: bool,
    limit: u64,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_transform(series, out, err, move |value| {
        let strategy = fill_null_strategy_from_code(strategy, has_limit, limit)?;
        Ok(value.fill_null(strategy)?)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_to_frame(
    series: *const phs_series,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { series_ref(series) }?;
        *out = dataframe_into_raw(handle.value.clone().into_frame());
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_rename(
    series: *const phs_series,
    name: *const c_char,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { series_ref(series) }?;
        let name = unsafe { c_str_to_str(name, "name") }?;
        *out = series_into_raw(handle.value.clone().with_name(name.into()));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_cast(
    series: *const phs_series,
    dtype_code: c_int,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_transform(series, out, err, |value| {
        let dtype = dtype_from_code(dtype_code)?;
        Ok(value.cast(&dtype)?)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_sort(
    series: *const phs_series,
    descending: bool,
    nulls_last: bool,
    multithreaded: bool,
    maintain_order: bool,
    has_limit: bool,
    limit: u64,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_transform(series, out, err, |value| {
        let mut options = SortOptions::default()
            .with_order_descending(descending)
            .with_nulls_last(nulls_last)
            .with_multithreaded(multithreaded)
            .with_maintain_order(maintain_order);
        if has_limit {
            options.limit = Some(idx_size_from_u64(limit, "series sort limit")?);
        }
        Ok(value.sort(options)?)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_unique(
    series: *const phs_series,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_transform(series, out, err, |value| Ok(value.unique()?))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_unique_counts(
    series: *const phs_series,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_transform(series, out, err, |value| Ok(polars_series_unique_counts(value)?))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_unique_stable(
    series: *const phs_series,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_transform(series, out, err, |value| Ok(value.unique_stable()?))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_arg_sort(
    series: *const phs_series,
    descending: bool,
    nulls_last: bool,
    multithreaded: bool,
    maintain_order: bool,
    has_limit: bool,
    limit: u64,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_transform(series, out, err, |value| {
        let mut options = SortOptions::default()
            .with_order_descending(descending)
            .with_nulls_last(nulls_last)
            .with_multithreaded(multithreaded)
            .with_maintain_order(maintain_order);
        if has_limit {
            options.limit = Some(idx_size_from_u64(limit, "series arg sort limit")?);
        }
        Ok(value.arg_sort(options).into_series())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_arg_unique(
    series: *const phs_series,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_transform(series, out, err, |value| {
        Ok(value.arg_unique()?.into_series())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_rank(
    series: *const phs_series,
    method: c_int,
    descending: bool,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_transform(series, out, err, |value| {
        let method = rank_method_from_code(method)?;
        Ok(value.rank(RankOptions { method, descending }, None))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_reverse(
    series: *const phs_series,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_transform(series, out, err, |value| Ok(value.reverse()))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_drop_nulls(
    series: *const phs_series,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_transform(series, out, err, |value| Ok(value.drop_nulls()))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_shift(
    series: *const phs_series,
    periods: i64,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_transform(series, out, err, |value| Ok(value.shift(periods)))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_append(
    left: *const phs_series,
    right: *const phs_series,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let left = unsafe { series_ref(left) }?;
        let right = unsafe { series_ref(right) }?;
        let mut output = left.value.clone();
        output.append(&right.value)?;
        *out = series_into_raw(output);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_binary_op(
    left: *const phs_series,
    right: *const phs_series,
    op: c_int,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let left = unsafe { series_ref(left) }?;
        let right = unsafe { series_ref(right) }?;
        *out = series_into_raw(series_binary_op_from_code(&left.value, &right.value, op)?);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_stat(
    series: *const phs_series,
    op: c_int,
    ddof: u8,
    has_value_out: *mut bool,
    value_out: *mut f64,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let has_value_out = unsafe { required_mut(has_value_out, "has_value_out") }?;
        let value_out = unsafe { required_mut(value_out, "value_out") }?;
        *has_value_out = false;
        *value_out = 0.0;
        let handle = unsafe { series_ref(series) }?;
        if let Some(value) = series_stat_from_code(&handle.value, op, ddof)? {
            *has_value_out = true;
            *value_out = value;
        }
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_values_bool(
    series: *const phs_series,
    out: *mut *mut phs_bytes,
    err: *mut *mut phs_error,
) -> c_int {
    series_values_bytes(series, out, err, encode_bool_series)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_values_i64(
    series: *const phs_series,
    out: *mut *mut phs_bytes,
    err: *mut *mut phs_error,
) -> c_int {
    series_values_bytes(series, out, err, encode_i64_series)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_values_i8(
    series: *const phs_series,
    out: *mut *mut phs_bytes,
    err: *mut *mut phs_error,
) -> c_int {
    series_values_bytes(series, out, err, encode_i8_series)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_values_i16(
    series: *const phs_series,
    out: *mut *mut phs_bytes,
    err: *mut *mut phs_error,
) -> c_int {
    series_values_bytes(series, out, err, encode_i16_series)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_values_i32(
    series: *const phs_series,
    out: *mut *mut phs_bytes,
    err: *mut *mut phs_error,
) -> c_int {
    series_values_bytes(series, out, err, encode_i32_series)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_values_u8(
    series: *const phs_series,
    out: *mut *mut phs_bytes,
    err: *mut *mut phs_error,
) -> c_int {
    series_values_bytes(series, out, err, encode_u8_series)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_values_u16(
    series: *const phs_series,
    out: *mut *mut phs_bytes,
    err: *mut *mut phs_error,
) -> c_int {
    series_values_bytes(series, out, err, encode_u16_series)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_values_u32(
    series: *const phs_series,
    out: *mut *mut phs_bytes,
    err: *mut *mut phs_error,
) -> c_int {
    series_values_bytes(series, out, err, encode_u32_series)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_values_u64(
    series: *const phs_series,
    out: *mut *mut phs_bytes,
    err: *mut *mut phs_error,
) -> c_int {
    series_values_bytes(series, out, err, encode_u64_series)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_values_f32(
    series: *const phs_series,
    out: *mut *mut phs_bytes,
    err: *mut *mut phs_error,
) -> c_int {
    series_values_bytes(series, out, err, encode_f32_series)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_values_f64(
    series: *const phs_series,
    out: *mut *mut phs_bytes,
    err: *mut *mut phs_error,
) -> c_int {
    series_values_bytes(series, out, err, encode_f64_series)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_series_values_text(
    series: *const phs_series,
    out: *mut *mut phs_bytes,
    err: *mut *mut phs_error,
) -> c_int {
    series_values_bytes(series, out, err, encode_text_series)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bytes::{phs_bytes_data, phs_bytes_free, phs_bytes_len};
    use crate::dataframe::{phs_dataframe_column, phs_dataframe_new, phs_read_csv};
    use crate::error::{PHS_INVALID_ARGUMENT, PHS_OK, PHS_POLARS_ERROR, phs_error_free, phs_error_message};
    use crate::handles::{phs_dataframe_free, phs_series_free};
    use std::ffi::CStr;
    use std::path::PathBuf;

    fn values_fixture_path() -> std::ffi::CString {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("test")
            .join("data")
            .join("values.csv");
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

    fn read_age_series() -> *mut phs_series {
        let dataframe = read_values_dataframe();
        let name = std::ffi::CString::new("age").unwrap();
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();
        let status = unsafe { phs_dataframe_column(dataframe, name.as_ptr(), &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert!(err.is_null());
        assert!(!out.is_null());
        unsafe { phs_dataframe_free(dataframe) };
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

    unsafe fn take_bool_values(raw: *mut phs_series) -> Vec<Option<bool>> {
        assert!(!raw.is_null());
        let series = unsafe { series_ref(raw) }.unwrap();
        let values = series.value.bool().unwrap().into_iter().collect();
        unsafe { phs_series_free(raw) };
        values
    }

    unsafe fn take_u32_values(raw: *mut phs_series) -> Vec<Option<u32>> {
        assert!(!raw.is_null());
        let series = unsafe { series_ref(raw) }.unwrap();
        let values = series.value.u32().unwrap().into_iter().collect();
        unsafe { phs_series_free(raw) };
        values
    }

    unsafe fn take_i64_values(raw: *mut phs_series) -> Vec<Option<i64>> {
        assert!(!raw.is_null());
        let series = unsafe { series_ref(raw) }.unwrap();
        let values = series.value.i64().unwrap().into_iter().collect();
        unsafe { phs_series_free(raw) };
        values
    }

    unsafe fn take_i32_values(raw: *mut phs_series) -> Vec<Option<i32>> {
        assert!(!raw.is_null());
        let series = unsafe { series_ref(raw) }.unwrap();
        let values = series.value.i32().unwrap().into_iter().collect();
        unsafe { phs_series_free(raw) };
        values
    }

    unsafe fn take_i16_values(raw: *mut phs_series) -> Vec<Option<i16>> {
        assert!(!raw.is_null());
        let series = unsafe { series_ref(raw) }.unwrap();
        let values = series.value.i16().unwrap().into_iter().collect();
        unsafe { phs_series_free(raw) };
        values
    }

    unsafe fn take_f64_values(raw: *mut phs_series) -> Vec<Option<f64>> {
        assert!(!raw.is_null());
        let series = unsafe { series_ref(raw) }.unwrap();
        let values = series.value.f64().unwrap().into_iter().collect();
        unsafe { phs_series_free(raw) };
        values
    }

    fn encoded_i64(values: &[Option<i64>]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for value in values {
            match value {
                None => bytes.push(0),
                Some(value) => {
                    bytes.push(1);
                    bytes.extend_from_slice(&value.to_le_bytes());
                },
            }
        }
        bytes
    }

    fn encoded_text(values: &[Option<&str>]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for value in values {
            match value {
                None => bytes.push(0),
                Some(value) => {
                    bytes.push(1);
                    bytes.extend_from_slice(&(value.len() as u64).to_le_bytes());
                    bytes.extend_from_slice(value.as_bytes());
                },
            }
        }
        bytes
    }

    #[test]
    fn series_constructors_build_typed_series() {
        let name = std::ffi::CString::new("age").unwrap();
        let bytes = encoded_i64(&[Some(34), None, Some(29)]);
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();
        let status = unsafe { phs_series_new_i64(name.as_ptr(), bytes.as_ptr(), bytes.len(), &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let series = unsafe { series_ref(out) }.unwrap();
        assert_eq!(series.value.name().as_str(), "age");
        assert_eq!(series.value.dtype(), &DataType::Int64);
        assert_eq!(series.value.len(), 3);
        assert_eq!(series.value.null_count(), 1);
        unsafe { phs_series_free(out) };
    }

    #[test]
    fn dataframe_constructor_builds_frame_from_series() {
        let age_name = std::ffi::CString::new("age").unwrap();
        let age_bytes = encoded_i64(&[Some(34), None]);
        let mut age = ptr::null_mut();
        let mut err = ptr::null_mut();
        let status = unsafe { phs_series_new_i64(age_name.as_ptr(), age_bytes.as_ptr(), age_bytes.len(), &mut age, &mut err) };
        assert_eq!(status, PHS_OK);

        let name_name = std::ffi::CString::new("name").unwrap();
        let name_bytes = encoded_text(&[Some("Alice"), Some("Bob")]);
        let mut name = ptr::null_mut();
        let status = unsafe { phs_series_new_text(name_name.as_ptr(), name_bytes.as_ptr(), name_bytes.len(), &mut name, &mut err) };
        assert_eq!(status, PHS_OK);

        let series = [name as *const phs_series, age as *const phs_series];
        let mut dataframe = ptr::null_mut();
        let status = unsafe { phs_dataframe_new(series.as_ptr(), series.len(), &mut dataframe, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { crate::handles::dataframe_ref(dataframe) }.unwrap().value.shape(), (2, 2));
        unsafe {
            phs_dataframe_free(dataframe);
            phs_series_free(name);
            phs_series_free(age);
        }
    }

    #[test]
    fn dataframe_constructor_reports_duplicate_names() {
        let first_name = std::ffi::CString::new("value").unwrap();
        let first_bytes = encoded_i64(&[Some(1), Some(2)]);
        let mut first = ptr::null_mut();
        let mut err = ptr::null_mut();
        let status = unsafe { phs_series_new_i64(first_name.as_ptr(), first_bytes.as_ptr(), first_bytes.len(), &mut first, &mut err) };
        assert_eq!(status, PHS_OK);

        let second_name = std::ffi::CString::new("value").unwrap();
        let second_bytes = encoded_i64(&[Some(3), Some(4)]);
        let mut second = ptr::null_mut();
        let status = unsafe { phs_series_new_i64(second_name.as_ptr(), second_bytes.as_ptr(), second_bytes.len(), &mut second, &mut err) };
        assert_eq!(status, PHS_OK);

        let series = [first as *const phs_series, second as *const phs_series];
        let mut dataframe = ptr::null_mut();
        let status = unsafe { phs_dataframe_new(series.as_ptr(), series.len(), &mut dataframe, &mut err) };
        assert_eq!(status, crate::error::PHS_POLARS_ERROR);
        assert!(dataframe.is_null());
        assert!(!err.is_null());
        unsafe {
            phs_error_free(err);
            phs_series_free(second);
            phs_series_free(first);
        }
    }

    #[test]
    fn series_metadata_reports_name_dtype_len_and_null_count() {
        let series = read_age_series();
        let mut bytes_out = ptr::null_mut();
        let mut err = ptr::null_mut();
        let status = unsafe { phs_series_name(series, &mut bytes_out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { take_raw_bytes(bytes_out) }, b"age");

        let status = unsafe { phs_series_dtype(series, &mut bytes_out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { take_raw_bytes(bytes_out) }, b"Int64");

        let mut len = 0;
        let status = unsafe { phs_series_len(series, &mut len, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(len, 3);

        let mut null_count = 0;
        let status = unsafe { phs_series_null_count(series, &mut null_count, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(null_count, 1);
        unsafe { phs_series_free(series) };
    }

    #[test]
    fn series_values_i64_encode_values_and_nulls() {
        let series = read_age_series();
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();
        let status = unsafe { phs_series_values_i64(series, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let mut expected = vec![1];
        expected.extend_from_slice(&34_i64.to_le_bytes());
        expected.push(0);
        expected.push(1);
        expected.extend_from_slice(&29_i64.to_le_bytes());
        assert_eq!(unsafe { take_raw_bytes(out) }, expected);
        unsafe { phs_series_free(series) };
    }

    #[test]
    fn series_head_tail_and_to_frame_return_owned_handles() {
        let series = read_age_series();
        let mut series_out = ptr::null_mut();
        let mut err = ptr::null_mut();
        let status = unsafe { phs_series_head(series, 2, &mut series_out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { series_ref(series_out) }.unwrap().value.len(), 2);
        unsafe { phs_series_free(series_out) };

        let status = unsafe { phs_series_tail(series, 1, &mut series_out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { series_ref(series_out) }.unwrap().value.len(), 1);
        unsafe { phs_series_free(series_out) };

        let mut dataframe_out = ptr::null_mut();
        let status = unsafe { phs_series_to_frame(series, &mut dataframe_out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { crate::handles::dataframe_ref(dataframe_out) }.unwrap().value.shape(), (3, 1));
        unsafe {
            phs_dataframe_free(dataframe_out);
            phs_series_free(series);
        }
    }

    #[test]
    fn series_rename_cast_sort_reverse_and_drop_nulls_work() {
        let series = read_age_series();
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();

        let name = std::ffi::CString::new("age_years").unwrap();
        let status = unsafe { phs_series_rename(series, name.as_ptr(), &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { series_ref(out) }.unwrap().value.name().as_str(), "age_years");
        unsafe { phs_series_free(out) };

        let status = unsafe { phs_series_cast(series, 10, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { series_ref(out) }.unwrap().value.dtype(), &DataType::Float64);
        unsafe { phs_series_free(out) };

        let status = unsafe { phs_series_sort(series, true, true, true, false, false, 0, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { series_ref(out) }.unwrap().value.len(), 3);
        unsafe { phs_series_free(out) };

        let status = unsafe { phs_series_reverse(series, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { series_ref(out) }.unwrap().value.len(), 3);
        unsafe { phs_series_free(out) };

        let status = unsafe { phs_series_drop_nulls(series, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { series_ref(out) }.unwrap().value.len(), 2);
        unsafe {
            phs_series_free(out);
            phs_series_free(series);
        }
    }

    #[test]
    fn series_sort_rejects_limit_overflow() {
        let series = read_age_series();
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();
        let overflowing_limit = u64::from(u32::MAX) + 1;

        let status = unsafe { phs_series_sort(series, false, false, true, false, true, overflowing_limit, &mut out, &mut err) };

        assert_eq!(status, PHS_INVALID_ARGUMENT);
        assert!(out.is_null());
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "series sort limit exceeds Polars index size");
        unsafe { phs_series_free(series) };
    }

    #[test]
    fn series_take_handles_empty_and_reordered_indices() {
        let series = read_age_series();
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();
        let indices = [2_u64, 0, 1, 1];

        let status = unsafe { phs_series_take(series, indices.as_ptr(), indices.len(), &mut out, &mut err) };

        assert_eq!(status, PHS_OK);
        assert!(err.is_null());
        let taken = unsafe { series_ref(out) }.unwrap();
        assert_eq!(taken.value.len(), 4);
        assert_eq!(taken.value.null_count(), 2);
        unsafe { phs_series_free(out) };

        let status = unsafe { phs_series_take(series, ptr::null(), 0, &mut out, &mut err) };

        assert_eq!(status, PHS_OK);
        assert!(err.is_null());
        assert_eq!(unsafe { series_ref(out) }.unwrap().value.len(), 0);
        unsafe {
            phs_series_free(out);
            phs_series_free(series);
        }
    }

    #[test]
    fn series_take_rejects_null_indices_pointer_with_positive_length() {
        let series = read_age_series();
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();

        let status = unsafe { phs_series_take(series, ptr::null(), 1, &mut out, &mut err) };

        assert_eq!(status, PHS_INVALID_ARGUMENT);
        assert!(out.is_null());
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "indices pointer was null");
        unsafe { phs_series_free(series) };
    }

    #[test]
    fn series_take_rejects_index_overflow() {
        let series = read_age_series();
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();
        let indices = [u64::from(u32::MAX) + 1];

        let status = unsafe { phs_series_take(series, indices.as_ptr(), indices.len(), &mut out, &mut err) };

        assert_eq!(status, PHS_INVALID_ARGUMENT);
        assert!(out.is_null());
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "series take index exceeds Polars index size");
        unsafe { phs_series_free(series) };
    }

    #[test]
    fn series_stat_sum_min_max_return_nullable_doubles() {
        let series = read_age_series();
        let mut err = ptr::null_mut();
        let mut has_value = false;
        let mut value = 0.0;

        let status = unsafe { phs_series_stat(series, 3, 0, &mut has_value, &mut value, &mut err) };
        assert_eq!(status, PHS_OK);
        assert!(has_value);
        assert_eq!(value, 63.0);

        let status = unsafe { phs_series_stat(series, 4, 0, &mut has_value, &mut value, &mut err) };
        assert_eq!(status, PHS_OK);
        assert!(has_value);
        assert_eq!(value, 29.0);

        let status = unsafe { phs_series_stat(series, 5, 0, &mut has_value, &mut value, &mut err) };
        assert_eq!(status, PHS_OK);
        assert!(has_value);
        assert_eq!(value, 34.0);
        unsafe { phs_series_free(series) };
    }

    #[test]
    fn series_median_and_n_unique_return_expected_values() {
        let values = series_into_raw(Series::new(
            "value".into(),
            &[Some(9.5_f64), None, Some(8.25)],
        ));
        let text = series_into_raw(Series::new("text".into(), &[Some("a"), Some("b"), Some("a"), None]));
        let flag = series_into_raw(Series::new("flag".into(), &[Some(true), Some(false), None, Some(true)]));
        let all_null = series_into_raw(Series::new("all_null".into(), &[None::<f64>, None]));
        let empty = series_into_raw(Series::new("empty".into(), Vec::<Option<f64>>::new()));
        let mut err = ptr::null_mut();
        let mut has_value = false;
        let mut value = 0.0;
        let mut count = 0_u64;

        let status = unsafe { phs_series_stat(values, 6, 0, &mut has_value, &mut value, &mut err) };
        assert_eq!(status, PHS_OK);
        assert!(has_value);
        assert_eq!(value, 8.875);

        let status = unsafe { phs_series_stat(text, 6, 0, &mut has_value, &mut value, &mut err) };
        assert_eq!(status, PHS_OK);
        assert!(!has_value);

        let status = unsafe { phs_series_stat(all_null, 6, 0, &mut has_value, &mut value, &mut err) };
        assert_eq!(status, PHS_OK);
        assert!(!has_value);

        let status = unsafe { phs_series_stat(empty, 6, 0, &mut has_value, &mut value, &mut err) };
        assert_eq!(status, PHS_OK);
        assert!(!has_value);

        let status = unsafe { phs_series_n_unique(values, &mut count, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(count, 3);

        let status = unsafe { phs_series_n_unique(text, &mut count, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(count, 3);

        let status = unsafe { phs_series_n_unique(flag, &mut count, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(count, 3);

        let status = unsafe { phs_series_n_unique(all_null, &mut count, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(count, 1);

        let status = unsafe { phs_series_n_unique(empty, &mut count, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(count, 0);

        unsafe {
            phs_series_free(values);
            phs_series_free(text);
            phs_series_free(flag);
            phs_series_free(all_null);
            phs_series_free(empty);
        }
    }

    #[test]
    fn series_arg_unique_returns_first_indexes() {
        let values = series_into_raw(Series::new(
            "value".into(),
            &[Some(1_i64), Some(2), Some(1), None, Some(3), None],
        ));
        let text = series_into_raw(Series::new("text".into(), &[Some("a"), Some("b"), Some("a"), None]));
        let flag = series_into_raw(Series::new("flag".into(), &[Some(true), Some(false), None, Some(true)]));
        let all_null = series_into_raw(Series::new("all_null".into(), &[None::<f64>, None]));
        let empty = series_into_raw(Series::new("empty".into(), Vec::<Option<f64>>::new()));
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();

        let status = unsafe { phs_series_arg_unique(values, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { series_ref(out) }.unwrap().value.u32().unwrap().into_iter().collect::<Vec<_>>(),
            vec![Some(0), Some(1), Some(3), Some(4)]
        );
        unsafe { phs_series_free(out) };

        let status = unsafe { phs_series_arg_unique(text, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { series_ref(out) }.unwrap().value.u32().unwrap().into_iter().collect::<Vec<_>>(),
            vec![Some(0), Some(1), Some(3)]
        );
        unsafe { phs_series_free(out) };

        let status = unsafe { phs_series_arg_unique(flag, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { series_ref(out) }.unwrap().value.u32().unwrap().into_iter().collect::<Vec<_>>(),
            vec![Some(0), Some(1), Some(2)]
        );
        unsafe { phs_series_free(out) };

        let status = unsafe { phs_series_arg_unique(all_null, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { series_ref(out) }.unwrap().value.u32().unwrap().into_iter().collect::<Vec<_>>(),
            vec![Some(0)]
        );
        unsafe { phs_series_free(out) };

        let status = unsafe { phs_series_arg_unique(empty, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { series_ref(out) }.unwrap().value.u32().unwrap().into_iter().collect::<Vec<_>>(),
            Vec::<Option<u32>>::new()
        );
        unsafe { phs_series_free(out) };

        unsafe {
            phs_series_free(values);
            phs_series_free(text);
            phs_series_free(flag);
            phs_series_free(all_null);
            phs_series_free(empty);
        }
    }

    #[test]
    fn series_arg_sort_returns_sort_indexes() {
        let values = series_into_raw(Series::new("value".into(), &[Some(3_i64), None, Some(1), Some(2)]));
        let ties = series_into_raw(Series::new("tie".into(), &[Some(2_i64), Some(1), Some(2), Some(1)]));
        let text = series_into_raw(Series::new("text".into(), &[Some("b"), Some("a"), None, Some("a")]));
        let empty = series_into_raw(Series::new("empty".into(), Vec::<Option<i64>>::new()));
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();

        let status = unsafe { phs_series_arg_sort(values, false, false, true, false, false, 0, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { take_u32_values(out) },
            vec![Some(1), Some(2), Some(3), Some(0)]
        );

        let status = unsafe { phs_series_arg_sort(values, false, true, true, false, false, 0, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { take_u32_values(out) },
            vec![Some(2), Some(3), Some(0), Some(1)]
        );

        let status = unsafe { phs_series_arg_sort(values, true, true, true, false, false, 0, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { take_u32_values(out) },
            vec![Some(0), Some(3), Some(2), Some(1)]
        );

        let status = unsafe { phs_series_arg_sort(ties, false, false, true, true, false, 0, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { take_u32_values(out) },
            vec![Some(1), Some(3), Some(0), Some(2)]
        );

        let status = unsafe { phs_series_arg_sort(text, false, true, true, false, false, 0, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { take_u32_values(out) },
            vec![Some(1), Some(3), Some(0), Some(2)]
        );

        let status = unsafe { phs_series_arg_sort(empty, false, false, true, false, false, 0, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { take_u32_values(out) }, Vec::<Option<u32>>::new());

        let status = unsafe {
            phs_series_arg_sort(
                values,
                false,
                false,
                true,
                false,
                true,
                u64::from(u32::MAX) + 1,
                &mut out,
                &mut err,
            )
        };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        assert_eq!(
            unsafe { take_error_message(err) },
            "series arg sort limit exceeds Polars index size"
        );

        unsafe {
            phs_series_free(values);
            phs_series_free(ties);
            phs_series_free(text);
            phs_series_free(empty);
        }
    }

    #[test]
    fn series_rank_returns_deterministic_ranks() {
        let values = series_into_raw(Series::new(
            "rank".into(),
            &[Some(1_i64), Some(2), Some(3), Some(2), Some(2), Some(3), Some(0)],
        ));
        let null_values = series_into_raw(Series::new(
            "rank_nulls".into(),
            &[Some(1_i64), Some(2), Some(3), Some(2), None, None, Some(0)],
        ));
        let descending_values =
            series_into_raw(Series::new("rank_desc".into(), &[None, Some(1_i64), Some(1), Some(5), None]));
        let text_values = series_into_raw(Series::new("rank_text".into(), &[Some("b"), Some("a"), None, Some("b")]));
        let all_null = series_into_raw(Series::new("rank_all_null".into(), &[None::<u32>, None, None]));
        let empty = series_into_raw(Series::new("rank_empty".into(), Vec::<u32>::new()));
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();

        let status = unsafe { phs_series_rank(values, 3, false, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { take_u32_values(out) },
            vec![Some(2), Some(3), Some(4), Some(3), Some(3), Some(4), Some(1)]
        );

        let status = unsafe { phs_series_rank(values, 1, false, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { take_u32_values(out) },
            vec![Some(2), Some(3), Some(6), Some(3), Some(3), Some(6), Some(1)]
        );

        let status = unsafe { phs_series_rank(values, 2, false, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { take_u32_values(out) },
            vec![Some(2), Some(5), Some(7), Some(5), Some(5), Some(7), Some(1)]
        );

        let status = unsafe { phs_series_rank(values, 4, false, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { take_u32_values(out) },
            vec![Some(2), Some(3), Some(6), Some(4), Some(5), Some(7), Some(1)]
        );

        let status = unsafe { phs_series_rank(values, 0, false, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { take_f64_values(out) },
            vec![Some(2.0), Some(4.0), Some(6.5), Some(4.0), Some(4.0), Some(6.5), Some(1.0)]
        );

        let status = unsafe { phs_series_rank(null_values, 0, false, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { take_f64_values(out) },
            vec![Some(2.0), Some(3.5), Some(5.0), Some(3.5), None, None, Some(1.0)]
        );

        let status = unsafe { phs_series_rank(descending_values, 3, true, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { take_u32_values(out) }, vec![None, Some(2), Some(2), Some(1), None]);

        let status = unsafe { phs_series_rank(text_values, 3, false, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { take_u32_values(out) }, vec![Some(2), Some(1), None, Some(2)]);

        let status = unsafe { phs_series_rank(all_null, 3, false, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { take_u32_values(out) }, vec![None, None, None]);

        let status = unsafe { phs_series_rank(empty, 0, false, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { take_f64_values(out) }, Vec::<Option<f64>>::new());

        let status = unsafe { phs_series_rank(empty, 2, false, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { take_u32_values(out) }, Vec::<Option<u32>>::new());

        let status = unsafe { phs_series_rank(values, 99, false, &mut out, &mut err) };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        assert_eq!(unsafe { take_error_message(err) }, "unknown rank method code 99");

        unsafe {
            phs_series_free(values);
            phs_series_free(null_values);
            phs_series_free(descending_values);
            phs_series_free(text_values);
            phs_series_free(all_null);
            phs_series_free(empty);
        }
    }

    #[test]
    fn series_unique_counts_returns_counts_in_first_seen_order() {
        let values = series_into_raw(Series::new(
            "value".into(),
            &[Some(1_i64), Some(2), Some(1), None, Some(3), None],
        ));
        let text = series_into_raw(Series::new(
            "text".into(),
            &[Some("b"), Some("a"), None, Some("b"), Some("a")],
        ));
        let flag = series_into_raw(Series::new(
            "flag".into(),
            &[Some(true), Some(false), None, Some(true), Some(false)],
        ));
        let all_null = series_into_raw(Series::new("all_null".into(), &[None::<f64>, None, None]));
        let empty = series_into_raw(Series::new("empty".into(), Vec::<Option<f64>>::new()));
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();

        let status = unsafe { phs_series_unique_counts(values, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { take_u32_values(out) },
            vec![Some(2), Some(1), Some(2), Some(1)]
        );

        let status = unsafe { phs_series_unique_counts(text, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { take_u32_values(out) }, vec![Some(2), Some(2), Some(1)]);

        let status = unsafe { phs_series_unique_counts(flag, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { take_u32_values(out) }, vec![Some(2), Some(2), Some(1)]);

        let status = unsafe { phs_series_unique_counts(all_null, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { take_u32_values(out) }, vec![Some(3)]);

        let status = unsafe { phs_series_unique_counts(empty, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { take_u32_values(out) }, Vec::<Option<u32>>::new());

        unsafe {
            phs_series_free(values);
            phs_series_free(text);
            phs_series_free(flag);
            phs_series_free(all_null);
            phs_series_free(empty);
        }
    }

    #[test]
    fn series_mode_returns_most_frequent_values() {
        let values = series_into_raw(Series::new(
            "value".into(),
            &[Some(1_i64), Some(2), Some(2), Some(3), Some(3)],
        ));
        let text = series_into_raw(Series::new("text".into(), &[Some("a"), Some("b"), Some("a"), Some("c")]));
        let nullable = series_into_raw(Series::new("nullable".into(), &[None, Some(1_i64), None, Some(2)]));
        let empty = series_into_raw(Series::new("empty".into(), Vec::<Option<i64>>::new()));
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();

        let status = unsafe { phs_series_mode(values, true, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { take_i64_values(out) }, vec![Some(2), Some(3)]);

        let status = unsafe { phs_series_mode(text, true, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { series_ref(out) }.unwrap().value.str().unwrap().into_iter().collect::<Vec<_>>(),
            vec![Some("a")]
        );
        unsafe { phs_series_free(out) };

        let status = unsafe { phs_series_mode(nullable, true, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { take_i64_values(out) }, vec![None]);

        let status = unsafe { phs_series_mode(empty, true, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { take_i64_values(out) }, Vec::<Option<i64>>::new());

        unsafe {
            phs_series_free(values);
            phs_series_free(text);
            phs_series_free(nullable);
            phs_series_free(empty);
        }
    }

    #[test]
    fn series_zip_with_selects_with_boolean_mask() {
        let mask = series_into_raw(Series::new(
            "mask".into(),
            &[Some(true), Some(false), None, Some(true)],
        ));
        let true_values = series_into_raw(Series::new(
            "true".into(),
            &[Some(10_i64), Some(10), Some(10), Some(10)],
        ));
        let false_values = series_into_raw(Series::new(
            "false".into(),
            &[Some(1_i64), Some(2), Some(3), Some(4)],
        ));
        let broadcast_mask = series_into_raw(Series::new(
            "mask".into(),
            &[Some(true), Some(false), Some(true), Some(false)],
        ));
        let broadcast_true = series_into_raw(Series::new("true".into(), &[Some(99_i64)]));
        let numeric_true = series_into_raw(Series::new(
            "true".into(),
            &[Some(1_i64), Some(2)],
        ));
        let numeric_false = series_into_raw(Series::new(
            "false".into(),
            &[Some(0.5_f64), Some(0.25)],
        ));
        let text_mask = series_into_raw(Series::new("mask".into(), &[Some(false), Some(true), None]));
        let text_true = series_into_raw(Series::new(
            "true".into(),
            &[Some("left"), Some("yes"), Some("skip")],
        ));
        let text_false = series_into_raw(Series::new(
            "false".into(),
            &[Some("right"), Some("no"), Some("fallback")],
        ));
        let short_mask = series_into_raw(Series::new("short_mask".into(), &[Some(true), Some(false)]));
        let non_bool_mask = series_into_raw(Series::new(
            "not_mask".into(),
            &[Some(1_i64), Some(0), Some(1), Some(0)],
        ));
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();

        let status = unsafe {
            phs_series_zip_with(mask, true_values, false_values, &mut out, &mut err)
        };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { take_i64_values(out) },
            vec![Some(10), Some(2), Some(3), Some(10)]
        );

        let status = unsafe {
            phs_series_zip_with(broadcast_mask, broadcast_true, false_values, &mut out, &mut err)
        };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { take_i64_values(out) },
            vec![Some(99), Some(2), Some(99), Some(4)]
        );

        let status = unsafe {
            phs_series_zip_with(short_mask, numeric_true, numeric_false, &mut out, &mut err)
        };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { take_f64_values(out) }, vec![Some(1.0), Some(0.25)]);

        let status = unsafe {
            phs_series_zip_with(text_mask, text_true, text_false, &mut out, &mut err)
        };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { series_ref(out) }
                .unwrap()
                .value
                .str()
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>(),
            vec![Some("right"), Some("yes"), Some("fallback")]
        );
        unsafe { phs_series_free(out) };

        let status = unsafe {
            phs_series_zip_with(short_mask, true_values, false_values, &mut out, &mut err)
        };
        assert_eq!(status, PHS_POLARS_ERROR);
        let message = unsafe { take_error_message(err) };
        assert!(message.contains("zip_with"));

        let status = unsafe {
            phs_series_zip_with(non_bool_mask, true_values, false_values, &mut out, &mut err)
        };
        assert_eq!(status, PHS_POLARS_ERROR);
        let message = unsafe { take_error_message(err) };
        assert!(message.contains("Boolean"));

        unsafe {
            phs_series_free(mask);
            phs_series_free(true_values);
            phs_series_free(false_values);
            phs_series_free(broadcast_mask);
            phs_series_free(broadcast_true);
            phs_series_free(numeric_true);
            phs_series_free(numeric_false);
            phs_series_free(text_mask);
            phs_series_free(text_true);
            phs_series_free(text_false);
            phs_series_free(short_mask);
            phs_series_free(non_bool_mask);
        }
    }

    #[test]
    fn series_gather_every_returns_strided_values() {
        let numeric = series_into_raw(Series::new(
            "value".into(),
            &[0_i64, 1, 2, 3, 4],
        ));
        let text = series_into_raw(Series::new(
            "text".into(),
            &[Some("a"), None, Some("c"), Some("d"), Some("e")],
        ));
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();

        let status = unsafe { phs_series_gather_every(numeric, 2, 0, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { take_i64_values(out) }, vec![Some(0), Some(2), Some(4)]);

        let status = unsafe { phs_series_gather_every(text, 2, 1, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { series_ref(out) }
                .unwrap()
                .value
                .str()
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>(),
            vec![None, Some("d")]
        );
        unsafe { phs_series_free(out) };

        let status = unsafe { phs_series_gather_every(numeric, 10, 0, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { take_i64_values(out) }, vec![Some(0)]);

        let status = unsafe { phs_series_gather_every(text, 2, 5, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { series_ref(out) }
                .unwrap()
                .value
                .str()
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>(),
            Vec::<Option<&str>>::new()
        );
        unsafe { phs_series_free(out) };

        let status = unsafe { phs_series_gather_every(numeric, 0, 0, &mut out, &mut err) };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        assert_eq!(
            unsafe { take_error_message(err) },
            "series gather every step must be positive"
        );

        unsafe {
            phs_series_free(numeric);
            phs_series_free(text);
        }
    }

    #[test]
    fn series_pct_change_returns_relative_change() {
        let base_values = series_into_raw(Series::new("value".into(), &[10_i64, 15, 30]));
        let zero_denominator = series_into_raw(Series::new("zero".into(), &[0_i64, 0, 1, -1]));
        let nullable = series_into_raw(Series::new(
            "nullable".into(),
            &[Some(10_i64), None, Some(15), Some(30)],
        ));
        let floats = series_into_raw(Series::new("float".into(), &[1.0_f32, 2.0, 4.0]));
        let text = series_into_raw(Series::new("text".into(), ["10", "15", "30"]));
        let invalid_text = series_into_raw(Series::new("bad_text".into(), ["a", "b"]));
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();

        let status = unsafe { phs_series_pct_change(base_values, 1, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { take_f64_values(out) }, vec![None, Some(0.5), Some(1.0)]);

        let status = unsafe { phs_series_pct_change(base_values, 2, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { take_f64_values(out) }, vec![None, None, Some(2.0)]);

        let status = unsafe { phs_series_pct_change(base_values, -1, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { take_f64_values(out) },
            vec![Some(-1.0 / 3.0), Some(-0.5), None]
        );

        let status = unsafe { phs_series_pct_change(zero_denominator, 1, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let values = unsafe { take_f64_values(out) };
        assert_eq!(values[0], None);
        assert!(values[1].unwrap().is_nan());
        assert_eq!(values[2], Some(f64::INFINITY));
        assert_eq!(values[3], Some(-2.0));

        let status = unsafe { phs_series_pct_change(zero_denominator, 0, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let values = unsafe { take_f64_values(out) };
        assert!(values[0].unwrap().is_nan());
        assert!(values[1].unwrap().is_nan());
        assert_eq!(values[2], Some(0.0));
        assert_eq!(values[3], Some(-0.0));

        let status = unsafe { phs_series_pct_change(nullable, 1, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { take_f64_values(out) }, vec![None, None, None, Some(1.0)]);

        let status = unsafe { phs_series_pct_change(floats, 1, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let result = unsafe { series_ref(out) }.unwrap();
        assert_eq!(result.value.dtype(), &DataType::Float32);
        assert_eq!(
            result.value.f32().unwrap().into_iter().collect::<Vec<_>>(),
            vec![None, Some(1.0), Some(1.0)]
        );
        unsafe { phs_series_free(out) };

        let status = unsafe { phs_series_pct_change(text, 1, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { take_f64_values(out) }, vec![None, Some(0.5), Some(1.0)]);

        let status = unsafe { phs_series_pct_change(invalid_text, 1, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { take_f64_values(out) }, vec![None, None]);

        unsafe {
            phs_series_free(base_values);
            phs_series_free(zero_denominator);
            phs_series_free(nullable);
            phs_series_free(floats);
            phs_series_free(text);
            phs_series_free(invalid_text);
        }
    }

    #[test]
    fn series_value_counts_returns_count_and_proportion_frames() {
        let colors = series_into_raw(Series::new(
            "color".into(),
            ["blue", "red", "blue", "green", "blue", "red"],
        ));
        let empty = series_into_raw(Series::new("color".into(), Vec::<&str>::new()));
        let count_name = std::ffi::CString::new("n").unwrap();
        let fraction_name = std::ffi::CString::new("fraction").unwrap();
        let duplicate_name = std::ffi::CString::new("color").unwrap();
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();

        let status = unsafe {
            phs_series_value_counts(colors, true, false, count_name.as_ptr(), false, &mut out, &mut err)
        };
        assert_eq!(status, PHS_OK);
        assert!(err.is_null());
        let frame = unsafe { crate::handles::dataframe_ref(out) }.unwrap();
        assert_eq!(frame.value.shape(), (3, 2));
        let color_values: Vec<Option<&str>> = frame
            .value
            .column("color")
            .unwrap()
            .as_materialized_series()
            .str()
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(color_values, vec![Some("blue"), Some("red"), Some("green")]);
        let count_values: Vec<Option<u32>> = frame
            .value
            .column("n")
            .unwrap()
            .as_materialized_series()
            .u32()
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(count_values, vec![Some(3), Some(2), Some(1)]);
        unsafe { phs_dataframe_free(out) };

        let status = unsafe {
            phs_series_value_counts(colors, true, false, fraction_name.as_ptr(), true, &mut out, &mut err)
        };
        assert_eq!(status, PHS_OK);
        assert!(err.is_null());
        let frame = unsafe { crate::handles::dataframe_ref(out) }.unwrap();
        let fraction_values: Vec<Option<f64>> = frame
            .value
            .column("fraction")
            .unwrap()
            .as_materialized_series()
            .f64()
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(fraction_values, vec![Some(0.5), Some(2.0 / 6.0), Some(1.0 / 6.0)]);
        unsafe { phs_dataframe_free(out) };

        let status = unsafe {
            phs_series_value_counts(empty, true, false, count_name.as_ptr(), false, &mut out, &mut err)
        };
        assert_eq!(status, PHS_OK);
        assert!(err.is_null());
        let frame = unsafe { crate::handles::dataframe_ref(out) }.unwrap();
        assert_eq!(frame.value.shape(), (0, 2));
        unsafe { phs_dataframe_free(out) };

        let status = unsafe {
            phs_series_value_counts(colors, true, false, duplicate_name.as_ptr(), false, &mut out, &mut err)
        };
        assert_eq!(status, PHS_POLARS_ERROR);
        assert!(unsafe { take_error_message(err) }.contains("duplicate column names"));

        unsafe {
            phs_series_free(colors);
            phs_series_free(empty);
        }
    }

    #[test]
    fn series_stat_sum_min_max_reject_non_numeric_dtype() {
        let dataframe = read_values_dataframe();
        let name = std::ffi::CString::new("name").unwrap();
        let mut series = ptr::null_mut();
        let mut err = ptr::null_mut();
        let status = unsafe { phs_dataframe_column(dataframe, name.as_ptr(), &mut series, &mut err) };
        assert_eq!(status, PHS_OK);

        let mut has_value = false;
        let mut value = 0.0;
        let status = unsafe { phs_series_stat(series, 3, 0, &mut has_value, &mut value, &mut err) };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "series sum requires numeric dtype");

        let status = unsafe { phs_series_stat(series, 4, 0, &mut has_value, &mut value, &mut err) };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "series min requires numeric dtype");

        let status = unsafe { phs_series_stat(series, 5, 0, &mut has_value, &mut value, &mut err) };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "series max requires numeric dtype");
        unsafe {
            phs_series_free(series);
            phs_dataframe_free(dataframe);
        }
    }

    #[test]
    fn series_float_predicates_handle_special_values() {
        let series = series_into_raw(Series::new(
            "value".into(),
            [1.0_f64, f64::NAN, f64::INFINITY, f64::NEG_INFINITY],
        ));
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();

        let status = unsafe { phs_series_is_nan(series, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert!(err.is_null());
        assert_eq!(
            unsafe { take_bool_values(out) },
            vec![Some(false), Some(true), Some(false), Some(false)]
        );

        let status = unsafe { phs_series_is_not_nan(series, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert!(err.is_null());
        assert_eq!(
            unsafe { take_bool_values(out) },
            vec![Some(true), Some(false), Some(true), Some(true)]
        );

        let status = unsafe { phs_series_is_finite(series, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert!(err.is_null());
        assert_eq!(
            unsafe { take_bool_values(out) },
            vec![Some(true), Some(false), Some(false), Some(false)]
        );

        let status = unsafe { phs_series_is_infinite(series, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert!(err.is_null());
        assert_eq!(
            unsafe { take_bool_values(out) },
            vec![Some(false), Some(false), Some(true), Some(true)]
        );

        unsafe { phs_series_free(series) };
    }

    #[test]
    fn series_float_predicates_preserve_integer_null_validity() {
        let series = series_into_raw(Series::new("numbers".into(), &[Some(1_i64), None, Some(3)]));
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();

        let status = unsafe { phs_series_is_nan(series, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { take_bool_values(out) },
            vec![Some(false), None, Some(false)]
        );

        let status = unsafe { phs_series_is_not_nan(series, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { take_bool_values(out) },
            vec![Some(true), None, Some(true)]
        );

        let status = unsafe { phs_series_is_finite(series, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { take_bool_values(out) },
            vec![Some(true), None, Some(true)]
        );

        let status = unsafe { phs_series_is_infinite(series, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { take_bool_values(out) },
            vec![Some(false), None, Some(false)]
        );

        assert!(err.is_null());
        unsafe { phs_series_free(series) };
    }

    #[test]
    fn series_float_predicates_reject_text_dtype() {
        let series = series_into_raw(Series::new("text".into(), ["a", "b"]));
        let actions = [
            phs_series_is_nan as unsafe extern "C" fn(*const phs_series, *mut *mut phs_series, *mut *mut phs_error) -> c_int,
            phs_series_is_not_nan,
            phs_series_is_finite,
            phs_series_is_infinite,
        ];

        for action in actions {
            let mut out = ptr::null_mut();
            let mut err = ptr::null_mut();
            let status = unsafe { action(series, &mut out, &mut err) };
            assert_eq!(status, crate::error::PHS_POLARS_ERROR);
            assert!(out.is_null());
            assert!(!unsafe { take_error_message(err) }.is_empty());
        }

        unsafe { phs_series_free(series) };
    }

    #[test]
    fn series_distinct_predicates_return_boolean_masks() {
        let series = series_into_raw(Series::new(
            "value".into(),
            &[Some("a"), Some("b"), Some("a"), None, None, Some("c")],
        ));
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();

        let status = unsafe { phs_series_is_duplicated(series, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { take_bool_values(out) },
            vec![Some(true), Some(false), Some(true), Some(true), Some(true), Some(false)]
        );

        let status = unsafe { phs_series_is_unique(series, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { take_bool_values(out) },
            vec![Some(false), Some(true), Some(false), Some(false), Some(false), Some(true)]
        );

        let status = unsafe { phs_series_is_first_distinct(series, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { take_bool_values(out) },
            vec![Some(true), Some(true), Some(false), Some(true), Some(false), Some(true)]
        );

        let status = unsafe { phs_series_is_last_distinct(series, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { take_bool_values(out) },
            vec![Some(false), Some(true), Some(true), Some(false), Some(true), Some(true)]
        );

        assert!(err.is_null());
        unsafe { phs_series_free(series) };
    }

    #[test]
    fn series_distinct_predicates_handle_numeric_and_edge_inputs() {
        let numbers = series_into_raw(Series::new(
            "number".into(),
            &[Some(1_i64), Some(1), Some(2), None],
        ));
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();

        let status = unsafe { phs_series_is_duplicated(numbers, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { take_bool_values(out) },
            vec![Some(true), Some(true), Some(false), Some(false)]
        );

        let status = unsafe { phs_series_is_unique(numbers, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { take_bool_values(out) },
            vec![Some(false), Some(false), Some(true), Some(true)]
        );

        let status = unsafe { phs_series_is_first_distinct(numbers, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { take_bool_values(out) },
            vec![Some(true), Some(false), Some(true), Some(true)]
        );

        let status = unsafe { phs_series_is_last_distinct(numbers, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { take_bool_values(out) },
            vec![Some(false), Some(true), Some(true), Some(true)]
        );

        let flags = series_into_raw(Series::new("flag".into(), &[true, false, true]));
        let status = unsafe { phs_series_is_duplicated(flags, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { take_bool_values(out) },
            vec![Some(true), Some(false), Some(true)]
        );

        let empty_values: Vec<Option<i64>> = Vec::new();
        let empty = series_into_raw(Series::new("empty".into(), empty_values));
        let status = unsafe { phs_series_is_first_distinct(empty, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { take_bool_values(out) }, Vec::<Option<bool>>::new());

        let singleton = series_into_raw(Series::new("single".into(), &[true]));
        let status = unsafe { phs_series_is_last_distinct(singleton, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { take_bool_values(out) }, vec![Some(true)]);

        assert!(err.is_null());
        unsafe {
            phs_series_free(numbers);
            phs_series_free(flags);
            phs_series_free(empty);
            phs_series_free(singleton);
        }
    }

    #[test]
    fn series_numeric_unary_transforms_values() {
        let numbers = series_into_raw(Series::new(
            "number".into(),
            &[Some(-3_i64), Some(0), None, Some(4)],
        ));
        let values = series_into_raw(Series::new(
            "value".into(),
            &[Some(2.5_f64), Some(3.5), Some(-2.5), Some(1.25), None],
        ));
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();

        let status = unsafe { phs_series_abs(numbers, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { take_i64_values(out) },
            vec![Some(3), Some(0), None, Some(4)]
        );

        let status = unsafe { phs_series_abs(values, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { take_f64_values(out) },
            vec![Some(2.5), Some(3.5), Some(2.5), Some(1.25), None]
        );

        let status = unsafe { phs_series_round(values, 0, 0, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { take_f64_values(out) },
            vec![Some(2.0), Some(4.0), Some(-2.0), Some(1.0), None]
        );

        let status = unsafe { phs_series_round(values, 0, 1, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { take_f64_values(out) },
            vec![Some(3.0), Some(4.0), Some(-3.0), Some(1.0), None]
        );

        let precise = series_into_raw(Series::new(
            "precise".into(),
            &[Some(1.234_f64), Some(-1.235), None],
        ));
        let status = unsafe { phs_series_round(precise, 2, 1, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { take_f64_values(out) },
            vec![Some(1.23), Some(-1.24), None]
        );

        let status = unsafe { phs_series_floor(values, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { take_f64_values(out) },
            vec![Some(2.0), Some(3.0), Some(-3.0), Some(1.0), None]
        );

        let status = unsafe { phs_series_ceil(values, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { take_f64_values(out) },
            vec![Some(3.0), Some(4.0), Some(-2.0), Some(2.0), None]
        );

        let integers = series_into_raw(Series::new("integer".into(), &[Some(2_i64), Some(-3), None]));
        let status = unsafe { phs_series_round(integers, 0, 1, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { take_i64_values(out) }, vec![Some(2), Some(-3), None]);

        let status = unsafe { phs_series_floor(integers, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { take_i64_values(out) }, vec![Some(2), Some(-3), None]);

        let status = unsafe { phs_series_ceil(integers, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { take_i64_values(out) }, vec![Some(2), Some(-3), None]);

        assert!(err.is_null());
        unsafe {
            phs_series_free(numbers);
            phs_series_free(values);
            phs_series_free(precise);
            phs_series_free(integers);
        }
    }

    #[test]
    fn series_numeric_unary_transforms_report_errors() {
        let text = series_into_raw(Series::new("text".into(), ["a"]));
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();

        let status = unsafe { phs_series_abs(text, &mut out, &mut err) };
        assert_eq!(status, crate::error::PHS_POLARS_ERROR);
        assert!(out.is_null());
        assert!(!unsafe { take_error_message(err) }.is_empty());

        let status = unsafe { phs_series_round(text, 0, 0, &mut out, &mut err) };
        assert_eq!(status, crate::error::PHS_POLARS_ERROR);
        assert!(out.is_null());
        assert!(!unsafe { take_error_message(err) }.is_empty());

        let status = unsafe { phs_series_floor(text, &mut out, &mut err) };
        assert_eq!(status, crate::error::PHS_POLARS_ERROR);
        assert!(out.is_null());
        assert!(!unsafe { take_error_message(err) }.is_empty());

        let status = unsafe { phs_series_ceil(text, &mut out, &mut err) };
        assert_eq!(status, crate::error::PHS_POLARS_ERROR);
        assert!(out.is_null());
        assert!(!unsafe { take_error_message(err) }.is_empty());

        let status = unsafe { phs_series_round(text, 0, 99, &mut out, &mut err) };
        assert_eq!(status, crate::error::PHS_INVALID_ARGUMENT);
        assert!(out.is_null());
        assert_eq!(unsafe { take_error_message(err) }, "unknown round mode code 99");

        unsafe { phs_series_free(text) };
    }

    #[test]
    fn series_diff_returns_expected_values() {
        let values = series_into_raw(Series::new(
            "value".into(),
            &[Some(10_i64), Some(13), None, Some(20)],
        ));
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();

        let status = unsafe { phs_series_diff(values, 1, 0, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { take_i64_values(out) },
            vec![None, Some(3), None, None]
        );

        let status = unsafe { phs_series_diff(values, 1, 1, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { take_i64_values(out) }, vec![Some(3), None, None]);

        let status = unsafe { phs_series_diff(values, -1, 1, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { take_i64_values(out) }, vec![Some(-3), None, None]);

        let status = unsafe { phs_series_diff(values, 10, 0, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { take_i64_values(out) }, vec![None, None, None, None]);

        let status = unsafe { phs_series_diff(values, 10, 1, &mut out, &mut err) };
        assert_eq!(status, crate::error::PHS_INVALID_ARGUMENT);
        assert!(out.is_null());
        assert_eq!(
            unsafe { take_error_message(err) },
            "series diff period 10 exceeds series length 4"
        );

        let status = unsafe { phs_series_diff(values, -10, 1, &mut out, &mut err) };
        assert_eq!(status, crate::error::PHS_INVALID_ARGUMENT);
        assert!(out.is_null());
        assert_eq!(
            unsafe { take_error_message(err) },
            "series diff period 10 exceeds series length 4"
        );

        let unsigned = series_into_raw(Series::new("small".into(), &[1_u8, 4, 9]));
        let status = unsafe { phs_series_diff(unsigned, 1, 0, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { take_i16_values(out) }, vec![None, Some(3), Some(5)]);

        let unsigned16 = series_into_raw(Series::new("medium".into(), &[1_u16, 4, 9]));
        let status = unsafe { phs_series_diff(unsigned16, 1, 0, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { take_i32_values(out) }, vec![None, Some(3), Some(5)]);

        let unsigned32 = series_into_raw(Series::new("large".into(), &[1_u32, 4, 9]));
        let status = unsafe { phs_series_diff(unsigned32, 1, 0, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { take_i64_values(out) }, vec![None, Some(3), Some(5)]);

        let unsigned64 = series_into_raw(Series::new("wide".into(), &[1_u64, 4, 9]));
        let status = unsafe { phs_series_diff(unsigned64, 1, 0, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { take_i64_values(out) }, vec![None, Some(3), Some(5)]);

        let overflow_u64 = series_into_raw(Series::new(
            "overflow".into(),
            &[(i64::MAX as u64) + 1, (i64::MAX as u64) + 3],
        ));
        let status = unsafe { phs_series_diff(overflow_u64, 1, 0, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { take_i64_values(out) }, vec![None, None]);

        assert!(err.is_null());
        unsafe {
            phs_series_free(values);
            phs_series_free(unsigned);
            phs_series_free(unsigned16);
            phs_series_free(unsigned32);
            phs_series_free(unsigned64);
            phs_series_free(overflow_u64);
        }
    }

    #[test]
    fn series_diff_reports_errors() {
        let text = series_into_raw(Series::new("text".into(), ["a", "b"]));
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();

        let status = unsafe { phs_series_diff(text, 1, 0, &mut out, &mut err) };
        assert_eq!(status, crate::error::PHS_POLARS_ERROR);
        assert!(out.is_null());
        assert!(!unsafe { take_error_message(err) }.is_empty());

        let status = unsafe { phs_series_diff(text, 1, 99, &mut out, &mut err) };
        assert_eq!(status, crate::error::PHS_INVALID_ARGUMENT);
        assert!(out.is_null());
        assert_eq!(unsafe { take_error_message(err) }, "unknown null behavior code 99");

        unsafe { phs_series_free(text) };
    }

    #[test]
    fn series_interpolate_returns_expected_values() {
        let values = series_into_raw(Series::new(
            "value".into(),
            &[None, Some(1_u32), None, None, Some(4), Some(5), None],
        ));
        let descending = series_into_raw(Series::new(
            "descending".into(),
            &[Some(4_u32), None, None, Some(1)],
        ));
        let all_null = series_into_raw(Series::new("all_null".into(), &[None::<u32>, None, None]));
        let text = series_into_raw(Series::new("text".into(), &[Some("a"), None, Some("b")]));
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();

        let status = unsafe { phs_series_interpolate(values, 0, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { take_f64_values(out) },
            vec![None, Some(1.0), Some(2.0), Some(3.0), Some(4.0), Some(5.0), None]
        );

        let status = unsafe { phs_series_interpolate(values, 1, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { series_ref(out) }.unwrap().value.u32().unwrap().into_iter().collect::<Vec<_>>(),
            vec![None, Some(1), Some(1), Some(4), Some(4), Some(5), None]
        );
        unsafe { phs_series_free(out) };

        let status = unsafe { phs_series_interpolate(descending, 0, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { take_f64_values(out) },
            vec![Some(4.0), Some(3.0), Some(2.0), Some(1.0)]
        );

        let status = unsafe { phs_series_interpolate(all_null, 0, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { take_f64_values(out) }, vec![None, None, None]);

        let status = unsafe { phs_series_interpolate(text, 0, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(
            unsafe { series_ref(out) }.unwrap().value.str().unwrap().into_iter().collect::<Vec<_>>(),
            vec![Some("a"), None, Some("b")]
        );
        unsafe { phs_series_free(out) };

        assert!(err.is_null());
        unsafe {
            phs_series_free(values);
            phs_series_free(descending);
            phs_series_free(all_null);
            phs_series_free(text);
        }
    }

    #[test]
    fn series_interpolate_rejects_nearest_unsupported_dtypes() {
        let text = series_into_raw(Series::new("text".into(), &[Some("a"), None, Some("b")]));
        let flag = series_into_raw(Series::new(
            "flag".into(),
            &[Some(true), None, Some(false)],
        ));
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();

        let status = unsafe { phs_series_interpolate(text, 1, &mut out, &mut err) };
        assert_eq!(status, crate::error::PHS_INVALID_ARGUMENT);
        assert!(out.is_null());
        assert_eq!(
            unsafe { take_error_message(err) },
            "series interpolate nearest is unsupported for dtype String"
        );

        let status = unsafe { phs_series_interpolate(flag, 1, &mut out, &mut err) };
        assert_eq!(status, crate::error::PHS_INVALID_ARGUMENT);
        assert!(out.is_null());
        assert_eq!(
            unsafe { take_error_message(err) },
            "series interpolate nearest is unsupported for dtype Boolean"
        );

        unsafe {
            phs_series_free(text);
            phs_series_free(flag);
        }
    }

    #[test]
    fn series_interpolate_rejects_unknown_method_code() {
        let values = series_into_raw(Series::new("value".into(), &[Some(1_u32), None, Some(3)]));
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();

        let status = unsafe { phs_series_interpolate(values, 99, &mut out, &mut err) };
        assert_eq!(status, crate::error::PHS_INVALID_ARGUMENT);
        assert!(out.is_null());
        assert_eq!(
            unsafe { take_error_message(err) },
            "unknown interpolation method code 99"
        );

        unsafe { phs_series_free(values) };
    }

    #[test]
    fn series_unique_and_unique_stable_work() {
        let dataframe = read_values_dataframe();
        let name = std::ffi::CString::new("active").unwrap();
        let mut series = ptr::null_mut();
        let mut err = ptr::null_mut();
        let status = unsafe { phs_dataframe_column(dataframe, name.as_ptr(), &mut series, &mut err) };
        assert_eq!(status, PHS_OK);

        let mut out = ptr::null_mut();
        let status = unsafe { phs_series_unique(series, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { series_ref(out) }.unwrap().value.len(), 3);
        unsafe { phs_series_free(out) };

        let status = unsafe { phs_series_unique_stable(series, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { series_ref(out) }.unwrap().value.len(), 3);
        unsafe {
            phs_series_free(out);
            phs_series_free(series);
            phs_dataframe_free(dataframe);
        }
    }

    #[test]
    fn series_cast_rejects_unknown_dtype_code() {
        let series = read_age_series();
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();
        let status = unsafe { phs_series_cast(series, 99, &mut out, &mut err) };
        assert_eq!(status, crate::error::PHS_INVALID_ARGUMENT);
        assert!(out.is_null());
        assert!(!err.is_null());
        unsafe {
            phs_error_free(err);
            phs_series_free(series);
        }
    }

    #[test]
    fn series_shift_returns_owned_handle() {
        let series = read_age_series();
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();
        let status = unsafe { phs_series_shift(series, 1, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert_eq!(unsafe { series_ref(out) }.unwrap().value.len(), 3);
        assert_eq!(unsafe { series_ref(series) }.unwrap().value.len(), 3);
        unsafe {
            phs_series_free(out);
            phs_series_free(series);
        }
    }

    #[test]
    fn series_append_keeps_left_name_and_combines_lengths() {
        let series = read_age_series();
        let mut head = ptr::null_mut();
        let mut err = ptr::null_mut();
        let status = unsafe { phs_series_head(series, 1, &mut head, &mut err) };
        assert_eq!(status, PHS_OK);

        let mut out = ptr::null_mut();
        let status = unsafe { phs_series_append(series, head, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let appended = unsafe { series_ref(out) }.unwrap();
        assert_eq!(appended.value.name().as_str(), "age");
        assert_eq!(appended.value.len(), 4);
        assert_eq!(unsafe { series_ref(series) }.unwrap().value.len(), 3);
        unsafe {
            phs_series_free(out);
            phs_series_free(head);
            phs_series_free(series);
        }
    }

    #[test]
    fn series_append_reports_dtype_conflict() {
        let dataframe = read_values_dataframe();
        let age_name = std::ffi::CString::new("age").unwrap();
        let text_name = std::ffi::CString::new("name").unwrap();
        let mut age = ptr::null_mut();
        let mut text = ptr::null_mut();
        let mut err = ptr::null_mut();
        let status = unsafe { phs_dataframe_column(dataframe, age_name.as_ptr(), &mut age, &mut err) };
        assert_eq!(status, PHS_OK);
        let status = unsafe { phs_dataframe_column(dataframe, text_name.as_ptr(), &mut text, &mut err) };
        assert_eq!(status, PHS_OK);

        let mut out = ptr::null_mut();
        let status = unsafe { phs_series_append(age, text, &mut out, &mut err) };
        assert_eq!(status, crate::error::PHS_POLARS_ERROR);
        assert!(out.is_null());
        assert!(!err.is_null());
        unsafe {
            phs_error_free(err);
            phs_series_free(age);
            phs_series_free(text);
            phs_dataframe_free(dataframe);
        }
    }

    #[test]
    fn series_values_i64_reports_dtype_mismatch() {
        let dataframe = read_values_dataframe();
        let name = std::ffi::CString::new("name").unwrap();
        let mut series = ptr::null_mut();
        let mut err = ptr::null_mut();
        let status = unsafe { phs_dataframe_column(dataframe, name.as_ptr(), &mut series, &mut err) };
        assert_eq!(status, PHS_OK);
        let mut out = ptr::null_mut();
        let status = unsafe { phs_series_values_i64(series, &mut out, &mut err) };
        assert_eq!(status, crate::error::PHS_POLARS_ERROR);
        assert!(out.is_null());
        assert!(!err.is_null());
        unsafe {
            phs_error_free(err);
            phs_series_free(series);
            phs_dataframe_free(dataframe);
        }
    }
}
