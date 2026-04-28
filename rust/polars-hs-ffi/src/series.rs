use std::os::raw::{c_char, c_int};
use std::ptr;

use polars::prelude::*;

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
        1 => Ok(DataType::Int64),
        2 => Ok(DataType::Float64),
        3 => Ok(DataType::String),
        value => Err(PhsError::invalid_argument(format!("unknown series cast dtype code {value}"))),
    }
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
    let end = offset
        .checked_add(8)
        .ok_or_else(|| PhsError::invalid_argument(format!("{context} length overflow")))?;
    if end > bytes.len() {
        return Err(PhsError::invalid_argument(format!("{context} ended early")));
    }
    let value = u64::from_le_bytes(bytes[*offset..end].try_into().expect("slice length checked"));
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
            options.limit = Some(limit as IdxSize);
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
pub unsafe extern "C" fn phs_series_unique_stable(
    series: *const phs_series,
    out: *mut *mut phs_series,
    err: *mut *mut phs_error,
) -> c_int {
    series_transform(series, out, err, |value| Ok(value.unique_stable()?))
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
    use crate::error::{PHS_OK, phs_error_free};
    use crate::handles::{phs_dataframe_free, phs_series_free};
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

        let status = unsafe { phs_series_cast(series, 2, &mut out, &mut err) };
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
