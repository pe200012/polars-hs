use std::os::raw::c_int;
use std::ptr;

use polars::prelude::*;

use crate::bytes::{bytes_into_raw, phs_bytes};
use crate::error::{PhsResult, ffi_boundary, phs_error, required_mut};
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
    use crate::dataframe::{phs_dataframe_column, phs_read_csv};
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
