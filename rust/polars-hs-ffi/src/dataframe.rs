use std::fs::File;
use std::os::raw::{c_char, c_int};
use std::path::PathBuf;
use std::ptr;

use polars::prelude::*;

use crate::bytes::{bytes_into_raw, phs_bytes};
use crate::error::{PhsResult, c_str_to_str, ffi_boundary, phs_error, required_mut};
use crate::handles::{dataframe_into_raw, dataframe_ref, phs_dataframe, phs_series, series_into_raw};
use crate::series::{encode_bool_series, encode_f64_series, encode_i64_series, encode_text_series};

unsafe fn c_path(path: *const c_char) -> PhsResult<PathBuf> {
    Ok(PathBuf::from(unsafe { c_str_to_str(path, "path") }?))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_read_csv(
    path: *const c_char,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let path = unsafe { c_path(path) }?;
        let df = CsvReadOptions::default()
            .with_has_header(true)
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
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let path = unsafe { c_path(path) }?;
        let file = File::open(path)?;
        let df = ParquetReader::new(file).finish()?;
        *out = dataframe_into_raw(df);
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
        let mut bytes = Vec::new();
        for field in handle.value.schema().iter_fields() {
            bytes.extend_from_slice(field.name().as_str().as_bytes());
            bytes.push(0);
            bytes.extend_from_slice(format!("{:?}", field.dtype()).as_bytes());
            bytes.push(0);
        }
        *out = bytes_into_raw(bytes);
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
