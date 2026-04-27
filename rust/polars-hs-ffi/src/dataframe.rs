use std::fs::File;
use std::os::raw::{c_char, c_int};
use std::path::PathBuf;
use std::ptr;

use polars::prelude::*;

use crate::bytes::{bytes_into_raw, phs_bytes};
use crate::error::{PhsResult, c_str_to_str, ffi_boundary, phs_error, required_mut};
use crate::handles::{dataframe_into_raw, dataframe_ref, phs_dataframe};

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{PHS_OK, phs_error_free};

    fn fixture_path() -> std::ffi::CString {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("test")
            .join("data")
            .join("people.csv");
        std::ffi::CString::new(path.to_string_lossy().as_bytes()).unwrap()
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
}
