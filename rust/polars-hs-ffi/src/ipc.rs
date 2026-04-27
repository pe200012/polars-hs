use std::fs::File;
use std::io::Cursor;
use std::os::raw::{c_char, c_int, c_uchar};
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
pub unsafe extern "C" fn phs_dataframe_to_ipc_bytes(
    dataframe: *const phs_dataframe,
    out: *mut *mut phs_bytes,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { dataframe_ref(dataframe) }?;
        let mut df = handle.value.clone();
        let mut cursor = Cursor::new(Vec::new());
        IpcWriter::new(&mut cursor).finish(&mut df)?;
        *out = bytes_into_raw(cursor.into_inner());
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_from_ipc_bytes(
    data: *const c_uchar,
    len: usize,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        if data.is_null() && len > 0 {
            return Err(crate::error::PhsError::invalid_argument("data pointer was null"));
        }
        let bytes = if len == 0 {
            Vec::new()
        } else {
            unsafe { std::slice::from_raw_parts(data, len) }.to_vec()
        };
        let cursor = Cursor::new(bytes);
        let df = IpcReader::new(cursor).finish()?;
        *out = dataframe_into_raw(df);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_read_ipc_file(
    path: *const c_char,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let file = File::open(unsafe { c_path(path) }?)?;
        let df = IpcReader::new(file).finish()?;
        *out = dataframe_into_raw(df);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_write_ipc_file(
    path: *const c_char,
    dataframe: *const phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let path = unsafe { c_path(path) }?;
        let handle = unsafe { dataframe_ref(dataframe) }?;
        let mut df = handle.value.clone();
        let mut file = File::create(path)?;
        IpcWriter::new(&mut file).finish(&mut df)?;
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataframe::phs_read_csv;
    use crate::error::PHS_OK;

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
    fn ipc_bytes_roundtrip_preserves_shape() {
        let path = fixture_path();
        let mut df0 = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(unsafe { phs_read_csv(path.as_ptr(), &mut df0, &mut err) }, PHS_OK);

        let mut bytes = ptr::null_mut();
        assert_eq!(unsafe { phs_dataframe_to_ipc_bytes(df0, &mut bytes, &mut err) }, PHS_OK);
        let len = unsafe { crate::bytes::phs_bytes_len(bytes) };
        let data = unsafe { crate::bytes::phs_bytes_data(bytes) };
        assert!(len > 0);

        let mut df1 = ptr::null_mut();
        assert_eq!(unsafe { phs_dataframe_from_ipc_bytes(data, len, &mut df1, &mut err) }, PHS_OK);

        let mut height = 0;
        let mut width = 0;
        assert_eq!(unsafe { crate::dataframe::phs_dataframe_shape(df1, &mut height, &mut width, &mut err) }, PHS_OK);
        assert_eq!((height, width), (3, 2));

        unsafe {
            crate::bytes::phs_bytes_free(bytes);
            crate::handles::phs_dataframe_free(df0);
            crate::handles::phs_dataframe_free(df1);
        }
    }
}
