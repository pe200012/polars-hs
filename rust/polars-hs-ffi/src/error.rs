use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};
use std::panic::{self, AssertUnwindSafe};
use std::ptr;

use polars::prelude::PolarsError;

pub const PHS_OK: c_int = 0;
pub const PHS_POLARS_ERROR: c_int = 1;
pub const PHS_INVALID_ARGUMENT: c_int = 2;
pub const PHS_UTF8_ERROR: c_int = 3;
pub const PHS_PANIC: c_int = 4;

#[repr(C)]
pub struct phs_error {
    _private: [u8; 0],
}

struct ErrorHandle {
    code: c_int,
    message: CString,
}

#[derive(Debug)]
pub struct PhsError {
    code: c_int,
    message: String,
}

impl PhsError {
    pub fn polars(message: impl Into<String>) -> Self {
        Self {
            code: PHS_POLARS_ERROR,
            message: message.into(),
        }
    }

    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self {
            code: PHS_INVALID_ARGUMENT,
            message: message.into(),
        }
    }

    pub fn utf8(message: impl Into<String>) -> Self {
        Self {
            code: PHS_UTF8_ERROR,
            message: message.into(),
        }
    }

    fn panic(message: impl Into<String>) -> Self {
        Self {
            code: PHS_PANIC,
            message: message.into(),
        }
    }

    fn into_raw(self) -> *mut phs_error {
        let sanitized = self.message.replace('\0', "\\0");
        let handle = ErrorHandle {
            code: self.code,
            message: CString::new(sanitized).expect("sanitized error message contains no NUL"),
        };
        Box::into_raw(Box::new(handle)) as *mut phs_error
    }
}

impl From<PolarsError> for PhsError {
    fn from(value: PolarsError) -> Self {
        PhsError::polars(value.to_string())
    }
}

impl From<std::io::Error> for PhsError {
    fn from(value: std::io::Error) -> Self {
        PhsError::polars(value.to_string())
    }
}

impl From<std::str::Utf8Error> for PhsError {
    fn from(value: std::str::Utf8Error) -> Self {
        PhsError::utf8(value.to_string())
    }
}

pub type PhsResult<T> = Result<T, PhsError>;

pub unsafe fn c_str_to_str<'a>(value: *const c_char, name: &str) -> PhsResult<&'a str> {
    if value.is_null() {
        return Err(PhsError::invalid_argument(format!("{name} pointer was null")));
    }
    unsafe { CStr::from_ptr(value) }.to_str().map_err(PhsError::from)
}

pub unsafe fn required_mut<'a, T>(value: *mut T, name: &str) -> PhsResult<&'a mut T> {
    if value.is_null() {
        return Err(PhsError::invalid_argument(format!("{name} pointer was null")));
    }
    Ok(unsafe { &mut *value })
}

pub fn ffi_boundary<F>(err_out: *mut *mut phs_error, action: F) -> c_int
where
    F: FnOnce() -> PhsResult<()> + panic::UnwindSafe,
{
    if !err_out.is_null() {
        unsafe {
            *err_out = ptr::null_mut();
        }
    }

    match panic::catch_unwind(AssertUnwindSafe(action)) {
        Ok(Ok(())) => PHS_OK,
        Ok(Err(err)) => write_error(err_out, err),
        Err(payload) => {
            let message = if let Some(text) = payload.downcast_ref::<&str>() {
                (*text).to_owned()
            } else if let Some(text) = payload.downcast_ref::<String>() {
                text.clone()
            } else {
                "Rust panic crossed the FFI boundary".to_owned()
            };
            write_error(err_out, PhsError::panic(message))
        }
    }
}

fn write_error(err_out: *mut *mut phs_error, err: PhsError) -> c_int {
    let code = err.code;
    if !err_out.is_null() {
        unsafe {
            *err_out = err.into_raw();
        }
    }
    code
}

unsafe fn error_handle<'a>(error: *const phs_error) -> Option<&'a ErrorHandle> {
    if error.is_null() {
        None
    } else {
        Some(unsafe { &*(error as *const ErrorHandle) })
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_error_code(error: *const phs_error) -> c_int {
    unsafe { error_handle(error) }.map_or(PHS_INVALID_ARGUMENT, |handle| handle.code)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_error_message(error: *const phs_error) -> *const c_char {
    unsafe { error_handle(error) }.map_or(ptr::null(), |handle| handle.message.as_ptr())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_error_free(error: *mut phs_error) {
    if error.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(error as *mut ErrorHandle));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_roundtrip_keeps_code_and_message() {
        let raw = PhsError::invalid_argument("bad input").into_raw();
        unsafe {
            assert_eq!(phs_error_code(raw), PHS_INVALID_ARGUMENT);
            let message = CStr::from_ptr(phs_error_message(raw));
            assert_eq!(message.to_str().unwrap(), "bad input");
            phs_error_free(raw);
        }
    }

    #[test]
    fn boundary_converts_panic_to_error() {
        let mut err = ptr::null_mut();
        let status = ffi_boundary(&mut err, || -> PhsResult<()> {
            panic!("boom");
        });
        assert_eq!(status, PHS_PANIC);
        unsafe {
            assert_eq!(CStr::from_ptr(phs_error_message(err)).to_str().unwrap(), "boom");
            phs_error_free(err);
        }
    }
}
