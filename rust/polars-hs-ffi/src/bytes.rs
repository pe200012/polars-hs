use std::os::raw::c_uchar;
use std::ptr;

#[repr(C)]
pub struct phs_bytes {
    _private: [u8; 0],
}

pub struct BytesHandle {
    bytes: Vec<u8>,
}

pub fn bytes_into_raw(bytes: Vec<u8>) -> *mut phs_bytes {
    Box::into_raw(Box::new(BytesHandle { bytes })) as *mut phs_bytes
}

unsafe fn bytes_handle<'a>(ptr: *const phs_bytes) -> Option<&'a BytesHandle> {
    if ptr.is_null() {
        None
    } else {
        Some(unsafe { &*(ptr as *const BytesHandle) })
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_bytes_len(ptr: *const phs_bytes) -> usize {
    unsafe { bytes_handle(ptr) }.map_or(0, |handle| handle.bytes.len())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_bytes_data(ptr: *const phs_bytes) -> *const c_uchar {
    unsafe { bytes_handle(ptr) }.map_or(ptr::null(), |handle| handle.bytes.as_ptr())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_bytes_free(ptr: *mut phs_bytes) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(ptr as *mut BytesHandle));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_roundtrip_keeps_len_and_data() {
        let raw = bytes_into_raw(vec![1, 2, 3]);
        unsafe {
            assert_eq!(phs_bytes_len(raw), 3);
            assert_eq!(*phs_bytes_data(raw), 1);
            phs_bytes_free(raw);
        }
    }
}
