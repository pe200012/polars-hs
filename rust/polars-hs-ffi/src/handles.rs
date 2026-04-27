use polars::prelude::{DataFrame, Expr, LazyFrame};

use crate::error::{PhsError, PhsResult};

#[repr(C)]
pub struct phs_dataframe {
    _private: [u8; 0],
}

#[repr(C)]
pub struct phs_lazyframe {
    _private: [u8; 0],
}

#[repr(C)]
pub struct phs_expr {
    _private: [u8; 0],
}

pub struct DataFrameHandle {
    pub value: DataFrame,
}

pub struct LazyFrameHandle {
    pub value: LazyFrame,
}

pub struct ExprHandle {
    pub value: Expr,
}

pub fn dataframe_into_raw(value: DataFrame) -> *mut phs_dataframe {
    Box::into_raw(Box::new(DataFrameHandle { value })) as *mut phs_dataframe
}

pub fn lazyframe_into_raw(value: LazyFrame) -> *mut phs_lazyframe {
    Box::into_raw(Box::new(LazyFrameHandle { value })) as *mut phs_lazyframe
}

pub fn expr_into_raw(value: Expr) -> *mut phs_expr {
    Box::into_raw(Box::new(ExprHandle { value })) as *mut phs_expr
}

pub unsafe fn dataframe_ref<'a>(ptr: *const phs_dataframe) -> PhsResult<&'a DataFrameHandle> {
    if ptr.is_null() {
        return Err(PhsError::invalid_argument("dataframe pointer was null"));
    }
    Ok(unsafe { &*(ptr as *const DataFrameHandle) })
}

pub unsafe fn lazyframe_ref<'a>(ptr: *const phs_lazyframe) -> PhsResult<&'a LazyFrameHandle> {
    if ptr.is_null() {
        return Err(PhsError::invalid_argument("lazyframe pointer was null"));
    }
    Ok(unsafe { &*(ptr as *const LazyFrameHandle) })
}

pub unsafe fn expr_ref<'a>(ptr: *const phs_expr) -> PhsResult<&'a ExprHandle> {
    if ptr.is_null() {
        return Err(PhsError::invalid_argument("expr pointer was null"));
    }
    Ok(unsafe { &*(ptr as *const ExprHandle) })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_free(ptr: *mut phs_dataframe) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(ptr as *mut DataFrameHandle));
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_free(ptr: *mut phs_lazyframe) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(ptr as *mut LazyFrameHandle));
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_free(ptr: *mut phs_expr) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(ptr as *mut ExprHandle));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::DataFrame;

    #[test]
    fn dataframe_handle_roundtrip() {
        let raw = dataframe_into_raw(DataFrame::empty());
        unsafe {
            assert_eq!(dataframe_ref(raw).unwrap().value.shape(), (0, 0));
            phs_dataframe_free(raw);
        }
    }
}
