use std::os::raw::{c_char, c_double, c_int};
use std::ptr;

use polars::prelude::*;

use crate::error::{PhsError, c_str_to_str, ffi_boundary, phs_error, required_mut};
use crate::handles::{expr_into_raw, expr_ref, phs_expr};

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_col(
    name: *const c_char,
    out: *mut *mut phs_expr,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let name = unsafe { c_str_to_str(name, "name") }?;
        *out = expr_into_raw(col(name));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_lit_bool(
    value: bool,
    out: *mut *mut phs_expr,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        *out = expr_into_raw(lit(value));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_lit_int(
    value: i64,
    out: *mut *mut phs_expr,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        *out = expr_into_raw(lit(value));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_lit_double(
    value: c_double,
    out: *mut *mut phs_expr,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        *out = expr_into_raw(lit(value));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_lit_text(
    value: *const c_char,
    out: *mut *mut phs_expr,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let value = unsafe { c_str_to_str(value, "value") }?;
        *out = expr_into_raw(lit(value.to_owned()));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_alias(
    expr: *const phs_expr,
    name: *const c_char,
    out: *mut *mut phs_expr,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let expr = unsafe { expr_ref(expr) }?.value.clone();
        let name = unsafe { c_str_to_str(name, "name") }?;
        *out = expr_into_raw(expr.alias(name));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_not(
    expr: *const phs_expr,
    out: *mut *mut phs_expr,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let expr = unsafe { expr_ref(expr) }?.value.clone();
        *out = expr_into_raw(expr.not());
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_binary(
    op: c_int,
    left: *const phs_expr,
    right: *const phs_expr,
    out: *mut *mut phs_expr,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let left = unsafe { expr_ref(left) }?.value.clone();
        let right = unsafe { expr_ref(right) }?.value.clone();
        let expr = match op {
            0 => left.eq(right),
            1 => left.neq(right),
            2 => left.gt(right),
            3 => left.gt_eq(right),
            4 => left.lt(right),
            5 => left.lt_eq(right),
            6 => left.logical_and(right),
            7 => left.logical_or(right),
            8 => left + right,
            9 => left - right,
            10 => left * right,
            11 => left / right,
            _ => return Err(PhsError::invalid_argument(format!("unknown binary operator code {op}"))),
        };
        *out = expr_into_raw(expr);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_agg(
    op: c_int,
    expr: *const phs_expr,
    out: *mut *mut phs_expr,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let expr = unsafe { expr_ref(expr) }?.value.clone();
        let agg = match op {
            0 => expr.sum(),
            1 => expr.mean(),
            2 => expr.min(),
            3 => expr.max(),
            4 => expr.count(),
            5 => expr.len(),
            6 => expr.first(),
            7 => expr.last(),
            _ => return Err(PhsError::invalid_argument(format!("unknown aggregation operator code {op}"))),
        };
        *out = expr_into_raw(agg);
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::PHS_OK;

    #[test]
    fn builds_binary_expression() {
        let name = std::ffi::CString::new("age").unwrap();
        let mut col_expr = ptr::null_mut();
        let mut lit_expr = ptr::null_mut();
        let mut out: *mut phs_expr = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(unsafe { phs_expr_col(name.as_ptr(), &mut col_expr, &mut err) }, PHS_OK);
        assert_eq!(unsafe { phs_expr_lit_int(35, &mut lit_expr, &mut err) }, PHS_OK);
        assert_eq!(unsafe { phs_expr_binary(2, col_expr, lit_expr, &mut out, &mut err) }, PHS_OK);
        assert!(!out.is_null());
        unsafe {
            crate::handles::phs_expr_free(col_expr);
            crate::handles::phs_expr_free(lit_expr);
            crate::handles::phs_expr_free(out);
        }
    }

    #[test]
    fn builds_aggregation_expressions() {
        let name = std::ffi::CString::new("salary").unwrap();
        for op in 0..=7 {
            let mut col_expr = ptr::null_mut();
            let mut agg_expr: *mut phs_expr = ptr::null_mut();
            let mut err = ptr::null_mut();
            assert_eq!(unsafe { phs_expr_col(name.as_ptr(), &mut col_expr, &mut err) }, PHS_OK);
            assert_eq!(unsafe { phs_expr_agg(op, col_expr, &mut agg_expr, &mut err) }, PHS_OK);
            assert!(!agg_expr.is_null());
            unsafe {
                crate::handles::phs_expr_free(col_expr);
                crate::handles::phs_expr_free(agg_expr);
            }
        }
    }

    #[test]
    fn unknown_aggregation_operator_returns_error() {
        let name = std::ffi::CString::new("salary").unwrap();
        let mut col_expr = ptr::null_mut();
        let mut out: *mut phs_expr = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(unsafe { phs_expr_col(name.as_ptr(), &mut col_expr, &mut err) }, PHS_OK);
        assert_eq!(unsafe { phs_expr_agg(99, col_expr, &mut out, &mut err) }, crate::error::PHS_INVALID_ARGUMENT);
        assert!(out.is_null());
        assert!(!err.is_null());
        unsafe {
            crate::handles::phs_expr_free(col_expr);
            crate::error::phs_error_free(err);
        }
    }
}
