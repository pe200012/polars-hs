use std::os::raw::{c_char, c_int};
use std::ptr;

use polars::prelude::*;

use crate::error::{PhsError, PhsResult, c_str_to_str, ffi_boundary, phs_error, required_mut};
use crate::handles::{dataframe_into_raw, expr_ref, lazyframe_into_raw, lazyframe_ref, phs_dataframe, phs_expr, phs_lazyframe};

unsafe fn path_string(path: *const c_char) -> PhsResult<String> {
    Ok(unsafe { c_str_to_str(path, "path") }?.to_owned())
}

unsafe fn expr_vec(exprs: *const *const phs_expr, len: usize) -> PhsResult<Vec<Expr>> {
    if exprs.is_null() && len > 0 {
        return Err(PhsError::invalid_argument("exprs pointer was null"));
    }
    let slice = if len == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(exprs, len) }
    };
    slice
        .iter()
        .map(|expr| unsafe { expr_ref(*expr) }.map(|handle| handle.value.clone()))
        .collect()
}

unsafe fn name_vec(names: *const *const c_char, len: usize) -> PhsResult<Vec<PlSmallStr>> {
    if names.is_null() && len > 0 {
        return Err(PhsError::invalid_argument("names pointer was null"));
    }
    let slice = if len == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(names, len) }
    };
    slice
        .iter()
        .map(|name| unsafe { c_str_to_str(*name, "name") }.map(PlSmallStr::from_str))
        .collect()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_scan_csv(
    path: *const c_char,
    out: *mut *mut phs_lazyframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let path = unsafe { path_string(path) }?;
        let lf = LazyCsvReader::new(PlRefPath::new(path)).finish()?;
        *out = lazyframe_into_raw(lf);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_scan_parquet(
    path: *const c_char,
    out: *mut *mut phs_lazyframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let path = unsafe { path_string(path) }?;
        let lf = LazyFrame::scan_parquet(PlRefPath::new(path), Default::default())?;
        *out = lazyframe_into_raw(lf);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_collect(
    lazyframe: *const phs_lazyframe,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let lf = unsafe { lazyframe_ref(lazyframe) }?.value.clone();
        let df = lf.collect()?;
        *out = dataframe_into_raw(df);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_filter(
    lazyframe: *const phs_lazyframe,
    predicate: *const phs_expr,
    out: *mut *mut phs_lazyframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let lf = unsafe { lazyframe_ref(lazyframe) }?.value.clone();
        let predicate = unsafe { expr_ref(predicate) }?.value.clone();
        *out = lazyframe_into_raw(lf.filter(predicate));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_select(
    lazyframe: *const phs_lazyframe,
    exprs: *const *const phs_expr,
    len: usize,
    out: *mut *mut phs_lazyframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let lf = unsafe { lazyframe_ref(lazyframe) }?.value.clone();
        let exprs = unsafe { expr_vec(exprs, len) }?;
        *out = lazyframe_into_raw(lf.select(exprs));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_with_columns(
    lazyframe: *const phs_lazyframe,
    exprs: *const *const phs_expr,
    len: usize,
    out: *mut *mut phs_lazyframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let lf = unsafe { lazyframe_ref(lazyframe) }?.value.clone();
        let exprs = unsafe { expr_vec(exprs, len) }?;
        *out = lazyframe_into_raw(lf.with_columns(exprs));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_sort(
    lazyframe: *const phs_lazyframe,
    names: *const *const c_char,
    len: usize,
    out: *mut *mut phs_lazyframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let lf = unsafe { lazyframe_ref(lazyframe) }?.value.clone();
        let names = unsafe { name_vec(names, len) }?;
        *out = lazyframe_into_raw(lf.sort(names, Default::default()));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_limit(
    lazyframe: *const phs_lazyframe,
    n: u64,
    out: *mut *mut phs_lazyframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let lf = unsafe { lazyframe_ref(lazyframe) }?.value.clone();
        let n: IdxSize = n.try_into().map_err(|_| PhsError::invalid_argument("limit exceeds Polars index size"))?;
        *out = lazyframe_into_raw(lf.limit(n));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_group_by_agg(
    lazyframe: *const phs_lazyframe,
    keys: *const *const phs_expr,
    key_len: usize,
    aggs: *const *const phs_expr,
    agg_len: usize,
    maintain_order: bool,
    out: *mut *mut phs_lazyframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let lf = unsafe { lazyframe_ref(lazyframe) }?.value.clone();
        let keys = unsafe { expr_vec(keys, key_len) }?;
        let aggs = unsafe { expr_vec(aggs, agg_len) }?;
        let grouped = if maintain_order { lf.group_by_stable(keys) } else { lf.group_by(keys) };
        *out = lazyframe_into_raw(grouped.agg(aggs));
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::PHS_OK;

    fn fixture_path() -> std::ffi::CString {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("test")
            .join("data")
            .join("people.csv");
        std::ffi::CString::new(path.to_string_lossy().as_bytes()).unwrap()
    }

    fn sales_fixture_path() -> std::ffi::CString {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("test")
            .join("data")
            .join("sales.csv");
        std::ffi::CString::new(path.to_string_lossy().as_bytes()).unwrap()
    }

    #[test]
    fn lazy_filter_select_collect_reports_shape() {
        let path = fixture_path();
        let mut lf0 = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(unsafe { phs_scan_csv(path.as_ptr(), &mut lf0, &mut err) }, PHS_OK);

        let age = std::ffi::CString::new("age").unwrap();
        let mut age_expr = ptr::null_mut();
        let mut lit_expr = ptr::null_mut();
        let mut pred_expr = ptr::null_mut();
        assert_eq!(unsafe { crate::expr::phs_expr_col(age.as_ptr(), &mut age_expr, &mut err) }, PHS_OK);
        assert_eq!(unsafe { crate::expr::phs_expr_lit_int(35, &mut lit_expr, &mut err) }, PHS_OK);
        assert_eq!(unsafe { crate::expr::phs_expr_binary(2, age_expr, lit_expr, &mut pred_expr, &mut err) }, PHS_OK);

        let mut lf1 = ptr::null_mut();
        assert_eq!(unsafe { phs_lazyframe_filter(lf0, pred_expr, &mut lf1, &mut err) }, PHS_OK);

        let name = std::ffi::CString::new("name").unwrap();
        let mut name_expr = ptr::null_mut();
        assert_eq!(unsafe { crate::expr::phs_expr_col(name.as_ptr(), &mut name_expr, &mut err) }, PHS_OK);
        let exprs = [name_expr as *const phs_expr];
        let mut lf2 = ptr::null_mut();
        assert_eq!(unsafe { phs_lazyframe_select(lf1, exprs.as_ptr(), exprs.len(), &mut lf2, &mut err) }, PHS_OK);

        let mut df = ptr::null_mut();
        assert_eq!(unsafe { phs_lazyframe_collect(lf2, &mut df, &mut err) }, PHS_OK);
        let mut height = 0;
        let mut width = 0;
        assert_eq!(unsafe { crate::dataframe::phs_dataframe_shape(df, &mut height, &mut width, &mut err) }, PHS_OK);
        assert_eq!((height, width), (1, 1));

        unsafe {
            crate::handles::phs_expr_free(age_expr);
            crate::handles::phs_expr_free(lit_expr);
            crate::handles::phs_expr_free(pred_expr);
            crate::handles::phs_expr_free(name_expr);
            crate::handles::phs_lazyframe_free(lf0);
            crate::handles::phs_lazyframe_free(lf1);
            crate::handles::phs_lazyframe_free(lf2);
            crate::handles::phs_dataframe_free(df);
        }
    }

    #[test]
    fn lazy_group_by_agg_collects_expected_shape() {
        let path = sales_fixture_path();
        let mut lf0 = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(unsafe { phs_scan_csv(path.as_ptr(), &mut lf0, &mut err) }, PHS_OK);

        let department = std::ffi::CString::new("department").unwrap();
        let mut department_expr = ptr::null_mut();
        assert_eq!(unsafe { crate::expr::phs_expr_col(department.as_ptr(), &mut department_expr, &mut err) }, PHS_OK);

        let salary = std::ffi::CString::new("salary").unwrap();
        let mut salary_expr = ptr::null_mut();
        let mut salary_sum_expr = ptr::null_mut();
        assert_eq!(unsafe { crate::expr::phs_expr_col(salary.as_ptr(), &mut salary_expr, &mut err) }, PHS_OK);
        assert_eq!(unsafe { crate::expr::phs_expr_agg(0, salary_expr, &mut salary_sum_expr, &mut err) }, PHS_OK);

        let keys = [department_expr as *const phs_expr];
        let aggs = [salary_sum_expr as *const phs_expr];
        let mut lf1: *mut phs_lazyframe = ptr::null_mut();
        assert_eq!(
            unsafe {
                phs_lazyframe_group_by_agg(
                    lf0,
                    keys.as_ptr(),
                    keys.len(),
                    aggs.as_ptr(),
                    aggs.len(),
                    true,
                    &mut lf1,
                    &mut err,
                )
            },
            PHS_OK
        );

        let mut df = ptr::null_mut();
        assert_eq!(unsafe { phs_lazyframe_collect(lf1, &mut df, &mut err) }, PHS_OK);
        let mut height = 0;
        let mut width = 0;
        assert_eq!(unsafe { crate::dataframe::phs_dataframe_shape(df, &mut height, &mut width, &mut err) }, PHS_OK);
        assert_eq!((height, width), (2, 2));

        unsafe {
            crate::handles::phs_expr_free(department_expr);
            crate::handles::phs_expr_free(salary_expr);
            crate::handles::phs_expr_free(salary_sum_expr);
            crate::handles::phs_lazyframe_free(lf0);
            crate::handles::phs_lazyframe_free(lf1);
            crate::handles::phs_dataframe_free(df);
        }
    }

    #[test]
    fn lazy_group_by_agg_rejects_null_key_array_with_positive_length() {
        let path = sales_fixture_path();
        let mut lf0 = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(unsafe { phs_scan_csv(path.as_ptr(), &mut lf0, &mut err) }, PHS_OK);

        let salary = std::ffi::CString::new("salary").unwrap();
        let mut salary_expr = ptr::null_mut();
        let mut salary_sum_expr = ptr::null_mut();
        assert_eq!(unsafe { crate::expr::phs_expr_col(salary.as_ptr(), &mut salary_expr, &mut err) }, PHS_OK);
        assert_eq!(unsafe { crate::expr::phs_expr_agg(0, salary_expr, &mut salary_sum_expr, &mut err) }, PHS_OK);

        let aggs = [salary_sum_expr as *const phs_expr];
        let mut lf1: *mut phs_lazyframe = ptr::null_mut();
        let status = unsafe { phs_lazyframe_group_by_agg(lf0, ptr::null(), 1, aggs.as_ptr(), aggs.len(), false, &mut lf1, &mut err) };
        assert_eq!(status, crate::error::PHS_INVALID_ARGUMENT);
        assert!(lf1.is_null());
        assert!(!err.is_null());

        unsafe {
            crate::error::phs_error_free(err);
            crate::handles::phs_expr_free(salary_expr);
            crate::handles::phs_expr_free(salary_sum_expr);
            crate::handles::phs_lazyframe_free(lf0);
        }
    }
}
