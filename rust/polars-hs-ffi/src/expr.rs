use std::os::raw::{c_char, c_double, c_int};
use std::ptr;

use polars::prelude::*;

use crate::error::{c_str_to_str, ffi_boundary, phs_error, required_mut, PhsError, PhsResult};
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
            _ => {
                return Err(PhsError::invalid_argument(format!(
                    "unknown binary operator code {op}"
                )))
            }
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
            _ => {
                return Err(PhsError::invalid_argument(format!(
                    "unknown aggregation operator code {op}"
                )))
            }
        };
        *out = expr_into_raw(agg);
        Ok(())
    })
}

fn dtype_from_code(code: c_int) -> PhsResult<DataType> {
    match code {
        0 => Ok(DataType::Boolean),
        1 => Ok(DataType::Int8),
        2 => Ok(DataType::Int16),
        3 => Ok(DataType::Int32),
        4 => Ok(DataType::Int64),
        5 => Ok(DataType::UInt8),
        6 => Ok(DataType::UInt16),
        7 => Ok(DataType::UInt32),
        8 => Ok(DataType::UInt64),
        9 => Ok(DataType::Float32),
        10 => Ok(DataType::Float64),
        11 => Ok(DataType::String),
        12 => Ok(DataType::Date),
        13 => Ok(DataType::Datetime(TimeUnit::Milliseconds, None)),
        14 => Ok(DataType::Duration(TimeUnit::Milliseconds)),
        15 => Ok(DataType::Time),
        16 => Ok(DataType::Binary),
        17 => Ok(DataType::Null),
        value => Err(PhsError::invalid_argument(format!(
            "unknown dtype code {value}"
        ))),
    }
}

fn quantile_method_from_code(code: c_int) -> PhsResult<QuantileMethod> {
    match code {
        0 => Ok(QuantileMethod::Nearest),
        1 => Ok(QuantileMethod::Lower),
        2 => Ok(QuantileMethod::Higher),
        3 => Ok(QuantileMethod::Midpoint),
        4 => Ok(QuantileMethod::Linear),
        5 => Ok(QuantileMethod::Equiprobable),
        value => Err(PhsError::invalid_argument(format!(
            "unknown quantile method code {value}"
        ))),
    }
}

fn rank_method_from_code(code: c_int) -> PhsResult<RankMethod> {
    match code {
        0 => Ok(RankMethod::Average),
        1 => Ok(RankMethod::Min),
        2 => Ok(RankMethod::Max),
        3 => Ok(RankMethod::Dense),
        4 => Ok(RankMethod::Ordinal),
        value => Err(PhsError::invalid_argument(format!(
            "unknown rank method code {value}"
        ))),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_cast(
    strict: bool,
    dtype_code: c_int,
    expr: *const phs_expr,
    out: *mut *mut phs_expr,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let expr = unsafe { expr_ref(expr) }?.value.clone();
        let dtype = dtype_from_code(dtype_code)?;
        if strict {
            *out = expr_into_raw(expr.strict_cast(dtype));
        } else {
            *out = expr_into_raw(expr.cast(dtype));
        }
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_unary(
    op: c_int,
    expr: *const phs_expr,
    out: *mut *mut phs_expr,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let expr = unsafe { expr_ref(expr) }?.value.clone();
        let result = match op {
            0 => expr.is_null(),
            1 => expr.is_not_null(),
            2 => expr.is_nan(),
            3 => expr.is_not_nan(),
            4 => expr.is_finite(),
            5 => expr.is_infinite(),
            6 => expr.median(),
            7 => expr.n_unique(),
            8 => expr.cum_count(false),
            9 => expr.cum_count(true),
            10 => expr.cum_sum(false),
            11 => expr.cum_sum(true),
            12 => expr.cum_prod(false),
            13 => expr.cum_prod(true),
            14 => expr.cum_min(false),
            15 => expr.cum_min(true),
            16 => expr.cum_max(false),
            17 => expr.cum_max(true),
            _ => {
                return Err(PhsError::invalid_argument(format!(
                    "unknown unary expression opcode {op}"
                )))
            }
        };
        *out = expr_into_raw(result);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_unary_i64(
    op: c_int,
    arg: i64,
    expr: *const phs_expr,
    out: *mut *mut phs_expr,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let expr = unsafe { expr_ref(expr) }?.value.clone();
        let result = match op {
            0 | 1 => {
                if arg < 0 || arg > 255 {
                    return Err(PhsError::invalid_argument(format!(
                        "ddof {arg} is out of range, must be 0..=255"
                    )));
                }
                let ddof = arg as u8;
                match op {
                    0 => expr.std(ddof),
                    1 => expr.var(ddof),
                    _ => unreachable!(),
                }
            }
            _ => {
                return Err(PhsError::invalid_argument(format!(
                    "unknown unary_i64 expression opcode {op}"
                )))
            }
        };
        *out = expr_into_raw(result);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_binary_function(
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
        let result = match op {
            0 => left.fill_null(right),
            1 => left.fill_nan(right),
            2 => left.filter(right),
            _ => {
                return Err(PhsError::invalid_argument(format!(
                    "unknown binary function opcode {op}"
                )))
            }
        };
        *out = expr_into_raw(result);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_ternary(
    predicate: *const phs_expr,
    truthy: *const phs_expr,
    falsy: *const phs_expr,
    out: *mut *mut phs_expr,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let predicate = unsafe { expr_ref(predicate) }?.value.clone();
        let truthy = unsafe { expr_ref(truthy) }?.value.clone();
        let falsy = unsafe { expr_ref(falsy) }?.value.clone();
        *out = expr_into_raw(when(predicate).then(truthy).otherwise(falsy));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_quantile(
    method: c_int,
    quantile: *const phs_expr,
    expr: *const phs_expr,
    out: *mut *mut phs_expr,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let expr = unsafe { expr_ref(expr) }?.value.clone();
        let quantile = unsafe { expr_ref(quantile) }?.value.clone();
        let method = quantile_method_from_code(method)?;
        *out = expr_into_raw(expr.quantile(quantile, method));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_rank(
    method: c_int,
    descending: bool,
    expr: *const phs_expr,
    out: *mut *mut phs_expr,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let expr = unsafe { expr_ref(expr) }?.value.clone();
        let method = rank_method_from_code(method)?;
        *out = expr_into_raw(expr.rank(RankOptions { method, descending }, None));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_slice(
    expr: *const phs_expr,
    offset: *const phs_expr,
    length: *const phs_expr,
    out: *mut *mut phs_expr,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let expr = unsafe { expr_ref(expr) }?.value.clone();
        let offset = unsafe { expr_ref(offset) }?.value.clone();
        let length = unsafe { expr_ref(length) }?.value.clone();
        *out = expr_into_raw(expr.slice(offset, length));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_sort_by(
    expr: *const phs_expr,
    by: *const *const phs_expr,
    by_len: usize,
    descending: bool,
    nulls_last: bool,
    multithreaded: bool,
    maintain_order: bool,
    out: *mut *mut phs_expr,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        if by.is_null() && by_len > 0 {
            return Err(PhsError::invalid_argument("by pointer was null"));
        }
        let expr = unsafe { expr_ref(expr) }?.value.clone();
        let mut by_exprs = Vec::with_capacity(by_len);
        for i in 0..by_len {
            let by_ptr = unsafe { *by.add(i) };
            let by_handle = unsafe { expr_ref(by_ptr) }?;
            by_exprs.push(by_handle.value.clone());
        }
        let options = SortMultipleOptions::default()
            .with_order_descending(descending)
            .with_nulls_last(nulls_last)
            .with_multithreaded(multithreaded)
            .with_maintain_order(maintain_order);
        *out = expr_into_raw(expr.sort_by(by_exprs, options));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_over(
    expr: *const phs_expr,
    partition_by: *const *const phs_expr,
    partition_len: usize,
    out: *mut *mut phs_expr,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        if partition_by.is_null() && partition_len > 0 {
            return Err(PhsError::invalid_argument("partition_by pointer was null"));
        }
        let expr = unsafe { expr_ref(expr) }?.value.clone();
        let mut partitions = Vec::with_capacity(partition_len);
        for i in 0..partition_len {
            let p_ptr = unsafe { *partition_by.add(i) };
            let p_handle = unsafe { expr_ref(p_ptr) }?;
            partitions.push(p_handle.value.clone());
        }
        *out = expr_into_raw(expr.over(partitions));
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{PHS_INVALID_ARGUMENT, PHS_OK};

    #[test]
    fn builds_binary_expression() {
        let name = std::ffi::CString::new("age").unwrap();
        let mut col_expr = ptr::null_mut();
        let mut lit_expr = ptr::null_mut();
        let mut out: *mut phs_expr = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_col(name.as_ptr(), &mut col_expr, &mut err) },
            PHS_OK
        );
        assert_eq!(
            unsafe { phs_expr_lit_int(35, &mut lit_expr, &mut err) },
            PHS_OK
        );
        assert_eq!(
            unsafe { phs_expr_binary(2, col_expr, lit_expr, &mut out, &mut err) },
            PHS_OK
        );
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
            assert_eq!(
                unsafe { phs_expr_col(name.as_ptr(), &mut col_expr, &mut err) },
                PHS_OK
            );
            assert_eq!(
                unsafe { phs_expr_agg(op, col_expr, &mut agg_expr, &mut err) },
                PHS_OK
            );
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
        assert_eq!(
            unsafe { phs_expr_col(name.as_ptr(), &mut col_expr, &mut err) },
            PHS_OK
        );
        assert_eq!(
            unsafe { phs_expr_agg(99, col_expr, &mut out, &mut err) },
            crate::error::PHS_INVALID_ARGUMENT
        );
        assert!(out.is_null());
        assert!(!err.is_null());
        unsafe {
            crate::handles::phs_expr_free(col_expr);
            crate::error::phs_error_free(err);
        }
    }

    #[test]
    fn builds_cast_and_unary_expressions() {
        let name = std::ffi::CString::new("age").unwrap();
        let mut col_expr = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_col(name.as_ptr(), &mut col_expr, &mut err) },
            PHS_OK
        );
        assert!(!col_expr.is_null());

        // cast to Float64 (code 10)
        let mut cast_expr = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_cast(false, 10, col_expr, &mut cast_expr, &mut err) },
            PHS_OK
        );
        assert!(!cast_expr.is_null());

        // is_not_null (op 1)
        let mut not_null_expr = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_unary(1, col_expr, &mut not_null_expr, &mut err) },
            PHS_OK
        );
        assert!(!not_null_expr.is_null());

        // is_null (op 0)
        let mut null_expr = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_unary(0, col_expr, &mut null_expr, &mut err) },
            PHS_OK
        );
        assert!(!null_expr.is_null());

        // cum_sum (op 10)
        let mut cum_sum_expr = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_unary(10, col_expr, &mut cum_sum_expr, &mut err) },
            PHS_OK
        );
        assert!(!cum_sum_expr.is_null());

        // strict_cast to Int32 (code 3)
        let mut strict_cast_expr = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_cast(true, 3, col_expr, &mut strict_cast_expr, &mut err) },
            PHS_OK
        );
        assert!(!strict_cast_expr.is_null());

        unsafe {
            crate::handles::phs_expr_free(col_expr);
            crate::handles::phs_expr_free(cast_expr);
            crate::handles::phs_expr_free(not_null_expr);
            crate::handles::phs_expr_free(null_expr);
            crate::handles::phs_expr_free(cum_sum_expr);
            crate::handles::phs_expr_free(strict_cast_expr);
        }
    }

    #[test]
    fn builds_ternary_quantile_rank_and_window_expressions() {
        let name = std::ffi::CString::new("age").unwrap();
        let mut col_expr = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_col(name.as_ptr(), &mut col_expr, &mut err) },
            PHS_OK
        );

        // ternary: when(col("age") > lit(30)).then(lit(1)).otherwise(lit(0))
        let predicate_name = std::ffi::CString::new("age").unwrap();
        let mut predicate = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_col(predicate_name.as_ptr(), &mut predicate, &mut err) },
            PHS_OK
        );
        let mut thirty = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_lit_int(30, &mut thirty, &mut err) },
            PHS_OK
        );
        let mut cond = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_binary(2, predicate, thirty, &mut cond, &mut err) },
            PHS_OK
        );
        let mut one_lit = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_lit_int(1, &mut one_lit, &mut err) },
            PHS_OK
        );
        let mut zero_lit = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_lit_int(0, &mut zero_lit, &mut err) },
            PHS_OK
        );
        let mut ternary_expr = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_ternary(cond, one_lit, zero_lit, &mut ternary_expr, &mut err) },
            PHS_OK
        );
        assert!(!ternary_expr.is_null());

        // quantile: col("age").quantile(lit(0.5), Nearest)
        let mut half = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_lit_double(0.5, &mut half, &mut err) },
            PHS_OK
        );
        let mut quantile_expr = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_quantile(0, half, col_expr, &mut quantile_expr, &mut err) },
            PHS_OK
        );
        assert!(!quantile_expr.is_null());

        // rank: col("age").rank(Average, descending=false)
        let mut rank_expr = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_rank(0, false, col_expr, &mut rank_expr, &mut err) },
            PHS_OK
        );
        assert!(!rank_expr.is_null());

        // slice: col("age").slice(lit(0), lit(2))
        let mut offset_expr = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_lit_int(0, &mut offset_expr, &mut err) },
            PHS_OK
        );
        let mut len_expr = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_lit_int(2, &mut len_expr, &mut err) },
            PHS_OK
        );
        let mut slice_expr = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_slice(col_expr, offset_expr, len_expr, &mut slice_expr, &mut err) },
            PHS_OK
        );
        assert!(!slice_expr.is_null());

        // sort_by: col("age").sort_by([col("age")], descending=true, nulls_last=true)
        let sort_cols = [col_expr as *const phs_expr];
        let mut sort_by_expr = ptr::null_mut();
        assert_eq!(
            unsafe {
                phs_expr_sort_by(
                    col_expr,
                    sort_cols.as_ptr(),
                    1,
                    true,
                    true,
                    true,
                    false,
                    &mut sort_by_expr,
                    &mut err,
                )
            },
            PHS_OK
        );
        assert!(!sort_by_expr.is_null());

        // over: col("age").over([col("name")])
        let n_name = std::ffi::CString::new("name").unwrap();
        let mut name_expr = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_col(n_name.as_ptr(), &mut name_expr, &mut err) },
            PHS_OK
        );
        let partitions = [name_expr as *const phs_expr];
        let mut over_expr = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_over(col_expr, partitions.as_ptr(), 1, &mut over_expr, &mut err) },
            PHS_OK
        );
        assert!(!over_expr.is_null());

        // fill_null (op 0 binary_function)
        let mut fill_null_expr = ptr::null_mut();
        assert_eq!(
            unsafe {
                phs_expr_binary_function(0, col_expr, zero_lit, &mut fill_null_expr, &mut err)
            },
            PHS_OK
        );
        assert!(!fill_null_expr.is_null());

        // fill_nan (op 1 binary_function)
        let mut fill_nan_expr = ptr::null_mut();
        assert_eq!(
            unsafe {
                phs_expr_binary_function(1, col_expr, zero_lit, &mut fill_nan_expr, &mut err)
            },
            PHS_OK
        );
        assert!(!fill_nan_expr.is_null());

        // std (unary_i64 op 0)
        let mut std_expr = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_unary_i64(0, 1, col_expr, &mut std_expr, &mut err) },
            PHS_OK
        );
        assert!(!std_expr.is_null());

        // var (unary_i64 op 1)
        let mut var_expr = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_unary_i64(1, 0, col_expr, &mut var_expr, &mut err) },
            PHS_OK
        );
        assert!(!var_expr.is_null());

        unsafe {
            crate::handles::phs_expr_free(col_expr);
            crate::handles::phs_expr_free(predicate);
            crate::handles::phs_expr_free(thirty);
            crate::handles::phs_expr_free(cond);
            crate::handles::phs_expr_free(one_lit);
            crate::handles::phs_expr_free(zero_lit);
            crate::handles::phs_expr_free(ternary_expr);
            crate::handles::phs_expr_free(half);
            crate::handles::phs_expr_free(quantile_expr);
            crate::handles::phs_expr_free(rank_expr);
            crate::handles::phs_expr_free(offset_expr);
            crate::handles::phs_expr_free(len_expr);
            crate::handles::phs_expr_free(slice_expr);
            crate::handles::phs_expr_free(sort_by_expr);
            crate::handles::phs_expr_free(name_expr);
            crate::handles::phs_expr_free(over_expr);
            crate::handles::phs_expr_free(fill_null_expr);
            crate::handles::phs_expr_free(fill_nan_expr);
            crate::handles::phs_expr_free(std_expr);
            crate::handles::phs_expr_free(var_expr);
        }
    }

    #[test]
    fn unknown_expression_opcode_returns_error() {
        let name = std::ffi::CString::new("age").unwrap();
        let mut col_expr = ptr::null_mut();
        let mut out: *mut phs_expr = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_col(name.as_ptr(), &mut col_expr, &mut err) },
            PHS_OK
        );
        assert_eq!(
            unsafe { phs_expr_unary(999, col_expr, &mut out, &mut err) },
            PHS_INVALID_ARGUMENT
        );
        assert!(out.is_null());
        assert!(!err.is_null());
        unsafe {
            crate::handles::phs_expr_free(col_expr);
            crate::error::phs_error_free(err);
        }
    }

    #[test]
    fn unknown_dtype_code_returns_error() {
        let name = std::ffi::CString::new("age").unwrap();
        let mut col_expr = ptr::null_mut();
        let mut out: *mut phs_expr = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_col(name.as_ptr(), &mut col_expr, &mut err) },
            PHS_OK
        );
        assert_eq!(
            unsafe { phs_expr_cast(false, 99, col_expr, &mut out, &mut err) },
            PHS_INVALID_ARGUMENT
        );
        assert!(out.is_null());
        assert!(!err.is_null());
        unsafe {
            crate::handles::phs_expr_free(col_expr);
            crate::error::phs_error_free(err);
        }
    }

    #[test]
    fn unknown_quantile_method_code_returns_error() {
        let name = std::ffi::CString::new("age").unwrap();
        let mut col_expr = ptr::null_mut();
        let mut half = ptr::null_mut();
        let mut out: *mut phs_expr = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_col(name.as_ptr(), &mut col_expr, &mut err) },
            PHS_OK
        );
        assert_eq!(
            unsafe { phs_expr_lit_double(0.5, &mut half, &mut err) },
            PHS_OK
        );
        assert_eq!(
            unsafe { phs_expr_quantile(99, half, col_expr, &mut out, &mut err) },
            PHS_INVALID_ARGUMENT
        );
        assert!(out.is_null());
        assert!(!err.is_null());
        unsafe {
            crate::handles::phs_expr_free(col_expr);
            crate::handles::phs_expr_free(half);
            crate::error::phs_error_free(err);
        }
    }

    #[test]
    fn unknown_rank_method_code_returns_error() {
        let name = std::ffi::CString::new("age").unwrap();
        let mut col_expr = ptr::null_mut();
        let mut out: *mut phs_expr = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_col(name.as_ptr(), &mut col_expr, &mut err) },
            PHS_OK
        );
        assert_eq!(
            unsafe { phs_expr_rank(99, false, col_expr, &mut out, &mut err) },
            PHS_INVALID_ARGUMENT
        );
        assert!(out.is_null());
        assert!(!err.is_null());
        unsafe {
            crate::handles::phs_expr_free(col_expr);
            crate::error::phs_error_free(err);
        }
    }

    #[test]
    fn unknown_unary_i64_opcode_returns_error() {
        let name = std::ffi::CString::new("age").unwrap();
        let mut col_expr = ptr::null_mut();
        let mut out: *mut phs_expr = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_col(name.as_ptr(), &mut col_expr, &mut err) },
            PHS_OK
        );
        assert_eq!(
            unsafe { phs_expr_unary_i64(99, 1, col_expr, &mut out, &mut err) },
            PHS_INVALID_ARGUMENT
        );
        assert!(out.is_null());
        assert!(!err.is_null());
        unsafe {
            crate::handles::phs_expr_free(col_expr);
            crate::error::phs_error_free(err);
        }
    }

    #[test]
    fn invalid_ddof_returns_error() {
        let name = std::ffi::CString::new("age").unwrap();
        let mut col_expr = ptr::null_mut();
        let mut out: *mut phs_expr = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_col(name.as_ptr(), &mut col_expr, &mut err) },
            PHS_OK
        );
        assert_eq!(
            unsafe { phs_expr_unary_i64(0, -1, col_expr, &mut out, &mut err) },
            PHS_INVALID_ARGUMENT
        );
        assert!(out.is_null());
        assert!(!err.is_null());
        unsafe { crate::error::phs_error_free(err) };

        err = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_unary_i64(1, 256, col_expr, &mut out, &mut err) },
            PHS_INVALID_ARGUMENT
        );
        assert!(out.is_null());
        assert!(!err.is_null());
        unsafe {
            crate::handles::phs_expr_free(col_expr);
            crate::error::phs_error_free(err);
        }
    }

    #[test]
    fn unknown_binary_function_opcode_returns_error() {
        let name = std::ffi::CString::new("age").unwrap();
        let mut col_expr = ptr::null_mut();
        let mut lit_expr = ptr::null_mut();
        let mut out: *mut phs_expr = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_col(name.as_ptr(), &mut col_expr, &mut err) },
            PHS_OK
        );
        assert_eq!(
            unsafe { phs_expr_lit_int(0, &mut lit_expr, &mut err) },
            PHS_OK
        );
        assert_eq!(
            unsafe { phs_expr_binary_function(99, col_expr, lit_expr, &mut out, &mut err) },
            PHS_INVALID_ARGUMENT
        );
        assert!(out.is_null());
        assert!(!err.is_null());
        unsafe {
            crate::handles::phs_expr_free(col_expr);
            crate::handles::phs_expr_free(lit_expr);
            crate::error::phs_error_free(err);
        }
    }
}
