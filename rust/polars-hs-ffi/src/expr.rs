use std::os::raw::{c_char, c_double, c_int};
use std::ptr;

use polars::prelude::*;
use polars_plan::dsl::functions::{
    all_horizontal, any_horizontal, coalesce as polars_coalesce, concat_str,
    format_str, max_horizontal, mean_horizontal, min_horizontal,
    sum_horizontal,
};

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
                if !(0..=255).contains(&arg) {
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

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_string_function(
    op: c_int,
    expr: *const phs_expr,
    args: *const *const phs_expr,
    arg_len: usize,
    out: *mut *mut phs_expr,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let expr = unsafe { expr_ref(expr) }?.value.clone();
        let args = expr_args(args, arg_len)?;
        let result = match op {
            0 => {
                require_expr_arity("string", op, &args, 1)?;
                expr.str().contains_literal(args[0].clone())
            }
            1 => {
                require_expr_arity("string", op, &args, 1)?;
                expr.str().starts_with(args[0].clone())
            }
            2 => {
                require_expr_arity("string", op, &args, 1)?;
                expr.str().ends_with(args[0].clone())
            }
            3 => {
                require_expr_arity("string", op, &args, 1)?;
                expr.str().strip_chars(args[0].clone())
            }
            4 => {
                require_expr_arity("string", op, &args, 1)?;
                expr.str().strip_chars_start(args[0].clone())
            }
            5 => {
                require_expr_arity("string", op, &args, 1)?;
                expr.str().strip_chars_end(args[0].clone())
            }
            6 => {
                require_expr_arity("string", op, &args, 0)?;
                expr.str().to_lowercase()
            }
            7 => {
                require_expr_arity("string", op, &args, 0)?;
                expr.str().to_uppercase()
            }
            8 => {
                require_expr_arity("string", op, &args, 0)?;
                expr.str().len_bytes()
            }
            9 => {
                require_expr_arity("string", op, &args, 0)?;
                expr.str().len_chars()
            }
            10 => {
                require_expr_arity("string", op, &args, 2)?;
                expr.str().slice(args[0].clone(), args[1].clone())
            }
            11 => {
                require_expr_arity("string", op, &args, 1)?;
                expr.str().head(args[0].clone())
            }
            12 => {
                require_expr_arity("string", op, &args, 1)?;
                expr.str().tail(args[0].clone())
            }
            13 => {
                require_expr_arity("string", op, &args, 1)?;
                expr.str().contains(args[0].clone(), false)
            }
            14 => {
                require_expr_arity("string", op, &args, 1)?;
                expr.str().contains(args[0].clone(), true)
            }
            15 => {
                require_expr_arity("string", op, &args, 1)?;
                expr.str().find_literal(args[0].clone())
            }
            16 => {
                require_expr_arity("string", op, &args, 1)?;
                expr.str().find(args[0].clone(), false)
            }
            17 => {
                require_expr_arity("string", op, &args, 1)?;
                expr.str().find(args[0].clone(), true)
            }
            18 => {
                require_expr_arity("string", op, &args, 1)?;
                expr.str().count_matches(args[0].clone(), false)
            }
            19 => {
                require_expr_arity("string", op, &args, 1)?;
                expr.str().count_matches(args[0].clone(), true)
            }
            20 => {
                require_expr_arity("string", op, &args, 2)?;
                expr.str().replace(args[0].clone(), args[1].clone(), false)
            }
            21 => {
                require_expr_arity("string", op, &args, 2)?;
                expr.str().replace(args[0].clone(), args[1].clone(), true)
            }
            22 => {
                require_expr_arity("string", op, &args, 2)?;
                expr.str().replace_all(args[0].clone(), args[1].clone(), false)
            }
            23 => {
                require_expr_arity("string", op, &args, 2)?;
                expr.str().replace_all(args[0].clone(), args[1].clone(), true)
            }
            24 => {
                require_expr_arity("string", op, &args, 1)?;
                expr.str().split(args[0].clone())
            }
            25 => {
                require_expr_arity("string", op, &args, 1)?;
                expr.str().split_inclusive(args[0].clone())
            }
            26 => {
                require_expr_arity("string", op, &args, 1)?;
                expr.str().strip_prefix(args[0].clone())
            }
            27 => {
                require_expr_arity("string", op, &args, 1)?;
                expr.str().strip_suffix(args[0].clone())
            }
            28 => {
                require_expr_arity("string", op, &args, 0)?;
                expr.str().escape_regex()
            }
            29 => {
                require_expr_arity("string", op, &args, 1)?;
                expr.str().extract_all(args[0].clone())
            }
            _ => {
                return Err(PhsError::invalid_argument(format!(
                    "unknown string expression opcode {op}"
                )))
            }
        };
        *out = expr_into_raw(result);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_string_function_i64(
    op: c_int,
    arg: i64,
    expr: *const phs_expr,
    args: *const *const phs_expr,
    arg_len: usize,
    out: *mut *mut phs_expr,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let expr = unsafe { expr_ref(expr) }?.value.clone();
        let args = expr_args(args, arg_len)?;
        let result = match op {
            0 => {
                require_expr_arity("string", op, &args, 1)?;
                if arg < 0 {
                    return Err(PhsError::invalid_argument(format!(
                        "string extract group index {arg} is negative"
                    )));
                }
                expr.str().extract(args[0].clone(), arg as usize)
            }
            _ => {
                return Err(PhsError::invalid_argument(format!(
                    "unknown string_i64 expression opcode {op}"
                )))
            }
        };
        *out = expr_into_raw(result);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_list_function(
    op: c_int,
    expr: *const phs_expr,
    args: *const *const phs_expr,
    arg_len: usize,
    out: *mut *mut phs_expr,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let expr = unsafe { expr_ref(expr) }?.value.clone();
        let args = expr_args(args, arg_len)?;
        let result = match op {
            0 => {
                require_expr_arity("list", op, &args, 0)?;
                expr.list().len()
            }
            1 => {
                require_expr_arity("list", op, &args, 0)?;
                expr.list().first()
            }
            2 => {
                require_expr_arity("list", op, &args, 0)?;
                expr.list().last()
            }
            3 => {
                require_expr_arity("list", op, &args, 1)?;
                expr.list().get(args[0].clone(), false)
            }
            4 => {
                require_expr_arity("list", op, &args, 1)?;
                expr.list().get(args[0].clone(), true)
            }
            5 => {
                require_expr_arity("list", op, &args, 1)?;
                expr.list().join(args[0].clone(), false)
            }
            6 => {
                require_expr_arity("list", op, &args, 1)?;
                expr.list().join(args[0].clone(), true)
            }
            7 => {
                require_expr_arity("list", op, &args, 1)?;
                expr.list().contains(args[0].clone(), false)
            }
            8 => {
                require_expr_arity("list", op, &args, 1)?;
                expr.list().contains(args[0].clone(), true)
            }
            9 => {
                require_expr_arity("list", op, &args, 1)?;
                expr.list().count_matches(args[0].clone())
            }
            _ => {
                return Err(PhsError::invalid_argument(format!(
                    "unknown list expression opcode {op}"
                )))
            }
        };
        *out = expr_into_raw(result);
        Ok(())
    })
}

fn expr_args(args: *const *const phs_expr, len: usize) -> PhsResult<Vec<Expr>> {
    if args.is_null() && len > 0 {
        return Err(PhsError::invalid_argument("expression args pointer was null"));
    }
    let slice = if len == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(args, len) }
    };
    slice
        .iter()
        .map(|expr| unsafe { expr_ref(*expr) }.map(|handle| handle.value.clone()))
        .collect()
}

fn require_expr_arity(namespace: &str, op: c_int, args: &[Expr], expected: usize) -> PhsResult<()> {
    if args.len() == expected {
        Ok(())
    } else {
        Err(PhsError::invalid_argument(format!(
            "{namespace} expression opcode {op} expected {expected} args, got {}",
            args.len()
        )))
    }
}

fn time_unit_from_code(code: c_int) -> PhsResult<TimeUnit> {
    match code {
        0 => Ok(TimeUnit::Milliseconds),
        1 => Ok(TimeUnit::Microseconds),
        2 => Ok(TimeUnit::Nanoseconds),
        value => Err(PhsError::invalid_argument(format!(
            "unknown time unit code {value}"
        ))),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_temporal_function(
    op: c_int,
    expr: *const phs_expr,
    out: *mut *mut phs_expr,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let expr = unsafe { expr_ref(expr) }?.value.clone();
        let dt = expr.dt();
        let result = match op {
            0 => dt.year(),
            1 => dt.iso_year(),
            2 => dt.quarter(),
            3 => dt.month(),
            4 => dt.week(),
            5 => dt.weekday(),
            6 => dt.day(),
            7 => dt.ordinal_day(),
            8 => dt.hour(),
            9 => dt.minute(),
            10 => dt.second(),
            11 => dt.millisecond(),
            12 => dt.microsecond(),
            13 => dt.nanosecond(),
            14 => dt.millennium(),
            15 => dt.century(),
            16 => dt.days_in_month(),
            17 => dt.is_leap_year(),
            value => {
                return Err(PhsError::invalid_argument(format!(
                    "unknown temporal function opcode {value}"
                )))
            }
        };
        *out = expr_into_raw(result);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_temporal_time_unit(
    op: c_int,
    time_unit: c_int,
    expr: *const phs_expr,
    out: *mut *mut phs_expr,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let expr = unsafe { expr_ref(expr) }?.value.clone();
        match op {
            0 => {
                let tu = time_unit_from_code(time_unit)?;
                *out = expr_into_raw(expr.dt().timestamp(tu));
            }
            _ => {
                return Err(PhsError::invalid_argument(format!(
                    "unknown temporal time_unit opcode {op}"
                )))
            }
        }
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_temporal_string(
    op: c_int,
    value: *const c_char,
    expr: *const phs_expr,
    out: *mut *mut phs_expr,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let expr = unsafe { expr_ref(expr) }?.value.clone();
        match op {
            0 => {
                let format = unsafe { c_str_to_str(value, "format") }?;
                *out = expr_into_raw(expr.dt().to_string(format));
            }
            _ => {
                return Err(PhsError::invalid_argument(format!(
                    "unknown temporal string opcode {op}"
                )))
            }
        }
        Ok(())
    })
}

fn closed_interval_from_code(code: c_int) -> PhsResult<ClosedInterval> {
    match code {
        0 => Ok(ClosedInterval::Both),
        1 => Ok(ClosedInterval::Left),
        2 => Ok(ClosedInterval::Right),
        3 => Ok(ClosedInterval::None),
        _ => Err(PhsError::invalid_argument(format!(
            "unknown closed interval code {code}"
        ))),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_boolean_unary(
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
            0 => expr.is_duplicated(),
            1 => expr.is_unique(),
            2 => expr.is_first_distinct(),
            3 => expr.is_last_distinct(),
            _ => {
                return Err(PhsError::invalid_argument(format!(
                    "unknown boolean unary opcode {op}"
                )))
            }
        };
        *out = expr_into_raw(result);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_is_between(
    closed: c_int,
    expr: *const phs_expr,
    lower: *const phs_expr,
    upper: *const phs_expr,
    out: *mut *mut phs_expr,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let expr = unsafe { expr_ref(expr) }?.value.clone();
        let lower = unsafe { expr_ref(lower) }?.value.clone();
        let upper = unsafe { expr_ref(upper) }?.value.clone();
        let closed = closed_interval_from_code(closed)?;
        *out = expr_into_raw(expr.is_between(lower, upper, closed));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_is_close(
    abs_tol: c_double,
    rel_tol: c_double,
    nans_equal: bool,
    expr: *const phs_expr,
    other: *const phs_expr,
    out: *mut *mut phs_expr,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let expr = unsafe { expr_ref(expr) }?.value.clone();
        let other = unsafe { expr_ref(other) }?.value.clone();
        *out = expr_into_raw(expr.is_close(other, abs_tol, rel_tol, nans_equal));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_is_in(
    nulls_equal: bool,
    expr: *const phs_expr,
    other: *const phs_expr,
    out: *mut *mut phs_expr,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let expr = unsafe { expr_ref(expr) }?.value.clone();
        let other = unsafe { expr_ref(other) }?.value.clone();
        *out = expr_into_raw(expr.is_in(other, nulls_equal));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_clip(
    op: c_int,
    expr: *const phs_expr,
    args: *const *const phs_expr,
    arg_len: usize,
    out: *mut *mut phs_expr,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let expr = unsafe { expr_ref(expr) }?.value.clone();
        let args = expr_args(args, arg_len)?;
        let result = match op {
            0 => {
                require_expr_arity("clip", op, &args, 2)?;
                expr.clip(args[0].clone(), args[1].clone())
            }
            1 => {
                require_expr_arity("clip_min", op, &args, 1)?;
                expr.clip_min(args[0].clone())
            }
            2 => {
                require_expr_arity("clip_max", op, &args, 1)?;
                expr.clip_max(args[0].clone())
            }
            _ => {
                return Err(PhsError::invalid_argument(format!(
                    "unknown clip opcode {op}"
                )))
            }
        };
        *out = expr_into_raw(result);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_horizontal_function(
    op: c_int,
    flag: bool,
    exprs: *const *const phs_expr,
    len: usize,
    out: *mut *mut phs_expr,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let exprs = expr_args(exprs, len)?;
        if exprs.is_empty() {
            return Err(PhsError::invalid_argument(
                "horizontal function requires at least one expression",
            ));
        }
        let result = match op {
            0 => sum_horizontal(&exprs, flag).map_err(PhsError::from)?,
            1 => mean_horizontal(&exprs, flag).map_err(PhsError::from)?,
            2 => max_horizontal(&exprs).map_err(PhsError::from)?,
            3 => min_horizontal(&exprs).map_err(PhsError::from)?,
            4 => any_horizontal(&exprs).map_err(PhsError::from)?,
            5 => all_horizontal(&exprs).map_err(PhsError::from)?,
            6 => polars_coalesce(&exprs),
            _ => {
                return Err(PhsError::invalid_argument(format!(
                    "unknown horizontal function opcode {op}"
                )))
            }
        };
        *out = expr_into_raw(result);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_name_function(
    op: c_int,
    first: *const c_char,
    second: *const c_char,
    flag: bool,
    expr: *const phs_expr,
    out: *mut *mut phs_expr,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let expr = unsafe { expr_ref(expr) }?.value.clone();
        let result = match op {
            0 => {
                // keep — ignores strings and flag
                expr.name().keep()
            }
            1 => {
                // prefix — requires `first`
                let prefix = unsafe { c_str_to_str(first, "first") }?;
                expr.name().prefix(prefix)
            }
            2 => {
                // suffix — requires `first`
                let suffix = unsafe { c_str_to_str(first, "first") }?;
                expr.name().suffix(suffix)
            }
            3 => {
                // replace — requires `first` pattern and `second` value; `flag` is literal
                let pattern = unsafe { c_str_to_str(first, "first") }?;
                let value = unsafe { c_str_to_str(second, "second") }?;
                expr.name().replace(pattern, value, flag)
            }
            4 => {
                // to_lowercase — ignores strings and flag
                expr.name().to_lowercase()
            }
            5 => {
                // to_uppercase — ignores strings and flag
                expr.name().to_uppercase()
            }
            _ => {
                return Err(PhsError::invalid_argument(format!(
                    "unknown name expression opcode {op}"
                )))
            }
        };
        *out = expr_into_raw(result);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_expr_string_nary_function(
    op: c_int,
    text: *const c_char,
    flag: bool,
    exprs: *const *const phs_expr,
    len: usize,
    out: *mut *mut phs_expr,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let exprs = expr_args(exprs, len)?;
        if exprs.is_empty() {
            return Err(PhsError::invalid_argument(
                "string nary function requires at least one expression",
            ));
        }
        let result = match op {
            0 => {
                // concat_str(exprs, separator, ignore_nulls)
                let separator = unsafe { c_str_to_str(text, "text") }?;
                concat_str(&exprs, separator, flag)
            }
            1 => {
                // format_str(format, exprs) — ignore flag
                let format = unsafe { c_str_to_str(text, "text") }?;
                format_str(format, &exprs).map_err(PhsError::from)?
            }
            _ => {
                return Err(PhsError::invalid_argument(format!(
                    "unknown string nary function opcode {op}"
                )))
            }
        };
        *out = expr_into_raw(result);
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

    #[test]
    fn builds_string_namespace_expressions() {
        let name = std::ffi::CString::new("text").unwrap();
        let pattern = std::ffi::CString::new("li").unwrap();
        let mut col_expr = ptr::null_mut();
        let mut pat_expr = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_col(name.as_ptr(), &mut col_expr, &mut err) },
            PHS_OK
        );
        assert_eq!(
            unsafe { phs_expr_lit_text(pattern.as_ptr(), &mut pat_expr, &mut err) },
            PHS_OK
        );
        let args = [pat_expr as *const phs_expr];
        for op in [0, 1, 2, 3, 4, 5, 11, 12, 13, 14, 15, 16, 17, 18, 19, 24, 25, 26, 27, 29] {
            let mut out = ptr::null_mut();
            assert_eq!(
                unsafe {
                    phs_expr_string_function(op, col_expr, args.as_ptr(), 1, &mut out, &mut err)
                },
                PHS_OK
            );
            assert!(!out.is_null());
            unsafe { crate::handles::phs_expr_free(out) };
        }
        for op in [6, 7, 8, 9, 28] {
            let mut out = ptr::null_mut();
            assert_eq!(
                unsafe {
                    phs_expr_string_function(op, col_expr, ptr::null(), 0, &mut out, &mut err)
                },
                PHS_OK
            );
            assert!(!out.is_null());
            unsafe { crate::handles::phs_expr_free(out) };
        }
        let mut offset = ptr::null_mut();
        let mut len = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_lit_int(0, &mut offset, &mut err) },
            PHS_OK
        );
        assert_eq!(
            unsafe { phs_expr_lit_int(2, &mut len, &mut err) },
            PHS_OK
        );
        let slice_args = [offset as *const phs_expr, len as *const phs_expr];
        let mut slice_out = ptr::null_mut();
        assert_eq!(
            unsafe {
                phs_expr_string_function(
                    10,
                    col_expr,
                    slice_args.as_ptr(),
                    2,
                    &mut slice_out,
                    &mut err,
                )
            },
            PHS_OK
        );
        assert!(!slice_out.is_null());
        unsafe {
            crate::handles::phs_expr_free(col_expr);
            crate::handles::phs_expr_free(pat_expr);
            crate::handles::phs_expr_free(offset);
            crate::handles::phs_expr_free(len);
            crate::handles::phs_expr_free(slice_out);
        }
    }

    #[test]
    fn string_namespace_errors_validate_opcode_and_arity() {
        let name = std::ffi::CString::new("text").unwrap();
        let mut col_expr = ptr::null_mut();
        let mut out: *mut phs_expr = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_col(name.as_ptr(), &mut col_expr, &mut err) },
            PHS_OK
        );
        assert_eq!(
            unsafe {
                phs_expr_string_function(99, col_expr, ptr::null(), 0, &mut out, &mut err)
            },
            PHS_INVALID_ARGUMENT
        );
        assert!(out.is_null());
        assert!(!err.is_null());
        unsafe { crate::error::phs_error_free(err) };
        err = ptr::null_mut();
        assert_eq!(
            unsafe {
                phs_expr_string_function(0, col_expr, ptr::null(), 0, &mut out, &mut err)
            },
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
    fn builds_string_i64_extract_expression() {
        let name = std::ffi::CString::new("text").unwrap();
        let pattern = std::ffi::CString::new(r"(\w+)").unwrap();
        let mut col_expr = ptr::null_mut();
        let mut pat_expr = ptr::null_mut();
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_col(name.as_ptr(), &mut col_expr, &mut err) },
            PHS_OK
        );
        assert_eq!(
            unsafe { phs_expr_lit_text(pattern.as_ptr(), &mut pat_expr, &mut err) },
            PHS_OK
        );
        let args = [pat_expr as *const phs_expr];
        assert_eq!(
            unsafe {
                phs_expr_string_function_i64(
                    0,
                    0,
                    col_expr,
                    args.as_ptr(),
                    1,
                    &mut out,
                    &mut err,
                )
            },
            PHS_OK
        );
        assert!(!out.is_null());
        unsafe {
            crate::handles::phs_expr_free(col_expr);
            crate::handles::phs_expr_free(pat_expr);
            crate::handles::phs_expr_free(out);
        }
    }

    #[test]
    fn string_i64_errors_validate_opcode_and_negative_arg() {
        let name = std::ffi::CString::new("text").unwrap();
        let mut col_expr = ptr::null_mut();
        let mut pat_expr = ptr::null_mut();
        let mut out: *mut phs_expr = ptr::null_mut();
        let mut err = ptr::null_mut();
        let pattern = std::ffi::CString::new(r"(\w+)").unwrap();
        assert_eq!(
            unsafe { phs_expr_col(name.as_ptr(), &mut col_expr, &mut err) },
            PHS_OK
        );
        assert_eq!(
            unsafe { phs_expr_lit_text(pattern.as_ptr(), &mut pat_expr, &mut err) },
            PHS_OK
        );
        let args = [pat_expr as *const phs_expr];
        // unknown opcode
        assert_eq!(
            unsafe {
                phs_expr_string_function_i64(
                    99,
                    0,
                    col_expr,
                    args.as_ptr(),
                    1,
                    &mut out,
                    &mut err,
                )
            },
            PHS_INVALID_ARGUMENT
        );
        assert!(out.is_null());
        assert!(!err.is_null());
        unsafe { crate::error::phs_error_free(err) };
        err = ptr::null_mut();
        // negative group index
        assert_eq!(
            unsafe {
                phs_expr_string_function_i64(
                    0,
                    -1,
                    col_expr,
                    args.as_ptr(),
                    1,
                    &mut out,
                    &mut err,
                )
            },
            PHS_INVALID_ARGUMENT
        );
        assert!(out.is_null());
        assert!(!err.is_null());
        unsafe {
            crate::handles::phs_expr_free(col_expr);
            crate::handles::phs_expr_free(pat_expr);
            crate::error::phs_error_free(err);
        }
    }

    #[test]
    fn builds_temporal_function_expressions() {
        let name = std::ffi::CString::new("ts").unwrap();
        let mut col_expr = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_col(name.as_ptr(), &mut col_expr, &mut err) },
            PHS_OK
        );
        // Test all 18 temporal function opcodes (0-17)
        for op in 0..=17 {
            let mut out = ptr::null_mut();
            assert_eq!(
                unsafe { phs_expr_temporal_function(op, col_expr, &mut out, &mut err) },
                PHS_OK,
                "temporal op {op} should succeed"
            );
            assert!(!out.is_null());
            unsafe { crate::handles::phs_expr_free(out) };
        }
        // timestamp (time_unit op 0) with all three time units
        for tu in 0..=2 {
            let mut out = ptr::null_mut();
            assert_eq!(
                unsafe { phs_expr_temporal_time_unit(0, tu, col_expr, &mut out, &mut err) },
                PHS_OK,
                "temporal time_unit op 0 with time unit {tu} should succeed"
            );
            assert!(!out.is_null());
            unsafe { crate::handles::phs_expr_free(out) };
        }
        // to_string (temporal string op 0)
        let fmt = std::ffi::CString::new("%Y-%m-%d").unwrap();
        let mut out = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_temporal_string(0, fmt.as_ptr(), col_expr, &mut out, &mut err) },
            PHS_OK
        );
        assert!(!out.is_null());
        unsafe {
            crate::handles::phs_expr_free(col_expr);
            crate::handles::phs_expr_free(out);
        }
    }

    #[test]
    fn temporal_function_errors_validate_opcodes() {
        let name = std::ffi::CString::new("ts").unwrap();
        let mut col_expr = ptr::null_mut();
        let mut out: *mut phs_expr = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_col(name.as_ptr(), &mut col_expr, &mut err) },
            PHS_OK
        );
        // unknown temporal function opcode
        assert_eq!(
            unsafe { phs_expr_temporal_function(99, col_expr, &mut out, &mut err) },
            PHS_INVALID_ARGUMENT
        );
        assert!(out.is_null());
        assert!(!err.is_null());
        unsafe { crate::error::phs_error_free(err) };
        err = ptr::null_mut();
        out = ptr::null_mut();
        // unknown temporal time_unit opcode
        assert_eq!(
            unsafe { phs_expr_temporal_time_unit(99, 0, col_expr, &mut out, &mut err) },
            PHS_INVALID_ARGUMENT
        );
        assert!(out.is_null());
        assert!(!err.is_null());
        unsafe { crate::error::phs_error_free(err) };
        err = ptr::null_mut();
        out = ptr::null_mut();
        // unknown time unit code
        assert_eq!(
            unsafe { phs_expr_temporal_time_unit(0, 99, col_expr, &mut out, &mut err) },
            PHS_INVALID_ARGUMENT
        );
        assert!(out.is_null());
        assert!(!err.is_null());
        unsafe { crate::error::phs_error_free(err) };
        err = ptr::null_mut();
        out = ptr::null_mut();
        // unknown temporal string opcode
        let fmt = std::ffi::CString::new("").unwrap();
        assert_eq!(
            unsafe { phs_expr_temporal_string(99, fmt.as_ptr(), col_expr, &mut out, &mut err) },
            PHS_INVALID_ARGUMENT
        );
        assert!(out.is_null());
        assert!(!err.is_null());
        unsafe { crate::error::phs_error_free(err) };
        err = ptr::null_mut();
        out = ptr::null_mut();
        // null format string
        assert_eq!(
            unsafe { phs_expr_temporal_string(0, ptr::null(), col_expr, &mut out, &mut err) },
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
    fn builds_string_split_expressions() {
        let name = std::ffi::CString::new("text").unwrap();
        let by_str = std::ffi::CString::new(" ").unwrap();
        let mut col_expr = ptr::null_mut();
        let mut by_expr = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_col(name.as_ptr(), &mut col_expr, &mut err) },
            PHS_OK
        );
        assert_eq!(
            unsafe { phs_expr_lit_text(by_str.as_ptr(), &mut by_expr, &mut err) },
            PHS_OK
        );
        let args = [by_expr as *const phs_expr];
        // split (op 24)
        let mut out = ptr::null_mut();
        assert_eq!(
            unsafe {
                phs_expr_string_function(24, col_expr, args.as_ptr(), 1, &mut out, &mut err)
            },
            PHS_OK
        );
        assert!(!out.is_null());
        unsafe { crate::handles::phs_expr_free(out) };
        // split_inclusive (op 25)
        let mut out = ptr::null_mut();
        assert_eq!(
            unsafe {
                phs_expr_string_function(25, col_expr, args.as_ptr(), 1, &mut out, &mut err)
            },
            PHS_OK
        );
        assert!(!out.is_null());
        unsafe {
            crate::handles::phs_expr_free(col_expr);
            crate::handles::phs_expr_free(by_expr);
            crate::handles::phs_expr_free(out);
        }
    }

    #[test]
    fn builds_list_expression_ops() {
        let name = std::ffi::CString::new("text").unwrap();
        let separator = std::ffi::CString::new("-").unwrap();
        let mut col_expr = ptr::null_mut();
        let mut sep_expr = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_col(name.as_ptr(), &mut col_expr, &mut err) },
            PHS_OK
        );
        assert_eq!(
            unsafe { phs_expr_lit_text(separator.as_ptr(), &mut sep_expr, &mut err) },
            PHS_OK
        );
        let args = [sep_expr as *const phs_expr];
        // arity 0 ops: len (0), first (1), last (2)
        for op in 0..=2 {
            let mut out = ptr::null_mut();
            assert_eq!(
                unsafe {
                    phs_expr_list_function(op, col_expr, ptr::null(), 0, &mut out, &mut err)
                },
                PHS_OK,
                "list op {op} should succeed"
            );
            assert!(!out.is_null());
            unsafe { crate::handles::phs_expr_free(out) };
        }
        // arity 1 ops: get null_on_oob=false (3), get null_on_oob=true (4),
        // join ignore_nulls=false (5), join ignore_nulls=true (6),
        // contains nulls_equal=false (7), contains nulls_equal=true (8),
        // count_matches (9)
        for op in 3..=9 {
            let mut out = ptr::null_mut();
            assert_eq!(
                unsafe {
                    phs_expr_list_function(op, col_expr, args.as_ptr(), 1, &mut out, &mut err)
                },
                PHS_OK,
                "list op {op} should succeed"
            );
            assert!(!out.is_null());
            unsafe { crate::handles::phs_expr_free(out) };
        }
        unsafe {
            crate::handles::phs_expr_free(col_expr);
            crate::handles::phs_expr_free(sep_expr);
        }
    }

    #[test]
    fn list_expression_errors_validate_opcode_and_arity() {
        let name = std::ffi::CString::new("text").unwrap();
        let mut col_expr = ptr::null_mut();
        let mut out: *mut phs_expr = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_col(name.as_ptr(), &mut col_expr, &mut err) },
            PHS_OK
        );
        // unknown opcode
        assert_eq!(
            unsafe {
                phs_expr_list_function(99, col_expr, ptr::null(), 0, &mut out, &mut err)
            },
            PHS_INVALID_ARGUMENT
        );
        assert!(out.is_null());
        assert!(!err.is_null());
        unsafe { crate::error::phs_error_free(err) };
        err = ptr::null_mut();
        out = ptr::null_mut();
        // wrong arity (len expects 0 args)
        assert_eq!(
            unsafe {
                phs_expr_list_function(0, col_expr, ptr::null(), 1, &mut out, &mut err)
            },
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
    fn builds_boolean_unary_expressions() {
        let name = std::ffi::CString::new("value").unwrap();
        let mut col_expr = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_col(name.as_ptr(), &mut col_expr, &mut err) },
            PHS_OK
        );
        for op in 0..=3 {
            let mut out = ptr::null_mut();
            assert_eq!(
                unsafe {
                    phs_expr_boolean_unary(op, col_expr, &mut out, &mut err)
                },
                PHS_OK,
                "boolean_unary op {op} should succeed"
            );
            assert!(!out.is_null());
            unsafe { crate::handles::phs_expr_free(out) };
        }
        unsafe {
            crate::handles::phs_expr_free(col_expr);
        }
    }

    #[test]
    fn boolean_unary_unknown_opcode_returns_error() {
        let name = std::ffi::CString::new("value").unwrap();
        let mut col_expr = ptr::null_mut();
        let mut out: *mut phs_expr = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_col(name.as_ptr(), &mut col_expr, &mut err) },
            PHS_OK
        );
        assert_eq!(
            unsafe { phs_expr_boolean_unary(99, col_expr, &mut out, &mut err) },
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
    fn builds_is_between_expression() {
        let name = std::ffi::CString::new("value").unwrap();
        let mut col_expr = ptr::null_mut();
        let mut low_expr = ptr::null_mut();
        let mut high_expr = ptr::null_mut();
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_col(name.as_ptr(), &mut col_expr, &mut err) },
            PHS_OK
        );
        assert_eq!(
            unsafe { phs_expr_lit_int(2, &mut low_expr, &mut err) },
            PHS_OK
        );
        assert_eq!(
            unsafe { phs_expr_lit_int(3, &mut high_expr, &mut err) },
            PHS_OK
        );
        for closed in 0..=3 {
            assert_eq!(
                unsafe {
                    phs_expr_is_between(closed, col_expr, low_expr, high_expr, &mut out, &mut err)
                },
                PHS_OK,
                "is_between closed={closed} should succeed"
            );
            assert!(!out.is_null());
            unsafe { crate::handles::phs_expr_free(out) };
            out = ptr::null_mut();
        }
        unsafe {
            crate::handles::phs_expr_free(col_expr);
            crate::handles::phs_expr_free(low_expr);
            crate::handles::phs_expr_free(high_expr);
        }
    }

    #[test]
    fn is_between_unknown_closed_interval_returns_error() {
        let name = std::ffi::CString::new("value").unwrap();
        let mut col_expr = ptr::null_mut();
        let mut low_expr = ptr::null_mut();
        let mut high_expr = ptr::null_mut();
        let mut out: *mut phs_expr = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_col(name.as_ptr(), &mut col_expr, &mut err) },
            PHS_OK
        );
        assert_eq!(
            unsafe { phs_expr_lit_int(2, &mut low_expr, &mut err) },
            PHS_OK
        );
        assert_eq!(
            unsafe { phs_expr_lit_int(3, &mut high_expr, &mut err) },
            PHS_OK
        );
        assert_eq!(
            unsafe {
                phs_expr_is_between(99, col_expr, low_expr, high_expr, &mut out, &mut err)
            },
            PHS_INVALID_ARGUMENT
        );
        assert!(out.is_null());
        assert!(!err.is_null());
        unsafe {
            crate::handles::phs_expr_free(col_expr);
            crate::handles::phs_expr_free(low_expr);
            crate::handles::phs_expr_free(high_expr);
            crate::error::phs_error_free(err);
        }
    }

    #[test]
    fn builds_is_close_expression() {
        let name = std::ffi::CString::new("value").unwrap();
        let other_name = std::ffi::CString::new("near").unwrap();
        let mut col_expr = ptr::null_mut();
        let mut other_expr = ptr::null_mut();
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_col(name.as_ptr(), &mut col_expr, &mut err) },
            PHS_OK
        );
        assert_eq!(
            unsafe { phs_expr_col(other_name.as_ptr(), &mut other_expr, &mut err) },
            PHS_OK
        );
        assert_eq!(
            unsafe {
                phs_expr_is_close(
                    0.15,
                    0.0,
                    false,
                    col_expr,
                    other_expr,
                    &mut out,
                    &mut err,
                )
            },
            PHS_OK
        );
        assert!(!out.is_null());
        unsafe {
            crate::handles::phs_expr_free(col_expr);
            crate::handles::phs_expr_free(other_expr);
            crate::handles::phs_expr_free(out);
        }
    }

    #[test]
    fn builds_is_in_expression() {
        let name = std::ffi::CString::new("value").unwrap();
        let list_name = std::ffi::CString::new("items").unwrap();
        let mut col_expr = ptr::null_mut();
        let mut list_expr = ptr::null_mut();
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_col(name.as_ptr(), &mut col_expr, &mut err) },
            PHS_OK
        );
        assert_eq!(
            unsafe { phs_expr_col(list_name.as_ptr(), &mut list_expr, &mut err) },
            PHS_OK
        );
        assert_eq!(
            unsafe { phs_expr_is_in(false, col_expr, list_expr, &mut out, &mut err) },
            PHS_OK
        );
        assert!(!out.is_null());
        assert_eq!(
            unsafe { phs_expr_is_in(true, col_expr, list_expr, &mut out, &mut err) },
            PHS_OK
        );
        assert!(!out.is_null());
        unsafe {
            crate::handles::phs_expr_free(col_expr);
            crate::handles::phs_expr_free(list_expr);
            crate::handles::phs_expr_free(out);
        }
    }

    #[test]
    fn builds_clip_expressions() {
        let name = std::ffi::CString::new("value").unwrap();
        let mut col_expr = ptr::null_mut();
        let mut lo_expr = ptr::null_mut();
        let mut hi_expr = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_col(name.as_ptr(), &mut col_expr, &mut err) },
            PHS_OK
        );
        assert_eq!(
            unsafe { phs_expr_lit_int(2, &mut lo_expr, &mut err) },
            PHS_OK
        );
        assert_eq!(
            unsafe { phs_expr_lit_int(3, &mut hi_expr, &mut err) },
            PHS_OK
        );
        // clip (op 0, arity 2)
        let clip_args = [lo_expr as *const phs_expr, hi_expr as *const phs_expr];
        let mut out = ptr::null_mut();
        assert_eq!(
            unsafe {
                phs_expr_clip(0, col_expr, clip_args.as_ptr(), 2, &mut out, &mut err)
            },
            PHS_OK
        );
        assert!(!out.is_null());
        unsafe { crate::handles::phs_expr_free(out) };
        // clip_min (op 1, arity 1)
        let mut out = ptr::null_mut();
        assert_eq!(
            unsafe {
                phs_expr_clip(1, col_expr, clip_args.as_ptr(), 1, &mut out, &mut err)
            },
            PHS_OK
        );
        assert!(!out.is_null());
        unsafe { crate::handles::phs_expr_free(out) };
        // clip_max (op 2, arity 1)
        let mut out = ptr::null_mut();
        assert_eq!(
            unsafe {
                phs_expr_clip(2, col_expr, clip_args.as_ptr(), 1, &mut out, &mut err)
            },
            PHS_OK
        );
        assert!(!out.is_null());
        unsafe {
            crate::handles::phs_expr_free(col_expr);
            crate::handles::phs_expr_free(lo_expr);
            crate::handles::phs_expr_free(hi_expr);
            crate::handles::phs_expr_free(out);
        }
    }

    #[test]
    fn clip_unknown_opcode_and_wrong_arity_return_errors() {
        let name = std::ffi::CString::new("value").unwrap();
        let mut col_expr = ptr::null_mut();
        let mut lo_expr = ptr::null_mut();
        let mut hi_expr = ptr::null_mut();
        let mut out: *mut phs_expr = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_col(name.as_ptr(), &mut col_expr, &mut err) },
            PHS_OK
        );
        assert_eq!(
            unsafe { phs_expr_lit_int(2, &mut lo_expr, &mut err) },
            PHS_OK
        );
        assert_eq!(
            unsafe { phs_expr_lit_int(3, &mut hi_expr, &mut err) },
            PHS_OK
        );
        let clip_args = [lo_expr as *const phs_expr, hi_expr as *const phs_expr];
        // unknown opcode
        assert_eq!(
            unsafe {
                phs_expr_clip(99, col_expr, clip_args.as_ptr(), 2, &mut out, &mut err)
            },
            PHS_INVALID_ARGUMENT
        );
        assert!(out.is_null());
        assert!(!err.is_null());
        unsafe { crate::error::phs_error_free(err) };
        err = ptr::null_mut();
        out = ptr::null_mut();
        // clip op 0 with wrong arity (1 instead of 2)
        assert_eq!(
            unsafe {
                phs_expr_clip(0, col_expr, clip_args.as_ptr(), 1, &mut out, &mut err)
            },
            PHS_INVALID_ARGUMENT
        );
        assert!(out.is_null());
        assert!(!err.is_null());
        unsafe { crate::error::phs_error_free(err) };
        err = ptr::null_mut();
        out = ptr::null_mut();
        // clip_min op 1 with wrong arity (2 instead of 1)
        assert_eq!(
            unsafe {
                phs_expr_clip(1, col_expr, clip_args.as_ptr(), 2, &mut out, &mut err)
            },
            PHS_INVALID_ARGUMENT
        );
        assert!(out.is_null());
        assert!(!err.is_null());
        unsafe {
            crate::handles::phs_expr_free(col_expr);
            crate::handles::phs_expr_free(lo_expr);
            crate::handles::phs_expr_free(hi_expr);
            crate::error::phs_error_free(err);
        }
    }

    #[test]
    fn builds_horizontal_function_expressions() {
        let name_a = std::ffi::CString::new("a").unwrap();
        let name_b = std::ffi::CString::new("b").unwrap();
        let mut col_a = ptr::null_mut();
        let mut col_b = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_col(name_a.as_ptr(), &mut col_a, &mut err) },
            PHS_OK
        );
        assert_eq!(
            unsafe { phs_expr_col(name_b.as_ptr(), &mut col_b, &mut err) },
            PHS_OK
        );
        let exprs = [col_a as *const phs_expr, col_b as *const phs_expr];
        // sum_horizontal (op 0, ignore_nulls=true)
        for op in 0..=6 {
            let mut out = ptr::null_mut();
            assert_eq!(
                unsafe {
                    phs_expr_horizontal_function(
                        op,
                        true,
                        exprs.as_ptr(),
                        2,
                        &mut out,
                        &mut err,
                    )
                },
                PHS_OK,
                "horizontal op {op} should succeed"
            );
            assert!(!out.is_null());
            unsafe { crate::handles::phs_expr_free(out) };
        }
        unsafe {
            crate::handles::phs_expr_free(col_a);
            crate::handles::phs_expr_free(col_b);
        }
    }

    #[test]
    fn horizontal_function_empty_exprs_rejected() {
        for op in 0..=6 {
            let mut out: *mut phs_expr = ptr::null_mut();
            let mut err = ptr::null_mut();
            let result = unsafe {
                phs_expr_horizontal_function(op, false, ptr::null(), 0, &mut out, &mut err)
            };
            assert_eq!(result, PHS_INVALID_ARGUMENT, "horizontal op {op} empty should fail");
            assert!(out.is_null());
            assert!(!err.is_null());
            unsafe { crate::error::phs_error_free(err) };
        }
    }

    #[test]
    fn horizontal_function_unknown_opcode_rejected() {
        let name_a = std::ffi::CString::new("a").unwrap();
        let mut col_a = ptr::null_mut();
        let mut out: *mut phs_expr = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_col(name_a.as_ptr(), &mut col_a, &mut err) },
            PHS_OK
        );
        let exprs = [col_a as *const phs_expr];
        assert_eq!(
            unsafe {
                phs_expr_horizontal_function(99, false, exprs.as_ptr(), 1, &mut out, &mut err)
            },
            PHS_INVALID_ARGUMENT
        );
        assert!(out.is_null());
        assert!(!err.is_null());
        unsafe {
            crate::handles::phs_expr_free(col_a);
            crate::error::phs_error_free(err);
        }
    }

    #[test]
    fn horizontal_function_null_pointer_with_positive_len_rejected() {
        let mut out: *mut phs_expr = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(
            unsafe {
                phs_expr_horizontal_function(0, false, ptr::null(), 1, &mut out, &mut err)
            },
            PHS_INVALID_ARGUMENT
        );
        assert!(out.is_null());
        assert!(!err.is_null());
        unsafe { crate::error::phs_error_free(err) };
    }

    #[test]
    fn builds_name_function_expressions() {
        let name = std::ffi::CString::new("value").unwrap();
        let prefix = std::ffi::CString::new("pre_").unwrap();
        let suffix = std::ffi::CString::new("_suf").unwrap();
        let pattern = std::ffi::CString::new("score").unwrap();
        let replace = std::ffi::CString::new("points").unwrap();
        let mut col_expr = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_col(name.as_ptr(), &mut col_expr, &mut err) },
            PHS_OK
        );
        // op 0: keep
        let mut out = ptr::null_mut();
        assert_eq!(
            unsafe {
                phs_expr_name_function(
                    0,
                    ptr::null(),
                    ptr::null(),
                    false,
                    col_expr,
                    &mut out,
                    &mut err,
                )
            },
            PHS_OK
        );
        assert!(!out.is_null());
        unsafe { crate::handles::phs_expr_free(out) };
        // op 1: prefix
        let mut out = ptr::null_mut();
        assert_eq!(
            unsafe {
                phs_expr_name_function(
                    1,
                    prefix.as_ptr(),
                    ptr::null(),
                    false,
                    col_expr,
                    &mut out,
                    &mut err,
                )
            },
            PHS_OK
        );
        assert!(!out.is_null());
        unsafe { crate::handles::phs_expr_free(out) };
        // op 2: suffix
        let mut out = ptr::null_mut();
        assert_eq!(
            unsafe {
                phs_expr_name_function(
                    2,
                    suffix.as_ptr(),
                    ptr::null(),
                    false,
                    col_expr,
                    &mut out,
                    &mut err,
                )
            },
            PHS_OK
        );
        assert!(!out.is_null());
        unsafe { crate::handles::phs_expr_free(out) };
        // op 3: replace (literal=true)
        let mut out = ptr::null_mut();
        assert_eq!(
            unsafe {
                phs_expr_name_function(
                    3,
                    pattern.as_ptr(),
                    replace.as_ptr(),
                    true,
                    col_expr,
                    &mut out,
                    &mut err,
                )
            },
            PHS_OK
        );
        assert!(!out.is_null());
        unsafe { crate::handles::phs_expr_free(out) };
        // op 3: replace (literal=false, regex)
        let mut out = ptr::null_mut();
        assert_eq!(
            unsafe {
                phs_expr_name_function(
                    3,
                    pattern.as_ptr(),
                    replace.as_ptr(),
                    false,
                    col_expr,
                    &mut out,
                    &mut err,
                )
            },
            PHS_OK
        );
        assert!(!out.is_null());
        unsafe { crate::handles::phs_expr_free(out) };
        // op 4: to_lowercase
        let mut out = ptr::null_mut();
        assert_eq!(
            unsafe {
                phs_expr_name_function(
                    4,
                    ptr::null(),
                    ptr::null(),
                    false,
                    col_expr,
                    &mut out,
                    &mut err,
                )
            },
            PHS_OK
        );
        assert!(!out.is_null());
        unsafe { crate::handles::phs_expr_free(out) };
        // op 5: to_uppercase
        let mut out = ptr::null_mut();
        assert_eq!(
            unsafe {
                phs_expr_name_function(
                    5,
                    ptr::null(),
                    ptr::null(),
                    false,
                    col_expr,
                    &mut out,
                    &mut err,
                )
            },
            PHS_OK
        );
        assert!(!out.is_null());
        unsafe {
            crate::handles::phs_expr_free(col_expr);
            crate::handles::phs_expr_free(out);
        }
    }

    #[test]
    fn name_function_unknown_opcode_returns_error() {
        let name = std::ffi::CString::new("value").unwrap();
        let mut col_expr = ptr::null_mut();
        let mut out: *mut phs_expr = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_col(name.as_ptr(), &mut col_expr, &mut err) },
            PHS_OK
        );
        assert_eq!(
            unsafe {
                phs_expr_name_function(
                    99,
                    ptr::null(),
                    ptr::null(),
                    false,
                    col_expr,
                    &mut out,
                    &mut err,
                )
            },
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
    fn name_function_missing_required_string_returns_error() {
        let name = std::ffi::CString::new("value").unwrap();
        let mut col_expr = ptr::null_mut();
        let mut out: *mut phs_expr = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_col(name.as_ptr(), &mut col_expr, &mut err) },
            PHS_OK
        );
        // prefix (op 1) with null first string
        assert_eq!(
            unsafe {
                phs_expr_name_function(
                    1,
                    ptr::null(),
                    ptr::null(),
                    false,
                    col_expr,
                    &mut out,
                    &mut err,
                )
            },
            PHS_INVALID_ARGUMENT
        );
        assert!(out.is_null());
        assert!(!err.is_null());
        unsafe { crate::error::phs_error_free(err) };
        err = ptr::null_mut();
        out = ptr::null_mut();
        // suffix (op 2) with null first string
        assert_eq!(
            unsafe {
                phs_expr_name_function(
                    2,
                    ptr::null(),
                    ptr::null(),
                    false,
                    col_expr,
                    &mut out,
                    &mut err,
                )
            },
            PHS_INVALID_ARGUMENT
        );
        assert!(out.is_null());
        assert!(!err.is_null());
        unsafe { crate::error::phs_error_free(err) };
        err = ptr::null_mut();
        out = ptr::null_mut();
        // replace (op 3) with null first string
        let second = std::ffi::CString::new("points").unwrap();
        assert_eq!(
            unsafe {
                phs_expr_name_function(
                    3,
                    ptr::null(),
                    second.as_ptr(),
                    false,
                    col_expr,
                    &mut out,
                    &mut err,
                )
            },
            PHS_INVALID_ARGUMENT
        );
        assert!(out.is_null());
        assert!(!err.is_null());
        unsafe { crate::error::phs_error_free(err) };
        err = ptr::null_mut();
        out = ptr::null_mut();
        // replace (op 3) with null second string
        let first = std::ffi::CString::new("score").unwrap();
        assert_eq!(
            unsafe {
                phs_expr_name_function(
                    3,
                    first.as_ptr(),
                    ptr::null(),
                    false,
                    col_expr,
                    &mut out,
                    &mut err,
                )
            },
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
    fn builds_string_nary_concat_str_expression() {
        let first = std::ffi::CString::new("first").unwrap();
        let last = std::ffi::CString::new("last").unwrap();
        let sep = std::ffi::CString::new(" ").unwrap();
        let mut col_first = ptr::null_mut();
        let mut col_last = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_col(first.as_ptr(), &mut col_first, &mut err) },
            PHS_OK
        );
        assert_eq!(
            unsafe { phs_expr_col(last.as_ptr(), &mut col_last, &mut err) },
            PHS_OK
        );
        let exprs = [col_first as *const phs_expr, col_last as *const phs_expr];
        // concat_str (op 0, ignore_nulls=false)
        let mut out = ptr::null_mut();
        assert_eq!(
            unsafe {
                phs_expr_string_nary_function(
                    0,
                    sep.as_ptr(),
                    false,
                    exprs.as_ptr(),
                    2,
                    &mut out,
                    &mut err,
                )
            },
            PHS_OK
        );
        assert!(!out.is_null());
        unsafe { crate::handles::phs_expr_free(out) };
        // concat_str (op 0, ignore_nulls=true)
        let mut out = ptr::null_mut();
        assert_eq!(
            unsafe {
                phs_expr_string_nary_function(
                    0,
                    sep.as_ptr(),
                    true,
                    exprs.as_ptr(),
                    2,
                    &mut out,
                    &mut err,
                )
            },
            PHS_OK
        );
        assert!(!out.is_null());
        unsafe { crate::handles::phs_expr_free(out) };
        unsafe {
            crate::handles::phs_expr_free(col_first);
            crate::handles::phs_expr_free(col_last);
        }
    }

    #[test]
    fn builds_string_nary_format_str_expression() {
        let first = std::ffi::CString::new("first").unwrap();
        let age = std::ffi::CString::new("age").unwrap();
        let fmt = std::ffi::CString::new("{}:{}").unwrap();
        let mut col_first = ptr::null_mut();
        let mut col_age = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_col(first.as_ptr(), &mut col_first, &mut err) },
            PHS_OK
        );
        assert_eq!(
            unsafe { phs_expr_col(age.as_ptr(), &mut col_age, &mut err) },
            PHS_OK
        );
        let exprs = [col_first as *const phs_expr, col_age as *const phs_expr];
        // format_str (op 1)
        let mut out = ptr::null_mut();
        assert_eq!(
            unsafe {
                phs_expr_string_nary_function(
                    1,
                    fmt.as_ptr(),
                    false,
                    exprs.as_ptr(),
                    2,
                    &mut out,
                    &mut err,
                )
            },
            PHS_OK
        );
        assert!(!out.is_null());
        unsafe {
            crate::handles::phs_expr_free(col_first);
            crate::handles::phs_expr_free(col_age);
            crate::handles::phs_expr_free(out);
        }
    }

    #[test]
    fn string_nary_function_errors_unknown_opcode_empty_list_null_text() {
        let first = std::ffi::CString::new("first").unwrap();
        let sep = std::ffi::CString::new(" ").unwrap();
        let mut col_first = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_col(first.as_ptr(), &mut col_first, &mut err) },
            PHS_OK
        );
        let exprs = [col_first as *const phs_expr];
        // unknown opcode
        let mut out: *mut phs_expr = ptr::null_mut();
        assert_eq!(
            unsafe {
                phs_expr_string_nary_function(
                    99,
                    sep.as_ptr(),
                    false,
                    exprs.as_ptr(),
                    1,
                    &mut out,
                    &mut err,
                )
            },
            PHS_INVALID_ARGUMENT
        );
        assert!(out.is_null());
        assert!(!err.is_null());
        unsafe { crate::error::phs_error_free(err) };
        err = ptr::null_mut();
        out = ptr::null_mut();
        // null text pointer for concat_str (op 0)
        assert_eq!(
            unsafe {
                phs_expr_string_nary_function(
                    0,
                    ptr::null(),
                    false,
                    exprs.as_ptr(),
                    1,
                    &mut out,
                    &mut err,
                )
            },
            PHS_INVALID_ARGUMENT
        );
        assert!(out.is_null());
        assert!(!err.is_null());
        unsafe { crate::error::phs_error_free(err) };
        err = ptr::null_mut();
        out = ptr::null_mut();
        // null text pointer for format_str (op 1)
        assert_eq!(
            unsafe {
                phs_expr_string_nary_function(
                    1,
                    ptr::null(),
                    false,
                    exprs.as_ptr(),
                    1,
                    &mut out,
                    &mut err,
                )
            },
            PHS_INVALID_ARGUMENT
        );
        assert!(out.is_null());
        assert!(!err.is_null());
        unsafe { crate::error::phs_error_free(err) };
        err = ptr::null_mut();
        out = ptr::null_mut();
        // empty exprs list for concat_str (op 0)
        assert_eq!(
            unsafe {
                phs_expr_string_nary_function(
                    0,
                    sep.as_ptr(),
                    false,
                    ptr::null(),
                    0,
                    &mut out,
                    &mut err,
                )
            },
            PHS_INVALID_ARGUMENT
        );
        assert!(out.is_null());
        assert!(!err.is_null());
        unsafe { crate::error::phs_error_free(err) };
        err = ptr::null_mut();
        out = ptr::null_mut();
        // empty exprs list for format_str (op 1)
        assert_eq!(
            unsafe {
                phs_expr_string_nary_function(
                    1,
                    sep.as_ptr(),
                    false,
                    ptr::null(),
                    0,
                    &mut out,
                    &mut err,
                )
            },
            PHS_INVALID_ARGUMENT
        );
        assert!(out.is_null());
        assert!(!err.is_null());
        unsafe {
            crate::handles::phs_expr_free(col_first);
            crate::error::phs_error_free(err);
        }
    }

    #[test]
    fn builds_string_more_expressions() {
        let name = std::ffi::CString::new("text").unwrap();
        let arg_str = std::ffi::CString::new(" ").unwrap();
        let mut col_expr = ptr::null_mut();
        let mut arg_expr = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_col(name.as_ptr(), &mut col_expr, &mut err) },
            PHS_OK
        );
        assert_eq!(
            unsafe { phs_expr_lit_text(arg_str.as_ptr(), &mut arg_expr, &mut err) },
            PHS_OK
        );
        let args = [arg_expr as *const phs_expr];
        // strip_prefix (op 26) - arity 1
        let mut out = ptr::null_mut();
        assert_eq!(
            unsafe {
                phs_expr_string_function(26, col_expr, args.as_ptr(), 1, &mut out, &mut err)
            },
            PHS_OK
        );
        assert!(!out.is_null());
        unsafe { crate::handles::phs_expr_free(out) };
        // strip_suffix (op 27) - arity 1
        out = ptr::null_mut();
        assert_eq!(
            unsafe {
                phs_expr_string_function(27, col_expr, args.as_ptr(), 1, &mut out, &mut err)
            },
            PHS_OK
        );
        assert!(!out.is_null());
        unsafe { crate::handles::phs_expr_free(out) };
        // escape_regex (op 28) - arity 0
        out = ptr::null_mut();
        assert_eq!(
            unsafe {
                phs_expr_string_function(28, col_expr, ptr::null(), 0, &mut out, &mut err)
            },
            PHS_OK
        );
        assert!(!out.is_null());
        unsafe { crate::handles::phs_expr_free(out) };
        // extract_all (op 29) - arity 1
        let pattern = std::ffi::CString::new("[A-Z]").unwrap();
        let mut pat_expr = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_lit_text(pattern.as_ptr(), &mut pat_expr, &mut err) },
            PHS_OK
        );
        let pat_args = [pat_expr as *const phs_expr];
        out = ptr::null_mut();
        assert_eq!(
            unsafe {
                phs_expr_string_function(29, col_expr, pat_args.as_ptr(), 1, &mut out, &mut err)
            },
            PHS_OK
        );
        assert!(!out.is_null());
        unsafe {
            crate::handles::phs_expr_free(col_expr);
            crate::handles::phs_expr_free(arg_expr);
            crate::handles::phs_expr_free(pat_expr);
            crate::handles::phs_expr_free(out);
        }
    }

    #[test]
    fn string_more_arity_errors() {
        let name = std::ffi::CString::new("text").unwrap();
        let mut col_expr = ptr::null_mut();
        let mut out: *mut phs_expr = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_col(name.as_ptr(), &mut col_expr, &mut err) },
            PHS_OK
        );
        // strip_prefix (op 26) expects 1 arg, give 0
        assert_eq!(
            unsafe {
                phs_expr_string_function(26, col_expr, ptr::null(), 0, &mut out, &mut err)
            },
            PHS_INVALID_ARGUMENT
        );
        assert!(out.is_null());
        assert!(!err.is_null());
        unsafe { crate::error::phs_error_free(err) };
        err = ptr::null_mut();
        out = ptr::null_mut();
        // escape_regex (op 28) expects 0 args, give 1
        assert_eq!(
            unsafe {
                phs_expr_string_function(28, col_expr, ptr::null(), 1, &mut out, &mut err)
            },
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
    fn string_nary_format_placeholder_mismatch_returns_error() {
        let first = std::ffi::CString::new("first").unwrap();
        let fmt = std::ffi::CString::new("{}:{}").unwrap();
        let mut col_first = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(
            unsafe { phs_expr_col(first.as_ptr(), &mut col_first, &mut err) },
            PHS_OK
        );
        let exprs = [col_first as *const phs_expr];
        // 2 placeholders but only 1 expression -> should error
        let mut out: *mut phs_expr = ptr::null_mut();
        let result = unsafe {
            phs_expr_string_nary_function(
                1,
                fmt.as_ptr(),
                false,
                exprs.as_ptr(),
                1,
                &mut out,
                &mut err,
            )
        };
        assert_ne!(result, PHS_OK, "format_str with mismatched placeholders should error");
        assert!(out.is_null());
        if !err.is_null() {
            unsafe { crate::error::phs_error_free(err) };
        }
        unsafe {
            crate::handles::phs_expr_free(col_first);
        }
    }
}
