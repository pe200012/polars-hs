use std::os::raw::{c_char, c_int, c_uchar};
use std::ptr;

use polars::prelude::*;

use crate::bytes::{bytes_into_raw, phs_bytes};
use crate::error::{PhsError, PhsResult, c_str_to_str, ffi_boundary, phs_error, required_mut};
use crate::handles::{dataframe_into_raw, expr_ref, lazyframe_into_raw, lazyframe_ref, phs_dataframe, phs_expr, phs_lazyframe};
use crate::schema::encode_schema;

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

unsafe fn bool_vec(values: *const u8, len: usize, label: &str) -> PhsResult<Vec<bool>> {
    if values.is_null() && len > 0 {
        return Err(PhsError::invalid_argument(format!("{label} pointer was null")));
    }
    let slice = if len == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(values, len) }
    };
    slice
        .iter()
        .map(|value| match *value {
            0 => Ok(false),
            1 => Ok(true),
            other => Err(PhsError::invalid_argument(format!(
                "{label} value must be 0 or 1, received {other}"
            ))),
        })
        .collect()
}

fn validate_sort_bool_options(label: &str, expr_count: usize, values: &[bool]) -> PhsResult<()> {
    let option_count = values.len();
    if option_count == 1 || option_count == expr_count {
        Ok(())
    } else {
        Err(PhsError::invalid_argument(format!(
            "{label} must contain one value or one value per sort expression"
        )))
    }
}

fn join_type_from_code(code: c_int) -> PhsResult<JoinType> {
    match code {
        0 => Ok(JoinType::Inner),
        1 => Ok(JoinType::Left),
        2 => Ok(JoinType::Right),
        3 => Ok(JoinType::Full),
        4 => Ok(JoinType::Semi),
        5 => Ok(JoinType::Anti),
        6 => Ok(JoinType::Cross),
        _ => Err(PhsError::invalid_argument(format!("unknown join type code {code}"))),
    }
}

fn keep_strategy_from_code(code: c_int) -> PhsResult<UniqueKeepStrategy> {
    match code {
        0 => Ok(UniqueKeepStrategy::First),
        1 => Ok(UniqueKeepStrategy::Last),
        2 => Ok(UniqueKeepStrategy::None),
        3 => Ok(UniqueKeepStrategy::Any),
        _ => Err(PhsError::invalid_argument(format!("unknown unique keep strategy code {code}"))),
    }
}

unsafe fn optional_suffix(suffix: *const c_char) -> PhsResult<Option<PlSmallStr>> {
    if suffix.is_null() {
        Ok(None)
    } else {
        let suffix = unsafe { c_str_to_str(suffix, "suffix") }?;
        Ok(Some(PlSmallStr::from_str(suffix)))
    }
}

fn selector_from_names(names: Vec<PlSmallStr>, label: &str) -> PhsResult<Selector> {
    if names.is_empty() {
        Err(PhsError::invalid_argument(format!("{label} requires at least one column name")))
    } else {
        Ok(by_name(names, true, false))
    }
}

fn selector_from_names_allow_empty(names: Vec<PlSmallStr>) -> Selector {
    if names.is_empty() {
        empty()
    } else {
        by_name(names, true, false)
    }
}

unsafe fn optional_selector(
    names: *const *const c_char,
    len: usize,
    has_subset: bool,
    label: &str,
) -> PhsResult<Option<Selector>> {
    if !has_subset {
        return Ok(None);
    }
    let names = unsafe { name_vec(names, len) }?;
    selector_from_names(names, label).map(Some)
}

fn idx_size_from_u64(value: u64, label: &str) -> PhsResult<IdxSize> {
    value
        .try_into()
        .map_err(|_| PhsError::invalid_argument(format!("{label} exceeds Polars index size")))
}

fn usize_from_u64(value: u64, label: &str) -> PhsResult<usize> {
    value
        .try_into()
        .map_err(|_| PhsError::invalid_argument(format!("{label} exceeded usize")))
}

fn parquet_parallel_from_code(code: c_int) -> PhsResult<ParallelStrategy> {
    match code {
        0 => Ok(ParallelStrategy::Auto),
        1 => Ok(ParallelStrategy::None),
        2 => Ok(ParallelStrategy::Columns),
        3 => Ok(ParallelStrategy::RowGroups),
        4 => Ok(ParallelStrategy::Prefiltered),
        _ => Err(PhsError::invalid_argument(format!(
            "unknown parquet parallel strategy code {code}"
        ))),
    }
}

fn empty_profile_frame() -> DataFrame {
    let schema = Schema::from_iter([
        Field::new(PlSmallStr::from_static("node"), DataType::String),
        Field::new(PlSmallStr::from_static("start"), DataType::UInt64),
        Field::new(PlSmallStr::from_static("end"), DataType::UInt64),
    ]);
    DataFrame::empty_with_schema(&schema)
}

fn is_empty_profile_error(error: &PolarsError) -> bool {
    matches!(error, PolarsError::ComputeError(message) if message.as_ref() == "no data to time")
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_scan_csv(
    path: *const c_char,
    out: *mut *mut phs_lazyframe,
    err: *mut *mut phs_error,
) -> c_int {
    unsafe {
        phs_scan_csv_options(
            path,
            true,
            b',',
            false,
            ptr::null(),
            false,
            0,
            0,
            0,
            true,
            100,
            false,
            false,
            true,
            false,
            false,
            out,
            err,
        )
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_scan_csv_options(
    path: *const c_char,
    has_header: bool,
    separator: c_uchar,
    has_null_value: bool,
    null_value: *const c_char,
    has_n_rows: bool,
    n_rows: u64,
    skip_rows: u64,
    skip_rows_after_header: u64,
    has_infer_schema_length: bool,
    infer_schema_length: u64,
    ignore_errors: bool,
    truncate_ragged_lines: bool,
    missing_is_null: bool,
    low_memory: bool,
    rechunk: bool,
    out: *mut *mut phs_lazyframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let path = unsafe { path_string(path) }?;
        let n_rows = if has_n_rows { Some(usize_from_u64(n_rows, "csv n rows")?) } else { None };
        let infer_schema_length = if has_infer_schema_length {
            Some(usize_from_u64(infer_schema_length, "csv infer schema length")?)
        } else {
            None
        };
        let mut reader = LazyCsvReader::new(PlRefPath::new(path))
            .with_has_header(has_header)
            .with_separator(separator)
            .with_n_rows(n_rows)
            .with_skip_rows(usize_from_u64(skip_rows, "csv skip rows")?)
            .with_skip_rows_after_header(usize_from_u64(skip_rows_after_header, "csv skip rows after header")?)
            .with_infer_schema_length(infer_schema_length)
            .with_ignore_errors(ignore_errors)
            .with_missing_is_null(missing_is_null)
            .with_truncate_ragged_lines(truncate_ragged_lines)
            .with_low_memory(low_memory)
            .with_rechunk(rechunk);
        if has_null_value {
            let value = unsafe { c_str_to_str(null_value, "csv null value") }?;
            reader = reader.with_null_values(Some(NullValues::AllColumnsSingle(value.into())));
        }
        let lf = reader.finish()?;
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
    unsafe {
        phs_scan_parquet_options(path, false, 0, 0, true, false, false, true, out, err)
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_scan_parquet_options(
    path: *const c_char,
    has_n_rows: bool,
    n_rows: u64,
    parallel: c_int,
    use_statistics: bool,
    low_memory: bool,
    rechunk: bool,
    cache: bool,
    out: *mut *mut phs_lazyframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let path = unsafe { path_string(path) }?;
        let mut args = ScanArgsParquet {
            parallel: parquet_parallel_from_code(parallel)?,
            use_statistics,
            low_memory,
            rechunk,
            cache,
            ..Default::default()
        };
        if has_n_rows {
            args.n_rows = Some(usize_from_u64(n_rows, "parquet n rows")?);
        }
        let lf = LazyFrame::scan_parquet(PlRefPath::new(path), args)?;
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
pub unsafe extern "C" fn phs_lazyframe_collect_schema(
    lazyframe: *const phs_lazyframe,
    out: *mut *mut phs_bytes,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let mut lf = unsafe { lazyframe_ref(lazyframe) }?.value.clone();
        let schema = lf.collect_schema()?;
        *out = bytes_into_raw(encode_schema(schema.as_ref()));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_explain(
    lazyframe: *const phs_lazyframe,
    optimized: bool,
    out: *mut *mut phs_bytes,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let lf = unsafe { lazyframe_ref(lazyframe) }?.value.clone();
        *out = bytes_into_raw(lf.explain(optimized)?.into_bytes());
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_describe_plan(
    lazyframe: *const phs_lazyframe,
    optimized: bool,
    tree: bool,
    out: *mut *mut phs_bytes,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let lf = unsafe { lazyframe_ref(lazyframe) }?.value.clone();
        let plan = match (optimized, tree) {
            (false, false) => lf.describe_plan()?,
            (false, true) => lf.describe_plan_tree()?,
            (true, false) => lf.describe_optimized_plan()?,
            (true, true) => lf.describe_optimized_plan_tree()?,
        };
        *out = bytes_into_raw(plan.into_bytes());
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_to_dot(
    lazyframe: *const phs_lazyframe,
    optimized: bool,
    out: *mut *mut phs_bytes,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let lf = unsafe { lazyframe_ref(lazyframe) }?.value.clone();
        *out = bytes_into_raw(lf.to_dot(optimized)?.into_bytes());
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_profile(
    lazyframe: *const phs_lazyframe,
    result_out: *mut *mut phs_dataframe,
    profile_out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let result_out = unsafe { required_mut(result_out, "result_out") }?;
        let profile_out = unsafe { required_mut(profile_out, "profile_out") }?;
        *result_out = ptr::null_mut();
        *profile_out = ptr::null_mut();
        let lf = unsafe { lazyframe_ref(lazyframe) }?.value.clone();
        let (result, profile) = match lf.clone().profile() {
            Ok(profiled) => profiled,
            Err(error) if is_empty_profile_error(&error) => (lf.collect()?, empty_profile_frame()),
            Err(error) => return Err(error.into()),
        };
        *result_out = dataframe_into_raw(result);
        *profile_out = dataframe_into_raw(profile);
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
pub unsafe extern "C" fn phs_lazyframe_with_row_index(
    lazyframe: *const phs_lazyframe,
    name: *const c_char,
    has_offset: bool,
    offset: u64,
    out: *mut *mut phs_lazyframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let lf = unsafe { lazyframe_ref(lazyframe) }?.value.clone();
        let name = PlSmallStr::from_str(unsafe { c_str_to_str(name, "name") }?);
        let offset = if has_offset { Some(idx_size_from_u64(offset, "row index offset")?) } else { None };
        *out = lazyframe_into_raw(lf.with_row_index(name, offset));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_gather_every(
    lazyframe: *const phs_lazyframe,
    step: u64,
    offset: u64,
    out: *mut *mut phs_lazyframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let lf = unsafe { lazyframe_ref(lazyframe) }?.value.clone();
        let step = usize_from_u64(step, "lazyframe gather-every step")?;
        if step == 0 {
            return Err(PhsError::invalid_argument(
                "lazyframe gather-every step must be positive",
            ));
        }
        let offset = usize_from_u64(offset, "lazyframe gather-every offset")?;
        *out = lazyframe_into_raw(lf.select([all().as_expr().gather_every(step, offset)]));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_unpivot(
    lazyframe: *const phs_lazyframe,
    has_on: bool,
    on: *const *const c_char,
    on_len: usize,
    index: *const *const c_char,
    index_len: usize,
    variable_name: *const c_char,
    value_name: *const c_char,
    out: *mut *mut phs_lazyframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let lf = unsafe { lazyframe_ref(lazyframe) }?.value.clone();
        let on = if has_on {
            Some(selector_from_names_allow_empty(unsafe { name_vec(on, on_len) }?))
        } else {
            None
        };
        let index = selector_from_names_allow_empty(unsafe { name_vec(index, index_len) }?);
        let variable_name = if variable_name.is_null() {
            None
        } else {
            Some(PlSmallStr::from_str(unsafe {
                c_str_to_str(variable_name, "variable_name")?
            }))
        };
        let value_name = if value_name.is_null() {
            None
        } else {
            Some(PlSmallStr::from_str(unsafe { c_str_to_str(value_name, "value_name")? }))
        };
        let args = UnpivotArgsDSL {
            on,
            index,
            variable_name,
            value_name,
        };
        *out = lazyframe_into_raw(lf.unpivot(args));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_clear(
    lazyframe: *const phs_lazyframe,
    out: *mut *mut phs_lazyframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let lf = unsafe { lazyframe_ref(lazyframe) }?.value.clone();
        *out = lazyframe_into_raw(lf.limit(0));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_cache(
    lazyframe: *const phs_lazyframe,
    out: *mut *mut phs_lazyframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let lf = unsafe { lazyframe_ref(lazyframe) }?.value.clone();
        *out = lazyframe_into_raw(lf.cache());
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_first(
    lazyframe: *const phs_lazyframe,
    out: *mut *mut phs_lazyframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let lf = unsafe { lazyframe_ref(lazyframe) }?.value.clone();
        *out = lazyframe_into_raw(lf.first());
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_last(
    lazyframe: *const phs_lazyframe,
    out: *mut *mut phs_lazyframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let lf = unsafe { lazyframe_ref(lazyframe) }?.value.clone();
        *out = lazyframe_into_raw(lf.last());
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
        let n = idx_size_from_u64(n, "limit")?;
        *out = lazyframe_into_raw(lf.limit(n));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_top_k(
    lazyframe: *const phs_lazyframe,
    k: u64,
    by: *const *const phs_expr,
    by_len: usize,
    reverse: *const u8,
    reverse_len: usize,
    maintain_order: bool,
    out: *mut *mut phs_lazyframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let lf = unsafe { lazyframe_ref(lazyframe) }?.value.clone();
        let k = idx_size_from_u64(k, "lazyframe top_k count")?;
        let by = unsafe { expr_vec(by, by_len) }?;
        let reverse = unsafe { bool_vec(reverse, reverse_len, "reverse") }?;
        validate_sort_bool_options("reverse", by.len(), &reverse)?;
        let options = SortMultipleOptions::default()
            .with_order_descending_multi(reverse)
            .with_maintain_order(maintain_order);
        *out = lazyframe_into_raw(lf.top_k(k, by, options));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_bottom_k(
    lazyframe: *const phs_lazyframe,
    k: u64,
    by: *const *const phs_expr,
    by_len: usize,
    reverse: *const u8,
    reverse_len: usize,
    maintain_order: bool,
    out: *mut *mut phs_lazyframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let lf = unsafe { lazyframe_ref(lazyframe) }?.value.clone();
        let k = idx_size_from_u64(k, "lazyframe bottom_k count")?;
        let by = unsafe { expr_vec(by, by_len) }?;
        let reverse = unsafe { bool_vec(reverse, reverse_len, "reverse") }?;
        validate_sort_bool_options("reverse", by.len(), &reverse)?;
        let options = SortMultipleOptions::default()
            .with_order_descending_multi(reverse)
            .with_maintain_order(maintain_order);
        *out = lazyframe_into_raw(lf.bottom_k(k, by, options));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_explode(
    lazyframe: *const phs_lazyframe,
    names: *const *const c_char,
    names_len: usize,
    empty_as_null: bool,
    keep_nulls: bool,
    out: *mut *mut phs_lazyframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let lf = unsafe { lazyframe_ref(lazyframe) }?.value.clone();
        let names = unsafe { name_vec(names, names_len) }?;
        let selector = selector_from_names(names, "explode")?;
        let options = ExplodeOptions { empty_as_null, keep_nulls };
        *out = lazyframe_into_raw(lf.explode(selector, options));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_reverse(
    lazyframe: *const phs_lazyframe,
    out: *mut *mut phs_lazyframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let lf = unsafe { lazyframe_ref(lazyframe) }?.value.clone();
        *out = lazyframe_into_raw(lf.reverse());
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_drop(
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
        let selector = selector_from_names(names, "drop")?;
        *out = lazyframe_into_raw(lf.drop(selector));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_rename(
    lazyframe: *const phs_lazyframe,
    existing: *const *const c_char,
    new: *const *const c_char,
    len: usize,
    strict: bool,
    out: *mut *mut phs_lazyframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let lf = unsafe { lazyframe_ref(lazyframe) }?.value.clone();
        let existing = unsafe { name_vec(existing, len) }?;
        let new = unsafe { name_vec(new, len) }?;
        if existing.is_empty() {
            return Err(PhsError::invalid_argument("rename requires at least one column pair"));
        }
        *out = lazyframe_into_raw(lf.rename(existing, new, strict));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_slice(
    lazyframe: *const phs_lazyframe,
    offset: i64,
    len: u64,
    out: *mut *mut phs_lazyframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let lf = unsafe { lazyframe_ref(lazyframe) }?.value.clone();
        let len = idx_size_from_u64(len, "slice length")?;
        *out = lazyframe_into_raw(lf.slice(offset, len));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_head(
    lazyframe: *const phs_lazyframe,
    n: u64,
    out: *mut *mut phs_lazyframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let lf = unsafe { lazyframe_ref(lazyframe) }?.value.clone();
        let n = idx_size_from_u64(n, "head count")?;
        *out = lazyframe_into_raw(lf.limit(n));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_tail(
    lazyframe: *const phs_lazyframe,
    n: u64,
    out: *mut *mut phs_lazyframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let lf = unsafe { lazyframe_ref(lazyframe) }?.value.clone();
        let n = idx_size_from_u64(n, "tail count")?;
        *out = lazyframe_into_raw(lf.tail(n));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_drop_nulls(
    lazyframe: *const phs_lazyframe,
    names: *const *const c_char,
    len: usize,
    has_subset: bool,
    out: *mut *mut phs_lazyframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let lf = unsafe { lazyframe_ref(lazyframe) }?.value.clone();
        let subset = unsafe { optional_selector(names, len, has_subset, "drop_nulls subset") }?;
        *out = lazyframe_into_raw(lf.drop_nulls(subset));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_fill_null(
    lazyframe: *const phs_lazyframe,
    value: *const phs_expr,
    out: *mut *mut phs_lazyframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let lf = unsafe { lazyframe_ref(lazyframe) }?.value.clone();
        let value = unsafe { expr_ref(value) }?.value.clone();
        *out = lazyframe_into_raw(lf.fill_null(value));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_fill_nan(
    lazyframe: *const phs_lazyframe,
    value: *const phs_expr,
    out: *mut *mut phs_lazyframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let lf = unsafe { lazyframe_ref(lazyframe) }?.value.clone();
        let value = unsafe { expr_ref(value) }?.value.clone();
        *out = lazyframe_into_raw(lf.fill_nan(value));
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_null_count(
    lazyframe: *const phs_lazyframe,
    out: *mut *mut phs_lazyframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let lf = unsafe { lazyframe_ref(lazyframe) }?.value.clone();
        *out = lazyframe_into_raw(lf.null_count());
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_unique(
    lazyframe: *const phs_lazyframe,
    names: *const *const c_char,
    len: usize,
    has_subset: bool,
    keep_strategy: c_int,
    maintain_order: bool,
    out: *mut *mut phs_lazyframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let lf = unsafe { lazyframe_ref(lazyframe) }?.value.clone();
        let subset = unsafe { optional_selector(names, len, has_subset, "unique subset") }?;
        let keep_strategy = keep_strategy_from_code(keep_strategy)?;
        let output = if maintain_order {
            lf.unique_stable(subset, keep_strategy)
        } else {
            lf.unique(subset, keep_strategy)
        };
        *out = lazyframe_into_raw(output);
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

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_lazyframe_join(
    left: *const phs_lazyframe,
    right: *const phs_lazyframe,
    left_on: *const *const phs_expr,
    left_len: usize,
    right_on: *const *const phs_expr,
    right_len: usize,
    join_type: c_int,
    suffix: *const c_char,
    out: *mut *mut phs_lazyframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let join_type = join_type_from_code(join_type)?;
        let left_frame = unsafe { lazyframe_ref(left) }?.value.clone();
        let right_frame = unsafe { lazyframe_ref(right) }?.value.clone();
        let suffix = unsafe { optional_suffix(suffix) }?;
        if matches!(join_type, JoinType::Cross) {
            if left_len != 0 || right_len != 0 {
                return Err(PhsError::invalid_argument("cross join requires empty join key lists"));
            }
            *out = lazyframe_into_raw(left_frame.cross_join(right_frame, suffix));
            return Ok(());
        }
        if left_len == 0 {
            return Err(PhsError::invalid_argument("left join keys must contain at least one expression"));
        }
        if right_len == 0 {
            return Err(PhsError::invalid_argument("right join keys must contain at least one expression"));
        }
        if left_len != right_len {
            return Err(PhsError::invalid_argument("left and right join key counts must match"));
        }
        let left_on = unsafe { expr_vec(left_on, left_len) }?;
        let right_on = unsafe { expr_vec(right_on, right_len) }?;
        let mut args = JoinArgs::new(join_type);
        if let Some(suffix) = suffix {
            args = args.with_suffix(Some(suffix));
        }
        *out = lazyframe_into_raw(left_frame.join(right_frame, left_on, right_on, args));
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::PHS_OK;
    use std::ffi::CStr;

    unsafe fn take_raw_bytes(raw: *mut phs_bytes) -> Vec<u8> {
        assert!(!raw.is_null());
        let len = unsafe { crate::bytes::phs_bytes_len(raw) };
        let data = unsafe { crate::bytes::phs_bytes_data(raw) };
        assert!(!data.is_null());
        let bytes = unsafe { std::slice::from_raw_parts(data, len) }.to_vec();
        unsafe { crate::bytes::phs_bytes_free(raw) };
        bytes
    }

    unsafe fn take_text(raw: *mut phs_bytes) -> String {
        String::from_utf8(unsafe { take_raw_bytes(raw) }).unwrap()
    }

    unsafe fn take_error_message(raw: *mut phs_error) -> String {
        assert!(!raw.is_null());
        let message = unsafe { CStr::from_ptr(crate::error::phs_error_message(raw)) }
            .to_str()
            .unwrap()
            .to_owned();
        unsafe { crate::error::phs_error_free(raw) };
        message
    }

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

    fn employees_fixture_path() -> std::ffi::CString {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("test")
            .join("data")
            .join("employees.csv");
        std::ffi::CString::new(path.to_string_lossy().as_bytes()).unwrap()
    }

    fn departments_fixture_path() -> std::ffi::CString {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("test")
            .join("data")
            .join("departments.csv");
        std::ffi::CString::new(path.to_string_lossy().as_bytes()).unwrap()
    }

    fn list_lazyframe() -> *mut phs_lazyframe {
        let part0 = Series::new(PlSmallStr::from_static(""), ["red", "green", "blue"]);
        let part1 = Series::new(PlSmallStr::from_static(""), ["red", "red"]);
        let part2 = Series::new(PlSmallStr::from_static(""), ["日本", "語"]);
        let part3 = Series::new(PlSmallStr::from_static(""), ["solo"]);
        let parts = Column::new(PlSmallStr::from_static("parts"), [part0, part1, part2, part3]);
        let id = Column::new(PlSmallStr::from_static("id"), [1i32, 2, 3, 4]);
        lazyframe_into_raw(DataFrame::new_infer_height(vec![id, parts]).unwrap().lazy())
    }

    fn push_schema_field(bytes: &mut Vec<u8>, name: &[u8], dtype_tag: u16, dtype_detail: &[u8]) {
        bytes.extend_from_slice(&(name.len() as u64).to_le_bytes());
        bytes.extend_from_slice(name);
        bytes.extend_from_slice(&dtype_tag.to_le_bytes());
        bytes.extend_from_slice(&(dtype_detail.len() as u64).to_le_bytes());
        bytes.extend_from_slice(dtype_detail);
    }

    #[test]
    fn lazy_collect_schema_reports_current_plan_fields() {
        let path = employees_fixture_path();
        let mut lf0 = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(unsafe { phs_scan_csv(path.as_ptr(), &mut lf0, &mut err) }, PHS_OK);

        let mut out = ptr::null_mut();
        let status = unsafe { phs_lazyframe_collect_schema(lf0, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert!(err.is_null());
        let bytes = unsafe { take_raw_bytes(out) };
        let mut expected = b"PHS1SCH\0".to_vec();
        expected.extend_from_slice(&4_u64.to_le_bytes());
        push_schema_field(&mut expected, b"id", 4, b"Int64");
        push_schema_field(&mut expected, b"name", 11, b"String");
        push_schema_field(&mut expected, b"department", 11, b"String");
        push_schema_field(&mut expected, b"salary", 4, b"Int64");
        assert_eq!(bytes, expected);

        let name = std::ffi::CString::new("name").unwrap();
        let mut name_expr = ptr::null_mut();
        assert_eq!(unsafe { crate::expr::phs_expr_col(name.as_ptr(), &mut name_expr, &mut err) }, PHS_OK);
        let exprs = [name_expr as *const phs_expr];
        let mut selected = ptr::null_mut();
        assert_eq!(unsafe { phs_lazyframe_select(lf0, exprs.as_ptr(), exprs.len(), &mut selected, &mut err) }, PHS_OK);
        let status = unsafe { phs_lazyframe_collect_schema(selected, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        assert!(err.is_null());
        let bytes = unsafe { take_raw_bytes(out) };
        let mut expected = b"PHS1SCH\0".to_vec();
        expected.extend_from_slice(&1_u64.to_le_bytes());
        push_schema_field(&mut expected, b"name", 11, b"String");
        assert_eq!(bytes, expected);

        let missing = std::ffi::CString::new("missing").unwrap();
        let mut missing_expr = ptr::null_mut();
        assert_eq!(unsafe { crate::expr::phs_expr_col(missing.as_ptr(), &mut missing_expr, &mut err) }, PHS_OK);
        let exprs = [missing_expr as *const phs_expr];
        let mut missing_lf = ptr::null_mut();
        assert_eq!(unsafe { phs_lazyframe_select(lf0, exprs.as_ptr(), exprs.len(), &mut missing_lf, &mut err) }, PHS_OK);
        let status = unsafe { phs_lazyframe_collect_schema(missing_lf, &mut out, &mut err) };
        assert_eq!(status, crate::error::PHS_POLARS_ERROR);
        assert!(!err.is_null());
        unsafe { crate::error::phs_error_free(err) };
        err = ptr::null_mut();

        let status = unsafe { phs_lazyframe_collect_schema(ptr::null(), &mut out, &mut err) };
        assert_eq!(status, crate::error::PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "lazyframe pointer was null");
        err = ptr::null_mut();

        let status = unsafe { phs_lazyframe_collect_schema(lf0, ptr::null_mut(), &mut err) };
        assert_eq!(status, crate::error::PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "out pointer was null");

        unsafe {
            crate::handles::phs_expr_free(name_expr);
            crate::handles::phs_expr_free(missing_expr);
            crate::handles::phs_lazyframe_free(lf0);
            crate::handles::phs_lazyframe_free(selected);
            crate::handles::phs_lazyframe_free(missing_lf);
        }
    }

    #[test]
    fn lazy_plan_introspection_returns_text_and_dot_outputs() {
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

        let mut filtered = ptr::null_mut();
        assert_eq!(unsafe { phs_lazyframe_filter(lf0, pred_expr, &mut filtered, &mut err) }, PHS_OK);

        let mut out = ptr::null_mut();
        let status = unsafe { phs_lazyframe_describe_plan(filtered, false, false, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let plan = unsafe { take_text(out) };
        assert!(plan.contains("FILTER"));
        assert!(plan.contains("SCAN"));

        let status = unsafe { phs_lazyframe_describe_plan(filtered, false, true, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let tree = unsafe { take_text(out) };
        assert!(tree.contains("FILTER"));
        assert!(tree.contains("SCAN"));

        let status = unsafe { phs_lazyframe_to_dot(filtered, true, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let dot = unsafe { take_text(out) };
        assert!(dot.contains("digraph"));
        assert!(dot.contains("SCAN"));

        let missing = std::ffi::CString::new("missing").unwrap();
        let mut missing_expr = ptr::null_mut();
        assert_eq!(unsafe { crate::expr::phs_expr_col(missing.as_ptr(), &mut missing_expr, &mut err) }, PHS_OK);
        let exprs = [missing_expr as *const phs_expr];
        let mut missing_lf = ptr::null_mut();
        assert_eq!(unsafe { phs_lazyframe_select(lf0, exprs.as_ptr(), exprs.len(), &mut missing_lf, &mut err) }, PHS_OK);
        let status = unsafe { phs_lazyframe_describe_plan(missing_lf, true, false, &mut out, &mut err) };
        assert_eq!(status, crate::error::PHS_POLARS_ERROR);
        unsafe { crate::error::phs_error_free(err) };
        err = ptr::null_mut();

        let status = unsafe { phs_lazyframe_describe_plan(ptr::null(), false, false, &mut out, &mut err) };
        assert_eq!(status, crate::error::PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "lazyframe pointer was null");
        err = ptr::null_mut();

        let status = unsafe { phs_lazyframe_to_dot(filtered, false, ptr::null_mut(), &mut err) };
        assert_eq!(status, crate::error::PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "out pointer was null");

        unsafe {
            crate::handles::phs_expr_free(age_expr);
            crate::handles::phs_expr_free(lit_expr);
            crate::handles::phs_expr_free(pred_expr);
            crate::handles::phs_expr_free(missing_expr);
            crate::handles::phs_lazyframe_free(lf0);
            crate::handles::phs_lazyframe_free(filtered);
            crate::handles::phs_lazyframe_free(missing_lf);
        }
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

    #[test]
    fn lazy_join_inner_collects_expected_shape() {
        let employees_path = employees_fixture_path();
        let departments_path = departments_fixture_path();
        let mut left = ptr::null_mut();
        let mut right = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(unsafe { phs_scan_csv(employees_path.as_ptr(), &mut left, &mut err) }, PHS_OK);
        assert_eq!(unsafe { phs_scan_csv(departments_path.as_ptr(), &mut right, &mut err) }, PHS_OK);

        let department = std::ffi::CString::new("department").unwrap();
        let mut left_key = ptr::null_mut();
        let mut right_key = ptr::null_mut();
        assert_eq!(unsafe { crate::expr::phs_expr_col(department.as_ptr(), &mut left_key, &mut err) }, PHS_OK);
        assert_eq!(unsafe { crate::expr::phs_expr_col(department.as_ptr(), &mut right_key, &mut err) }, PHS_OK);

        let left_keys = [left_key as *const phs_expr];
        let right_keys = [right_key as *const phs_expr];
        let mut joined: *mut phs_lazyframe = ptr::null_mut();
        assert_eq!(
            unsafe {
                phs_lazyframe_join(
                    left,
                    right,
                    left_keys.as_ptr(),
                    left_keys.len(),
                    right_keys.as_ptr(),
                    right_keys.len(),
                    0,
                    ptr::null(),
                    &mut joined,
                    &mut err,
                )
            },
            PHS_OK
        );

        let mut df = ptr::null_mut();
        assert_eq!(unsafe { phs_lazyframe_collect(joined, &mut df, &mut err) }, PHS_OK);
        let mut height = 0;
        let mut width = 0;
        assert_eq!(unsafe { crate::dataframe::phs_dataframe_shape(df, &mut height, &mut width, &mut err) }, PHS_OK);
        assert_eq!((height, width), (3, 6));

        unsafe {
            crate::handles::phs_expr_free(left_key);
            crate::handles::phs_expr_free(right_key);
            crate::handles::phs_lazyframe_free(left);
            crate::handles::phs_lazyframe_free(right);
            crate::handles::phs_lazyframe_free(joined);
            crate::handles::phs_dataframe_free(df);
        }
    }

    #[test]
    fn lazy_top_and_bottom_k_select_rows_by_expressions() {
        let path = employees_fixture_path();
        let mut lf0 = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(unsafe { phs_scan_csv(path.as_ptr(), &mut lf0, &mut err) }, PHS_OK);

        let salary = std::ffi::CString::new("salary").unwrap();
        let mut salary_expr = ptr::null_mut();
        assert_eq!(unsafe { crate::expr::phs_expr_col(salary.as_ptr(), &mut salary_expr, &mut err) }, PHS_OK);
        let by = [salary_expr as *const phs_expr];
        let forward = [0_u8];
        let reverse = [1_u8];
        let mismatched_reverse = [0_u8, 1_u8];
        let mut out = ptr::null_mut();

        let status = unsafe {
            phs_lazyframe_top_k(
                lf0,
                2,
                by.as_ptr(),
                by.len(),
                forward.as_ptr(),
                forward.len(),
                false,
                &mut out,
                &mut err,
            )
        };
        assert_eq!(status, PHS_OK);
        let top_lf = out;
        let mut top_df = ptr::null_mut();
        assert_eq!(unsafe { phs_lazyframe_collect(top_lf, &mut top_df, &mut err) }, PHS_OK);
        let top_salaries: Vec<Option<i64>> = unsafe { crate::handles::dataframe_ref(top_df) }
            .unwrap()
            .value
            .column("salary")
            .unwrap()
            .as_materialized_series()
            .i64()
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(top_salaries, vec![Some(150), Some(100)]);

        let status = unsafe {
            phs_lazyframe_bottom_k(
                lf0,
                2,
                by.as_ptr(),
                by.len(),
                forward.as_ptr(),
                forward.len(),
                false,
                &mut out,
                &mut err,
            )
        };
        assert_eq!(status, PHS_OK);
        let bottom_lf = out;
        let mut bottom_df = ptr::null_mut();
        assert_eq!(unsafe { phs_lazyframe_collect(bottom_lf, &mut bottom_df, &mut err) }, PHS_OK);
        let bottom_salaries: Vec<Option<i64>> = unsafe { crate::handles::dataframe_ref(bottom_df) }
            .unwrap()
            .value
            .column("salary")
            .unwrap()
            .as_materialized_series()
            .i64()
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(bottom_salaries, vec![Some(80), Some(90)]);

        let status = unsafe {
            phs_lazyframe_top_k(
                lf0,
                2,
                by.as_ptr(),
                by.len(),
                reverse.as_ptr(),
                reverse.len(),
                true,
                &mut out,
                &mut err,
            )
        };
        assert_eq!(status, PHS_OK);
        let reversed_top_lf = out;
        let mut reversed_top_df = ptr::null_mut();
        assert_eq!(unsafe { phs_lazyframe_collect(reversed_top_lf, &mut reversed_top_df, &mut err) }, PHS_OK);
        let reversed_top_salaries: Vec<Option<i64>> =
            unsafe { crate::handles::dataframe_ref(reversed_top_df) }
                .unwrap()
                .value
                .column("salary")
                .unwrap()
                .as_materialized_series()
                .i64()
                .unwrap()
                .into_iter()
                .collect();
        assert_eq!(reversed_top_salaries, vec![Some(80), Some(90)]);

        let status = unsafe {
            phs_lazyframe_top_k(
                lf0,
                u64::MAX,
                by.as_ptr(),
                by.len(),
                forward.as_ptr(),
                forward.len(),
                false,
                &mut out,
                &mut err,
            )
        };
        assert_eq!(status, crate::error::PHS_INVALID_ARGUMENT);
        unsafe { crate::error::phs_error_free(err) };
        err = ptr::null_mut();

        let status = unsafe {
            phs_lazyframe_top_k(
                lf0,
                1,
                ptr::null(),
                1,
                forward.as_ptr(),
                forward.len(),
                false,
                &mut out,
                &mut err,
            )
        };
        assert_eq!(status, crate::error::PHS_INVALID_ARGUMENT);
        unsafe { crate::error::phs_error_free(err) };
        err = ptr::null_mut();

        let status = unsafe {
            phs_lazyframe_top_k(
                lf0,
                1,
                by.as_ptr(),
                by.len(),
                ptr::null(),
                1,
                false,
                &mut out,
                &mut err,
            )
        };
        assert_eq!(status, crate::error::PHS_INVALID_ARGUMENT);
        unsafe { crate::error::phs_error_free(err) };
        err = ptr::null_mut();

        let status = unsafe {
            phs_lazyframe_top_k(
                lf0,
                1,
                by.as_ptr(),
                by.len(),
                mismatched_reverse.as_ptr(),
                mismatched_reverse.len(),
                false,
                &mut out,
                &mut err,
            )
        };
        assert_eq!(status, crate::error::PHS_INVALID_ARGUMENT);
        unsafe { crate::error::phs_error_free(err) };
        err = ptr::null_mut();

        let status = unsafe {
            phs_lazyframe_top_k(
                lf0,
                1,
                by.as_ptr(),
                by.len(),
                forward.as_ptr(),
                forward.len(),
                false,
                ptr::null_mut(),
                &mut err,
            )
        };
        assert_eq!(status, crate::error::PHS_INVALID_ARGUMENT);
        unsafe { crate::error::phs_error_free(err) };

        unsafe {
            crate::handles::phs_expr_free(salary_expr);
            crate::handles::phs_lazyframe_free(lf0);
            crate::handles::phs_lazyframe_free(top_lf);
            crate::handles::phs_lazyframe_free(bottom_lf);
            crate::handles::phs_lazyframe_free(reversed_top_lf);
            crate::handles::phs_dataframe_free(top_df);
            crate::handles::phs_dataframe_free(bottom_df);
            crate::handles::phs_dataframe_free(reversed_top_df);
        }
    }

    #[test]
    fn lazy_explode_list_columns_work() {
        let lf0 = list_lazyframe();
        let parts_name = std::ffi::CString::new("parts").unwrap();
        let id_name = std::ffi::CString::new("id").unwrap();
        let missing_name = std::ffi::CString::new("missing").unwrap();
        let names = [parts_name.as_ptr()];
        let id_names = [id_name.as_ptr()];
        let missing_names = [missing_name.as_ptr()];
        let mut out = ptr::null_mut();
        let mut err = ptr::null_mut();

        let status = unsafe { phs_lazyframe_explode(lf0, names.as_ptr(), names.len(), true, true, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let exploded_lf = out;
        let mut exploded_df = ptr::null_mut();
        assert_eq!(unsafe { phs_lazyframe_collect(exploded_lf, &mut exploded_df, &mut err) }, PHS_OK);
        let exploded_ref = unsafe { crate::handles::dataframe_ref(exploded_df) }.unwrap();
        assert_eq!(exploded_ref.value.shape(), (8, 2));
        let ids: Vec<Option<i32>> = exploded_ref
            .value
            .column("id")
            .unwrap()
            .as_materialized_series()
            .i32()
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(ids, vec![Some(1), Some(1), Some(1), Some(2), Some(2), Some(3), Some(3), Some(4)]);
        let values: Vec<Option<&str>> = exploded_ref
            .value
            .column("parts")
            .unwrap()
            .as_materialized_series()
            .str()
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(
            values,
            vec![Some("red"), Some("green"), Some("blue"), Some("red"), Some("red"), Some("日本"), Some("語"), Some("solo")]
        );

        let status = unsafe { phs_lazyframe_explode(lf0, ptr::null(), 0, true, true, &mut out, &mut err) };
        assert_eq!(status, crate::error::PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "explode requires at least one column name");
        err = ptr::null_mut();

        let status = unsafe { phs_lazyframe_explode(lf0, ptr::null(), 1, true, true, &mut out, &mut err) };
        assert_eq!(status, crate::error::PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "names pointer was null");
        err = ptr::null_mut();

        let null_name = [ptr::null()];
        let status = unsafe { phs_lazyframe_explode(lf0, null_name.as_ptr(), null_name.len(), true, true, &mut out, &mut err) };
        assert_eq!(status, crate::error::PHS_INVALID_ARGUMENT);
        unsafe { take_error_message(err) };
        err = ptr::null_mut();

        let status = unsafe { phs_lazyframe_explode(lf0, missing_names.as_ptr(), missing_names.len(), true, true, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let missing_lf = out;
        let mut missing_df = ptr::null_mut();
        let collect_status = unsafe { phs_lazyframe_collect(missing_lf, &mut missing_df, &mut err) };
        assert_ne!(collect_status, PHS_OK);
        unsafe { take_error_message(err) };
        err = ptr::null_mut();

        let status = unsafe { phs_lazyframe_explode(lf0, id_names.as_ptr(), id_names.len(), true, true, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let scalar_lf = out;
        let mut scalar_df = ptr::null_mut();
        let collect_status = unsafe { phs_lazyframe_collect(scalar_lf, &mut scalar_df, &mut err) };
        assert_ne!(collect_status, PHS_OK);
        unsafe { take_error_message(err) };
        err = ptr::null_mut();

        let status = unsafe { phs_lazyframe_explode(lf0, names.as_ptr(), names.len(), true, true, ptr::null_mut(), &mut err) };
        assert_eq!(status, crate::error::PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "out pointer was null");

        let status = unsafe { phs_lazyframe_explode(ptr::null(), names.as_ptr(), names.len(), true, true, &mut out, &mut err) };
        assert_eq!(status, crate::error::PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "lazyframe pointer was null");

        unsafe {
            crate::handles::phs_lazyframe_free(lf0);
            crate::handles::phs_lazyframe_free(exploded_lf);
            crate::handles::phs_lazyframe_free(missing_lf);
            crate::handles::phs_lazyframe_free(scalar_lf);
            crate::handles::phs_dataframe_free(exploded_df);
        }
    }

    #[test]
    fn lazy_reverse_flips_row_order() {
        let path = employees_fixture_path();
        let mut lf0 = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(unsafe { phs_scan_csv(path.as_ptr(), &mut lf0, &mut err) }, PHS_OK);

        let mut out = ptr::null_mut();
        let status = unsafe { phs_lazyframe_reverse(lf0, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let reversed_lf = out;
        let mut reversed_df = ptr::null_mut();
        assert_eq!(unsafe { phs_lazyframe_collect(reversed_lf, &mut reversed_df, &mut err) }, PHS_OK);
        let names: Vec<Option<&str>> = unsafe { crate::handles::dataframe_ref(reversed_df) }
            .unwrap()
            .value
            .column("name")
            .unwrap()
            .as_materialized_series()
            .str()
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(names, vec![Some("Eve"), Some("Carol"), Some("Bob"), Some("Alice")]);

        let status = unsafe { phs_lazyframe_reverse(ptr::null(), &mut out, &mut err) };
        assert_eq!(status, crate::error::PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "lazyframe pointer was null");
        err = ptr::null_mut();

        let status = unsafe { phs_lazyframe_reverse(lf0, ptr::null_mut(), &mut err) };
        assert_eq!(status, crate::error::PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "out pointer was null");

        unsafe {
            crate::handles::phs_lazyframe_free(lf0);
            crate::handles::phs_lazyframe_free(reversed_lf);
            crate::handles::phs_dataframe_free(reversed_df);
        }
    }

    #[test]
    fn lazy_with_row_index_inserts_index_column() {
        let path = employees_fixture_path();
        let mut lf0 = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(unsafe { phs_scan_csv(path.as_ptr(), &mut lf0, &mut err) }, PHS_OK);

        let row_nr = std::ffi::CString::new("row_nr").unwrap();
        let mut out = ptr::null_mut();
        let status = unsafe { phs_lazyframe_with_row_index(lf0, row_nr.as_ptr(), true, 5, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let indexed_lf = out;
        let mut indexed_df = ptr::null_mut();
        assert_eq!(unsafe { phs_lazyframe_collect(indexed_lf, &mut indexed_df, &mut err) }, PHS_OK);
        let indexed_ref = unsafe { crate::handles::dataframe_ref(indexed_df) }.unwrap();
        let names: Vec<&str> = indexed_ref.value.get_column_names().into_iter().map(|name| name.as_str()).collect();
        assert_eq!(names, vec!["row_nr", "id", "name", "department", "salary"]);
        let row_ids: Vec<Option<IdxSize>> = indexed_ref
            .value
            .column("row_nr")
            .unwrap()
            .as_materialized_series()
            .idx()
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(row_ids, vec![Some(5), Some(6), Some(7), Some(8)]);

        let status = unsafe { phs_lazyframe_with_row_index(lf0, row_nr.as_ptr(), false, 999, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let default_lf = out;
        let mut default_df = ptr::null_mut();
        assert_eq!(unsafe { phs_lazyframe_collect(default_lf, &mut default_df, &mut err) }, PHS_OK);
        let default_ids: Vec<Option<IdxSize>> = unsafe { crate::handles::dataframe_ref(default_df) }
            .unwrap()
            .value
            .column("row_nr")
            .unwrap()
            .as_materialized_series()
            .idx()
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(default_ids, vec![Some(0), Some(1), Some(2), Some(3)]);

        let status = unsafe { phs_lazyframe_with_row_index(ptr::null(), row_nr.as_ptr(), false, 0, &mut out, &mut err) };
        assert_eq!(status, crate::error::PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "lazyframe pointer was null");
        err = ptr::null_mut();

        let status = unsafe { phs_lazyframe_with_row_index(lf0, ptr::null(), false, 0, &mut out, &mut err) };
        assert_eq!(status, crate::error::PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "name pointer was null");
        err = ptr::null_mut();

        let status = unsafe { phs_lazyframe_with_row_index(lf0, row_nr.as_ptr(), true, u64::MAX, &mut out, &mut err) };
        assert_eq!(status, crate::error::PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "row index offset exceeds Polars index size");
        err = ptr::null_mut();

        let status = unsafe { phs_lazyframe_with_row_index(lf0, row_nr.as_ptr(), false, 0, ptr::null_mut(), &mut err) };
        assert_eq!(status, crate::error::PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "out pointer was null");

        unsafe {
            crate::handles::phs_lazyframe_free(lf0);
            crate::handles::phs_lazyframe_free(indexed_lf);
            crate::handles::phs_lazyframe_free(default_lf);
            crate::handles::phs_dataframe_free(indexed_df);
            crate::handles::phs_dataframe_free(default_df);
        }
    }

    #[test]
    fn lazy_basic_views_cache_clear_first_last_work() {
        let path = employees_fixture_path();
        let mut lf0 = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(unsafe { phs_scan_csv(path.as_ptr(), &mut lf0, &mut err) }, PHS_OK);

        let mut out = ptr::null_mut();
        let status = unsafe { phs_lazyframe_clear(lf0, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let clear_lf = out;
        let mut clear_df = ptr::null_mut();
        assert_eq!(unsafe { phs_lazyframe_collect(clear_lf, &mut clear_df, &mut err) }, PHS_OK);
        let clear_ref = unsafe { crate::handles::dataframe_ref(clear_df) }.unwrap();
        assert_eq!(clear_ref.value.shape(), (0, 4));
        let names: Vec<&str> = clear_ref.value.get_column_names().into_iter().map(|name| name.as_str()).collect();
        assert_eq!(names, vec!["id", "name", "department", "salary"]);

        let status = unsafe { phs_lazyframe_cache(lf0, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let cache_lf = out;
        let mut cache_df = ptr::null_mut();
        assert_eq!(unsafe { phs_lazyframe_collect(cache_lf, &mut cache_df, &mut err) }, PHS_OK);
        assert_eq!(unsafe { crate::handles::dataframe_ref(cache_df) }.unwrap().value.shape(), (4, 4));

        let status = unsafe { phs_lazyframe_first(lf0, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let first_lf = out;
        let mut first_df = ptr::null_mut();
        assert_eq!(unsafe { phs_lazyframe_collect(first_lf, &mut first_df, &mut err) }, PHS_OK);
        let names: Vec<Option<&str>> = unsafe { crate::handles::dataframe_ref(first_df) }
            .unwrap()
            .value
            .column("name")
            .unwrap()
            .as_materialized_series()
            .str()
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(names, vec![Some("Alice")]);

        let status = unsafe { phs_lazyframe_last(lf0, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let last_lf = out;
        let mut last_df = ptr::null_mut();
        assert_eq!(unsafe { phs_lazyframe_collect(last_lf, &mut last_df, &mut err) }, PHS_OK);
        let names: Vec<Option<&str>> = unsafe { crate::handles::dataframe_ref(last_df) }
            .unwrap()
            .value
            .column("name")
            .unwrap()
            .as_materialized_series()
            .str()
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(names, vec![Some("Eve")]);

        let status = unsafe { phs_lazyframe_first(ptr::null(), &mut out, &mut err) };
        assert_eq!(status, crate::error::PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "lazyframe pointer was null");
        err = ptr::null_mut();

        let status = unsafe { phs_lazyframe_clear(lf0, ptr::null_mut(), &mut err) };
        assert_eq!(status, crate::error::PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "out pointer was null");

        unsafe {
            crate::handles::phs_lazyframe_free(lf0);
            crate::handles::phs_lazyframe_free(clear_lf);
            crate::handles::phs_lazyframe_free(cache_lf);
            crate::handles::phs_lazyframe_free(first_lf);
            crate::handles::phs_lazyframe_free(last_lf);
            crate::handles::phs_dataframe_free(clear_df);
            crate::handles::phs_dataframe_free(cache_df);
            crate::handles::phs_dataframe_free(first_df);
            crate::handles::phs_dataframe_free(last_df);
        }
    }

    #[test]
    fn lazy_gather_every_returns_strided_rows() {
        let path = employees_fixture_path();
        let mut lf0 = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(unsafe { phs_scan_csv(path.as_ptr(), &mut lf0, &mut err) }, PHS_OK);

        let mut out = ptr::null_mut();
        let status = unsafe { phs_lazyframe_gather_every(lf0, 2, 0, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let every_two_lf = out;
        let mut every_two_df = ptr::null_mut();
        assert_eq!(unsafe { phs_lazyframe_collect(every_two_lf, &mut every_two_df, &mut err) }, PHS_OK);
        let names: Vec<Option<&str>> = unsafe { crate::handles::dataframe_ref(every_two_df) }
            .unwrap()
            .value
            .column("name")
            .unwrap()
            .as_materialized_series()
            .str()
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(names, vec![Some("Alice"), Some("Carol")]);

        let status = unsafe { phs_lazyframe_gather_every(lf0, 2, 1, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let offset_lf = out;
        let mut offset_df = ptr::null_mut();
        assert_eq!(unsafe { phs_lazyframe_collect(offset_lf, &mut offset_df, &mut err) }, PHS_OK);
        let names: Vec<Option<&str>> = unsafe { crate::handles::dataframe_ref(offset_df) }
            .unwrap()
            .value
            .column("name")
            .unwrap()
            .as_materialized_series()
            .str()
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(names, vec![Some("Bob"), Some("Eve")]);

        let status = unsafe { phs_lazyframe_gather_every(lf0, 2, 10, &mut out, &mut err) };
        assert_eq!(status, PHS_OK);
        let empty_lf = out;
        let mut empty_df = ptr::null_mut();
        assert_eq!(unsafe { phs_lazyframe_collect(empty_lf, &mut empty_df, &mut err) }, PHS_OK);
        assert_eq!(unsafe { crate::handles::dataframe_ref(empty_df) }.unwrap().value.shape(), (0, 4));

        let status = unsafe { phs_lazyframe_gather_every(lf0, 0, 0, &mut out, &mut err) };
        assert_eq!(status, crate::error::PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "lazyframe gather-every step must be positive");
        err = ptr::null_mut();

        let status = unsafe { phs_lazyframe_gather_every(ptr::null(), 2, 0, &mut out, &mut err) };
        assert_eq!(status, crate::error::PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "lazyframe pointer was null");
        err = ptr::null_mut();

        let status = unsafe { phs_lazyframe_gather_every(lf0, 2, 0, ptr::null_mut(), &mut err) };
        assert_eq!(status, crate::error::PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "out pointer was null");

        unsafe {
            crate::handles::phs_lazyframe_free(lf0);
            crate::handles::phs_lazyframe_free(every_two_lf);
            crate::handles::phs_lazyframe_free(offset_lf);
            crate::handles::phs_lazyframe_free(empty_lf);
            crate::handles::phs_dataframe_free(every_two_df);
            crate::handles::phs_dataframe_free(offset_df);
            crate::handles::phs_dataframe_free(empty_df);
        }
    }

    #[test]
    fn lazy_unpivot_returns_long_form_frames() {
        let path = employees_fixture_path();
        let mut lf0 = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(unsafe { phs_scan_csv(path.as_ptr(), &mut lf0, &mut err) }, PHS_OK);

        let department = std::ffi::CString::new("department").unwrap();
        let salary = std::ffi::CString::new("salary").unwrap();
        let missing = std::ffi::CString::new("missing").unwrap();
        let metric = std::ffi::CString::new("metric").unwrap();
        let amount = std::ffi::CString::new("amount").unwrap();
        let index = [department.as_ptr()];
        let on = [salary.as_ptr()];
        let missing_index = [missing.as_ptr()];
        let mut out = ptr::null_mut();

        let status = unsafe {
            phs_lazyframe_unpivot(
                lf0,
                true,
                on.as_ptr(),
                on.len(),
                index.as_ptr(),
                index.len(),
                metric.as_ptr(),
                amount.as_ptr(),
                &mut out,
                &mut err,
            )
        };
        assert_eq!(status, PHS_OK);
        let explicit_lf = out;
        let mut explicit_df = ptr::null_mut();
        assert_eq!(unsafe { phs_lazyframe_collect(explicit_lf, &mut explicit_df, &mut err) }, PHS_OK);
        assert_eq!(unsafe { crate::handles::dataframe_ref(explicit_df) }.unwrap().value.shape(), (4, 3));
        let values: Vec<Option<i64>> = unsafe { crate::handles::dataframe_ref(explicit_df) }
            .unwrap()
            .value
            .column("amount")
            .unwrap()
            .as_materialized_series()
            .i64()
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(values, vec![Some(100), Some(150), Some(90), Some(80)]);

        let status = unsafe {
            phs_lazyframe_unpivot(
                lf0,
                false,
                ptr::null(),
                0,
                index.as_ptr(),
                index.len(),
                ptr::null(),
                ptr::null(),
                &mut out,
                &mut err,
            )
        };
        assert_eq!(status, PHS_OK);
        let default_lf = out;
        let mut default_df = ptr::null_mut();
        assert_eq!(unsafe { phs_lazyframe_collect(default_lf, &mut default_df, &mut err) }, PHS_OK);
        assert_eq!(unsafe { crate::handles::dataframe_ref(default_df) }.unwrap().value.shape(), (12, 3));

        let status = unsafe {
            phs_lazyframe_unpivot(
                lf0,
                true,
                ptr::null(),
                0,
                index.as_ptr(),
                index.len(),
                ptr::null(),
                ptr::null(),
                &mut out,
                &mut err,
            )
        };
        assert_eq!(status, PHS_OK);
        let empty_lf = out;
        let mut empty_df = ptr::null_mut();
        assert_eq!(unsafe { phs_lazyframe_collect(empty_lf, &mut empty_df, &mut err) }, PHS_OK);
        assert_eq!(unsafe { crate::handles::dataframe_ref(empty_df) }.unwrap().value.shape(), (0, 3));

        let status = unsafe {
            phs_lazyframe_unpivot(
                lf0,
                true,
                on.as_ptr(),
                on.len(),
                missing_index.as_ptr(),
                missing_index.len(),
                ptr::null(),
                ptr::null(),
                &mut out,
                &mut err,
            )
        };
        assert_eq!(status, PHS_OK);
        let missing_lf = out;
        let mut missing_df = ptr::null_mut();
        let status = unsafe { phs_lazyframe_collect(missing_lf, &mut missing_df, &mut err) };
        assert_ne!(status, PHS_OK);
        unsafe { take_error_message(err) };
        err = ptr::null_mut();

        let status = unsafe {
            phs_lazyframe_unpivot(
                lf0,
                true,
                ptr::null(),
                1,
                index.as_ptr(),
                index.len(),
                ptr::null(),
                ptr::null(),
                &mut out,
                &mut err,
            )
        };
        assert_eq!(status, crate::error::PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "names pointer was null");
        err = ptr::null_mut();

        let status = unsafe {
            phs_lazyframe_unpivot(
                ptr::null(),
                true,
                on.as_ptr(),
                on.len(),
                index.as_ptr(),
                index.len(),
                ptr::null(),
                ptr::null(),
                &mut out,
                &mut err,
            )
        };
        assert_eq!(status, crate::error::PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "lazyframe pointer was null");
        err = ptr::null_mut();

        let status = unsafe {
            phs_lazyframe_unpivot(
                lf0,
                true,
                on.as_ptr(),
                on.len(),
                index.as_ptr(),
                index.len(),
                ptr::null(),
                ptr::null(),
                ptr::null_mut(),
                &mut err,
            )
        };
        assert_eq!(status, crate::error::PHS_INVALID_ARGUMENT);
        let message = unsafe { take_error_message(err) };
        assert_eq!(message, "out pointer was null");

        unsafe {
            crate::handles::phs_lazyframe_free(lf0);
            crate::handles::phs_lazyframe_free(explicit_lf);
            crate::handles::phs_lazyframe_free(default_lf);
            crate::handles::phs_lazyframe_free(empty_lf);
            crate::handles::phs_lazyframe_free(missing_lf);
            crate::handles::phs_dataframe_free(explicit_df);
            crate::handles::phs_dataframe_free(default_df);
            crate::handles::phs_dataframe_free(empty_df);
        }
    }

    #[test]
    fn lazy_join_rejects_mismatched_key_lengths() {
        let employees_path = employees_fixture_path();
        let departments_path = departments_fixture_path();
        let mut left = ptr::null_mut();
        let mut right = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(unsafe { phs_scan_csv(employees_path.as_ptr(), &mut left, &mut err) }, PHS_OK);
        assert_eq!(unsafe { phs_scan_csv(departments_path.as_ptr(), &mut right, &mut err) }, PHS_OK);

        let department = std::ffi::CString::new("department").unwrap();
        let name = std::ffi::CString::new("name").unwrap();
        let mut left_department = ptr::null_mut();
        let mut left_name = ptr::null_mut();
        let mut right_department = ptr::null_mut();
        assert_eq!(unsafe { crate::expr::phs_expr_col(department.as_ptr(), &mut left_department, &mut err) }, PHS_OK);
        assert_eq!(unsafe { crate::expr::phs_expr_col(name.as_ptr(), &mut left_name, &mut err) }, PHS_OK);
        assert_eq!(unsafe { crate::expr::phs_expr_col(department.as_ptr(), &mut right_department, &mut err) }, PHS_OK);

        let left_keys = [left_department as *const phs_expr, left_name as *const phs_expr];
        let right_keys = [right_department as *const phs_expr];
        let mut joined: *mut phs_lazyframe = ptr::null_mut();
        let status = unsafe {
            phs_lazyframe_join(
                left,
                right,
                left_keys.as_ptr(),
                left_keys.len(),
                right_keys.as_ptr(),
                right_keys.len(),
                0,
                ptr::null(),
                &mut joined,
                &mut err,
            )
        };
        assert_eq!(status, crate::error::PHS_INVALID_ARGUMENT);
        assert!(joined.is_null());
        assert!(!err.is_null());

        unsafe {
            crate::error::phs_error_free(err);
            crate::handles::phs_expr_free(left_department);
            crate::handles::phs_expr_free(left_name);
            crate::handles::phs_expr_free(right_department);
            crate::handles::phs_lazyframe_free(left);
            crate::handles::phs_lazyframe_free(right);
        }
    }

    #[test]
    fn lazy_join_rejects_unknown_join_type() {
        let employees_path = employees_fixture_path();
        let departments_path = departments_fixture_path();
        let mut left = ptr::null_mut();
        let mut right = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(unsafe { phs_scan_csv(employees_path.as_ptr(), &mut left, &mut err) }, PHS_OK);
        assert_eq!(unsafe { phs_scan_csv(departments_path.as_ptr(), &mut right, &mut err) }, PHS_OK);

        let department = std::ffi::CString::new("department").unwrap();
        let mut left_key = ptr::null_mut();
        let mut right_key = ptr::null_mut();
        assert_eq!(unsafe { crate::expr::phs_expr_col(department.as_ptr(), &mut left_key, &mut err) }, PHS_OK);
        assert_eq!(unsafe { crate::expr::phs_expr_col(department.as_ptr(), &mut right_key, &mut err) }, PHS_OK);

        let left_keys = [left_key as *const phs_expr];
        let right_keys = [right_key as *const phs_expr];
        let mut joined: *mut phs_lazyframe = ptr::null_mut();
        let status = unsafe {
            phs_lazyframe_join(
                left,
                right,
                left_keys.as_ptr(),
                left_keys.len(),
                right_keys.as_ptr(),
                right_keys.len(),
                99,
                ptr::null(),
                &mut joined,
                &mut err,
            )
        };
        assert_eq!(status, crate::error::PHS_INVALID_ARGUMENT);
        assert!(joined.is_null());
        assert!(!err.is_null());

        unsafe {
            crate::error::phs_error_free(err);
            crate::handles::phs_expr_free(left_key);
            crate::handles::phs_expr_free(right_key);
            crate::handles::phs_lazyframe_free(left);
            crate::handles::phs_lazyframe_free(right);
        }
    }
}
