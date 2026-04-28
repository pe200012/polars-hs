use std::ffi::{c_char, c_void};
use std::os::raw::c_int;
use std::ptr;

use polars::prelude::*;
use polars_arrow::array::StructArray;
use polars_arrow::datatypes::{ArrowDataType, Field as ArrowField};
use polars_arrow::ffi::{
    ArrowArray, ArrowSchema, export_array_to_c, export_field_to_c, import_array_from_c, import_field_from_c,
};

use crate::error::{PhsError, PhsResult, ffi_boundary, phs_error, required_mut};
use crate::handles::{dataframe_into_raw, dataframe_ref, phs_dataframe};

#[repr(C)]
pub struct phs_arrow_record_batch {
    _private: [u8; 0],
}

struct ArrowRecordBatchHandle {
    schema: Box<ArrowSchema>,
    array: Box<ArrowArray>,
}

#[repr(C)]
struct CArrowSchema {
    format: *const c_char,
    name: *const c_char,
    metadata: *const c_char,
    flags: i64,
    n_children: i64,
    children: *mut *mut ArrowSchema,
    dictionary: *mut ArrowSchema,
    release: Option<unsafe extern "C" fn(*mut ArrowSchema)>,
    private_data: *mut c_void,
}

#[repr(C)]
struct CArrowArray {
    length: i64,
    null_count: i64,
    offset: i64,
    n_buffers: i64,
    n_children: i64,
    buffers: *mut *const c_void,
    children: *mut *mut ArrowArray,
    dictionary: *mut ArrowArray,
    release: Option<unsafe extern "C" fn(*mut ArrowArray)>,
    private_data: *mut c_void,
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_to_arrow_record_batch(
    dataframe: *const phs_dataframe,
    out: *mut *mut phs_arrow_record_batch,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let handle = unsafe { dataframe_ref(dataframe) }?;
        let df = handle.value.clone();
        let compat_level = CompatLevel::newest();
        let arrays = df.rechunk_to_arrow(compat_level);
        let fields = df
            .columns()
            .iter()
            .map(|column| column.field().to_arrow(compat_level))
            .collect::<Vec<_>>();
        let dtype = ArrowDataType::Struct(fields);
        let struct_array = StructArray::new(dtype.clone(), df.height(), arrays, None);
        let field = ArrowField::new("".into(), dtype, false);
        let schema = Box::new(export_field_to_c(&field));
        let array = Box::new(export_array_to_c(Box::new(struct_array)));
        *out = Box::into_raw(Box::new(ArrowRecordBatchHandle { schema, array })) as *mut phs_arrow_record_batch;
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_arrow_record_batch_schema(batch: *mut phs_arrow_record_batch) -> *mut c_void {
    let Some(handle) = (unsafe { arrow_record_batch_handle(batch) }) else {
        return ptr::null_mut();
    };
    handle.schema.as_mut() as *mut ArrowSchema as *mut c_void
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_arrow_record_batch_array(batch: *mut phs_arrow_record_batch) -> *mut c_void {
    let Some(handle) = (unsafe { arrow_record_batch_handle(batch) }) else {
        return ptr::null_mut();
    };
    handle.array.as_mut() as *mut ArrowArray as *mut c_void
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_arrow_record_batch_free(batch: *mut phs_arrow_record_batch) {
    if batch.is_null() {
        return;
    }
    let mut handle = unsafe { Box::from_raw(batch as *mut ArrowRecordBatchHandle) };
    unsafe {
        release_schema(handle.schema.as_mut());
        release_array(handle.array.as_mut());
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phs_dataframe_from_arrow_record_batch(
    schema: *mut c_void,
    array: *mut c_void,
    out: *mut *mut phs_dataframe,
    err: *mut *mut phs_error,
) -> c_int {
    ffi_boundary(err, || {
        let out = unsafe { required_mut(out, "out") }?;
        *out = ptr::null_mut();
        let schema = required_arrow_ptr::<ArrowSchema>(schema, "schema")?;
        let array = required_arrow_ptr::<ArrowArray>(array, "array")?;
        unsafe { ensure_schema_live(schema)? };
        unsafe { ensure_array_live(array)? };

        let field_result = unsafe { import_field_from_c(&*schema) };
        unsafe { release_schema(schema) };
        let field = match field_result {
            Ok(field) => field,
            Err(err) => {
                unsafe { release_array(array) };
                return Err(err.into());
            },
        };
        if !matches!(&field.dtype, ArrowDataType::Struct(_)) {
            unsafe { release_array(array) };
            return Err(PhsError::invalid_argument("Arrow RecordBatch schema must be a struct"));
        }

        let array_value = unsafe { take_array(array)? };
        let imported = unsafe { import_array_from_c(array_value, field.dtype.clone()) }?;
        let struct_array = imported
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| PhsError::invalid_argument("Arrow RecordBatch array must be a struct"))?
            .clone();
        let (fields, _, arrays, validity) = struct_array.into_data();
        if validity.as_ref().is_some_and(|bitmap| bitmap.unset_bits() > 0) {
            return Err(PhsError::invalid_argument("Arrow RecordBatch struct nulls are unsupported"));
        }

        let mut columns = Vec::with_capacity(fields.len());
        for (field, array) in fields.into_iter().zip(arrays) {
            let series = unsafe {
                Series::_try_from_arrow_unchecked_with_md(
                    field.name.clone(),
                    vec![array],
                    field.dtype(),
                    field.metadata.as_deref(),
                )?
            };
            columns.push(series.into());
        }
        *out = dataframe_into_raw(DataFrame::new_infer_height(columns)?);
        Ok(())
    })
}

fn required_arrow_ptr<T>(ptr: *mut c_void, name: &str) -> PhsResult<*mut T> {
    if ptr.is_null() {
        Err(PhsError::invalid_argument(format!("{name} pointer was null")))
    } else {
        Ok(ptr.cast::<T>())
    }
}

unsafe fn arrow_record_batch_handle<'a>(batch: *mut phs_arrow_record_batch) -> Option<&'a mut ArrowRecordBatchHandle> {
    if batch.is_null() {
        None
    } else {
        Some(unsafe { &mut *(batch as *mut ArrowRecordBatchHandle) })
    }
}

unsafe fn ensure_schema_live(schema: *mut ArrowSchema) -> PhsResult<()> {
    if unsafe { (*schema.cast::<CArrowSchema>()).release.is_none() } {
        Err(PhsError::invalid_argument("schema was already released"))
    } else {
        Ok(())
    }
}

unsafe fn ensure_array_live(array: *mut ArrowArray) -> PhsResult<()> {
    if unsafe { (*array.cast::<CArrowArray>()).release.is_none() } {
        Err(PhsError::invalid_argument("array was already released"))
    } else {
        Ok(())
    }
}

unsafe fn release_schema(schema: *mut ArrowSchema) {
    if let Some(release) = unsafe { (*schema.cast::<CArrowSchema>()).release } {
        unsafe { release(schema) };
    }
}

unsafe fn release_array(array: *mut ArrowArray) {
    if let Some(release) = unsafe { (*array.cast::<CArrowArray>()).release } {
        unsafe { release(array) };
    }
}

unsafe fn take_array(array: *mut ArrowArray) -> PhsResult<ArrowArray> {
    unsafe { ensure_array_live(array)? };
    let value = unsafe { ptr::read(array) };
    unsafe { (*array.cast::<CArrowArray>()).release = None };
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataframe::{phs_dataframe_column_i64, phs_dataframe_shape};
    use crate::error::{PHS_INVALID_ARGUMENT, PHS_OK, PHS_POLARS_ERROR, phs_error_free};
    use crate::handles::phs_dataframe_free;
    use polars_arrow::array::{Array, Int64Array, Utf8Array};
    use polars_arrow::datatypes::{ArrowDataType, Field};
    use polars_arrow::ffi::{export_array_to_c, export_field_to_c};

    fn make_record_batch(names: [Field; 2]) -> (ArrowSchema, ArrowArray) {
        let name_array = Box::new(Utf8Array::<i32>::from([Some("Alice"), Some("Bob"), None])) as Box<dyn Array>;
        let age_array = Box::new(Int64Array::from(&[Some(34), None, Some(29)])) as Box<dyn Array>;
        let fields = Vec::from(names);
        let dtype = ArrowDataType::Struct(fields.clone());
        let struct_array = StructArray::new(dtype.clone(), 3, vec![name_array, age_array], None);
        let field = Field::new("batch".into(), dtype, false);
        (export_field_to_c(&field), export_array_to_c(Box::new(struct_array)))
    }

    fn default_fields() -> [Field; 2] {
        [
            Field::new("name".into(), ArrowDataType::Utf8, true),
            Field::new("age".into(), ArrowDataType::Int64, true),
        ]
    }

    fn make_export_dataframe() -> *mut phs_dataframe {
        let name = Series::new("name".into(), vec![Some("Alice"), Some("Bob"), None]);
        let age = Series::new("age".into(), vec![Some(34_i64), None, Some(29_i64)]);
        dataframe_into_raw(DataFrame::new_infer_height(vec![name.into(), age.into()]).unwrap())
    }

    #[test]
    fn arrow_record_batch_export_returns_live_pointers() {
        let df = make_export_dataframe();
        let mut batch = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(unsafe { phs_dataframe_to_arrow_record_batch(df, &mut batch, &mut err) }, PHS_OK);
        assert!(!batch.is_null());
        assert!(!unsafe { phs_arrow_record_batch_schema(batch) }.is_null());
        assert!(!unsafe { phs_arrow_record_batch_array(batch) }.is_null());
        unsafe {
            phs_arrow_record_batch_free(batch);
            phs_dataframe_free(df);
        }
    }

    #[test]
    fn arrow_record_batch_export_import_roundtrip_preserves_shape() {
        let df = make_export_dataframe();
        let mut batch = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(unsafe { phs_dataframe_to_arrow_record_batch(df, &mut batch, &mut err) }, PHS_OK);

        let schema = unsafe { phs_arrow_record_batch_schema(batch) };
        let array = unsafe { phs_arrow_record_batch_array(batch) };
        let mut imported = ptr::null_mut();
        assert_eq!(unsafe { phs_dataframe_from_arrow_record_batch(schema, array, &mut imported, &mut err) }, PHS_OK);

        let mut height = 0;
        let mut width = 0;
        assert_eq!(unsafe { phs_dataframe_shape(imported, &mut height, &mut width, &mut err) }, PHS_OK);
        assert_eq!((height, width), (3, 2));

        unsafe {
            phs_arrow_record_batch_free(batch);
            phs_dataframe_free(df);
            phs_dataframe_free(imported);
        }
    }

    #[test]
    fn arrow_record_batch_import_builds_dataframe() {
        let (mut schema, mut array) = make_record_batch(default_fields());
        let mut df = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(
            unsafe {
                phs_dataframe_from_arrow_record_batch(
                    (&mut schema as *mut ArrowSchema).cast(),
                    (&mut array as *mut ArrowArray).cast(),
                    &mut df,
                    &mut err,
                )
            },
            PHS_OK
        );
        let mut height = 0;
        let mut width = 0;
        assert_eq!(unsafe { phs_dataframe_shape(df, &mut height, &mut width, &mut err) }, PHS_OK);
        assert_eq!((height, width), (3, 2));
        let column_name = std::ffi::CString::new("age").unwrap();
        let mut bytes = ptr::null_mut();
        assert_eq!(unsafe { phs_dataframe_column_i64(df, column_name.as_ptr(), &mut bytes, &mut err) }, PHS_OK);
        unsafe {
            crate::bytes::phs_bytes_free(bytes);
            phs_dataframe_free(df);
        }
    }

    #[test]
    fn arrow_record_batch_import_rejects_released_array() {
        let (mut schema, mut array) = make_record_batch(default_fields());
        unsafe { (&mut array as *mut ArrowArray).cast::<CArrowArray>().as_mut().unwrap().release = None };
        let mut df = ptr::null_mut();
        let mut err = ptr::null_mut();
        let status = unsafe {
            phs_dataframe_from_arrow_record_batch(
                (&mut schema as *mut ArrowSchema).cast(),
                (&mut array as *mut ArrowArray).cast(),
                &mut df,
                &mut err,
            )
        };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        unsafe { phs_error_free(err) };
    }

    #[test]
    fn arrow_record_batch_import_rejects_plain_array() {
        let field = Field::new("age".into(), ArrowDataType::Int64, true);
        let mut schema = export_field_to_c(&field);
        let mut array = export_array_to_c(Box::new(Int64Array::from(&[Some(34), None, Some(29)])));
        let mut df = ptr::null_mut();
        let mut err = ptr::null_mut();
        let status = unsafe {
            phs_dataframe_from_arrow_record_batch(
                (&mut schema as *mut ArrowSchema).cast(),
                (&mut array as *mut ArrowArray).cast(),
                &mut df,
                &mut err,
            )
        };
        assert_eq!(status, PHS_INVALID_ARGUMENT);
        unsafe { phs_error_free(err) };
    }

    #[test]
    fn arrow_record_batch_import_reports_duplicate_child_names() {
        let duplicate_fields = [
            Field::new("value".into(), ArrowDataType::Utf8, true),
            Field::new("value".into(), ArrowDataType::Int64, true),
        ];
        let (mut schema, mut array) = make_record_batch(duplicate_fields);
        let mut df = ptr::null_mut();
        let mut err = ptr::null_mut();
        let status = unsafe {
            phs_dataframe_from_arrow_record_batch(
                (&mut schema as *mut ArrowSchema).cast(),
                (&mut array as *mut ArrowArray).cast(),
                &mut df,
                &mut err,
            )
        };
        assert_eq!(status, PHS_POLARS_ERROR);
        unsafe { phs_error_free(err) };
    }
}
