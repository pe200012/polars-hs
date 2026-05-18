//! Shared schema encoding for the Rust-owned C ABI.
//!
//! The byte format is length-prefixed so field names can contain embedded NUL
//! bytes while Haskell decodes the same payload for eager and lazy schemas.

use polars::prelude::*;

const SCHEMA_MAGIC: &[u8; 8] = b"PHS1SCH\0";

fn push_u64_le(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn push_u16_le(bytes: &mut Vec<u8>, value: u16) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn schema_dtype_tag(dtype: &DataType) -> u16 {
    match dtype {
        DataType::Boolean => 0,
        DataType::Int8 => 1,
        DataType::Int16 => 2,
        DataType::Int32 => 3,
        DataType::Int64 => 4,
        DataType::UInt8 => 5,
        DataType::UInt16 => 6,
        DataType::UInt32 => 7,
        DataType::UInt64 => 8,
        DataType::Float32 => 9,
        DataType::Float64 => 10,
        DataType::String => 11,
        DataType::Date => 12,
        DataType::Datetime(_, _) => 13,
        DataType::Duration(_) => 14,
        DataType::Time => 15,
        DataType::Binary | DataType::BinaryOffset => 16,
        DataType::Null => 17,
        dtype if dtype.is_categorical() || dtype.is_enum() => 18,
        _ => 255,
    }
}

pub(crate) fn encode_schema(schema: &Schema) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(SCHEMA_MAGIC);
    push_u64_le(&mut bytes, schema.len() as u64);
    for field in schema.iter_fields() {
        let name = field.name().as_str().as_bytes();
        let dtype = field.dtype();
        let detail = format!("{dtype:?}");
        push_u64_le(&mut bytes, name.len() as u64);
        bytes.extend_from_slice(name);
        push_u16_le(&mut bytes, schema_dtype_tag(dtype));
        push_u64_le(&mut bytes, detail.len() as u64);
        bytes.extend_from_slice(detail.as_bytes());
    }
    bytes
}
