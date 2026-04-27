# Polars Haskell Binding Phase 4 Typed Column Extraction Design

## Background

The binding can now read CSV and Parquet files, run lazy filters and projections, group and aggregate data, join lazy frames, render a `DataFrame` as text, and round-trip IPC bytes. Users can inspect output shape and schema, then print tables.

Phase 4 adds structured value access from a collected `DataFrame` into ordinary Haskell values. This is the first direct data extraction API and complements the whole-frame IPC path.

```mermaid
flowchart LR
    CSV[CSV / Parquet] --> LF[LazyFrame]
    LF --> Q[filter / select / groupBy / join]
    Q --> DF[DataFrame]
    DF --> COL[Polars.Column]
    COL --> HASKELL[[Maybe Bool] / [Maybe Int64] / [Maybe Double] / [Maybe Text]]
```

## Problem

`toText` is useful for display, and IPC is useful for interchange. Haskell programs also need typed vectors for tests, application logic, and downstream libraries. The first useful surface is column-by-name extraction with null preservation.

The API should preserve the current design rules:

- Rust owns Polars internals.
- Haskell exposes safe functions returning `Either PolarsError a`.
- Opaque handles stay internal.
- FFI bytes are copied into Haskell before Rust buffers are freed.
- Public API remains small and easy to extend.

## Questions and Answers

### Which Phase 4 scope should land first?

Answer: Typed columns MVP.

The first scope includes bool, int64, double, and text columns. Binary, date/time, list, struct, categorical, and row-oriented APIs are later phases.

### Where should the public functions live?

Answer: New module `Polars.Column`.

`Polars.Column` keeps extraction logic focused and gives users a clear import target. `Polars` re-exports it for the convenience path already used by examples and tests.

### How should nulls be represented?

Answer: `Maybe`.

Each returned list has one element per row. `Nothing` represents a Polars null value, and `Just value` represents a concrete cell.

### Should Phase 4 expose a `Series` handle?

Answer: Series handles are deferred.

Typed extraction delivers immediate user value and avoids a larger handle lifecycle surface. A future `Series` API can use the same Rust column access helpers and decoding rules.

## Design

### Public API

Create `src/Polars/Column.hs`:

```haskell
module Polars.Column
    ( columnBool
    , columnDouble
    , columnInt64
    , columnText
    ) where
```

Public signatures:

```haskell
columnBool :: DataFrame -> Text -> IO (Either PolarsError [Maybe Bool])
columnInt64 :: DataFrame -> Text -> IO (Either PolarsError [Maybe Int64])
columnDouble :: DataFrame -> Text -> IO (Either PolarsError [Maybe Double])
columnText :: DataFrame -> Text -> IO (Either PolarsError [Maybe Text])
```

`DataFrame` remains the input type. Users collect lazy results first, then extract columns:

```haskell
scanResult <- Pl.scanCsv "test/data/sales.csv"
case scanResult of
  Left err -> print err
  Right lf -> do
    grouped <- Pl.agg [Pl.alias "salary_sum" (Pl.sum_ (Pl.col "salary"))] (Pl.groupByStable [Pl.col "department"] lf)
    case grouped of
      Left err -> print err
      Right groupedLf -> do
        collected <- Pl.collect groupedLf
        case collected of
          Left err -> print err
          Right df -> print =<< Pl.columnInt64 df "salary_sum"
```

### Rust ABI

Add functions to `rust/polars-hs-ffi/src/dataframe.rs` and the generated header:

```c
int phs_dataframe_column_bool(
  const struct phs_dataframe *dataframe,
  const char *name,
  struct phs_bytes **out,
  struct phs_error **err
);

int phs_dataframe_column_i64(
  const struct phs_dataframe *dataframe,
  const char *name,
  struct phs_bytes **out,
  struct phs_error **err
);

int phs_dataframe_column_f64(
  const struct phs_dataframe *dataframe,
  const char *name,
  struct phs_bytes **out,
  struct phs_error **err
);

int phs_dataframe_column_text(
  const struct phs_dataframe *dataframe,
  const char *name,
  struct phs_bytes **out,
  struct phs_error **err
);
```

Rust implementation pattern:

```rust
let handle = unsafe { dataframe_ref(dataframe) }?;
let name = unsafe { c_str_to_str(name, "name") }?;
let column = handle.value.column(name)?;
let values = column.i64()?;
```

Typed Polars accessors provide `Option<T>` iterators:

```rust
for value in values {
    match value {
        None => push_null(&mut bytes),
        Some(value) => push_i64(&mut bytes, value),
    }
}
```

### Byte encoding

The Rust adapter serializes each column into `phs_bytes`. Haskell copies and frees that buffer through existing `copyAndFreeBytes`.

Common row tag:

| Tag | Meaning |
| --- | --- |
| `0` | null cell |
| `1` | value cell |

Numeric and bool payloads use little-endian encoding:

| Function | Value payload |
| --- | --- |
| `phs_dataframe_column_bool` | one byte: `0` or `1` |
| `phs_dataframe_column_i64` | eight bytes: signed little-endian `i64` |
| `phs_dataframe_column_f64` | eight bytes: IEEE-754 little-endian `f64` |

Text payload:

| Field | Encoding |
| --- | --- |
| tag | one byte |
| length | eight bytes unsigned little-endian byte length, present for tag `1` |
| bytes | UTF-8 bytes, present for tag `1` |

Examples:

```text
[Just 10, Nothing, Just 20]
=> 01 0a 00 00 00 00 00 00 00 00 01 14 00 00 00 00 00 00 00

[Just "A", Nothing, Just "Bob"]
=> 01 01 00 00 00 00 00 00 00 41 00 01 03 00 00 00 00 00 00 00 42 6f 62
```

### Haskell internal decoding

Create `src/Polars/Internal/ColumnDecode.hs`:

```haskell
decodeBoolColumn :: ByteString -> Either PolarsError [Maybe Bool]
decodeInt64Column :: ByteString -> Either PolarsError [Maybe Int64]
decodeDoubleColumn :: ByteString -> Either PolarsError [Maybe Double]
decodeTextColumn :: ByteString -> Either PolarsError [Maybe Text]
```

Decoder errors return `InvalidArgument` with specific messages:

- `column bool payload ended early`
- `column int64 payload ended early`
- `column double payload ended early`
- `column text payload ended early`
- `column payload contained an unknown tag`
- `column text payload contained invalid UTF-8`

Decode functions validate full consumption of the byte buffer.

### Haskell FFI integration

Extend `src/Polars/Internal/Raw.hs` with imports:

```haskell
foreign import ccall unsafe "phs_dataframe_column_bool"
    phs_dataframe_column_bool :: Ptr RawDataFrame -> CString -> Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt
```

Repeat the same shape for `i64`, `f64`, and `text`.

`Polars.Column` follows the existing `bytesOut` pattern:

```haskell
columnInt64 df name = columnBytesOut df name phs_dataframe_column_i64 decodeInt64Column
```

The implementation uses `withDataFrame`, `withTextCString`, `copyAndFreeBytes`, and `consumeError` just like existing safe wrappers.

### Module wiring

Update `package.yaml`:

```yaml
library:
  exposed-modules:
  - Polars.Column
  other-modules:
  - Polars.Internal.ColumnDecode
```

Update `src/Polars.hs` to re-export `Polars.Column`.

### Tests

Add CSV fixture `test/data/values.csv`:

```csv
name,age,score,active
Alice,34,9.5,true
Bob,,8.25,false
Carol,29,,
```

Test cases:

1. `columnText df "name"` returns `Right [Just "Alice", Just "Bob", Just "Carol"]`.
2. `columnInt64 df "age"` returns `Right [Just 34, Nothing, Just 29]`.
3. `columnDouble df "score"` returns `Right [Just 9.5, Just 8.25, Nothing]`.
4. `columnBool df "active"` returns `Right [Just True, Just False, Nothing]`.
5. Missing column returns `PolarsFailure`.
6. Type mismatch returns `PolarsFailure`.
7. Grouped aggregation result extraction reads `salary_sum` as `[Just 250, Just 200]` after sorting by department.
8. Join result extraction reads `name_dept` as text with null preservation.

Rust tests cover encoder behavior and Polars dtype paths for each typed function.

### Error handling

Rust errors:

- Missing column: Polars `col_not_found` maps through existing `PHS_POLARS_ERROR`.
- Type mismatch: Polars typed accessor error maps through existing `PHS_POLARS_ERROR`.
- Null pointer parameters: existing `required_mut` and `c_str_to_str` checks map to `PHS_INVALID_ARGUMENT`.

Haskell errors:

- Decode errors use `InvalidArgument`.
- Rust failures use existing `consumeError`.
- Successful empty columns return `Right []`.

## Implementation Plan

1. Add failing Haskell tests and `test/data/values.csv`.
2. Add Rust encoder helpers and Rust tests for bool, int64, f64, and text extraction.
3. Add Rust FFI functions and regenerate `include/polars_hs.h`.
4. Add Haskell raw FFI imports and `Polars.Internal.ColumnDecode`.
5. Add public `Polars.Column`, re-export from `Polars`, and update `package.yaml`.
6. Add `examples/columns.hs`, README documentation, and design implementation results.
7. Run full verification:

```bash
cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml
cargo clippy --manifest-path rust/polars-hs-ffi/Cargo.toml -- -D warnings
stack test --fast
hlint src app test
stack runghc examples/iris.hs
stack runghc examples/groupby.hs
stack runghc examples/join.hs
stack runghc examples/columns.hs
```

## Examples

### Basic extraction

```haskell
result <- Pl.readCsv "test/data/values.csv"
case result of
  Left err -> print err
  Right df -> do
    names <- Pl.columnText df "name"
    ages <- Pl.columnInt64 df "age"
    print names
    print ages
```

Expected values:

```haskell
Right [Just "Alice", Just "Bob", Just "Carol"]
Right [Just 34, Nothing, Just 29]
```

### Extraction after lazy work

```haskell
joined <- Pl.leftJoin [Pl.col "department"] [Pl.col "department"] employees departments
case joined of
  Left err -> print err
  Right lf -> do
    collected <- Pl.collect lf
    case collected of
      Left err -> print err
      Right df -> print =<< Pl.columnText df "name_right"
```

## Trade-offs

### Typed functions first

Four typed functions give clear error behavior, simple tests, and predictable Haskell types. This covers the columns produced by existing fixtures and examples.

### Bytes encoding over element callbacks

A single Rust allocation per column keeps FFI crossings small. Haskell decoders own all validation of the byte payload after copying.

### Series handle later

A future `Series` handle can expose metadata, slicing, and repeated typed access. The typed extraction MVP establishes value encoding and tests first.

### Arrow C Data Interface later

Arrow C Data Interface remains the zero-copy path for larger interoperability work. Typed lists give direct Haskell ergonomics for application logic and tests.

## Acceptance Criteria

- Public API exposes `Polars.Column` and re-exports it from `Polars`.
- `columnBool`, `columnInt64`, `columnDouble`, and `columnText` preserve nulls as `Nothing`.
- Missing columns and dtype mismatches return `Left PolarsError`.
- Existing DataFrame, LazyFrame, GroupBy, Join, and IPC tests continue to pass.
- Rust tests cover all four new FFI functions.
- README and example show typed extraction on a collected DataFrame.

## Implementation Results

Implementation begins after user approval. This section will record verification output, deviations, and final commit information after implementation work starts.
