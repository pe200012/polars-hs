# DataFrame To Dummies Design

## Background

Rust Polars 0.53 exposes eager dummy-variable creation through `DataFrameOps`:

```rust
fn to_dummies(
    &self,
    separator: Option<&str>,
    drop_first: bool,
    drop_nulls: bool,
) -> PolarsResult<DataFrame>

fn columns_to_dummies(
    &self,
    columns: Vec<&str>,
    separator: Option<&str>,
    drop_first: bool,
    drop_nulls: bool,
) -> PolarsResult<DataFrame>

fn _to_dummies(
    &self,
    columns: Option<Vec<&str>>,
    separator: Option<&str>,
    drop_first: bool,
    drop_nulls: bool,
) -> PolarsResult<DataFrame>
```

Upstream references:

- https://docs.rs/polars/0.53.0/polars/frame/struct.DataFrame.html
- Local `polars-ops-0.53.0/src/frame/mod.rs`.
- Local `polars-ops-0.53.0/src/series/ops/to_dummies.rs`.

The upstream `to_dummies` Cargo feature maps to `polars-ops/to_dummies`.
With `dtype-u8` enabled, dummy columns use `u8`. The current FFI already enables
`dtype-u8`.

## Problem

`polars-hs` has several eager reshape and row/column transform helpers, but it
lacks `DataFrame.to_dummies`. One-hot encoding is a common eager DataFrame
operation and fills a clear Polars 0.53 parity gap.

## Questions And Answers

Q: How should column selection be represented?

A: Use `Maybe [Text]`. `Nothing` maps to all columns. `Just names` maps to
`columns_to_dummies`. `Just []` preserves the upstream empty set behavior, so
no column is encoded and the source frame is returned through Polars.

Q: Should the separator default be exposed?

A: Use `Maybe Text`. `Nothing` maps to the Rust default underscore separator.
`Just value` passes a custom separator.

Q: How should missing selected columns behave?

A: Rust Polars 0.53 builds a set of selected names and only transforms columns
whose names occur in the frame. Unknown selected names are ignored and the
output preserves existing columns.

## Design

Public Haskell API:

```haskell
data DataFrameToDummiesOptions = DataFrameToDummiesOptions
    { dataFrameToDummiesColumns :: !(Maybe [Text])
    , dataFrameToDummiesSeparator :: !(Maybe Text)
    , dataFrameToDummiesDropFirst :: !Bool
    , dataFrameToDummiesDropNulls :: !Bool
    }

defaultDataFrameToDummiesOptions :: DataFrameToDummiesOptions

dataFrameToDummies :: DataFrameToDummiesOptions -> DataFrame -> IO (Either PolarsError DataFrame)
```

C ABI:

```c
int phs_dataframe_to_dummies(const struct phs_dataframe *dataframe,
                             bool has_columns,
                             const char *const *columns,
                             uintptr_t columns_len,
                             const char *separator,
                             bool drop_first,
                             bool drop_nulls,
                             struct phs_dataframe **out,
                             struct phs_error **err);
```

Rust implementation:

```rust
let columns = if has_columns {
    Some(name_vec(columns, columns_len, "columns")?)
} else {
    None
};
let separator = if separator.is_null() {
    None
} else {
    Some(c_str_to_str(separator, "separator")?)
};
let columns = columns
    .as_ref()
    .map(|names| names.iter().map(String::as_str).collect());
*out = dataframe_into_raw(handle.value._to_dummies(
    columns,
    separator,
    drop_first,
    drop_nulls,
)?);
```

## Implementation Plan

1. Enable `to_dummies` on the Rust `polars` and `polars-ops` dependencies.
2. Add RED Rust ABI tests for all-column encoding, selected-column encoding,
   custom separator, `drop_first`, `drop_nulls`, empty selection, null column
   pointer with positive length, and null output pointer.
3. Add RED Hspec tests over committed fixtures for default all-column mode,
   selected-column passthrough mode, custom separator, `drop_first`,
   `drop_nulls`, empty selection, and missing-column passthrough.
4. Add Rust ABI declaration/implementation and import the DataFrameOps trait.
5. Add Raw import, Haskell option record/default, wrapper, and export.
6. Run focused GREEN and full verification.

## Examples

Good:

```haskell
dataFrameToDummies defaultDataFrameToDummiesOptions df
```

Good:

```haskell
dataFrameToDummies
    defaultDataFrameToDummiesOptions
        { dataFrameToDummiesColumns = Just ["department"]
        , dataFrameToDummiesSeparator = Just ":"
        , dataFrameToDummiesDropFirst = True
        }
    df
```

## Trade-offs

- The ABI carries `has_columns` so `Nothing` and `Just []` remain distinct.
- `Maybe Text` gives the Rust default separator direct representation.
- The first implementation keeps selectors out of scope because the public API
  has explicit column-name lists throughout eager DataFrame helpers.

## Implementation Results

Implemented as designed, with the missing-column parity correction above.

Files changed:

- `rust/polars-hs-ffi/Cargo.toml`: enabled `to_dummies` on `polars` and `polars-ops`.
- `include/polars_hs.h`: added `phs_dataframe_to_dummies`.
- `rust/polars-hs-ffi/src/dataframe.rs`: imported `DataFrameOps`, added ABI wrapper, and added Rust unit coverage.
- `src/Polars/Internal/Raw.hs`: added safe FFI import.
- `src/Polars/DataFrame.hs`: exported and implemented `DataFrameToDummiesOptions`, `defaultDataFrameToDummiesOptions`, and `dataFrameToDummies`.
- `test/Spec.hs`: added Hspec coverage for all columns, selected columns, separator, `drop_first`, `drop_nulls`, empty selection, and missing-column passthrough.
- `docs/superpowers/specs/2026-05-18-polars-hs-dataframe-to-dummies-design.md`: design log plus implementation results.

Behavior covered:

- `columns = Nothing` encodes all DataFrame columns and produces `Word8` dummy columns.
- Selected-column encoding keeps unselected columns and inserts dummy columns for the selected column.
- `separator = Just ":"` customizes dummy column names.
- `drop_first` removes the first non-null category dummy column.
- `drop_nulls` skips null dummy columns while preserving row count.
- `columns = Just []` and unknown selected column names preserve the original frame shape and names.
- Rust ABI rejects null column arrays with positive length and null output pointers.

Verification so far:

- RED Rust failed on missing `phs_dataframe_to_dummies`.
- RED Hspec failed on missing `Pl.dataFrameToDummies` and `defaultDataFrameToDummiesOptions`.
- First focused Rust run surfaced the upstream missing-column passthrough behavior; tests were corrected for parity.
- Focused Rust FFI: 1/1 passing.
- Focused Hspec: 1/1 passing.
