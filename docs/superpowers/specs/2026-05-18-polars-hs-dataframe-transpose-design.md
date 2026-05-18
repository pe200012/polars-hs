# DataFrame Transpose Design

## Background

Rust Polars 0.53 exposes eager DataFrame transpose:

```rust
pub fn transpose(
    &mut self,
    keep_names_as: Option<&str>,
    new_col_names: Option<Either<String, Vec<String>>>,
) -> PolarsResult<DataFrame>
```

The method transposes over the diagonal. `keep_names_as` adds the original column names as the first output column. `new_col_names` can use default output names, an explicit list, or values from an existing column.

Upstream references:

- docs.rs `polars::frame::DataFrame::transpose`
- Local vendored source `polars-core-0.53.0/src/frame/row/transpose.rs`
- Python docs `polars.DataFrame.transpose`

## Problem

`polars-hs` has eager row and column transforms, but no eager transpose API. Transpose is useful for reshaping small metric tables and for matching Polars eager DataFrame coverage.

## Questions And Answers

Q: Should the Haskell wrapper mutate the source DataFrame?

A: No. Rust `transpose` takes `&mut self` because it rechunks internally. The FFI should clone the Rust DataFrame handle value, transpose the clone, and leave the source handle reusable.

Q: Which column-name modes should the first binding expose?

A: Expose all three Rust modes: default generated names, explicit names, and names from an existing column. This keeps the Haskell API aligned with Rust/Python semantics without multiple functions.

Q: Should empty DataFrames be rejected at Haskell boundary?

A: Let Rust Polars return its `NoData` error. The Rust implementation already has precise validation for empty frames, name length mismatch, null name source values, and duplicate output names.

## Design

Public Haskell API:

```haskell
data DataFrameTransposeColumnNames
    = TransposeDefaultColumnNames
    | TransposeColumnNames [Text]
    | TransposeColumnNamesFrom Text

data DataFrameTransposeOptions = DataFrameTransposeOptions
    { dataFrameTransposeKeepNamesAs :: Maybe Text
    , dataFrameTransposeColumnNames :: DataFrameTransposeColumnNames
    }

defaultDataFrameTransposeOptions :: DataFrameTransposeOptions

dataFrameTranspose :: DataFrameTransposeOptions -> DataFrame -> IO (Either PolarsError DataFrame)
```

C ABI:

```c
int phs_dataframe_transpose(const struct phs_dataframe *dataframe,
                            const char *keep_names_as,
                            int new_col_names_mode,
                            const char *new_col_names_source,
                            const char *const *new_col_names,
                            uintptr_t new_col_names_len,
                            struct phs_dataframe **out,
                            struct phs_error **err);
```

Mode mapping:

- `0`: default generated output names.
- `1`: read output names from `new_col_names_source`.
- `2`: use explicit `new_col_names` list.

Rust implementation:

```rust
let mut value = handle.value.clone();
let new_names = match mode { ... };
*out = dataframe_into_raw(value.transpose(keep_names_as, new_names)?);
```

## Implementation Plan

1. Add RED Rust ABI tests for default names, keep-names, explicit names, source-column names, invalid mode, null output pointer, and source handle reuse.
2. Add RED Hspec tests using constructed numeric DataFrames and a row-name source column.
3. Add C header declaration and Rust ABI implementation.
4. Add Raw import, Haskell option types, default options, and wrapper/export.
5. Run focused GREEN and full verification.

## Examples

Good:

```haskell
dataFrameTranspose
    defaultDataFrameTransposeOptions
        { dataFrameTransposeKeepNamesAs = Just "metric"
        , dataFrameTransposeColumnNames = TransposeColumnNames ["r1", "r2", "r3"]
        }
    df
```

Good:

```haskell
dataFrameTranspose
    defaultDataFrameTransposeOptions
        { dataFrameTransposeColumnNames = TransposeColumnNamesFrom "row_name" }
    df
```

## Trade-offs

- A mode-tagged ABI mirrors Rust's `Either` while keeping C simple.
- The FFI clones the input DataFrame before transposing, adding memory cost for API safety.

## Implementation Results

Implemented as designed.

Files changed:

- `rust/polars-hs-ffi/Cargo.toml`: added direct `either` dependency and enabled Polars `rows` feature required by transpose.
- `include/polars_hs.h`: added `phs_dataframe_transpose`.
- `rust/polars-hs-ffi/src/dataframe.rs`: added mode-tagged Rust ABI function and Rust unit coverage.
- `src/Polars/Internal/Raw.hs`: added a safe FFI import for `phs_dataframe_transpose`.
- `src/Polars/DataFrame.hs`: exported transpose option types, default options, and `dataFrameTranspose`.
- `test/Spec.hs`: added Hspec coverage for default names, explicit names, source-column names, keep-names column, mismatch errors, and source handle reuse.

Behavior covered:

- Default names produce `column_0`, `column_1`, and so on.
- `dataFrameTransposeKeepNamesAs = Just "metric"` inserts original value-column names.
- `TransposeColumnNames ["r1", "r2", "r3"]` sets explicit output value-column names.
- `TransposeColumnNamesFrom "row_name"` reads names from a string column and drops that source column before transposing values.
- The source DataFrame remains usable after transpose.
- Explicit name length mismatch returns a Polars failure.
- Rust ABI rejects unknown name modes and null output pointers.

Readonly review:

- Agent guidance in `research/api/dataframe-transpose-readonly-guidance-2026-05-18` confirmed `rows` as the required Cargo feature and recommended clone-before-transpose semantics.

Verification:

- RED Rust failed on missing `phs_dataframe_transpose`.
- RED Hspec failed on missing `Pl.dataFrameTranspose`, transpose options, and transpose column-name constructors.
- Focused Rust FFI: 1/1 passing.
- Focused Hspec: 1/1 passing.
- Full Rust FFI: 141/141 passing.
- Full Stack/Hspec: 188/188 passing.
- HLint: no hints.
- Whitespace gate: clean.

Deviation from design:

- The ABI uses an explicit `has_keep_names_as` boolean plus `keep_names_as` pointer so a false flag can ignore a null pointer safely.
