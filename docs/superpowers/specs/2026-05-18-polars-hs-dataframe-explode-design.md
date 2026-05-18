# DataFrame Explode Design

## Background

Rust Polars 0.53 exposes eager DataFrame list/array explosion:

- `DataFrame::explode<I, S>(&self, columns: I, options: ExplodeOptions) -> PolarsResult<DataFrame>`
- `ExplodeOptions { empty_as_null, keep_nulls }`
- docs.rs lists `DataFrame::explode`: https://docs.rs/polars/latest/polars/frame/struct.DataFrame.html
- Python API documents the same option semantics: https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.explode.html

The repo already has expression list helpers that can create list columns through lazy `strSplit`, so eager explode can be tested without adding Haskell list construction first.

## Problem

`polars-hs` can create list columns through lazy expressions, but eager `DataFrame` has no API to explode those list columns into long format. This blocks common unnesting workflows after collecting a lazy result.

## Questions And Answers

Q: Does this need a new Rust feature?

A: No. Local Polars 0.53 source exposes `DataFrame::explode` in `polars-core` without an additional Cargo feature.

Q: Should the first Haskell API accept selectors or expressions?

A: Use explicit column names first. This matches existing eager DataFrame wrappers and keeps selector work for a broader selector design.

Q: Should `empty_as_null` and `keep_nulls` be public?

A: Yes. They are direct upstream options and simple to expose as strict Boolean fields.

## Design

Public Haskell API:

```haskell
data DataFrameExplodeOptions = DataFrameExplodeOptions
    { dataFrameExplodeColumns :: ![Text]
    , dataFrameExplodeEmptyAsNull :: !Bool
    , dataFrameExplodeKeepNulls :: !Bool
    }

defaultDataFrameExplodeOptions :: DataFrameExplodeOptions
dataFrameExplode :: DataFrameExplodeOptions -> DataFrame -> IO (Either PolarsError DataFrame)
```

Defaults mirror Polars:

```haskell
defaultDataFrameExplodeOptions =
    DataFrameExplodeOptions
        { dataFrameExplodeColumns = []
        , dataFrameExplodeEmptyAsNull = True
        , dataFrameExplodeKeepNulls = True
        }
```

ABI:

```c
int phs_dataframe_explode(const struct phs_dataframe *dataframe,
                          const char *const *names,
                          uintptr_t names_len,
                          bool empty_as_null,
                          bool keep_nulls,
                          struct phs_dataframe **out,
                          struct phs_error **err);
```

Rust implementation:

```rust
let names = name_vec(names, names_len, "names")?;
let options = ExplodeOptions { empty_as_null, keep_nulls };
let output = handle.value.explode(names.iter().map(String::as_str), options)?;
```

## Implementation Plan

1. Add RED Rust tests for list-column explosion, empty column validation, missing column, scalar column, null names pointer, and null output pointer.
2. Add RED Hspec tests that create a list column with `strSplit`, collect it, explode it, and validate repeated rows plus exploded text values.
3. Add `phs_dataframe_explode` to Rust and the generated C header.
4. Add Raw import and safe Haskell wrapper.
5. Run focused GREEN checks and full verification.

## Examples

Good:

```haskell
exploded <- dataFrameExplode
    defaultDataFrameExplodeOptions {dataFrameExplodeColumns = ["parts"]}
    df
```

Bad:

```haskell
exploded <- dataFrameExplode defaultDataFrameExplodeOptions df
```

At least one explode column is required.

## Trade-offs

- Selector support is deferred to a dedicated selector/meta API.
- This batch tests string list explosion through existing lazy expression helpers. Native Haskell list construction belongs in the nested dtype track.
- Fixed-size Array explode requires the `dtype-array` feature and belongs in the array dtype batch.

## Implementation Results

Implemented on 2026-05-18:

- Added `phs_dataframe_explode` to the Rust ABI and C header.
- Added Raw import for `phs_dataframe_explode`.
- Added `DataFrameExplodeOptions`, `defaultDataFrameExplodeOptions`, and `dataFrameExplode`.
- Added Rust tests for list-column explosion, empty column validation, null names pointer, single null name pointer, missing column, scalar column, and null output pointer.
- Added Hspec coverage that creates a list column through lazy `strSplit`, collects it, explodes it eagerly, and validates repeated source rows plus exploded values.

Focused verification:

- `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml dataframe_explode_list_columns_work`: 1/1 passed.
- `stack test --fast --test-arguments='--match=explodes'`: 1/1 passed.

Full verification:

- `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml`: 138/138 passed.
- `stack test --fast`: 185/185 passed.
- `hlint src test app`: no hints.
- `jj diff --git --color=never | git apply --cached --check --whitespace=error -`: passed.

Deviations:

- Fixed-size Array explode is documented as later dtype-array scope.
