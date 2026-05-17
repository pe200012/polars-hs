# Polars HS Series ArgUnique Design

## Background

Rust Polars 0.53 exposes `SeriesTrait::arg_unique`:

```rust
fn arg_unique(&self) -> PolarsResult<IdxCa>
```

Docs and source checked:

- https://docs.rs/polars/latest/polars/series/trait.SeriesTrait.html
- `polars-core-0.53.0/src/series/series_trait.rs`
- `polars-core-0.53.0/src/series/implementations/mod.rs`
- `polars-core-0.53.0/src/series/implementations/string.rs`
- `polars-core-0.53.0/src/series/implementations/boolean.rs`
- `polars-core-0.53.0/src/chunked_array/ops/unique/mod.rs`

## Problem

`polars-hs` exposes `seriesUnique`, `seriesUniqueStable`, `seriesIsUnique`,
and `seriesNUnique`, but the index-returning `arg_unique` operation is still
missing. This operation lets users find the first row index for each unique
Series value and then compose with `seriesTake` or `dataFrameTake`.

## Questions and Answers

Q: Should the public API return a `Series`?

A: Yes. Polars returns `IdxCa`, and converting it to a Series keeps the existing
Series handle model. Haskell users can extract values with `seriesWord32` in the
current default index build.

Q: Does `arg_unique` keep first occurrence order?

A: Yes for the checked source path. The helper iterates with `enumerate()` and
pushes the index only when a value first enters the hash set.

Q: How should null values behave?

A: Null participates as one unique value. The first null index appears in the
output at the position where null first occurs during iteration.

Q: Does this need a new option record?

A: No. `arg_unique` has no user-facing options in Rust Polars 0.53.

## Design

Public API:

```haskell
seriesArgUnique :: Series -> IO (Either PolarsError Series)
```

C ABI:

```c
int phs_series_arg_unique(const phs_series *series, phs_series **out, phs_error **err);
```

Rust mapping:

```rust
value.arg_unique()?.into_series()
```

## Implementation Plan

1. Add RED Hspec tests for numeric, text, bool, null, and empty Series.
2. Add Rust FFI tests for the same core behavior.
3. Add Haskell export and wrapper using `seriesUnaryOut`.
4. Add Raw.hs import and C header declaration.
5. Add Rust ABI function.
6. Run focused Hspec and Rust tests.
7. Run full Rust, full Stack/Hspec, HLint, and `git diff --check`.
8. Append implementation results.

## Examples

Good:

```haskell
indices <- seriesArgUnique values
```

Good:

```haskell
Pl.seriesWord32 indices
```

## Trade-offs

This batch returns the native Polars index dtype as a Series. If a future build
enables big-index Polars, typed extraction will need a companion story for
`IdxSize` width.

## Implementation Results

Implemented eager Series arg-unique:

```haskell
seriesArgUnique :: Series -> IO (Either PolarsError Series)
```

Changed files:

- `src/Polars/Series.hs`: exported `seriesArgUnique` and wrapped the Raw FFI with `seriesUnaryOut`.
- `src/Polars/Internal/Raw.hs`: added safe FFI import for `phs_series_arg_unique`.
- `include/polars_hs.h`: added C ABI declaration.
- `rust/polars-hs-ffi/src/series.rs`: added `phs_series_arg_unique` using `Series::arg_unique()?.into_series()`.
- `test/Spec.hs`: added Hspec coverage for numeric, text, bool, all-null, and empty Series.

Verified behavior:

- Numeric nullable `[1,2,1,null,3,null]` returns `[0,1,3,4]`.
- Text nullable `["a","b","a",null]` returns `[0,1,3]`.
- Bool nullable `[true,false,null,true]` returns `[0,1,2]`.
- All-null non-empty Series returns `[0]`.
- Empty Series returns `[]`.
- Current default index build returns `UInt32`, verified through `seriesDataType`.

Implementation notes:

- A read-only review agent confirmed pinned Rust Polars 0.53 keeps first-occurrence order and currently maps `IdxCa` to `UInt32` because this build does not enable `bigidx`.
- Future `bigidx` support should add an index-width abstraction or `seriesIdx` helper.

Verification:

- RED Rust failed on missing `phs_series_arg_unique`.
- RED Hspec failed on missing `seriesArgUnique`.
- Focused Rust FFI: 1/1 passing.
- Focused Hspec: 1/1 passing.
- Full Rust FFI: 115/115 passing.
- Full Stack/Hspec: 162/162 passing.
- HLint: no hints.
- `git diff --check`: clean.
