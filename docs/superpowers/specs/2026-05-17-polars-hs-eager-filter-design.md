# Polars-HS Eager Filter Foundation Design

## Background

`polars-hs` has lazy filtering through expressions and eager DataFrame shape,
selection, slicing, reverse, null-count, and drop-null helpers. Series handles
already support construction, extraction, casts, sorting, unique, reverse,
drop-null, shift, append, and conversion to DataFrame.

Polars 0.53 supports boolean-mask based eager selection:

- `Series::slice(offset, length)`;
- `Series::is_null()` and `Series::is_not_null()`;
- `Series::filter(mask.bool()?)`;
- `DataFrame::filter(mask.bool()?)`.

## Problem

Eager filtering currently requires moving through lazy expressions or extracting
values into Haskell. Adding boolean mask Series operations gives a reusable
foundation for eager row filtering, future `take`, and many Series operation
tests while keeping the ABI compact.

## Questions and Answers

Q: Should eager filtering accept `Expr`?

A: No. That would require an eager expression evaluator surface. This batch
uses explicit boolean Series masks, matching Polars eager APIs directly.

Q: Should masks be nullable?

A: Yes. Polars defines the mask semantics; null mask values are handled by
Polars. The Haskell binding passes the mask Series through unchanged.

Q: Should slice accept negative offsets?

A: Yes. Polars `Series::slice` accepts signed offsets. Haskell validates only
non-negative lengths.

## Design

Public Series APIs:

```haskell
seriesSlice :: Int -> Int -> Series -> IO (Either PolarsError Series)
seriesIsNull :: Series -> IO (Either PolarsError Series)
seriesIsNotNull :: Series -> IO (Either PolarsError Series)
seriesFilter :: Series -> Series -> IO (Either PolarsError Series)
```

Public DataFrame API:

```haskell
dataFrameFilter :: Series -> DataFrame -> IO (Either PolarsError DataFrame)
```

The first argument to `seriesFilter` and `dataFrameFilter` is the mask to make
call sites read left-to-right with a precomputed boolean mask:

```haskell
mask <- Pl.seriesIsNotNull age
filtered <- Pl.dataFrameFilter mask df
```

Rust ABI:

```c
int phs_series_slice(const phs_series *series, int64_t offset, uint64_t length, ...);
int phs_series_is_null(const phs_series *series, ...);
int phs_series_is_not_null(const phs_series *series, ...);
int phs_series_filter(const phs_series *series, const phs_series *mask, ...);
int phs_dataframe_filter(const phs_dataframe *dataframe, const phs_series *mask, ...);
```

Rust mapping:

- `handle.value.slice(offset, length)`;
- `handle.value.is_null().into_series()`;
- `handle.value.is_not_null().into_series()`;
- `handle.value.filter(mask.value.bool()?)`;
- `handle.value.filter(mask.value.bool()?)`.

## Implementation Plan

1. Add RED Hspec tests for Series slice/null masks/filter and DataFrame filter.
2. Add Haskell exports and raw imports.
3. Add Rust FFI implementations and header declarations.
4. Verify wrong mask dtype and mismatched mask length return Polars errors.
5. Run focused tests and full verification.

## Examples

Good pattern:

```haskell
Right age <- Pl.column @Pl.Series df "age"
Right mask <- Pl.seriesIsNotNull age
Right filtered <- Pl.dataFrameFilter mask df
```

Bad pattern:

```haskell
names <- Pl.column @Text df "name"
let filtered = Vector.ifilter ...
```

## Trade-offs

This design keeps eager filtering explicit and type-light. It exposes Polars
errors for mask dtype and mask length mismatches instead of duplicating those
checks in Haskell. Future APIs such as eager `take`, DataFrame sort/unique, and
fill strategies can build on this same Series-handle flow.

## Implementation Results

Implemented files:

- `src/Polars/Series.hs`
- `src/Polars/DataFrame.hs`
- `src/Polars/Internal/Raw.hs`
- `include/polars_hs.h`
- `rust/polars-hs-ffi/src/series.rs`
- `rust/polars-hs-ffi/src/dataframe.rs`
- `test/Spec.hs`

Delivered APIs:

```haskell
seriesSlice :: Int -> Int -> Series -> IO (Either PolarsError Series)
seriesIsNull :: Series -> IO (Either PolarsError Series)
seriesIsNotNull :: Series -> IO (Either PolarsError Series)
seriesFilter :: Series -> Series -> IO (Either PolarsError Series)
dataFrameFilter :: Series -> DataFrame -> IO (Either PolarsError DataFrame)
```

Test coverage added:

- DataFrame filtering with a boolean Series mask.
- Series slicing through owned Series handles.
- Series null and not-null boolean mask generation.
- Series filtering with valid masks.
- DataFrame and Series error propagation for non-boolean and mismatched-length
  masks.
- DataFrame and Series nullable boolean masks, where null mask values behave as
  false.
- Haskell boundary validation for negative Series slice length.

Verification:

- Focused Hspec mask/slice tests: 7 examples, 0 failures.
- Rust FFI release tests: 87 passed, 0 failed.
- Full Stack/Hspec suite: 118 examples, 0 failures.
- HLint: no hints.
- `git diff --check`: clean.

Deviations:

- No Rust unit test was added for the new filter functions because the Hspec
  coverage exercises the public Haskell API through the same C ABI and verifies
  both success and error paths.
