# Polars HS Eager Fill Null Design

## Background

`polars-hs` already exposes expression-level `fillNull` and LazyFrame
`fillNulls`, but eager Series and DataFrame handles do not expose Polars
strategy-based `fill_null`.

Rust Polars 0.53 exposes:

```rust
Series::fill_null(FillNullStrategy)
DataFrame::fill_null(FillNullStrategy)
```

`FillNullStrategy` variants are `Forward(limit)`, `Backward(limit)`, `Mean`,
`Min`, `Max`, `Zero`, and `One`. The limit type is `Option<IdxSize>`.

## Problem

Eager users need common null-filling workflows without switching into lazy
expressions. The API also needs consistent validation for fill limits so large
Haskell `Int` values do not silently truncate to Polars `IdxSize`.

## Questions and Answers

Q: Should Series and DataFrame use one shared strategy type?

A: Yes. One Haskell `FillNullStrategy` mirrors Rust and keeps call sites
consistent across Series and DataFrame APIs.

Q: Where should the shared type live?

A: Put it in `Polars.DataFrame` for this batch and re-export it from
`Polars.Series`. The current module graph already has `Polars.Series` importing
`Polars.DataFrame`, while `Polars.DataFrame` only uses internal Series handles.

Q: Should limit overflow be checked in Haskell or Rust?

A: Haskell validates negative limits. Rust validates `u64 -> IdxSize` overflow
for direct C callers and for 64-bit Haskell values larger than the current
Polars index width.

## Design

Public API:

```haskell
data FillNullStrategy
    = FillForward !(Maybe Int)
    | FillBackward !(Maybe Int)
    | FillMean
    | FillMin
    | FillMax
    | FillZero
    | FillOne

seriesFillNull :: FillNullStrategy -> Series -> IO (Either PolarsError Series)
dataFrameFillNull :: FillNullStrategy -> DataFrame -> IO (Either PolarsError DataFrame)
```

FFI:

```c
int phs_series_fill_null(const phs_series *series,
                         int strategy,
                         bool has_limit,
                         uint64_t limit,
                         phs_series **out,
                         phs_error **err);

int phs_dataframe_fill_null(const phs_dataframe *dataframe,
                            int strategy,
                            bool has_limit,
                            uint64_t limit,
                            phs_dataframe **out,
                            phs_error **err);
```

Strategy codes:

- `0`: forward
- `1`: backward
- `2`: mean
- `3`: min
- `4`: max
- `5`: zero
- `6`: one

## Implementation Plan

1. Add RED Hspec tests for Series fill-null forward/backward/zero.
2. Add RED Hspec tests for DataFrame fill-null forward across mixed scalar
   columns.
3. Add RED Hspec tests for negative and overflow fill limits.
4. Add Haskell strategy type, validation helpers, exports, and public functions.
5. Add Raw.hs imports and C header declarations.
6. Add Rust strategy decoding and ABI functions.
7. Run focused tests, Rust release tests, full Stack/Hspec, HLint, and
   `git diff --check`.
8. Append implementation results to this design log.

## Examples

Good:

```haskell
filledAge <- Pl.seriesFillNull (Pl.FillForward Nothing) age
filledFrame <- Pl.dataFrameFillNull (Pl.FillForward Nothing) df
```

Good:

```haskell
limited <- Pl.seriesFillNull (Pl.FillBackward (Just 1)) age
```

Bad:

```haskell
Pl.seriesFillNull (Pl.FillForward (Just (-1))) age
```

## Trade-offs

This batch exposes strategy fills only. Literal value fills remain covered by
expression/lazy APIs and can be added to eager APIs later with typed scalar ABI
design. Forward and backward limits stay optional so the first public surface
matches Rust without introducing extra wrapper records.

## Implementation Results

Implemented files:

- `src/Polars/DataFrame.hs`
- `src/Polars/Series.hs`
- `src/Polars/Internal/Raw.hs`
- `include/polars_hs.h`
- `rust/polars-hs-ffi/src/dataframe.rs`
- `rust/polars-hs-ffi/src/series.rs`
- `test/Spec.hs`

Delivered API:

```haskell
data FillNullStrategy
    = FillForward !(Maybe Int)
    | FillBackward !(Maybe Int)
    | FillMean
    | FillMin
    | FillMax
    | FillZero
    | FillOne

dataFrameFillNull :: FillNullStrategy -> DataFrame -> IO (Either PolarsError DataFrame)
seriesFillNull :: FillNullStrategy -> Series -> IO (Either PolarsError Series)
```

Validation:

- Haskell rejects negative forward/backward limits with
  `fill null limit must be non-negative`.
- Rust rejects `u64 -> IdxSize` overflow with
  `fill null limit exceeds Polars index size`.
- Rust decodes strategy codes into Polars `FillNullStrategy` and reports unknown
  strategy codes as `InvalidArgument`.

Tests added:

- DataFrame forward fill over `values.csv` mixed scalar columns.
- DataFrame negative and overflow fill limit errors.
- Series forward, backward, and zero fill over the `age` column.
- Series negative and overflow fill limit errors.

Verification:

- RED: focused Hspec failed on missing `dataFrameFillNull`, `seriesFillNull`,
  `FillForward`, `FillBackward`, and `FillZero` exports.
- Focused Hspec: 4 examples, 0 failures.
- Rust release tests: 87 passed, 0 failed.
- Full Stack/Hspec: 127 examples, 0 failures.
- HLint: no hints.
- `git diff --check`: clean.

Deviations:

- The implementation kept the plan's strategy-code ABI and shared public
  Haskell strategy type.
- The new fill-null FFI imports use `safe` because fill operations scan data and
  may run long enough to block a Haskell capability.
