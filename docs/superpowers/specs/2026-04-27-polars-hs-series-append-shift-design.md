# Design Log: Polars-Haskell Series Append and Shift API

## Background

The binding now exposes owned `Series` handles, typed value readers, and unary transforms such as cast, rename, sort, unique, reverse, and dropNulls.

The next Series operations cover two common data preparation cases:

```haskell
seriesShift  1 age
seriesAppend age moreAge
```

Polars Rust 0.53 exposes the needed eager APIs:

```rust
Series::shift(&self, periods: i64) -> Series
Series::append(&mut self, other: &Series) -> PolarsResult<&mut Series>
```

`shift` fills newly opened slots with nulls. Positive periods place nulls at the start. Negative periods place nulls at the end. `append` adds chunks from another Series and uses Polars schema compatibility rules.

## Problem

Users need Series-level row movement and concatenation while keeping the current safety and ownership model:

- Rust owns Polars values behind opaque `phs_series` handles.
- Haskell owns `ForeignPtr` finalizers.
- Recoverable failures return `Either PolarsError a`.
- Transform APIs return fresh handles and keep input handles reusable.

## Questions and Answers

### Q1. What API shape should this phase expose?

Answer: Expose `seriesShift` and `seriesAppend` as the MVP.

```haskell
seriesShift  :: Int -> Series -> IO (Either PolarsError Series)
seriesAppend :: Series -> Series -> IO (Either PolarsError Series)
```

### Q2. What argument order should append use?

Answer: Use left-to-right ordering.

```haskell
seriesAppend left right
```

The result is the values of `left` followed by the values of `right`. The result name comes from `left`.

## Design

### Public module changes

Extend `Polars.Series` exports:

```haskell
module Polars.Series
    ( Series
    , SeriesCast (..)
    , SeriesSortOptions (..)
    , defaultSeriesSortOptions
    , seriesAppend
    , seriesBool
    , seriesCast
    , seriesDataType
    , seriesDouble
    , seriesDropNulls
    , seriesHead
    , seriesInt64
    , seriesLength
    , seriesName
    , seriesNullCount
    , seriesRename
    , seriesReverse
    , seriesShift
    , seriesSort
    , seriesTail
    , seriesText
    , seriesToFrame
    , seriesUnique
    , seriesUniqueStable
    ) where
```

New functions:

```haskell
seriesShift :: Int -> Series -> IO (Either PolarsError Series)
seriesAppend :: Series -> Series -> IO (Either PolarsError Series)
```

### Shift semantics

`seriesShift period series` maps directly to Rust `Series::shift(period)`.

Examples for `Vector (Maybe Int64)` values:

```haskell
seriesShift 1 age
-- [Nothing, Just 34, Nothing]

seriesShift (-1) age
-- [Nothing, Just 29, Nothing]

seriesShift 2 age
-- [Nothing, Nothing, Just 34]
```

The input `age` in these examples is:

```haskell
[Just 34, Nothing, Just 29]
```

The public argument type is `Int`. The FFI uses `int64_t` / `CLLong`. Current target platforms use a Haskell `Int` range that fits in `int64_t`; the implementation still keeps conversion at the Haskell boundary.

### Append semantics

`seriesAppend left right` clones `left`, appends `right`, and returns the clone as a fresh handle.

Rust implementation shape:

```rust
let mut out_series = left.value.clone();
out_series.append(&right.value)?;
*out = series_into_raw(out_series);
```

Semantics:

- Result values are `left` followed by `right`.
- Result name is the `left` name.
- Result dtype follows Polars append rules for the left Series and schema-compatible right Series.
- Dtype conflicts return a Rust Polars error surfaced as `PolarsFailure`.

### Rust ABI

Add functions to `rust/polars-hs-ffi/src/series.rs`:

```c
int phs_series_shift(
  const struct phs_series *series,
  int64_t periods,
  struct phs_series **out,
  struct phs_error **err
);

int phs_series_append(
  const struct phs_series *left,
  const struct phs_series *right,
  struct phs_series **out,
  struct phs_error **err
);
```

### Haskell internals

Extend `Polars.Internal.Raw`:

```haskell
foreign import ccall unsafe "phs_series_shift"
    phs_series_shift :: Ptr RawSeries -> CLLong -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt

foreign import ccall unsafe "phs_series_append"
    phs_series_append :: Ptr RawSeries -> Ptr RawSeries -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt
```

`seriesShift` uses `withSeries` and `seriesOut`:

```haskell
seriesShift :: Int -> Series -> IO (Either PolarsError Series)
seriesShift periods series =
    withSeries series $ \ptr -> seriesOut (phs_series_shift ptr (fromIntegral periods))
```

`seriesAppend` scopes both `ForeignPtr`s for the full FFI call:

```haskell
seriesAppend :: Series -> Series -> IO (Either PolarsError Series)
seriesAppend left right =
    withSeries left $ \leftPtr ->
        withSeries right $ \rightPtr ->
            seriesOut (phs_series_append leftPtr rightPtr)
```

## Examples

### Shift a Series

```haskell
Right age <- Pl.column @Pl.Series df "age"
Right shifted <- Pl.seriesShift 1 age
print =<< Pl.seriesInt64 shifted
-- Right [Nothing,Just 34,Nothing]
```

### Append two Series

```haskell
Right age <- Pl.column @Pl.Series df "age"
Right headAge <- Pl.seriesHead 1 age
Right appended <- Pl.seriesAppend age headAge
print =<< Pl.seriesInt64 appended
-- Right [Just 34,Nothing,Just 29,Just 34]
```

### Append after casting

```haskell
Right age <- Pl.column @Pl.Series df "age"
Right score <- Pl.column @Pl.Series df "score"
Right ageDouble <- Pl.seriesCast @Double age
Right combined <- Pl.seriesAppend ageDouble score
print =<< Pl.seriesDouble combined
```

## Testing Plan

- `seriesShift 1` creates leading nulls and preserves length.
- `seriesShift (-1)` creates trailing nulls and preserves length.
- `seriesAppend age headAge` produces left values followed by right values.
- `seriesAppend` preserves the left Series name.
- `seriesAppend` succeeds after explicit cast to a compatible dtype.
- `seriesAppend` returns `PolarsFailure` for incompatible dtypes.
- Input Series handles stay usable after append and shift.

## Trade-offs

### Benefits

- Adds common Series row-alignment and concatenation operations.
- Preserves immutable Haskell handle semantics with fresh result handles.
- Keeps append ordering intuitive: `seriesAppend left right` corresponds to `left <> right`.
- Uses Polars dtype compatibility rules through Rust `Series::append`.

### Costs

- `seriesAppend` is binary and uses a left/right order while unary Series operations place the Series argument last.
- Append compatibility is determined by Rust Polars, so callers see the same dtype behavior as Polars eager Series.

### Future extensions

- `seriesShiftAndFill` after scalar literal encoding is designed.
- `seriesAppendMany` for concatenating multiple Series in one FFI call.
- Typed constructors for building Series directly from Haskell vectors.

## Implementation Plan

1. Add RED Hspec tests for shift and append behavior.
2. Add Rust ABI functions and Rust unit tests in `series.rs`.
3. Add raw Haskell imports and public `Polars.Series` wrappers.
4. Update README, CHANGELOG, and `examples/series.hs`.
5. Verify with Rust tests, Clippy, Stack tests, HLint, and examples.

## Implementation Results

Implementation starts after design approval. Verification results, deviations, and final commit information are recorded during implementation.
