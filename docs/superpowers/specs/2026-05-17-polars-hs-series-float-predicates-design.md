# Polars HS Series Float Predicates Design

## Background

`polars-hs` already exposes Series null predicates and expression-level floating
predicates. Rust Polars 0.53 exposes eager Series methods:

```rust
series.is_nan()
series.is_not_nan()
series.is_finite()
series.is_infinite()
```

Each method returns `BooleanChunked`, preserving source null validity. Integer
Series return constant boolean values with the original null validity; string and
other unsupported dtypes return Polars errors.

## Problem

Users need eager boolean masks for NaN and infinity checks so they can filter or
inspect Series without building a lazy expression. The API should mirror existing
`seriesIsNull` and `seriesIsNotNull` by returning a Bool Series handle.

## Questions and Answers

Q: Should these functions return `Series` or `Vector (Maybe Bool)`?

A: Return `Series`. This matches `seriesIsNull`, lets the result serve as a mask
for `seriesFilter` and `dataFrameFilter`, and keeps extraction explicit through
`seriesBool`.

Q: Should integer Series be accepted?

A: Yes. Polars accepts primitive numeric dtypes and returns constant boolean
values with original null validity.

Q: Should unsupported dtypes be prevalidated in Haskell?

A: Let Rust Polars validate dtype support and return `PolarsFailure`.

## Design

Public API:

```haskell
seriesIsNan :: Series -> IO (Either PolarsError Series)
seriesIsNotNan :: Series -> IO (Either PolarsError Series)
seriesIsFinite :: Series -> IO (Either PolarsError Series)
seriesIsInfinite :: Series -> IO (Either PolarsError Series)
```

C ABI:

```c
int phs_series_is_nan(const phs_series *series, phs_series **out, phs_error **err);
int phs_series_is_not_nan(const phs_series *series, phs_series **out, phs_error **err);
int phs_series_is_finite(const phs_series *series, phs_series **out, phs_error **err);
int phs_series_is_infinite(const phs_series *series, phs_series **out, phs_error **err);
```

Rust mapping:

```rust
series.is_nan()?.into_series()
series.is_not_nan()?.into_series()
series.is_finite()?.into_series()
series.is_infinite()?.into_series()
```

## Implementation Plan

1. Add RED Hspec tests for float special values: `1.0`, `NaN`, `inf`, `-inf`.
2. Add RED Hspec tests for integer Series null-validity preservation.
3. Add RED Hspec test for unsupported text dtype errors.
4. Add Haskell exports and wrappers using `seriesUnaryOut`.
5. Add Raw.hs imports, C header declarations, and Rust extern functions.
6. Run focused Hspec, Rust release tests, full Stack/Hspec, HLint, and
   `git diff --check`.
7. Append implementation results to this design log.

## Examples

Good:

```haskell
mask <- Pl.seriesIsFinite values
filtered <- Pl.seriesFilter mask values
```

Bad:

```haskell
Pl.seriesIsNan textSeries
```

## Trade-offs

Returning Series masks keeps the eager API composable with existing filtering
helpers. Direct `Vector Bool` helper functions can be added later as convenience
wrappers over `seriesBool`.

## Implementation Results

Implemented public eager Series predicates:

```haskell
seriesIsNan :: Series -> IO (Either PolarsError Series)
seriesIsNotNan :: Series -> IO (Either PolarsError Series)
seriesIsFinite :: Series -> IO (Either PolarsError Series)
seriesIsInfinite :: Series -> IO (Either PolarsError Series)
```

Added C ABI declarations and Rust `phs_series_is_*` functions backed by Polars
0.53 `Series` methods. Haskell imports use safe FFI calls because each predicate
scans Series values.

Added Hspec coverage for:

- `1.0`, `NaN`, `inf`, and `-inf` special float values.
- Integer Series null-validity preservation.
- Unsupported text dtype error propagation.

Added Rust FFI coverage for the same success and error paths.

Verification on 2026-05-17:

- RED Hspec produced missing-export failures for `seriesIsNan`,
  `seriesIsNotNan`, `seriesIsFinite`, and `seriesIsInfinite`.
- Focused Hspec: 3/3 examples passing.
- Focused Rust FFI: 3/3 tests passing.
- Full Rust FFI: 98/98 tests passing.
- Full Stack/Hspec: 143/143 examples passing.
- HLint: no hints.
- `git diff --check`: clean.

`cargo fmt --check` reports broad pre-existing rustfmt drift across Rust files,
so this batch kept the local style and avoided unrelated formatting churn.
