# Polars HS Series Median And NUnique Design

## Background

Rust Polars 0.53 exposes Series aggregation helpers through `SeriesTrait`:

```rust
fn median(&self) -> Option<f64>
fn n_unique(&self) -> PolarsResult<usize>
```

Docs and source checked:

- https://docs.rs/polars/latest/polars/series/trait.SeriesTrait.html
- https://docs.pola.rs/py-polars/html/reference/series/aggregation.html
- `polars-core-0.53.0/src/series/series_trait.rs`
- `polars-core-0.53.0/src/series/implementations/mod.rs`
- `polars-core-0.53.0/src/series/implementations/floats.rs`
- `polars-core-0.53.0/src/series/implementations/string.rs`
- `polars-core-0.53.0/src/series/implementations/boolean.rs`
- `polars-core-0.53.0/src/series/implementations/null.rs`

## Problem

`polars-hs` already exposes Series `mean`, `std`, `var`, `sum`, `min`, and
`max`, but direct Series median and unique-count aggregation are still missing.
Expression-level `median_` and `nUnique_` exist, but eager Series users need the
same operations without building a lazy plan.

## Questions and Answers

Q: Should median reuse the existing nullable-double stat ABI?

A: Yes. Rust `Series::median()` returns `Option<f64>`, matching the existing
`seriesMaybeDoubleOut` helper.

Q: Should `seriesNUnique` return `Int`?

A: Yes. Existing size/count APIs such as `seriesLength`, `seriesNullCount`,
`height`, `width`, and `shape` return `Int` after a checked `Word64 -> Int`
conversion. `seriesNUnique` should follow that API style in this batch.

Q: Does `n_unique` include nulls?

A: Yes. Rust Polars docs state that null also counts as a unique value.

Q: Should quantile or product be included here?

A: No. Quantile needs a public method enum decision for Series APIs, and product
returns a typed `Scalar`. Median and n-unique are small and fit the current ABI.

## Design

Public API:

```haskell
seriesMedian :: Series -> IO (Either PolarsError (Maybe Double))
seriesNUnique :: Series -> IO (Either PolarsError Int)
```

C ABI:

```c
int phs_series_n_unique(const phs_series *series, uint64_t *out, phs_error **err);
```

Rust mapping:

```rust
// phs_series_stat op 6
Ok(series.median())

// phs_series_n_unique
*out = series.n_unique()?.try_into()?;
```

## Implementation Plan

1. Add RED Hspec tests for Series median and n-unique.
2. Add Rust FFI tests for median/n-unique value and error behavior.
3. Export `seriesMedian` and `seriesNUnique` from `Polars.Series`.
4. Extend `phs_series_stat` with median op code `6`.
5. Add `phs_series_n_unique` to Raw.hs, the C header, and Rust FFI.
6. Run focused Hspec and Rust tests.
7. Run full Rust, full Stack/Hspec, HLint, and `git diff --check`.
8. Append implementation results.

## Examples

Good:

```haskell
medianScore <- seriesMedian score
```

Good:

```haskell
uniqueNames <- seriesNUnique names
```

## Trade-offs

This batch deliberately keeps `n_unique` as an eager scalar count. Future APIs
for `arg_unique`, `value_counts`, and approximate unique counts should use
separate designs because they return Series/DataFrame outputs and need option
records.

## Implementation Results

Implemented eager Series median and unique count:

```haskell
seriesMedian :: Series -> IO (Either PolarsError (Maybe Double))
seriesNUnique :: Series -> IO (Either PolarsError Int)
```

Changed files:

- `src/Polars/Series.hs`: exported `seriesMedian` and `seriesNUnique`.
- `src/Polars/Internal/Raw.hs`: added safe FFI import for `phs_series_n_unique`.
- `include/polars_hs.h`: added C ABI declaration.
- `rust/polars-hs-ffi/src/series.rs`: added `phs_series_stat` op `6` for median and `phs_series_n_unique`.
- `test/Spec.hs`: added Hspec coverage for nullable numeric, text, bool, all-null, and empty Series.

Verified behavior:

- Numeric median returns `Just` a `Double`.
- Text median returns `Nothing`.
- All-null and empty numeric median return `Nothing`.
- `n_unique` counts null as one unique value.
- Empty Series unique count returns `0`.

Implementation notes:

- `seriesMedian` reuses the existing nullable-double ABI.
- `seriesNUnique` uses a `uint64_t` C ABI and the existing checked Haskell `Word64 -> Int` conversion.
- A read-only review agent confirmed this matches pinned Rust Polars 0.53 behavior and wrote the companion Serena research memory `research/api/series-median-nunique-2026-05-18`.

Verification:

- RED Rust failed on missing `phs_series_n_unique`.
- RED Hspec failed on missing `seriesMedian` and `seriesNUnique`.
- Focused Rust FFI: 1/1 passing.
- Focused Hspec: 1/1 passing.
- Full Rust FFI: 114/114 passing.
- Full Stack/Hspec: 161/161 passing.
- HLint: no hints.
- `git diff --check`: clean.
