# Polars HS Series Sum Min Max Design

## Background

`polars-hs` exposes eager Series arithmetic and scalar statistics for `mean`,
`std`, and `var`. Rust Polars 0.53 also exposes scalar numeric helpers:

```rust
series.sum::<f64>()
series.min::<f64>()
series.max::<f64>()
```

Docs.rs states that `sum` returns zero for an empty numeric Series. The local
Polars 0.53 source shows numeric `ChunkAgg::sum` also returns the zero value
when every value is null. `min` and `max` return `Option<T>` because nullable or
empty arrays may not have a value.

## Problem

Users need immediate eager scalar totals and extrema without collecting through a
lazy expression. The Haskell API should match the existing nullable scalar
statistics style, reject non-numeric dtypes explicitly, and preserve all-null and
empty semantics from the pinned Polars implementation.

## Questions and Answers

Q: Should the public API return `Maybe Double`?

A: Yes. This matches the existing `seriesMean`, `seriesStd`, and `seriesVar`
shape and preserves null results without sentinels.

Q: Should this batch expose typed integer scalar sums?

A: Keep this batch to `Double`. It is the same first scalar type used by
`seriesMean`, `seriesStd`, and `seriesVar`. Typed scalar extraction can be a
later wider scalar ABI.

Q: Should the existing `phs_series_stat` ABI be extended?

A: Yes. Add op codes for sum, min, and max because the ABI already returns a
nullable `double` scalar.

## Design

Public API:

```haskell
seriesSum :: Series -> IO (Either PolarsError (Maybe Double))
seriesMin :: Series -> IO (Either PolarsError (Maybe Double))
seriesMax :: Series -> IO (Either PolarsError (Maybe Double))
```

FFI:

```c
int phs_series_stat(const phs_series *series,
                    int op,
                    uint8_t ddof,
                    bool *has_value_out,
                    double *value_out,
                    phs_error **err);
```

Stat op codes:

- `0`: mean
- `1`: standard deviation
- `2`: variance
- `3`: sum
- `4`: minimum
- `5`: maximum

Rust mapping:

```rust
3 => {
    ensure_numeric_stat_dtype(series, "series sum")?;
    Ok(Some(series.sum::<f64>()?))
}
4 => {
    ensure_numeric_stat_dtype(series, "series min")?;
    Ok(series.min::<f64>()?)
}
5 => {
    ensure_numeric_stat_dtype(series, "series max")?;
    Ok(series.max::<f64>()?)
}
```

`sum::<f64>()` returns `Some(0.0)` for empty and all-null numeric Series in this
pinned Polars build. `min::<f64>()` and `max::<f64>()` return `None` for empty
and all-null numeric Series.

## Implementation Plan

1. Add RED Hspec tests for nullable numeric sum/min/max over `score`.
2. Add RED Hspec tests for all-null and empty numeric Series behavior.
3. Add RED Hspec test for invalid dtype sum/min/max errors.
4. Add Haskell exports and wrappers that call `seriesStat` op codes `3..5`.
5. Extend Rust `series_stat_from_code` with sum/min/max.
6. Run focused Hspec, Rust release tests, full Stack/Hspec, HLint, and
   `git diff --check`.
7. Append implementation results to this design log.

## Examples

Good:

```haskell
total <- Pl.seriesSum score
lo <- Pl.seriesMin score
hi <- Pl.seriesMax score
```

Bad:

```haskell
Pl.seriesSum textSeries
```

## Trade-offs

Returning `Maybe Double` gives a compact first scalar aggregation surface and
matches existing statistics. Exact integer-preserving scalar APIs can be added
later with a structured scalar ABI.

## Implementation Results

Implemented files:

- `src/Polars/Series.hs`
- `rust/polars-hs-ffi/src/series.rs`
- `test/Spec.hs`

Delivered API:

```haskell
seriesSum :: Series -> IO (Either PolarsError (Maybe Double))
seriesMin :: Series -> IO (Either PolarsError (Maybe Double))
seriesMax :: Series -> IO (Either PolarsError (Maybe Double))
```

Rust mapping:

- `phs_series_stat` op code `3` maps to `series.sum::<f64>()` after numeric
  dtype validation.
- `phs_series_stat` op code `4` maps to `series.min::<f64>()` after numeric
  dtype validation.
- `phs_series_stat` op code `5` maps to `series.max::<f64>()` after numeric
  dtype validation.

Validation and behavior:

- Non-numeric dtypes return `InvalidArgument` with operation-specific messages.
- Nullable numeric Series sum ignores nulls.
- All-null and empty numeric Series sum return `Just 0.0`, matching the pinned
  Polars 0.53 `ChunkAgg::sum` implementation.
- All-null and empty numeric Series min/max return `Nothing`.

Tests added:

- Hspec nullable numeric sum/min/max over `score`.
- Hspec all-null and empty numeric Series behavior.
- Hspec invalid dtype validation for sum/min/max.
- Rust FFI tests for sum/min/max values and non-numeric dtype validation.

Verification:

- RED: focused Hspec failed on missing `Pl.seriesSum`, `Pl.seriesMin`, and
  `Pl.seriesMax` exports.
- Focused Hspec: 3 examples, 0 failures.
- Focused Rust `series_stat_sum_min_max`: 2 tests, 0 failures.
- Rust release tests: 95 passed, 0 failed.
- Full Stack/Hspec: 140 examples, 0 failures.
- HLint: no hints.
- `git diff --check`: clean.

Deviations:

- The initial design followed the docs wording for all-null sum. The local
  pinned Polars implementation returns zero for all-null numeric arrays, so the
  tests and design were updated to match the actual dependency behavior.
- Non-numeric dtype validation is explicit in the binding. This avoids silently
  converting string min/max extraction failures into `Nothing`.
