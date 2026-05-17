# Polars HS Series Unique Counts Design

## Background

`polars-hs` now exposes `seriesValueCounts`, `seriesArgUnique`, and
`seriesRank`. Rust Polars 0.53 also exposes `unique_counts`, which returns the
counts for unique values in first-appearance order.

## Problem

Callers can build a two-column value-count DataFrame, but a compact count-only
Series is useful when the unique values are already known or when callers want
to compose counts with other Series-level operations.

## Questions and Answers

1. Which upstream feature is needed?

   Answer: enable Polars feature `unique_counts`, which maps to
   `polars-ops/unique_counts`.

2. What should the Haskell return type be?

   Answer: return `Series`. Upstream returns a Polars Series of index-sized
   counts.

3. What dtype should tests expect?

   Answer: current non-bigidx builds produce `UInt32`, matching existing
   `seriesArgUnique`, `seriesArgSort`, and `seriesRank` index outputs.

4. What order should tests assert?

   Answer: counts follow unique values in first-appearance order. For
   `[1,2,1,null,3,null]`, the unique value order is `[1,2,null,3]`, so counts
   are `[2,1,2,1]`.

## Design

```mermaid
flowchart LR
    H[seriesUniqueCounts Series] --> R[phs_series_unique_counts]
    R --> P[polars_ops::series::unique_counts]
    P --> S[count Series]
```

Public Haskell API:

```haskell
seriesUniqueCounts :: Series -> IO (Either PolarsError Series)
```

C ABI:

```c
int phs_series_unique_counts(const struct phs_series *series,
                             struct phs_series **out,
                             struct phs_error **err);
```

Rust implementation:

```rust
series_transform(series, out, err, |value| polars_series_unique_counts(value))
```

## Implementation Plan

1. Add Hspec tests first for numeric, text, bool, all-null, empty, and dtype.
2. Add Rust FFI tests for the same semantics.
3. Enable the `unique_counts` Polars feature in `rust/polars-hs-ffi/Cargo.toml`.
4. Export `seriesUniqueCounts` from `src/Polars/Series.hs`.
5. Add `phs_series_unique_counts` to Raw.hs and the C header.
6. Implement the Rust ABI near `phs_series_arg_unique` and
   `phs_series_value_counts`.
7. Run focused tests, full Rust tests, full Hspec, HLint, and diff checks.

## Examples

```haskell
counts <- seriesUniqueCounts values
```

```haskell
Pl.seriesWord32 counts
```

## Trade-offs

- The API exposes count-only output. `seriesValueCounts` remains the richer
  two-column frequency table for callers that need values and counts together.
- The output uses the Polars index dtype, so a future big-index build needs the
  same index-width abstraction planned for other index-producing Series APIs.

## Implementation Results

Files changed:

- `rust/polars-hs-ffi/Cargo.toml`: enables Polars `unique_counts` and
  `polars-ops/unique_counts`.
- `src/Polars/Series.hs`: exports `seriesUniqueCounts` and wraps the Raw FFI
  with `seriesUnaryOut`.
- `src/Polars/Internal/Raw.hs`: imports `phs_series_unique_counts` as a safe
  FFI call.
- `include/polars_hs.h`: declares the C ABI.
- `rust/polars-hs-ffi/src/series.rs`: implements `phs_series_unique_counts`
  with `polars_ops::series::unique_counts`.
- `test/Spec.hs`: adds Hspec coverage for numeric, text, bool, all-null,
  empty, and output dtype behavior.

Verified RED:

- Rust focused test failed because `phs_series_unique_counts` was missing.
- Hspec focused test failed because `Polars` did not export
  `seriesUniqueCounts`.

Verified GREEN:

- Focused Rust FFI: 1/1 passing.
- Focused Hspec: 1/1 passing.
- Full Rust FFI: 119/119 passing.
- Full Stack/Hspec: 166/166 passing.
- HLint: no hints.
- `git diff --check`: clean.

Deviations:

- The implementation matches the design. No functional deviations.
