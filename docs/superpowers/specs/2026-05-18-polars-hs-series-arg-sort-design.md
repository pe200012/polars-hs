# Polars HS Series ArgSort Design

## Background

`polars-hs` exposes eager Series sorting through `seriesSort` and exposes
index-producing distinct helpers through `seriesArgUnique`. Rust Polars 0.53
also exposes `SeriesTrait::arg_sort`, which returns the indexes that would sort
a Series.

## Problem

Users can sort Series values directly, but cannot ask for the row positions that
produce that sorted order. This blocks stable ordering workflows where callers
want to sort one Series and apply the resulting index Series to another Series
or DataFrame.

## Questions and Answers

1. Which Haskell option type should be used?

   Answer: reuse `SeriesSortOptions`. Rust `SortOptions` has the same fields:
   `descending`, `nulls_last`, `multithreaded`, `maintain_order`, and `limit`.

2. What should the Haskell return type be?

   Answer: return `Series`, matching `seriesArgUnique`. The Rust result is
   `IdxCa`, converted to a Polars Series with `into_series()`.

3. What index dtype should tests expect?

   Answer: current non-bigidx builds produce `UInt32`. A future big-index build
   should route through a shared index-width abstraction.

4. How should invalid limits be reported?

   Answer: Haskell validates negative limits with an arg-sort-specific
   `InvalidArgument` message. Rust validates `u64 -> IdxSize` overflow with the
   same arg-sort-specific label.

## Design

```mermaid
flowchart LR
    H[seriesArgSort options series] --> R[phs_series_arg_sort]
    R --> P[SeriesTrait::arg_sort SortOptions]
    P --> I[IdxCa]
    I --> S[index Series]
```

Public Haskell API:

```haskell
seriesArgSort :: SeriesSortOptions -> Series -> IO (Either PolarsError Series)
```

C ABI:

```c
int phs_series_arg_sort(const struct phs_series *series,
                        bool descending,
                        bool nulls_last,
                        bool multithreaded,
                        bool maintain_order,
                        bool has_limit,
                        uint64_t limit,
                        struct phs_series **out,
                        struct phs_error **err);
```

Rust implementation:

```rust
let mut options = SortOptions::default()
    .with_order_descending(descending)
    .with_nulls_last(nulls_last)
    .with_multithreaded(multithreaded)
    .with_maintain_order(maintain_order);
if has_limit {
    options.limit = Some(idx_size_from_u64(limit, "series arg sort limit")?);
}
Ok(value.arg_sort(options).into_series())
```

## Implementation Plan

1. Add Hspec tests first for default ascending indexes, descending/nulls-last
   indexes, stable tie ordering with `maintain_order`, empty Series, and
   invalid limits.
2. Add Rust FFI tests for sorted index output and overflow validation.
3. Export `seriesArgSort` from `src/Polars/Series.hs`.
4. Add `phs_series_arg_sort` to Raw.hs and the C header.
5. Implement the Rust ABI near `phs_series_sort`.
6. Run focused tests, full Rust tests, full Hspec, HLint, and diff checks.

## Examples

```haskell
idx <- seriesArgSort
    defaultSeriesSortOptions { seriesSortNullsLast = True }
    values
```

```haskell
idx <- seriesArgSort
    defaultSeriesSortOptions
        { seriesSortDescending = True
        , seriesSortNullsLast = True
        }
    values
```

## Trade-offs

- Reusing `SeriesSortOptions` avoids a second option record with identical
  fields.
- Returning `Series` keeps the API consistent with `seriesArgUnique` and lets
  callers decode with the existing scalar extraction matrix.
- `SortOptions.limit` is exposed because it already exists on
  `SeriesSortOptions`, though upstream documents it as an optimization hint that
  may be ignored.

## Implementation Results

Files changed:

- `src/Polars/Series.hs`: exports `seriesArgSort`, wraps the new Raw FFI, and
  shares sort-limit validation through a label-aware helper.
- `src/Polars/Internal/Raw.hs`: imports `phs_series_arg_sort` as a safe FFI
  call.
- `include/polars_hs.h`: declares the C ABI.
- `rust/polars-hs-ffi/src/series.rs`: implements `phs_series_arg_sort` with
  `SeriesTrait::arg_sort` and `IdxCa::into_series()`.
- `test/Spec.hs`: adds Hspec coverage for default ordering, null placement,
  descending order, stable tie ordering, text ordering, empty Series, output
  dtype, and negative limit validation.

Verified RED:

- Rust focused test failed because `phs_series_arg_sort` was missing.
- Hspec focused test failed because `Polars` did not export `seriesArgSort`.

Verified GREEN:

- Focused Rust FFI: 1/1 passing.
- Focused Hspec: 1/1 passing.
- Full Rust FFI: 117/117 passing.
- Full Stack/Hspec: 164/164 passing.
- HLint: no hints.
- `git diff --check`: clean.

Deviations:

- The implementation matches the design. No functional deviations.
