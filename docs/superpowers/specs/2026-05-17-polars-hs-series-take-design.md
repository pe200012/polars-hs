# Polars HS Series Take Design

## Background

`polars-hs` already exposes eager Series slicing, boolean filtering, null
predicates, fill-null strategies, arithmetic, and scalar statistics. Index-based
gather remains a useful Series-level operation for reordering, duplicating, or
selecting values by row position.

Rust Polars 0.53 exposes:

```rust
Series::take(&IdxCa) -> PolarsResult<Series>
```

Docs.rs shows `IdxCa` is `UInt32Chunked` in the default build and `UInt64Chunked`
with Polars' `bigidx` feature. The binding should accept a stable wide Haskell
payload and validate the conversion against the actual Rust `IdxSize` width.

## Problem

Users need eager positional Series selection without building a boolean mask or a
lazy expression. The ABI must carry an index array across the FFI boundary,
preserve duplicate and reordered indices, surface Polars out-of-bounds errors,
and report index-width overflow as `InvalidArgument`.

## Questions and Answers

Q: Should public Haskell indices use `Word32` or `Word64`?

A: Use `Vector Word64`. This keeps the public API stable across default and
`bigidx` Rust builds. Rust validates each value with `IdxSize::try_from`.

Q: Should empty index vectors be accepted?

A: Yes. `Series::take` supports an empty `IdxCa`, and returning an empty Series
matches Polars semantics.

Q: Should out-of-bounds indices be prevalidated in Haskell?

A: Let Polars validate bounds. Haskell does not always know the Rust Series
length cheaply at the call site, and Polars already returns a typed error.

Q: Should null index support be included?

A: Keep this batch to non-null positional indices. Null-aware take would require
a nullable index payload ABI and separate semantics.

## Design

Public API:

```haskell
seriesTake :: Vector Word64 -> Series -> IO (Either PolarsError Series)
```

FFI:

```c
int phs_series_take(const phs_series *series,
                    const uint64_t *indices,
                    uintptr_t len,
                    phs_series **out,
                    phs_error **err);
```

Rust conversion:

```rust
let values: Vec<IdxSize> = indices
    .iter()
    .map(|value| idx_size_from_u64(*value, "series take index"))
    .collect::<PhsResult<_>>()?;
let idx = IdxCa::from_vec(PlSmallStr::EMPTY, values);
let output = series.take(&idx)?;
```

Validation rules:

- Null `series` pointer uses existing `series_ref` validation.
- Null `out` pointer uses existing `required_mut` validation.
- Null `indices` pointer is accepted only when `len == 0`.
- Index values exceeding `IdxSize` return `InvalidArgument`.
- Bounds errors come from Polars.

## Implementation Plan

1. Add RED Hspec tests for duplicate/reordered Series take and empty take.
2. Add RED Hspec tests for out-of-bounds and index overflow errors.
3. Add `seriesTake` export and Haskell wrapper using `Vector Word64`.
4. Add Raw.hs import and C header declaration for `phs_series_take`.
5. Add Rust index-slice conversion helper and `phs_series_take`.
6. Run focused Hspec, Rust release tests, full Stack/Hspec, HLint, and
   `git diff --check`.
7. Append implementation results to this design log.

## Examples

Good:

```haskell
taken <- Pl.seriesTake (V.fromList [2, 0, 2]) age
```

Bad:

```haskell
Pl.seriesTake (V.fromList [fromIntegral (maxBound :: Word64)]) age
```

## Trade-offs

`Vector Word64` makes the Haskell payload slightly wider than default Polars
indices, but it keeps the API independent of Polars build flags. The Rust side
owns the final conversion because it knows the compiled `IdxSize` type.

## Implementation Results

Implemented files:

- `src/Polars/Series.hs`
- `src/Polars/Internal/Raw.hs`
- `include/polars_hs.h`
- `rust/polars-hs-ffi/src/series.rs`
- `test/Spec.hs`

Delivered API:

```haskell
seriesTake :: Vector Word64 -> Series -> IO (Either PolarsError Series)
```

Rust/C ABI:

```c
int phs_series_take(const phs_series *series,
                    const uint64_t *indices,
                    uintptr_t len,
                    phs_series **out,
                    phs_error **err);
```

Validation and behavior:

- `Vector Word64` crosses the Haskell boundary as a contiguous `uint64_t` array.
- Rust accepts `NULL` indices only with `len == 0`.
- Rust converts each index through `IdxSize::try_from` and reports
  `series take index exceeds Polars index size` on overflow.
- Polars reports out-of-bounds indices through the existing `PolarsFailure`
  channel.
- Duplicate, reordered, empty, and nullable-value gather cases are covered.

Tests added:

- Hspec duplicate/reordered gather over `[2, 0, 2]`.
- Hspec empty gather returning a length-zero Series.
- Hspec null preservation over `[1, 2, 1, 0]`.
- Hspec out-of-bounds and index-width overflow errors.
- Rust FFI tests for empty/reordered indices, `NULL` pointer with positive
  length, and index overflow.

Verification:

- RED: focused Hspec failed on missing `Pl.seriesTake` export.
- Focused Hspec: 3 examples, 0 failures.
- Focused Rust `series_take`: 3 tests, 0 failures.
- Rust release tests: 90 passed, 0 failed.
- Full Stack/Hspec: 134 examples, 0 failures.
- HLint: no hints.
- `git diff --check`: clean.

Deviations:

- The Haskell wrapper uses `V.toList` with `withArray`, matching existing small
  ABI payload style while keeping the first public API simple. A future large
  gather optimization can pass a pinned vector buffer directly.
