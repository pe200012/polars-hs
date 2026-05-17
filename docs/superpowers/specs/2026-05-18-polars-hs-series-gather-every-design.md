# Series Gather-Every Design

## Background

Rust Polars 0.53 exposes `Series::gather_every(&self, n: usize, offset: usize) -> PolarsResult<Series>`. The implementation validates that `n > 0`, then builds an index range from `offset` to the Series length with `step_by(n)`.

## Problem

`polars-hs` has eager `Series` `take` for explicit indexes and `slice` for contiguous windows, but it lacks the eager Polars helper for strided sampling. Users currently need to build an explicit index vector in Haskell for a common Polars operation.

## Questions and Answers

1. What is the public argument order?
   Answer: Use `seriesGatherEvery step offset input`, matching Polars parameter order and making validation messages clear.

2. Should `offset >= length` be rejected?
   Answer: Follow Polars and return an empty Series.

3. Where should overflow be checked?
   Answer: Validate non-negative values in Haskell and validate Rust `usize` and Polars `IdxSize` bounds in the ABI. Polars 0.53 casts offset through `IdxSize`, so the binding should reject offsets that would truncate.

## Design

Public Haskell API:

```haskell
seriesGatherEvery :: Int -> Int -> Series -> IO (Either PolarsError Series)
```

C ABI:

```c
int phs_series_gather_every(const struct phs_series *series,
                            uint64_t step,
                            uint64_t offset,
                            struct phs_series **out,
                            struct phs_error **err);
```

Rust implementation:

```rust
let step = usize_from_u64(step, "series gather every step")?;
let offset_idx = idx_size_from_u64(offset, "series gather every offset")?;
let offset = usize::try_from(offset_idx)?;
series.gather_every(step, offset)
```

## Implementation Plan

1. Add RED Rust FFI tests for strided numeric sampling, offset sampling with nulls, empty results for `offset >= len`, and invalid `step=0`.
2. Add RED Hspec tests for public API behavior and Haskell-side validation of zero/negative step and negative offset.
3. Add the Rust ABI function and C header declaration.
4. Add the raw Haskell FFI import and public wrapper.
5. Run focused and full verification.

## Examples

✅ Good:

```haskell
seriesGatherEvery 2 1 values
```

❌ Bad:

```haskell
seriesGatherEvery 0 1 values
```

## Trade-offs

- Keeping `step` and `offset` as `Int` matches the rest of the public row-count APIs.
- Rust-side `IdxSize` validation is stricter than upstream `gather_every`, but it prevents silent truncation on non-bigidx builds.

## Implementation Results

- Added `seriesGatherEvery :: Int -> Int -> Series -> IO (Either PolarsError Series)` to `Polars.Series`.
- Added `phs_series_gather_every` to the C header, raw Haskell FFI, and Rust adapter.
- Added Haskell validation for positive step and non-negative offset.
- Added Rust validation for `step=0`, `usize` conversion, and Polars `IdxSize` conversion for offset.
- Added Rust FFI and Hspec coverage for numeric stride, text/null offset selection, large step, empty offset output, and invalid step/offset inputs.
- RED verification:
  - `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml series_gather_every_returns_strided_values` failed on missing `phs_series_gather_every`.
  - `stack test --fast --test-arguments '--match "gathers every nth Series value with an offset"'` failed on missing `Pl.seriesGatherEvery`.
- GREEN verification:
  - Focused Rust FFI: 1/1 passing.
  - Focused Hspec: 1/1 passing.
  - Full Rust FFI: 122/122 passing.
  - Full Hspec: 169/169 passing.
  - HLint: no hints.

No deviations from the design plan.
