# Series Pct-Change Design

## Background

Rust Polars 0.53 exposes eager percentage change through `polars_ops::series::pct_change(s: &Series, n: &Series) -> PolarsResult<Series>` behind the `pct_change` feature. The function casts non-float input to `Float64`, keeps `Float32` and `Float64` input dtypes, casts `n` to `Int64`, then computes `diff(s, n, NullBehavior::Ignore) / s.shift(n)`.

## Problem

`polars-hs` already exposes eager Series arithmetic, diff, interpolation, ranking, and strided sampling, but it lacks a direct pct-change helper. Users calculating returns or relative deltas currently need to manually compose shift, subtraction, and division once those pieces exist at the right level.

## Questions and Answers

1. Should the Haskell API accept a scalar period or a Series period?
   Answer: Use a scalar period first. The upstream function requires a single-value `n` Series, so Haskell can expose the natural single-period API and keep the ABI small.

2. Should `n=0` and negative periods be rejected?
   Answer: Follow Polars. `n=0` computes `(s - s) / s`, preserving IEEE `NaN` for zero denominators. Negative `n` compares against future rows.

3. What integer type should represent the period?
   Answer: Use `Int64`, matching the upstream cast target and avoiding silent truncation for normal Haskell `Int` boundaries.

## Design

Public Haskell API:

```haskell
seriesPctChange :: Int64 -> Series -> IO (Either PolarsError Series)
```

C ABI:

```c
int phs_series_pct_change(const struct phs_series *series,
                          int64_t periods,
                          struct phs_series **out,
                          struct phs_error **err);
```

Rust implementation:

```rust
let periods = Series::new(PlSmallStr::EMPTY, [periods]);
polars_series_pct_change(value, &periods)
```

Feature flags:

- `polars`: add `pct_change`
- `polars-ops`: add `pct_change`

## Implementation Plan

1. Add RED Rust FFI tests covering `n=1`, `n=2`, `n=-1`, `n=0`, zero denominator IEEE values, null propagation, `Float32` dtype preservation, and text cast behavior.
2. Add RED Hspec tests for the same public behavior.
3. Enable Cargo features.
4. Add Rust ABI function, C header declaration, raw Haskell FFI import, and public Haskell wrapper.
5. Run focused tests, then full Rust/Hspec/HLint/diff verification.

## Examples

✅ Good:

```haskell
seriesPctChange 1 prices
```

✅ Good:

```haskell
seriesPctChange (-1) prices
```

## Trade-offs

- A scalar-period API matches the common Series use case and can later be extended with a Series-period variant if needed.
- Preserving Polars `n=0` and negative-period behavior keeps parity with Rust Polars, including IEEE floating-point edge cases.

## Implementation Results

- Added `seriesPctChange :: Int64 -> Series -> IO (Either PolarsError Series)` to `Polars.Series`.
- Enabled `pct_change` in both `polars` and `polars-ops` Cargo features.
- Added `phs_series_pct_change` to the C header, raw Haskell FFI, and Rust adapter.
- Rust implementation constructs the required single-value period Series and delegates to `polars_ops::series::pct_change`.
- Added Rust FFI and Hspec coverage for `n=1`, `n=2`, `n=-1`, `n=0`, zero-denominator `NaN`/`Infinity`, nullable data, `Float32` dtype preservation, numeric text casts, and invalid text casts.
- RED verification:
  - `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml series_pct_change_returns_relative_change` failed on missing `phs_series_pct_change`.
  - `stack test --fast --test-arguments '--match "computes Series percentage changes with Polars semantics"'` failed on missing `Pl.seriesPctChange`.
- GREEN verification:
  - Focused Rust FFI: 1/1 passing.
  - Focused Hspec: 1/1 passing.
  - Full Rust FFI: 123/123 passing.
  - Full Hspec: 170/170 passing.
  - HLint: no hints.
  - `git diff --check`: clean.

No deviations from the design plan.
