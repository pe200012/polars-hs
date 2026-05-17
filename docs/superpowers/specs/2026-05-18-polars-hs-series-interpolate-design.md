# Polars HS Series Interpolate Design

## Background

Rust Polars 0.53 exposes Series interpolation through `polars-ops`:

```rust
polars_ops::series::interpolate(&Series, InterpolationMethod)
```

The method enum has two variants:

```rust
InterpolationMethod::Linear
InterpolationMethod::Nearest
```

Docs and source checked:

- https://docs.rs/crate/polars-ops/latest/features
- `polars-ops-0.53.0/src/series/ops/interpolation/interpolate.rs`
- `polars-ops-0.53.0/src/series/ops/interpolation/mod.rs`
- `polars-ops-0.53.0/Cargo.toml`

## Problem

`polars-hs` now exposes eager Series arithmetic, diff, and numeric transforms,
but null interpolation is still available only through lazy/expression-style
usage. Direct Series interpolation is a small eager API addition that helps
time-series and sparse numeric workflows.

## Questions and Answers

Q: Should the method be a Haskell enum?

A: Yes. `SeriesInterpolateLinear` and `SeriesInterpolateNearest` mirror Polars
and avoid public raw integer codes.

Q: Should interpolation return `Series` for all dtypes?

A: Yes. Rust Polars returns a `Series` directly and clones unsupported dtypes in
some branches. The binding should preserve that behavior instead of adding
stricter Haskell-side dtype validation.

Q: Should linear interpolation over integer Series return Float64?

A: Yes. Rust Polars casts integer numeric values to Float64 for linear
interpolation.

## Design

Public API:

```haskell
data SeriesInterpolationMethod
    = SeriesInterpolateLinear
    | SeriesInterpolateNearest

seriesInterpolate :: SeriesInterpolationMethod -> Series -> IO (Either PolarsError Series)
```

C ABI:

```c
int phs_series_interpolate(const phs_series *series, int method, phs_series **out, phs_error **err);
```

Rust mapping:

```rust
interpolate(value, interpolation_method_from_code(method))
```

## Implementation Plan

1. Add RED Hspec tests for linear and nearest interpolation.
2. Add Haskell enum, export, and wrapper.
3. Add Raw.hs import and C header declaration.
4. Add `interpolate` feature to the direct `polars-ops` dependency.
5. Add Rust ABI function and Rust FFI tests.
6. Run focused Hspec and Rust tests.
7. Run full Rust, full Stack/Hspec, HLint, and `git diff --check`.
8. Append implementation results.

## Examples

Good:

```haskell
filled <- seriesInterpolate SeriesInterpolateLinear values
```

Good:

```haskell
nearest <- seriesInterpolate SeriesInterpolateNearest values
```

## Trade-offs

This batch covers plain Series interpolation. `interpolate_by` has extra
coordinate inputs and should use a separate API design.

## Implementation Results

Implemented eager Series interpolation:

```haskell
data SeriesInterpolationMethod
    = SeriesInterpolateLinear
    | SeriesInterpolateNearest

seriesInterpolate :: SeriesInterpolationMethod -> Series -> IO (Either PolarsError Series)
```

Changed files:

- `src/Polars/Series.hs`: exported `SeriesInterpolationMethod` and `seriesInterpolate`.
- `src/Polars/Internal/Raw.hs`: added safe FFI import for `phs_series_interpolate`.
- `include/polars_hs.h`: added C ABI declaration.
- `rust/polars-hs-ffi/Cargo.toml`: enabled direct `polars-ops/interpolate`.
- `rust/polars-hs-ffi/src/series.rs`: mapped method codes to `InterpolationMethod` and added dtype validation for nearest interpolation.
- `test/Spec.hs`: added Hspec coverage for linear, nearest, edge nulls, text clone behavior, and invalid nearest dtypes.

Observed behavior:

- Linear interpolation over `Word32` returns `Float64`.
- Linear interpolation preserves leading/trailing nulls.
- Linear interpolation over all-null `Word32` returns all-null `Float64`.
- Linear interpolation over text clones the text Series.
- Nearest interpolation over `Word32` returns `Word32`.
- Nearest interpolation over text and boolean returns `InvalidArgument`.
- Unknown Rust ABI method codes return `InvalidArgument`.

Deviation from initial design:

The initial design preserved all Rust Polars dtype behavior directly. A review
found that Polars 0.53 `InterpolationMethod::Nearest` panics for `String` and
`Boolean` through the physical downcast macro. The FFI now converts these cases,
plus `Null`, `BinaryOffset`, and `Unknown`, into stable `InvalidArgument`
errors before calling Polars.

Verification:

- RED Hspec failed on missing `seriesInterpolate`, `SeriesInterpolateLinear`, and `SeriesInterpolateNearest`.
- RED regression failed with `PanicError` for nearest text and boolean interpolation before the dtype guard.
- Focused Rust FFI: 3/3 passing.
- Focused Hspec: 1/1 passing.
- Full Rust FFI: 113/113 passing.
- Full Stack/Hspec: 160/160 passing.
- HLint: no hints.
- `git diff --check`: clean.
