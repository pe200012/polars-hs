# Polars HS Series Diff Design

## Background

Rust Polars 0.53 exposes Series difference through `polars-ops`:

```rust
polars_ops::series::diff(&Series, i64, NullBehavior)
```

The null behavior type is exported by `polars_core::series::ops` and available
through Polars prelude imports:

```rust
NullBehavior::Ignore
NullBehavior::Drop
```

Docs and source checked:

- https://docs.rs/crate/polars-ops/latest/features
- `polars-ops-0.53.0/src/series/ops/diff.rs`
- `polars-core-0.53.0/src/series/ops/mod.rs`
- `polars-ops-0.53.0/Cargo.toml`

## Problem

`polars-hs` has Series arithmetic, shift, and scalar stats, but eager users
still need a direct adjacent-difference operation. `Series.diff` is a compact
step toward time-series and window-style eager Series parity.

## Questions and Answers

Q: Should null behavior be a Haskell enum?

A: Yes. `SeriesDiffIgnore` and `SeriesDiffDrop` preserve the Rust options while
keeping public calls type-directed.

Q: Should negative periods be supported?

A: Yes. Rust Polars accepts negative `n` and computes forward differences, so
the binding should pass signed `Int64` through to Rust.

Q: Should oversized periods be rejected at the Haskell boundary?

A: No extra Haskell validation is needed for `Int64`. Rust receives the same
width as Polars and reports Polars errors for unsupported runtime cases.

## Design

Public API:

```haskell
data SeriesDiffNullBehavior
    = SeriesDiffIgnore
    | SeriesDiffDrop

seriesDiff :: Int64 -> SeriesDiffNullBehavior -> Series -> IO (Either PolarsError Series)
```

C ABI:

```c
int phs_series_diff(const phs_series *series, int64_t n, int null_behavior, phs_series **out, phs_error **err);
```

Rust mapping:

```rust
diff(value, n, null_behavior_from_code(code))
```

Flow:

```mermaid
flowchart LR
    H[seriesDiff n behavior] --> R[Raw FFI]
    R --> C[phs_series_diff]
    C --> P[polars_ops::series::diff]
    P --> S[Owned Series]
```

## Implementation Plan

1. Add RED Hspec tests for `Ignore`, `Drop`, negative period, unsigned cast, and text dtype error.
2. Add Haskell enum, export, and wrapper.
3. Add Raw.hs import and C header declaration.
4. Add `diff` feature to the direct `polars-ops` dependency.
5. Add Rust ABI function and Rust FFI tests.
6. Run focused Hspec and Rust tests.
7. Run full Rust, full Stack/Hspec, HLint, and `git diff --check`.
8. Append implementation results.

## Examples

Good:

```haskell
delta <- seriesDiff 1 SeriesDiffIgnore values
```

Good:

```haskell
forwardDelta <- seriesDiff (-1) SeriesDiffDrop values
```

## Trade-offs

This batch implements direct Series diff only. Percentage change and
interpolation use separate Polars features and should be added in their own
small batches with independent tests.

## Implementation Results

Implemented:

- `SeriesDiffNullBehavior`
- `seriesDiff`

Changed files:

- `src/Polars/Series.hs`
- `src/Polars/Internal/Raw.hs`
- `include/polars_hs.h`
- `rust/polars-hs-ffi/Cargo.toml`
- `rust/polars-hs-ffi/src/series.rs`
- `test/Spec.hs`

Semantics verified:

- `SeriesDiffIgnore` for `n = 1`, `n = -1`, `n = 0`, and `abs(n) > len`.
- `SeriesDiffDrop` for `n = 1` and `n = -1`.
- `SeriesDiffDrop` for `abs(n) > len` returns `InvalidArgument` at the FFI
  boundary, avoiding the upstream underflow path.
- Text dtype returns a Polars error.
- Unknown Rust ABI null behavior code returns `InvalidArgument`.
- Unsigned casts match Polars 0.53:
  - `UInt8 -> Int16`
  - `UInt16 -> Int32`
  - `UInt32 -> Int64`
  - `UInt64 -> Int64`
- `UInt64` values above `maxBound :: Int64` cast to null through Polars
  non-strict cast before diff.

Deviation:

- The design initially planned no extra Haskell validation. The implementation
  adds Rust FFI boundary validation for `SeriesDiffDrop` when `abs(n)` exceeds
  the Series length, returning `InvalidArgument` instead of allowing a panic.

Verification:

- RED Hspec failed on missing `seriesDiff`, `SeriesDiffIgnore`, and
  `SeriesDiffDrop`.
- Focused Rust FFI:
  `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml --release series_diff -- --nocapture`
  passed: 2/2.
- Focused Hspec:
  `PATH="$HOME/.ghcup/bin:$PATH" stack --system-ghc test --fast --test-arguments '--match "computes Series differences"'`
  passed: 1/1.
- Full Rust FFI:
  `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml --release`
  passed: 110/110.
- HLint:
  `PATH="$HOME/.ghcup/bin:$PATH" hlint src test`
  returned `No hints`.
- Whitespace:
  `git diff --check`
  returned no output.
- Full Stack/Hspec:
  `PATH="$HOME/.ghcup/bin:$PATH" stack --system-ghc test --fast`
  passed: 159/159.

Review follow-up:

- A read-only subagent recommended importing `NullBehavior` explicitly,
  guarding `Drop abs(n)>len`, and broadening unsigned cast tests. All three
  changes are included in this implementation.
