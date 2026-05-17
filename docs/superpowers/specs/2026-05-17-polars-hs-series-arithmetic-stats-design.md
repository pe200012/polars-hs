# Polars HS Series Arithmetic And Stats Design

## Background

`polars-hs` exposes Series handles, typed extraction, casts, sorting, unique,
filtering, fill-null, append, and shift. Eager Series arithmetic and scalar
statistics are still absent from the public API.

Rust Polars 0.53 exposes standard arithmetic for `&Series`:

```rust
&left + &right
&left - &right
&left * &right
&left / &right
&left % &right
```

It also exposes Series scalar statistics:

```rust
series.mean()
series.std(ddof)
series.var(ddof)
```

`mean`, `std`, and `var` return `Option<f64>`.

## Problem

Users need eager Series arithmetic without constructing lazy expressions, and
they need basic scalar statistics for immediate Haskell control flow. The API
must preserve Polars' dtype coercion and null propagation while keeping Haskell
validation for `ddof` explicit.

## Questions and Answers

Q: Should arithmetic be a single opcode ABI or separate ABI functions?

A: Use one opcode ABI function. The public Haskell functions stay named and
typed, while the Rust boundary remains compact.

Q: Should stats return `Maybe Double`?

A: Yes. Polars returns `Option<f64>` for these methods, and `Maybe Double`
preserves all-null or unsupported cases without inventing sentinel values.

Q: Should this batch include `sum`, `min`, and `max`?

A: Keep them for a later batch. `sum_reduce`, `min_reduce`, and `max_reduce`
involve Polars `Scalar` conversion and all-null semantics that deserve a
dedicated ABI decision.

Q: How should `ddof` be represented?

A: Accept Haskell `Int` and validate `0 <= ddof <= 255` before crossing the
FFI boundary, because Rust Polars expects `u8`.

## Design

Public API:

```haskell
seriesAdd :: Series -> Series -> IO (Either PolarsError Series)
seriesSub :: Series -> Series -> IO (Either PolarsError Series)
seriesMul :: Series -> Series -> IO (Either PolarsError Series)
seriesDiv :: Series -> Series -> IO (Either PolarsError Series)
seriesRem :: Series -> Series -> IO (Either PolarsError Series)

seriesMean :: Series -> IO (Either PolarsError (Maybe Double))
seriesStd :: Int -> Series -> IO (Either PolarsError (Maybe Double))
seriesVar :: Int -> Series -> IO (Either PolarsError (Maybe Double))
```

FFI:

```c
int phs_series_binary_op(const phs_series *left,
                         const phs_series *right,
                         int op,
                         phs_series **out,
                         phs_error **err);

int phs_series_stat(const phs_series *series,
                    int op,
                    uint8_t ddof,
                    bool *has_value_out,
                    double *value_out,
                    phs_error **err);
```

Binary op codes:

- `0`: add
- `1`: subtract
- `2`: multiply
- `3`: divide
- `4`: remainder

Stat op codes:

- `0`: mean
- `1`: standard deviation
- `2`: variance

## Implementation Plan

1. Add RED Hspec tests for Double Series arithmetic with null propagation.
2. Add RED Hspec tests for arithmetic errors from mismatched lengths or invalid
   dtypes.
3. Add RED Hspec tests for `mean`, `std`, and `var` on nullable Double Series.
4. Add RED Hspec tests for all-null stats and invalid `ddof`.
5. Add Haskell exports, wrappers, `ddof` validation, and `Maybe Double` FFI
   helper.
6. Add Raw.hs imports and C header declarations.
7. Add Rust opcode decoding for binary ops and stats.
8. Run focused tests, Rust release tests, full Stack/Hspec, HLint, and
   `git diff --check`.
9. Append implementation results to this design log.

## Examples

Good:

```haskell
ratio <- Pl.seriesDiv numerator denominator
meanScore <- Pl.seriesMean score
sampleStd <- Pl.seriesStd 1 score
```

Bad:

```haskell
Pl.seriesStd (-1) score
```

## Trade-offs

Borrowed Rust `&Series` arithmetic is used for all five operations. This keeps
the implementation uniform and lets Polars decide dtype coercion and error
behavior. Owned in-place optimization APIs can be considered later after the
public semantics are covered.

## Implementation Results

Implemented files:

- `src/Polars/Series.hs`
- `src/Polars/Internal/Series.hs`
- `src/Polars/Internal/Raw.hs`
- `include/polars_hs.h`
- `rust/polars-hs-ffi/src/series.rs`
- `test/Spec.hs`

Delivered API:

```haskell
seriesAdd :: Series -> Series -> IO (Either PolarsError Series)
seriesSub :: Series -> Series -> IO (Either PolarsError Series)
seriesMul :: Series -> Series -> IO (Either PolarsError Series)
seriesDiv :: Series -> Series -> IO (Either PolarsError Series)
seriesRem :: Series -> Series -> IO (Either PolarsError Series)

seriesMean :: Series -> IO (Either PolarsError (Maybe Double))
seriesStd :: Int -> Series -> IO (Either PolarsError (Maybe Double))
seriesVar :: Int -> Series -> IO (Either PolarsError (Maybe Double))
```

Rust mappings:

- `phs_series_binary_op` maps op codes `0..4` to borrowed `Add`, `Sub`, `Mul`,
  `Div`, and `Rem` over `&Series`.
- `phs_series_stat` maps op codes `0..2` to `mean`, `std(ddof)`, and
  `var(ddof)`.
- Stats cross the ABI as `bool has_value_out` plus `double value_out`, preserving
  `Option<f64>` as `Maybe Double`.

Validation:

- Haskell validates `seriesStd` and `seriesVar` `ddof` values are in
  `[0, 255]`.
- Invalid arithmetic operands and unknown Rust opcode paths surface as typed
  errors through the existing FFI error channel.

Tests added:

- Double Series add/sub/mul/div/rem with null propagation.
- Polars error paths for mismatched arithmetic lengths and invalid dtypes.
- `seriesMean`, `seriesStd 1`, and `seriesVar 1` over nullable scores.
- all-null statistics returning `Nothing`.
- negative and overflowing `ddof` validation.

Verification:

- RED: focused Hspec failed on missing `seriesAdd`, `seriesSub`, `seriesMul`,
  `seriesDiv`, `seriesRem`, `seriesMean`, `seriesStd`, and `seriesVar` exports.
- Focused Hspec: 4 examples, 0 failures.
- Rust release tests: 87 passed, 0 failed.
- Full Stack/Hspec: 131 examples, 0 failures.
- HLint: no hints.
- `git diff --check`: clean.

Deviations:

- `seriesMaybeDoubleOut` was added to `Polars.Internal.Series` to keep the
  nullable scalar output pattern centralized with existing Series result
  helpers.
