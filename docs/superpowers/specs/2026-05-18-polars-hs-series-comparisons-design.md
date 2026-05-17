# Design Log: Series Binary Comparisons

## Background

`polars-hs` already exposes eager Series arithmetic, predicates, `is_between`, and `search_sorted`.
Rust Polars 0.53 provides elementwise Series comparison through `ChunkCompareEq<&Series>` and
`ChunkCompareIneq<&Series>`. These methods return `PolarsResult<BooleanChunked>`, which fits the
existing Rust-owned `phs_series` ABI.

Relevant upstream methods:

- `equal`, `equal_missing`, `not_equal`, `not_equal_missing`
- `gt`, `gt_eq`, `lt`, `lt_eq`

## Problem

Haskell users can create comparison masks through lazy expressions, but eager Series comparison is still
missing from the public Series API. This blocks common eager workflows such as constructing a Boolean mask
before `seriesFilter` or DataFrame filtering.

## Questions and Answers

### Should comparison functions accept scalar Haskell values directly?

Answer: use `Series -> Series -> IO (Either PolarsError Series)` for this batch.
Rust Polars already treats length-one Series as scalar-style broadcast where supported, matching existing
`seriesIsBetween` tests and keeping the ABI small.

### Should `equal_missing` and `not_equal_missing` be separate functions?

Answer: expose them as separate functions. The null-equality behavior is semantically important and maps
directly to Polars methods.

### Should invalid opcodes be tested at Haskell level?

Answer: test public behavior in Haskell and opcode validation in Rust FFI tests. Public Haskell constructors
do not expose raw opcodes.

## Design

Add one Rust ABI function:

```c
int phs_series_compare_op(const struct phs_series *left,
                          const struct phs_series *right,
                          int op,
                          struct phs_series **out,
                          struct phs_error **err);
```

Opcode mapping:

| Code | Operation |
| ---- | --------- |
| 0 | `equal` |
| 1 | `not_equal` |
| 2 | `equal_missing` |
| 3 | `not_equal_missing` |
| 4 | `gt` |
| 5 | `gt_eq` |
| 6 | `lt` |
| 7 | `lt_eq` |

Public Haskell API:

```haskell
seriesEqual :: Series -> Series -> IO (Either PolarsError Series)
seriesNotEqual :: Series -> Series -> IO (Either PolarsError Series)
seriesEqualMissing :: Series -> Series -> IO (Either PolarsError Series)
seriesNotEqualMissing :: Series -> Series -> IO (Either PolarsError Series)
seriesGreater :: Series -> Series -> IO (Either PolarsError Series)
seriesGreaterEqual :: Series -> Series -> IO (Either PolarsError Series)
seriesLess :: Series -> Series -> IO (Either PolarsError Series)
seriesLessEqual :: Series -> Series -> IO (Either PolarsError Series)
```

The returned Series contains a Boolean mask with Polars null propagation. `equal_missing` treats matching
nulls as `true`; `not_equal_missing` treats matching nulls as `false`.

```mermaid
flowchart LR
    Haskell[Polars.Series wrapper] --> Raw[Raw safe FFI import]
    Raw --> Rust[phs_series_compare_op]
    Rust --> Polars[ChunkCompareEq / ChunkCompareIneq]
    Polars --> Mask[BooleanChunked -> Series]
```

## Implementation Plan

1. Add Rust FFI tests for all eight operations, scalar-style length-one broadcast, text comparison,
   length mismatch, dtype mismatch, and invalid opcode.
2. Add Hspec coverage for the public Haskell wrappers.
3. Add `phs_series_compare_op` to `rust/polars-hs-ffi/src/series.rs`.
4. Add the header declaration in `include/polars_hs.h`.
5. Add a `safe` Raw import and public wrappers in `src/Polars/Series.hs`.
6. Run focused RED/GREEN checks, then full Rust/Haskell verification.

## Examples

Good:

```haskell
maskResult <- seriesGreater values threshold
```

This keeps comparison semantics Polars-owned and returns a Boolean Series usable by `seriesFilter`.

Bad:

```haskell
maskResult <- fmap (fmap localCompare) (seriesInt64 values)
```

This round-trips data through Haskell, loses dtype breadth, and bypasses Polars comparison rules.

## Trade-offs

- A compact opcode ABI matches existing Series arithmetic and keeps the C surface small.
- Public named functions avoid exposing integer opcodes to Haskell users.
- Scalar-specific helpers can be added later with typed Haskell values after the base Series-to-Series
  comparison surface is stable.

## Implementation Results

Implemented as designed.

Files changed:

- `rust/polars-hs-ffi/src/series.rs`
- `include/polars_hs.h`
- `src/Polars/Internal/Raw.hs`
- `src/Polars/Series.hs`
- `test/Spec.hs`

Verification:

- RED Rust: `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml series_compare_ops_return_boolean_masks`
  failed because `phs_series_compare_op` was missing.
- RED Hspec: `stack test --fast --ta '--match=compares Series values into boolean masks'` failed because
  public `seriesEqual` and related wrappers were missing.
- Focused Rust: `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml series_compare_ops_return_boolean_masks`
  passed, 1/1.
- Focused Hspec: `stack test --fast --ta --match=compares` passed, 1/1.
- Full Rust: `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml` passed, 126/126.
- Full Haskell: `stack test --fast` passed, 173/173.
- HLint: `hlint src test app` returned `No hints`.
- Whitespace: `git diff --check` returned exit 0.

Deviations:

- None.
