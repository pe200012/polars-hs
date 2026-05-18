# LazyFrame Count Design Log

## Background

Rust Polars 0.53 exposes a lazy frame count aggregation through:

```rust
pub fn count(self) -> LazyFrame
```

The method returns the number of non-null elements for each column. The binding
already exposes the adjacent `nullCount` lazy aggregation.

## Problem

Haskell callers can aggregate null counts for lazy frames, yet the direct
non-null count aggregation is absent. This leaves a small LazyFrame parity gap
beside `nullCount`.

## Questions and Answers

Q: Should the public Haskell name be `count`?

A: Yes. There is no current public `count` export, and the name matches the
upstream LazyFrame method.

Q: What datatype should tests expect?

A: Upstream Polars returns unsigned 32-bit count columns in the current pinned
configuration, matching the existing `nullCount` tests that extract `Word32`.

Q: Does the ABI need extra parameters?

A: No. It has the same shape as `phs_lazyframe_null_count`: input LazyFrame,
output LazyFrame, error pointer.

## Design

Public API:

```haskell
count :: LazyFrame -> IO (Either PolarsError LazyFrame)
```

C ABI:

```c
int phs_lazyframe_count(const struct phs_lazyframe *lazyframe,
                        struct phs_lazyframe **out,
                        struct phs_error **err);
```

Validation rules:

- Null lazyframe pointer returns `InvalidArgument`.
- Null output pointer returns `InvalidArgument`.

## Implementation Plan

1. Add RED Rust ABI tests for non-null column counts, null lazyframe pointer,
   and null output pointer.
2. Add RED Hspec coverage next to `nullCount` using `values.csv`.
3. Add header declaration, Rust ABI function, Raw import, public Haskell export,
   and wrapper.
4. Run focused GREEN and full verification.

## Examples

```haskell
counts <- count lf
```

Good pattern:

```haskell
Right countsLf <- count lf
```

Bad pattern:

```haskell
Right countsLf <- select [nUnique (col "age")] lf
```

## Trade-offs

- The short name mirrors upstream and keeps the API natural.
- Future expression-level count helpers can use a distinct name if needed.
- The return dtype follows current Polars behavior and can change only with a
  pinned upstream version bump.

## Implementation Results

Implemented LazyFrame non-null count aggregation.

Changed files:

- `docs/superpowers/specs/2026-05-18-polars-hs-lazyframe-count-design.md`
- `include/polars_hs.h`
- `rust/polars-hs-ffi/src/lazyframe.rs`
- `src/Polars/Internal/Raw.hs`
- `src/Polars/LazyFrame.hs`
- `test/Spec.hs`

Public Haskell API:

```haskell
count :: LazyFrame -> IO (Either PolarsError LazyFrame)
```

Rust ABI:

```c
int phs_lazyframe_count(const struct phs_lazyframe *lazyframe,
                        struct phs_lazyframe **out,
                        struct phs_error **err);
```

Implementation notes:

- Rust uses `LazyFrame::count`.
- Haskell exports `count` from `Polars.LazyFrame` and the aggregate `Polars`
  module.
- Internal top/bottom-k parameter names were adjusted to avoid shadowing the new
  public `count` binding.

Verification:

- RED Rust focused test failed on missing `phs_lazyframe_count`.
- RED Hspec focused test failed on missing public `Pl.count`.
- Focused Rust count test: 1/1 passed.
- Focused Hspec lazy count/null-count/unique test: 1/1 passed.
- `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml`: 159/159 passed.
- `stack test --fast`: 204/204 passed.
- `hlint src test app`: no hints.
- `jj diff --git --color=never | git apply --cached --check --whitespace=error -`: passed.

Deviation notes:

- No public naming deviation. The pre-existing expression aggregate remains
  `count_`.
