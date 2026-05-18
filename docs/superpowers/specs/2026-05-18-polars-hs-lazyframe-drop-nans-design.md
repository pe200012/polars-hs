# LazyFrame Drop NaNs Design Log

## Background

Rust Polars 0.53 exposes lazy NaN row removal through:

```rust
pub fn drop_nans(self, subset: Option<Selector>) -> LazyFrame
```

The current binding already exposes `dropNulls` and `fillNans`, so the Haskell
surface has the adjacent null and NaN operations.

## Problem

Haskell callers can fill NaN values in a lazy frame, yet they cannot remove rows
that contain NaN values through the direct Polars lazy transform. This leaves a
small LazyFrame parity gap next to `dropNulls`.

## Questions and Answers

Q: Should the Haskell API mirror `dropNulls`?

A: Yes. Use `dropNans :: Maybe [Text] -> LazyFrame -> IO (Either PolarsError LazyFrame)`.
`Nothing` checks all floating point columns. `Just names` restricts the NaN scan
to those columns.

Q: How should empty subsets behave?

A: Match `dropNulls`: reject `Just []` at the Haskell boundary with
`InvalidArgument "dropNans subset requires at least one column name"`.

Q: Does this need a new selector ABI?

A: No. Reuse the existing Rust `optional_selector` helper and pass the selector
to `LazyFrame::drop_nans`.

## Design

Public API:

```haskell
dropNans :: Maybe [Text] -> LazyFrame -> IO (Either PolarsError LazyFrame)
```

C ABI:

```c
int phs_lazyframe_drop_nans(const struct phs_lazyframe *lazyframe,
                            const char *const *names,
                            uintptr_t len,
                            bool has_subset,
                            struct phs_lazyframe **out,
                            struct phs_error **err);
```

Validation rules:

- Null lazyframe pointer returns `InvalidArgument`.
- Null output pointer returns `InvalidArgument`.
- Null names pointer with positive length returns `InvalidArgument`.
- Empty subset with `has_subset = true` returns `InvalidArgument`.

## Implementation Plan

1. Add RED Rust ABI tests for all-column drop, subset drop, null lazyframe, null
   output pointer, null names pointer, and empty subset validation.
2. Add RED Hspec tests for `dropNans Nothing`, `dropNans (Just ["value"])`,
   and empty subset validation.
3. Add the C header declaration, Rust ABI function, Raw import, public Haskell
   export, and wrapper.
4. Run focused GREEN and full verification.

## Examples

```haskell
cleanAllFloats <- dropNans Nothing lf
cleanValueOnly <- dropNans (Just ["value"]) lf
```

Good pattern:

```haskell
dropNans (Just ["sensor_reading"]) lf
```

Bad pattern:

```haskell
filter (isNotNan (col "sensor_reading")) lf
```

## Trade-offs

- The API follows `dropNulls`, keeping lazy row-removal helpers consistent.
- Selector coverage stays column-name based through the current ABI helper.
- Rich selector expressions can be added later as a dedicated selector design.

## Implementation Results

Implemented LazyFrame NaN row dropping.

Changed files:

- `docs/superpowers/specs/2026-05-18-polars-hs-lazyframe-drop-nans-design.md`
- `include/polars_hs.h`
- `rust/polars-hs-ffi/src/lazyframe.rs`
- `src/Polars/Internal/Raw.hs`
- `src/Polars/LazyFrame.hs`
- `test/Spec.hs`

Public Haskell API:

```haskell
dropNans :: Maybe [Text] -> LazyFrame -> IO (Either PolarsError LazyFrame)
```

Rust ABI:

```c
int phs_lazyframe_drop_nans(const struct phs_lazyframe *lazyframe,
                            const char *const *names,
                            uintptr_t len,
                            bool has_subset,
                            struct phs_lazyframe **out,
                            struct phs_error **err);
```

Implementation notes:

- Rust uses `LazyFrame::drop_nans`.
- Haskell mirrors `dropNulls` and rejects `Just []` before crossing FFI.
- Rust ABI validation covers null lazyframe, null output pointer, null names
  pointer with positive length, and empty subset.

Verification:

- RED Rust focused test failed on missing `phs_lazyframe_drop_nans`.
- RED Hspec focused test failed on missing public `Pl.dropNans`.
- Focused Rust drop-nans test: 1/1 passed.
- Focused Hspec drop/fill NaN test: 1/1 passed.
- Focused Hspec validation test: 1/1 passed.
- `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml`: 158/158 passed.
- `stack test --fast`: 204/204 passed.
- `hlint src test app`: no hints.
- `jj diff --git --color=never | git apply --cached --check --whitespace=error -`: passed.

Deviation notes:

- The Haskell-facing empty-subset message uses `dropNans`; the raw ABI message
  uses Rust-style `drop_nans subset`, matching existing raw selector labels.
