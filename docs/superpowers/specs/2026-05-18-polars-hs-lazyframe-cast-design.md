# LazyFrame Cast Design Log

## Background

Rust Polars 0.53 exposes lazy frame casting through:

```rust
pub fn cast(self, dtypes: PlHashMap<&str, DataType>, strict: bool) -> Self
pub fn cast_all(self, dtype: impl Into<DataTypeExpr>, strict: bool) -> Self
```

`polars-hs` currently exposes expression casts and Series casts, but no direct
LazyFrame-level cast helpers.

## Problem

Haskell callers can cast selected expressions, yet they cannot apply Polars'
frame-level lazy casts to named columns or all columns. That leaves a direct
LazyFrame transform absent from the binding.

## Questions and Answers

Q: Should the Haskell API use the upstream name `cast`?

A: Use `castColumns` and `castAllColumns`. `Polars.Expr` already exports
`cast`, and the top-level `Polars` module re-exports both modules.

Q: Which datatypes should this batch support?

A: Reuse the existing Haskell `DataType` code mapping used by expression casts.
Unsupported/unknown Haskell datatypes fail at the Haskell boundary; direct Rust
ABI tests cover unknown numeric dtype codes.

Q: Should empty named casts be rejected?

A: No. Rust `LazyFrame::cast` treats an empty map as a no-op, so the binding
preserves that behavior.

## Design

Public API:

```haskell
castColumns :: [(Text, DataType)] -> Bool -> LazyFrame -> IO (Either PolarsError LazyFrame)
castAllColumns :: DataType -> Bool -> LazyFrame -> IO (Either PolarsError LazyFrame)
```

The `Bool` argument is `strict`: `True` uses strict casting, `False` uses
non-strict casting.

C ABI:

```c
int phs_lazyframe_cast(const struct phs_lazyframe *lazyframe,
                       const char *const *names,
                       const int *dtypes,
                       uintptr_t len,
                       bool strict,
                       struct phs_lazyframe **out,
                       struct phs_error **err);

int phs_lazyframe_cast_all(const struct phs_lazyframe *lazyframe,
                           int dtype,
                           bool strict,
                           struct phs_lazyframe **out,
                           struct phs_error **err);
```

Validation rules:

- Null lazyframe pointer returns `InvalidArgument`.
- Null output pointer returns `InvalidArgument`.
- Null names pointer with positive length returns `InvalidArgument`.
- Null dtype pointer with positive length returns `InvalidArgument`.
- Unknown dtype code returns `InvalidArgument`.

## Implementation Plan

1. Add RED Rust ABI tests for named cast, cast-all, empty named cast no-op,
   unknown dtype code, null names pointer, null dtype pointer, null lazyframe,
   and null output pointer.
2. Add RED Hspec tests for `castColumns` and `castAllColumns` over `people.csv`.
3. Export `dtypeCode` from `Polars.Internal.Expr` for wrapper reuse.
4. Add header declarations, Rust ABI functions, Raw imports, Haskell wrappers,
   and tests.
5. Run focused GREEN and full verification.

## Examples

```haskell
casted <- castColumns [("age", Float64)] True lf
allText <- castAllColumns Utf8 False lf
```

Good pattern:

```haskell
castColumns [("age", Float64)] True lf
```

Bad pattern:

```haskell
withColumns [strictCast Float64 (col "age")] lf
```

## Trade-offs

- Named wrappers avoid public export ambiguity.
- The ABI stays compact with dtype integer codes while Haskell exposes
  `DataType`.
- Parameterized temporal dtypes remain limited by the current `DataType`
  representation.

## Implementation Results

Implemented LazyFrame named-column casts and all-column casts.

Changed files:

- `docs/superpowers/specs/2026-05-18-polars-hs-lazyframe-cast-design.md`
- `include/polars_hs.h`
- `rust/polars-hs-ffi/src/lazyframe.rs`
- `src/Polars/Internal/Expr.hs`
- `src/Polars/Internal/Raw.hs`
- `src/Polars/LazyFrame.hs`
- `test/Spec.hs`

Public Haskell API:

```haskell
castColumns :: [(Text, DataType)] -> Bool -> LazyFrame -> IO (Either PolarsError LazyFrame)
castAllColumns :: DataType -> Bool -> LazyFrame -> IO (Either PolarsError LazyFrame)
```

Rust ABI:

```c
int phs_lazyframe_cast(const struct phs_lazyframe *lazyframe,
                       const char *const *names,
                       const int *dtypes,
                       uintptr_t len,
                       bool strict,
                       struct phs_lazyframe **out,
                       struct phs_error **err);

int phs_lazyframe_cast_all(const struct phs_lazyframe *lazyframe,
                           int dtype,
                           bool strict,
                           struct phs_lazyframe **out,
                           struct phs_error **err);
```

Implementation notes:

- Rust uses `LazyFrame::cast` and `LazyFrame::cast_all`.
- Haskell reuses `dtypeCode` from `Polars.Internal.Expr`; it is exported from
  that internal module for wrapper sharing.
- Empty named cast lists are accepted and behave as a no-op, matching upstream.

Verification:

- Focused Rust cast test: 1/1 passed.
- Focused Hspec cast test: 1/1 passed.
- `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml`: 157/157 passed.
- `stack test --fast`: 204/204 passed.
- `hlint src test app`: no hints.
- `jj diff --git --color=never | git apply --cached --check --whitespace=error -`: passed.

Deviation notes:

- Public names are `castColumns` and `castAllColumns` to avoid colliding with
  expression-level `cast` in the aggregate `Polars` module.
- Temporal unit/timezone selection remains constrained by the existing
  non-parameterized `DataType` constructors.
