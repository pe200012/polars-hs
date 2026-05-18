# LazyFrame Sequential Projection Design Log

## Background

Rust Polars 0.53 exposes sequential projection and mutation helpers:

```rust
pub fn select_seq<E: AsRef<[Expr]>>(self, exprs: E) -> Self
pub fn with_column(self, expr: Expr) -> LazyFrame
pub fn with_columns_seq<E: AsRef<[Expr]>>(self, exprs: E) -> LazyFrame
```

The binding already exposes `select` and `withColumns`, which use the parallel
projection paths.

## Problem

Haskell callers can select and add multiple columns, yet they cannot choose the
sequential Polars evaluation path or use the single-expression `with_column`
method directly.

## Questions and Answers

Q: What should the public names be?

A: Use `selectSeq`, `withColumn`, and `withColumnsSeq`. These names follow the
upstream method names while staying idiomatic in camelCase.

Q: Should empty expression lists be rejected?

A: Preserve current `select` and `withColumns` behavior. Let Polars decide plan
validity and error timing.

Q: Does this need new expression transport?

A: No. Reuse the existing expression array transport and single-expression
helper.

## Design

Public API:

```haskell
selectSeq :: [Expr] -> LazyFrame -> IO (Either PolarsError LazyFrame)
withColumn :: Expr -> LazyFrame -> IO (Either PolarsError LazyFrame)
withColumnsSeq :: [Expr] -> LazyFrame -> IO (Either PolarsError LazyFrame)
```

C ABI:

```c
int phs_lazyframe_select_seq(const struct phs_lazyframe *lazyframe,
                             const struct phs_expr *const *exprs,
                             uintptr_t len,
                             struct phs_lazyframe **out,
                             struct phs_error **err);

int phs_lazyframe_with_column(const struct phs_lazyframe *lazyframe,
                              const struct phs_expr *expr,
                              struct phs_lazyframe **out,
                              struct phs_error **err);

int phs_lazyframe_with_columns_seq(const struct phs_lazyframe *lazyframe,
                                   const struct phs_expr *const *exprs,
                                   uintptr_t len,
                                   struct phs_lazyframe **out,
                                   struct phs_error **err);
```

Validation rules:

- Null lazyframe pointer returns `InvalidArgument`.
- Null expression array with positive length returns `InvalidArgument`.
- Null expression pointer returns `InvalidArgument`.
- Null output pointer returns `InvalidArgument`.

## Implementation Plan

1. Add RED Rust ABI tests for `select_seq`, `with_column`,
   `with_columns_seq`, and null pointer validation.
2. Add RED Hspec tests over `values.csv` for sequential select, single-column
   addition, and sequential multi-column addition.
3. Add header declarations, Rust ABI functions, Raw imports, public Haskell
   exports, and wrappers.
4. Run focused GREEN and full verification.

## Examples

```haskell
selected <- selectSeq [col "name", alias "age_plus" (col "age" .+ litInt 1)] lf
extended <- withColumn (alias "age_plus" (col "age" .+ litInt 1)) lf
extendedSeq <- withColumnsSeq [alias "score_plus" (col "score" .+ litDouble 1.0)] lf
```

Good pattern:

```haskell
withColumn (alias "age_plus" (col "age" .+ litInt 1)) lf
```

Bad pattern:

```haskell
withColumns [alias "age_plus" (col "age" .+ litInt 1)] lf
```

## Trade-offs

- Keeping separate public functions mirrors upstream controls.
- The ABI remains compact by reusing expression arrays.
- Sequential execution is a planner/evaluation preference; result tests cover
  public behavior while plan internals stay delegated to Polars.

## Implementation Results

Implemented sequential projection and single-column lazy mutation helpers.

Changed files:

- `docs/superpowers/specs/2026-05-18-polars-hs-lazyframe-sequential-projection-design.md`
- `include/polars_hs.h`
- `rust/polars-hs-ffi/src/lazyframe.rs`
- `src/Polars/Internal/Raw.hs`
- `src/Polars/LazyFrame.hs`
- `test/Spec.hs`

Public Haskell API:

```haskell
selectSeq :: [Expr] -> LazyFrame -> IO (Either PolarsError LazyFrame)
withColumn :: Expr -> LazyFrame -> IO (Either PolarsError LazyFrame)
withColumnsSeq :: [Expr] -> LazyFrame -> IO (Either PolarsError LazyFrame)
```

Rust ABI:

```c
int phs_lazyframe_select_seq(const struct phs_lazyframe *lazyframe,
                             const struct phs_expr *const *exprs,
                             uintptr_t len,
                             struct phs_lazyframe **out,
                             struct phs_error **err);

int phs_lazyframe_with_column(const struct phs_lazyframe *lazyframe,
                              const struct phs_expr *expr,
                              struct phs_lazyframe **out,
                              struct phs_error **err);

int phs_lazyframe_with_columns_seq(const struct phs_lazyframe *lazyframe,
                                   const struct phs_expr *const *exprs,
                                   uintptr_t len,
                                   struct phs_lazyframe **out,
                                   struct phs_error **err);
```

Implementation notes:

- Rust uses `LazyFrame::select_seq`, `LazyFrame::with_column`, and
  `LazyFrame::with_columns_seq`.
- Haskell reuses `withCompiledExprs` for list APIs and `lazyFrameExprOut` for
  the single-expression API.

Verification:

- RED Rust focused test failed on missing sequential projection ABI functions.
- RED Hspec focused test failed on missing public `Pl.selectSeq`,
  `Pl.withColumn`, and `Pl.withColumnsSeq`.
- Focused Rust sequential projection test: 1/1 passed.
- Focused Hspec sequential projection test: 1/1 passed.
- `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml`: 162/162 passed.
- `stack test --fast`: 207/207 passed.
- `hlint src test app`: no hints.
- `jj diff --git --color=never | git apply --cached --check --whitespace=error -`: passed.

Deviations:

- No deviations from the design.
