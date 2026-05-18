# Lazy Join Extended Options Design Log

## Background

Rust Polars 0.53 exposes lazy equi join controls through `JoinArgs` and
`JoinBuilder`: validation, null key equality, key coalescing, result row order,
and parallel plan evaluation. The existing binding exposes join type, left and
right key expressions, and suffix only.

## Problem

Haskell lazy joins cannot configure common Polars join behavior:

- null keys matching each other;
- one-to-one and many-to-one validation;
- explicit join-key coalescing;
- result row order;
- `allow_parallel` and `force_parallel`.

## Questions and Answers

Q: Should the existing `JoinOptions` record gain fields?

A: Keep `JoinOptions` stable and add `ExtendedJoinOptions`. Existing callers can
continue using `joinWith`, `innerJoin`, `leftJoin`, and the other convenience
functions.

Q: What should the public entrypoint be?

A: Add `joinWithExtended :: ExtendedJoinOptions -> LazyFrame -> LazyFrame -> IO (Either PolarsError LazyFrame)`.

Q: Should the old ABI change?

A: Keep `phs_lazyframe_join` as the compatibility ABI and add
`phs_lazyframe_join_ex` for the new options. The old ABI can continue to map to
Polars defaults.

Q: Should build-side selection be exposed in this batch?

A: Defer build-side selection. Rust documents it as streaming-engine oriented,
and the immediate parity gap is covered by the stable JoinBuilder controls.

## Design

Public API:

```haskell
data JoinValidation
    = JoinManyToMany
    | JoinManyToOne
    | JoinOneToMany
    | JoinOneToOne

data JoinCoalesce
    = JoinCoalesceDefault
    | JoinCoalesceColumns
    | JoinKeepColumns

data JoinMaintainOrder
    = JoinMaintainOrderNone
    | JoinMaintainOrderLeft
    | JoinMaintainOrderRight
    | JoinMaintainOrderLeftRight
    | JoinMaintainOrderRightLeft

data ExtendedJoinOptions = ExtendedJoinOptions
    { extendedJoinBase :: !JoinOptions
    , extendedJoinValidation :: !JoinValidation
    , extendedJoinNullsEqual :: !Bool
    , extendedJoinCoalesce :: !JoinCoalesce
    , extendedJoinMaintainOrder :: !JoinMaintainOrder
    , extendedJoinAllowParallel :: !Bool
    , extendedJoinForceParallel :: !Bool
    }

defaultExtendedJoinOptions :: ExtendedJoinOptions
joinWithExtended :: ExtendedJoinOptions -> LazyFrame -> LazyFrame -> IO (Either PolarsError LazyFrame)
```

C ABI:

```c
int phs_lazyframe_join_ex(const struct phs_lazyframe *left,
                          const struct phs_lazyframe *right,
                          const struct phs_expr *const *left_on,
                          uintptr_t left_len,
                          const struct phs_expr *const *right_on,
                          uintptr_t right_len,
                          int join_type,
                          const char *suffix,
                          int validation,
                          bool nulls_equal,
                          int coalesce,
                          int maintain_order,
                          bool allow_parallel,
                          bool force_parallel,
                          struct phs_lazyframe **out,
                          struct phs_error **err);
```

Enum mappings:

- validation: `0` many-to-many, `1` many-to-one, `2` one-to-many, `3` one-to-one.
- coalesce: `0` JoinSpecific, `1` CoalesceColumns, `2` KeepColumns.
- maintain order: `0` none, `1` left, `2` right, `3` left-right, `4` right-left.

## Implementation Plan

1. Add RED Rust tests for null equality, keep-columns coalescing, maintain
   order, validation, and bad enum codes.
2. Add RED Hspec tests over temporary CSV fixtures.
3. Add `phs_lazyframe_join_ex`, Raw import, Haskell types, defaults, and
   wrapper.
4. Run focused GREEN and full verification.

## Examples

```haskell
joinWithExtended
    defaultExtendedJoinOptions
        { extendedJoinBase =
            defaultJoinOptions
                { joinType = JoinInner
                , leftOn = [col "key"]
                , rightOn = [col "key"]
                }
        , extendedJoinNullsEqual = True
        , extendedJoinCoalesce = JoinKeepColumns
        , extendedJoinMaintainOrder = JoinMaintainOrderLeft
        }
```

✅ Good pattern:

```haskell
joinWithExtended options left right
```

❌ Problem pattern:

```haskell
joinWith baseOptions left right
```

The second form uses Polars default controls.

## Trade-offs

- A new record avoids changing the existing `JoinOptions` constructor shape.
- Keeping `JoinOptions` nested reuses existing validation and convenience
  constructors.
- Build-side controls are a later streaming-focused batch.

## Implementation Results

Implemented extended lazy join options.

Changed files:

- `docs/superpowers/specs/2026-05-18-polars-hs-lazy-join-extended-options-design.md`
- `include/polars_hs.h`
- `rust/polars-hs-ffi/src/lazyframe.rs`
- `src/Polars/Internal/Raw.hs`
- `src/Polars/Join.hs`
- `test/Spec.hs`

Public Haskell API:

```haskell
data JoinValidation
data JoinCoalesce
data JoinMaintainOrder
data ExtendedJoinOptions
defaultExtendedJoinOptions :: ExtendedJoinOptions
joinWithExtended :: ExtendedJoinOptions -> LazyFrame -> LazyFrame -> IO (Either PolarsError LazyFrame)
```

Rust ABI:

```c
int phs_lazyframe_join_ex(const struct phs_lazyframe *left,
                          const struct phs_lazyframe *right,
                          const struct phs_expr *const *left_on,
                          uintptr_t left_len,
                          const struct phs_expr *const *right_on,
                          uintptr_t right_len,
                          int join_type,
                          const char *suffix,
                          int validation,
                          bool nulls_equal,
                          int coalesce,
                          int maintain_order,
                          bool allow_parallel,
                          bool force_parallel,
                          struct phs_lazyframe **out,
                          struct phs_error **err);
```

Implementation notes:

- The old `phs_lazyframe_join` remains available and maps to Polars defaults.
- Rust uses `LazyFrame::join_builder` for both old and extended lazy joins.
- Haskell keeps existing `JoinOptions` unchanged and nests it inside
  `ExtendedJoinOptions`.

Verification:

- RED Rust focused test failed on missing `phs_lazyframe_join_ex`.
- RED Hspec focused test failed on missing `defaultExtendedJoinOptions` and
  related public API.
- Focused Rust extended join test: 1/1 passed.
- Focused Hspec extended join test: 1/1 passed.
- `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml`: 164/164 passed.
- `stack test --fast`: 209/209 passed.
- `hlint src test app`: no hints.
- `jj diff --git --color=never | git apply --cached --check --whitespace=error -`: passed.

Deviation status:

- None.
