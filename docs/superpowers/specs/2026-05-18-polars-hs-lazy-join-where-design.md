# Lazy Join Where Design Log

## Background

Rust Polars 0.53 exposes non-equi lazy joins through
`JoinBuilder::join_where(Vec<Expr>)`. The planner accepts boolean predicates,
decomposes `and` expressions and `is_between` predicates, and can optimize the
plan through the `iejoin` feature.

## Problem

`polars-hs` currently supports equi joins only. Queries such as matching rows
where `cash > cost` or `event_time` falls between a start and end column need
cross join workarounds.

## Questions and Answers

Q: What should the public API be?

A: Add `joinWhere :: JoinWhereOptions -> [Expr] -> LazyFrame -> LazyFrame -> IO (Either PolarsError LazyFrame)`.

Q: Which options belong in the first batch?

A: Include suffix, `allow_parallel`, and `force_parallel`. Validation,
coalescing, and null-equality are keyed-join controls and are handled by the
extended equi join API.

Q: Should empty predicate lists be accepted?

A: Reject empty predicate lists at the Haskell and Rust boundaries. A non-equi
join with no predicates is a Cartesian product and should use `crossJoin`.

Q: Which Cargo feature is needed?

A: Add Polars feature `iejoin`. The existing feature set already includes
`is_between`, which lets Polars expand `is_between` predicates.

## Design

Public API:

```haskell
data JoinWhereOptions = JoinWhereOptions
    { joinWhereSuffix :: !(Maybe Text)
    , joinWhereAllowParallel :: !Bool
    , joinWhereForceParallel :: !Bool
    }

defaultJoinWhereOptions :: JoinWhereOptions
joinWhere :: JoinWhereOptions -> [Expr] -> LazyFrame -> LazyFrame -> IO (Either PolarsError LazyFrame)
```

C ABI:

```c
int phs_lazyframe_join_where(const struct phs_lazyframe *left,
                             const struct phs_lazyframe *right,
                             const struct phs_expr *const *predicates,
                             uintptr_t predicate_len,
                             const char *suffix,
                             bool allow_parallel,
                             bool force_parallel,
                             struct phs_lazyframe **out,
                             struct phs_error **err);
```

Validation rules:

- `predicates` must contain at least one expression.
- Null predicate array with positive length returns `InvalidArgument`.
- Null input or output handles return `InvalidArgument`.

## Implementation Plan

1. Add RED Rust ABI tests for `cash > cost`, `is_between`, empty predicates,
   and bad pointers.
2. Add RED Hspec tests over temporary CSV fixtures.
3. Enable `iejoin`, add Rust ABI, Raw import, Haskell options, and wrapper.
4. Run focused GREEN and full verification.

## Examples

```haskell
joinWhere
    defaultJoinWhereOptions
    [col "cash" .> col "cost"]
    customers
    offers
```

✅ Good pattern:

```haskell
joinWhere defaultJoinWhereOptions [col "cash" .> col "cost"] left right
```

❌ Problem pattern:

```haskell
crossJoin left right >>= filter (col "cash" .> col "cost")
```

The second form leaves the non-equi join optimization path implicit.

## Trade-offs

- The first API keeps predicates explicit and lets Polars decompose compound
  expressions.
- Distinct fixture column names avoid ambiguous expression resolution.
- A later asof batch will cover sorted nearest-key joins.

## Implementation Results

Implemented lazy non-equi join support.

Changed files:

- `docs/superpowers/specs/2026-05-18-polars-hs-lazy-join-where-design.md`
- `include/polars_hs.h`
- `rust/polars-hs-ffi/Cargo.toml`
- `rust/polars-hs-ffi/src/lazyframe.rs`
- `src/Polars/Internal/Raw.hs`
- `src/Polars/Join.hs`
- `test/Spec.hs`

Public Haskell API:

```haskell
data JoinWhereOptions = JoinWhereOptions
    { joinWhereSuffix :: !(Maybe Text)
    , joinWhereAllowParallel :: !Bool
    , joinWhereForceParallel :: !Bool
    }

defaultJoinWhereOptions :: JoinWhereOptions
joinWhere :: JoinWhereOptions -> [Expr] -> LazyFrame -> LazyFrame -> IO (Either PolarsError LazyFrame)
```

Rust ABI:

```c
int phs_lazyframe_join_where(const struct phs_lazyframe *left,
                             const struct phs_lazyframe *right,
                             const struct phs_expr *const *predicates,
                             uintptr_t predicate_len,
                             const char *suffix,
                             bool allow_parallel,
                             bool force_parallel,
                             struct phs_lazyframe **out,
                             struct phs_error **err);
```

Implementation notes:

- Enabled Polars feature `iejoin`.
- Rust uses `LazyFrame::join_builder().join_where(predicates)`.
- Haskell validates empty predicates before FFI.

Verification:

- RED Rust focused test failed on missing `phs_lazyframe_join_where`.
- RED Hspec focused test failed on missing `joinWhere` and
  `defaultJoinWhereOptions`.
- Focused Rust `lazy_join_where_filters_by_non_equi_predicates`: 1/1 passed.
- Focused Hspec `joins lazy frames with non-equi predicates`: 1/1 passed.
- `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml`: 165/165 passed.
- `stack test --fast`: 210/210 passed.
- `hlint src test app`: no hints.
- `jj diff --git --color=never | git apply --cached --check --whitespace=error -`: passed.

Deviation status:

- None.
