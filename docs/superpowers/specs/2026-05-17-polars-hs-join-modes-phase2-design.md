# Join Modes Phase 2 Design

## Background

`polars-hs` currently supports lazy inner, left, right, and full joins through
`Polars.Join`. Task 4 expands the same `phs_lazyframe_join` ABI family to cover
semi, anti, and cross joins from Rust Polars 0.53.

Upstream references:

- <https://docs.rs/polars/latest/polars/prelude/enum.JoinType.html>
- <https://docs.rs/polars/latest/polars/prelude/struct.LazyFrame.html>

Rust Polars 0.53 gates semi and anti joins behind the `semi_anti_join` feature.
Cross joins require the `cross_join` feature on the lazy API.

## Problem

Users need the common remaining join modes without dropping to Rust or manually
encoding join type integers. Semi and anti joins keep rows from the left frame.
Cross joins produce the Cartesian product and take zero join keys.

## Questions and Answers

Q: Should the existing ABI change?

A: Keep `phs_lazyframe_join` and add join type codes. The existing arrays can
represent keyed joins, and cross joins use empty arrays.

Q: How should cross joins validate keys?

A: `JoinCross` requires empty `leftOn` and `rightOn` lists. The convenience
`crossJoin` supplies empty keys. A keyed cross join returns `InvalidArgument`.

Q: Which Cargo features are required?

A: Add `semi_anti_join` and `cross_join` to the Polars dependency feature list
in `rust/polars-hs-ffi/Cargo.toml`.

## Design

Public API additions:

```haskell
data JoinType
    = JoinInner
    | JoinLeft
    | JoinRight
    | JoinFull
    | JoinSemi
    | JoinAnti
    | JoinCross

semiJoin :: [Expr] -> [Expr] -> LazyFrame -> LazyFrame -> IO (Either PolarsError LazyFrame)
antiJoin :: [Expr] -> [Expr] -> LazyFrame -> LazyFrame -> IO (Either PolarsError LazyFrame)
crossJoin :: LazyFrame -> LazyFrame -> IO (Either PolarsError LazyFrame)
```

Join type ABI codes:

```text
0 inner
1 left
2 right
3 full
4 semi
5 anti
6 cross
```

Rust mapping:

```rust
4 => Ok(JoinType::Semi),
5 => Ok(JoinType::Anti),
6 => Ok(JoinType::Cross),
```

`phs_lazyframe_join` branches on cross joins before keyed validation:

```rust
if matches!(join_type, JoinType::Cross) {
    if left_len != 0 || right_len != 0 {
        return Err(PhsError::invalid_argument("cross join requires empty join key lists"));
    }
    *out = lazyframe_into_raw(left_frame.cross_join(right_frame, suffix));
    return Ok(());
}
```

## Implementation Plan

1. Add Hspec RED tests for `semiJoin`, `antiJoin`, `crossJoin`, and keyed
   `JoinCross` validation.
2. Extend `JoinType`, exports, helper functions, validation, and join type
   codes in `src/Polars/Join.hs`.
3. Add `semi_anti_join` and `cross_join` Cargo features.
4. Extend Rust `join_type_from_code` and branch cross joins in
   `phs_lazyframe_join`.
5. Update README, CHANGELOG, and parity plan status.
6. Verify with Cargo, Stack, HLint, and `git diff --check`.

## Examples

```haskell
joined <- semiJoin [col "department"] [col "department"] employees departments
```

```haskell
joined <- antiJoin [col "department"] [col "department"] employees departments
```

```haskell
joined <- crossJoin employees departments
```

## Trade-offs

- One ABI function keeps join mode expansion compact.
- Cross joins use a dedicated Haskell convenience function because they have a
  different key shape.
- Feature flags are enabled in the Rust adapter so Haskell users get one stable
  binding surface.

## Implementation Results

Implemented on 2026-05-17.

Files changed:

- `src/Polars/Join.hs`
- `rust/polars-hs-ffi/Cargo.toml`
- `rust/polars-hs-ffi/src/lazyframe.rs`
- `test/Spec.hs`
- `README.md`
- `CHANGELOG.md`
- `docs/superpowers/plans/2026-05-16-polars-hs-polars-053-parity.md`

Public API:

```haskell
semiJoin :: [Expr] -> [Expr] -> LazyFrame -> LazyFrame -> IO (Either PolarsError LazyFrame)
antiJoin :: [Expr] -> [Expr] -> LazyFrame -> LazyFrame -> IO (Either PolarsError LazyFrame)
crossJoin :: LazyFrame -> LazyFrame -> IO (Either PolarsError LazyFrame)
```

Tests added:

- Semi join over employees/departments preserves matching left rows.
- Anti join over employees/departments preserves unmatched left rows.
- Cross join over employees/departments returns a 12-row Cartesian product.
- Keyed `JoinCross` returns `InvalidArgument`.

Verification:

```bash
cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml
# 85 passed

PATH="$HOME/.ghcup/bin:$PATH" stack --system-ghc test --fast
# 93 examples, 0 failures

hlint src app test
# No hints

git diff --check
# passed
```

Deviation notes:

- The C ABI function stayed unchanged. The implementation only extends join
  type codes and Rust feature flags.
