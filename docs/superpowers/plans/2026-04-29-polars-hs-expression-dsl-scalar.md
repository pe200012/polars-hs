# Plan: Expression DSL Scalar Predicates (Phase 5A)

**Date** 2026-04-29
**Status** Implemented
**Bookmark** `expression-dsl-string`

## Goal

Add high-value scalar predicate and clip expression helpers to the
Expression DSL.

## Scope

### New Haskell types

- `ClosedInterval` — `ClosedBoth | ClosedLeft | ClosedRight | ClosedNone`
- `ScalarFunction` — wraps one of
    `IsDuplicated | IsUnique | IsFirstDistinct | IsLastDistinct | IsBetween !ClosedInterval | IsClose !Double !Double !Bool | IsIn !Bool | Clip | ClipMin | ClipMax`
- `ScalarFunctionExpr !ScalarFunction !Expr ![Expr]` constructor on `Expr`

### New public functions in `Polars.Expr`

- `isDuplicated, isUnique, isFirstDistinct, isLastDistinct :: Expr -> Expr`
- `isBetween :: ClosedInterval -> Expr -> Expr -> Expr -> Expr`
- `isClose :: Double -> Double -> Bool -> Expr -> Expr -> Expr`
- `isIn :: Bool -> Expr -> Expr -> Expr`
- `clip :: Expr -> Expr -> Expr -> Expr`, `clipMin :: Expr -> Expr -> Expr`, `clipMax :: Expr -> Expr -> Expr`

### Rust ABI additions

Four new focused C ABI helpers, dispatched by opcode:

- `phs_expr_boolean_unary` — opcodes 0–3 for is_duplicated/unique/first/last distinct
- `phs_expr_is_between` — closed-interval code 0–3, rejects unknown with PHS_INVALID_ARGUMENT
- `phs_expr_is_close` — abs_tol, rel_tol, nans_equal → expr.is_close(other, …)
- `phs_expr_is_in` — nulls_equal → expr.is_in(other, …)
- `phs_expr_clip` — opcodes 0–2 (clip/clip_min/clip_max); rejects unknown op or wrong arity

### Cargo features

`is_between`, `is_unique`, `is_close`, `is_first_distinct`, `is_last_distinct`, `round_series` (`clip` helpers)

### Tests

- Rust construction tests for all helpers and error paths
- Hspec result-level tests over `test/data/predicates.csv` and `phrases.csv`
- All 6 examples continue to pass

## Verification

```bash
cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml   # 72 passed
cargo clippy --manifest-path rust/polars-hs-ffi/Cargo.toml -- -D warnings  # clean
stack test --fast                                           # 68 examples, 0 failures
hlint src app test                                          # No hints
python3 artifacts check                                     # clean
examples/{iris,groupby,join,columns,series,construction}.hs # all pass
```
