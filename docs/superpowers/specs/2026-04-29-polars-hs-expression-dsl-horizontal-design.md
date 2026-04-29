# Design Log — Expression DSL Horizontal and Coalesce Phase 5B

**Date**: 2026-04-29
**Status**: Implemented

## Background

Phase 5A delivered scalar predicates (`isBetween`, `isDuplicated`, `isUnique`,
`isFirstDistinct`, `isLastDistinct`, `isClose`, `isIn`, `clip`, `clipMin`,
`clipMax`). The expression DSL needed row-wise ("horizontal") helpers that
Polars users expect: sum, mean, max, min, any, all across columns, plus
`coalesce` for first non-null.

Polars 0.53 exposes these through `polars_plan::dsl::functions` while
`polars::prelude` does not re-export them. The Rust adapter calls those public
wrapper functions and receives ordinary `Expr` values.

## Problem

We needed to offer the seven horizontal functions as pure Haskell constructors,
compile them through the Rust FFI, and validate empty input lists on the
Haskell side before calling Rust.

## Q&A

**Q**: Why validate empty lists in Haskell?
**A**: The public constructors are pure, but producing `Expr` values that will
always fail at collection time is poor UX. Rejecting early with a typed
`InvalidArgument` is consistent with other validations (e.g., ddof range,
negative group indices).

**Q**: Why not use the `polars::prelude` path?
**A**: Polars 0.53 does not export `sum_horizontal` etc. from its public prelude.
The adapter adds `polars-plan` as an explicit 0.53 dependency and calls the
public wrapper functions from `polars_plan::dsl::functions`.

**Q**: Why does `coalesce` reject empty input?
**A**: For consistent public behavior with the other six helpers. Polars itself
does not reject empty `coalesce`, but producing an expression that would error
at collection is undesirable.

## Design

### Haskell side

```haskell
data HorizontalFunction
    = HorizontalSum !Bool      -- ignore_nulls
    | HorizontalMean !Bool     -- ignore_nulls
    | HorizontalMax
    | HorizontalMin
    | HorizontalAny
    | HorizontalAll
    | HorizontalCoalesce

  -- Added to Expr:
data Expr = ...
    | HorizontalFunctionExpr !HorizontalFunction ![Expr]

sumHorizontal :: Bool -> [Expr] -> Expr
meanHorizontal :: Bool -> [Expr] -> Expr
maxHorizontal :: [Expr] -> Expr
minHorizontal :: [Expr] -> Expr
anyHorizontal :: [Expr] -> Expr
allHorizontal :: [Expr] -> Expr
coalesce :: [Expr] -> Expr
```

Validation occurs in the compiler, returning `Left (PolarsError InvalidArgument ...)`.

### Rust ABI

```c
int phs_expr_horizontal_function(int op, bool flag,
    const struct phs_expr *const *exprs, uintptr_t len,
    struct phs_expr **out, struct phs_error **err);
```

Opcodes:
- 0: `SumHorizontal { ignore_nulls }`
- 1: `MeanHorizontal { ignore_nulls }`
- 2: `MaxHorizontal`
- 3: `MinHorizontal`
- 4: `Boolean(AnyHorizontal)`
- 5: `Boolean(AllHorizontal)`
- 6: `Coalesce`

Empty exprs rejected with `PHS_INVALID_ARGUMENT`. Unknown opcode and null
pointer with positive len also rejected.

## Implementation Plan

1. Add `HorizontalFunction` data type and `HorizontalFunctionExpr` constructor to `Polars.Expr`.
2. Add public API functions (`sumHorizontal`, `meanHorizontal`, etc.).
3. Add `phs_expr_horizontal_function` Rust ABI function with 7 opcodes.
4. Add FFI binding in `Polars.Internal.Raw`.
5. Add compiler case in `Polars.Internal.Expr` with empty-list check.
6. Add Rust unit tests for all opcodes and error paths.
7. Create `test/data/horizontal.csv` fixture.
8. Add Hspec result-level test and empty-input error test.
9. Update `polars-hs.cabal` extra-source-files.
10. Update CHANGELOG and README.

## Examples

```haskell
let nums = [Pl.col "a", Pl.col "b", Pl.col "c"]
in Pl.alias "row_sum" (Pl.sumHorizontal True nums)

let bools = [Pl.col "p", Pl.col "q"]
in Pl.alias "any_true" (Pl.anyHorizontal bools)

Pl.alias "first_value" (Pl.coalesce [col "a", col "b", col "c"])
```

## Trade-offs

- **Direct `polars-plan` dependency**: Adds an explicit dependency on a crate
  already used by Polars and lets the adapter call public horizontal wrapper
  functions.
- **Haskell-side validation**: Keeps the pure API safe; Rust still validates
  for direct FFI callers.
- **Boolean flag in ABI**: The `flag` parameter applies to sum/mean (ignore_nulls);
  unused for other ops (pass `false`). Keeps the ABI signature simple.

## Implementation Results

### Rust validation

```bash
cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml
```
Result: 76 passed, 0 failed.

```bash
cargo clippy --manifest-path rust/polars-hs-ffi/Cargo.toml -- -D warnings
```
Result: No warnings. Finished successfully.

### Haskell validation

```bash
stack clean polars-hs && stack test --fast
```
Result: 70 examples, 0 failures.

```bash
hlint src app test
```
Result: No hints.

### Completeness check

```bash
python3 marker scan
```
Result: No leftover markers in src, test, rust, docs.

### Examples

All six examples in `examples/` (iris, groupby, join, columns, series, construction) run successfully.
