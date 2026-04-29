# Design: Expression DSL Scalar Predicates (Phase 5A)

**Date** 2026-04-29
**Status** Implemented
**Bookmark** `expression-dsl-string`

## Background

Core, String, Temporal, and List expression namespaces were complete.
Polars provides a set of scalar-level predicate and clipping methods:
`is_duplicated`, `is_unique`, `is_first_distinct`, `is_last_distinct`,
`is_between`, `is_close`, `is_in`, `clip`, `clip_min`, and `clip_max`.
These methods operate on row-level values rather than belonging to a
named namespace, and several are gated behind Cargo feature flags.

## Problem

Users needed high-value scalar predicates for deduplication analysis,
range checks, floating-point closeness comparisons, set membership,
and value clipping. Without these, consumers had to drop into raw
SQL-like workarounds.

## Q&A

**Q: Why a unified `ScalarFunctionExpr` constructor instead of individual
   constructors per function?**
A: The scalar family has 10 functions with diverse signatures (unary,
binary, ternary). Adding 10 constructors to `Expr` would bloat the
AST and pattern-match cases in the compiler. A single constructor with
a typed function tag contains this complexity.

**Q: Why use a `ClosedInterval` algebraic type?**
A: `is_between` takes a `ClosedInterval` enum with four variants. An
algebraic Haskell type eliminates invalid states at compile time,
mirroring the Rust enum without runtime validation.

**Q: Why are boolean-unary ops dispatched through `phs_expr_boolean_unary`?**
A: All four boolean-unary operations (`is_duplicated`, `is_unique`,
`is_first_distinct`, `is_last_distinct`) have the same function shape
`Expr → Expr`. Grouping them under a single opcode-driven ABI avoids
adding four almost-identical C functions.

**Q: Why did `clip`, `clip_min`, `clip_max` need a separate ABI?**
A: `clip` takes 2 args and wraps `FunctionExpr::Clip { has_min: true, has_max: true }`.
`clip_min` and `clip_max` each take 1 arg. Rather than adding two more
ABIs, `phs_expr_clip` uses an opcode to select the variant and
validates arity for each.

## Design

### Haskell `Expr` additions

```haskell
data ClosedInterval = ClosedBoth | ClosedLeft | ClosedRight | ClosedNone

data ScalarFunction
    = IsDuplicated | IsUnique | IsFirstDistinct | IsLastDistinct
    | IsBetween !ClosedInterval
    | IsClose !Double !Double !Bool
    | IsIn !Bool
    | Clip | ClipMin | ClipMax

data Expr = ... | ScalarFunctionExpr !ScalarFunction !Expr ![Expr]
```

### Smart constructors

```haskell
isDuplicated, isUnique, isFirstDistinct, isLastDistinct :: Expr -> Expr
isBetween :: ClosedInterval -> Expr -> Expr -> Expr -> Expr
isClose :: Double -> Double -> Bool -> Expr -> Expr -> Expr
isIn :: Bool -> Expr -> Expr -> Expr
clip :: Expr -> Expr -> Expr -> Expr
clipMin :: Expr -> Expr -> Expr
clipMax :: Expr -> Expr -> Expr
```

### Rust ABI

```c
int phs_expr_boolean_unary(int op, const phs_expr *expr, phs_expr **out, phs_error **err);
int phs_expr_is_between(int closed, const phs_expr *expr, const phs_expr *lower, const phs_expr *upper, phs_expr **out, phs_error **err);
int phs_expr_is_close(double abs_tol, double rel_tol, bool nans_equal, const phs_expr *expr, const phs_expr *other, phs_expr **out, phs_error **err);
int phs_expr_is_in(bool nulls_equal, const phs_expr *expr, const phs_expr *other, phs_expr **out, phs_error **err);
int phs_expr_clip(int op, const phs_expr *expr, const phs_expr *const *args, uintptr_t arg_len, phs_expr **out, phs_error **err);
```

- `phs_expr_boolean_unary` opcodes: 0=is_duplicated, 1=is_unique, 2=is_first_distinct, 3=is_last_distinct
- `phs_expr_is_between` closed codes: 0=Both, 1=Left, 2=Right, 3=None; rejects unknown with `PHS_INVALID_ARGUMENT`
- `phs_expr_clip` opcodes: 0=clip (arity 2), 1=clip_min (arity 1), 2=clip_max (arity 1); rejects unknown opcode or wrong arity

### Cargo features

Added to `polars` dependency: `is_between`, `is_unique`, `is_close`,
`is_first_distinct`, `is_last_distinct`, and `round_series`. Polars 0.53 gates
`clip`, `clip_min`, and `clip_max` behind `round_series`.

`is_in` was already enabled from the List phase.

### Compiler dispatch

`ScalarFunctionExpr` pattern-match in `compileExpr` dispatches each
variant to the appropriate ABI, compiling sub-expressions recursively
and passing arguments as required.

- Boolean unary: compiled input → `phs_expr_boolean_unary opcode inputPtr`
- IsBetween: compiled lower, upper → `phs_expr_is_between closedCode inputPtr lowerPtr upperPtr`
- IsClose: compiled other → `phs_expr_is_close absTol relTol nansEqual inputPtr otherPtr`
- IsIn: compiled listExpr → `phs_expr_is_in nullsEqual inputPtr listPtr`
- Clip/ClipMin/ClipMax: compiled args → `phs_expr_clip opcode inputPtr argsArrayPtr argLen`

## Examples

```haskell
-- Range check with closed interval
Pl.isBetween Pl.ClosedBoth (Pl.col "value") (Pl.litInt 2) (Pl.litInt 3)

-- Floating-point closeness
Pl.isClose 0.15 0.0 False (Pl.col "value") (Pl.col "near")

-- Value clipping
Pl.clip (Pl.col "value") (Pl.litInt 2) (Pl.litInt 3)

-- Set membership (nested in select)
Pl.alias "red_in" (Pl.isIn False (Pl.litText "red") (Pl.strSplit (Pl.col "phrase") (Pl.litText " ")))
```

## Trade-offs

**Pro**: Compact Haskell surface with algebraic types. Single dispatch
pattern keeps ABI surface manageable. All error paths validated in
Rust unit tests. Hspec tests verify end-to-end behavior against real
Polars evaluation.

**Con**: Opcode dispatch adds a layer of indirection. For boolean-unary
ops, the Haskell compiler validates arg count before calling the ABI,
so arg-count errors are caught at Haskell compile-time rather than
deferring to Rust.

## Implementation Results

### Rust tests
```
test result: ok. 72 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

### Haskell tests
```
Finished in 0.0402 seconds
68 examples, 0 failures
```

### Lint
- `cargo clippy -- -D warnings` — clean
- `hlint src app test` — No hints
- `git diff --check` — clean
- Artifact scanner — clean

### Examples
All 6 examples (iris, groupby, join, columns, series, construction) pass.
