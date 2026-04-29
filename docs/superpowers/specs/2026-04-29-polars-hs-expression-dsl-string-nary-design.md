# Design Log — Expression DSL String N-ary Concat/Format Phase 5D

**Date**: 2026-04-29
**Status**: Implemented

## Background

Previous Expression DSL phases delivered core operators, string namespace
helpers (single-column operations like `strContainsLiteral`, `strSlice`),
temporal extraction, list helpers, scalar predicates, horizontal row-wise
operations, and name namespace transforms. Missing were string n-ary
functions that combine multiple columns: horizontal string concatenation
(`concat_str`) and placeholder-based string formatting (`format_str`).

Polars 0.53 exposes these through `polars_plan::dsl::functions::concat_str`
and `polars_plan::dsl::functions::format_str`, both gated behind the
`concat_str` feature flag. The adapter enables it on both `polars` and the
explicit `polars-plan` dependency: `polars-plan` exposes the wrapper symbols to
this crate, while `polars` propagates the execution-side feature set.

## Problem

We needed to add `concatStr` and `formatStr` as pure Haskell expression
constructors, compile them through the Rust FFI, validate empty expression
lists on the Haskell side, and allow Polars to report placeholder-count
mismatches for `formatStr` as typed `PolarsFailure` errors.

## Q&A

**Q**: Why validate empty lists in Haskell?
**A**: Consistent with horizontal functions (Phase 5B). Rejecting early with
`InvalidArgument` produces a clear error at expression-construction time
instead of a confusing error at collection time.

**Q**: Who validates format placeholder count?
**A**: Rust Polars validates it inside `format_str` and returns a
`PolarsResult<Expr>`. The FFI adapter propagates the error as
`PHS_POLARS_ERROR` via `PhsError::from`. The Haskell side receives it as
`PolarsFailure`.

**Q**: Why does `concat_str` not use `PolarsResult`?
**A**: `concat_str` is infallible for valid inputs; it always returns `Expr`
directly. The FFI adapter applies `c_str_to_str` for the separator and
validates the empty list, both of which produce `PHS_INVALID_ARGUMENT` on
failure.

**Q**: Why `concatStr` uses `Bool -> Text -> [Expr] -> Expr` argument order?
**A**: The pattern across the DSL puts flags/options first, then the
expressions last. This groups related arguments and is consistent with
`sumHorizontal :: Bool -> [Expr] -> Expr`.

## Design

### Haskell side

```haskell
data StringNaryFunction
    = ConcatStr !Bool !Text  -- ignore_nulls, separator
    | FormatStr !Text        -- format string with `{}` placeholders

data Expr = ...
    | StringNaryFunctionExpr !StringNaryFunction ![Expr]

concatStr :: Bool -> Text -> [Expr] -> Expr
formatStr :: Text -> [Expr] -> Expr
```

Validation in the compiler rejects empty expression lists.

### Rust ABI

```c
int phs_expr_string_nary_function(int op, const char *text, bool flag,
    const struct phs_expr *const *exprs, uintptr_t len,
    struct phs_expr **out, struct phs_error **err);
```

- op 0: `concat_str(&exprs, text, flag)` where `text` is separator, `flag` is ignore_nulls.
- op 1: `format_str(text, &exprs)` where `text` is format string; `flag` is ignored.
- Unknown opcode: `PHS_INVALID_ARGUMENT`.
- Null text pointer: `PHS_INVALID_ARGUMENT`.
- Empty exprs list: `PHS_INVALID_ARGUMENT`.

### Cargo features

`polars` and `polars-plan` dependencies include `concat_str`. `polars-plan`
also enables `strings` for the direct wrapper imports.

## Implementation Plan

1. Add `StringNaryFunction` data type and `StringNaryFunctionExpr` constructor to `Polars.Expr`.
2. Add public API functions (`concatStr`, `formatStr`).
3. Add `phs_expr_string_nary_function` Rust ABI function.
4. Add FFI binding in `Polars.Internal.Raw`.
5. Add compiler case in `Polars.Internal.Expr` with empty-list check.
6. Add Rust unit tests for both opcodes and error paths: unknown opcode, null text, empty list, format placeholder mismatch.
7. Create `test/data/concat.csv` fixture.
8. Add Hspec result-level test and error tests.
9. Update CHANGELOG and README.

## Examples

```haskell
let fullName = [Pl.col "first", Pl.col "last"]
Pl.alias "full_ignore" (Pl.concatStr True " " fullName)
Pl.alias "full_strict" (Pl.concatStr False " " fullName)
Pl.alias "formatted" (Pl.formatStr "{}:{}" [Pl.col "first", Pl.cast Pl.Utf8 (Pl.col "age")])
```

## Trade-offs

- **`concat_str` feature gate**: Needed for `polars-plan` to compile
  `concat_str` and `format_str`. The `strings` feature is already enabled in
  the `polars` dependency, but `concat_str` needs explicit opt-in.
- **Argument order**: Options/flag first, expression list last, consistent
  with horizontal functions.
- **Separate `flag` parameter in ABI**: Applies to concat_str (ignore_nulls);
  unused for format_str. Keeps the ABI signature uniform.

## Implementation Results

### Rust validation

```bash
cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml
```
Result: 83 passed, 0 failed.

```bash
cargo clippy --manifest-path rust/polars-hs-ffi/Cargo.toml -- -D warnings
```
Result: No warnings. Finished successfully.

### Haskell validation

```bash
stack clean polars-hs && stack test --fast
```
Result: 74 examples, 0 failures.

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
