# Implementation Plan — Expression DSL String N-ary Concat/Format Phase 5D

**Date**: 2026-04-29
**Status**: Complete

## Goal

Add `concatStr` and `formatStr` string n-ary expression DSL functions.

## Steps

1. Add `StringNaryFunction` data type and `StringNaryFunctionExpr` constructor to `Polars.Expr`.
2. Add public API functions `concatStr` and `formatStr`.
3. Add `phs_expr_string_nary_function` Rust ABI function with opcodes 0 (concat_str) and 1 (format_str).
4. Add FFI binding in `Polars.Internal.Raw`.
5. Add compiler case in `Polars.Internal.Expr` with empty-list validation.
6. Add Rust unit tests for both opcodes and error paths.
7. Create `test/data/concat.csv` fixture.
8. Add Hspec result-level test and error tests.
9. Add `concat_str` feature to both `polars` and `polars-plan` dependencies.
10. Update CHANGELOG and README.

## Files changed

| File | Change |
|------|--------|
| `src/Polars/Expr.hs` | Add `StringNaryFunction` type, `StringNaryFunctionExpr` constructor, `concatStr`/`formatStr` functions |
| `src/Polars/Internal/Expr.hs` | Add compiler case for `StringNaryFunctionExpr` with empty-list check |
| `src/Polars/Internal/Raw.hs` | Add `phs_expr_string_nary_function` FFI binding |
| `rust/polars-hs-ffi/Cargo.toml` | Add `concat_str` feature to `polars` and `polars-plan` |
| `rust/polars-hs-ffi/src/expr.rs` | Add `phs_expr_string_nary_function` and 4 test functions |
| `test/Spec.hs` | Add `concatCsv` fixture and 3 test cases |
| `test/data/concat.csv` | New test fixture |
| `CHANGELOG.md` | Add string n-ary entry |
| `README.md` | Update expression DSL wording |
| `docs/superpowers/specs/2026-04-29-polars-hs-expression-dsl-string-nary-design.md` | Design log |
| `docs/superpowers/plans/2026-04-29-polars-hs-expression-dsl-string-nary.md` | This plan |
