# Plan — Expression DSL Horizontal and Coalesce Phase 5B

**Date**: 2026-04-29
**Status**: Complete

## Objective

Add horizontal (row-wise) and coalesce expression helpers to the Haskell
expression DSL.

## Steps

1. **`Polars.Expr`**: Add `HorizontalFunction` data type and
   `HorizontalFunctionExpr` constructor. Add seven public API functions.

2. **Rust ABI**: Add `phs_expr_horizontal_function` with opcode dispatch
   (sum, mean, max, min, any, all, coalesce). Empty exprs → error.
   Unknown opcode → error. Null pointer + positive len → error.

3. **FFI binding**: Add `phs_expr_horizontal_function` to `Polars.Internal.Raw`.

4. **Compiler**: Add `HorizontalFunctionExpr` case to `compileExpr` with
   Haskell-side empty-list validation.

5. **Rust tests**: Test all opcodes; test empty, unknown opcode, and null
   pointer error paths.

6. **Hspec tests**: Result-level test with CSV fixture; empty-input error test.

7. **Docs**: Design log and plan documents. Update CHANGELOG and README.

## Verification

```bash
cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml
cargo clippy --manifest-path rust/polars-hs-ffi/Cargo.toml -- -D warnings
stack test --fast
hlint src app test
```
