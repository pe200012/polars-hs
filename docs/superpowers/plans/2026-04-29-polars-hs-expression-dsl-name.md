# Expression DSL Name Namespace – Implementation Plan

**Branch:** `expression-dsl-string`  
**Phase:** 5C  
**Date:** 2026-04-29  

## Goals

- Expose `name().keep()`, `name().prefix()`, `name().suffix()`,
  `name().replace()`, `name().to_lowercase()`, and `name().to_uppercase()`
from the Polars expression DSL.
- Verify through DataFrame schema field names.

## Steps

1. **Haskell AST** (`src/Polars/Expr.hs`)
   - Add `NameFunction` ADT with six constructors.
   - Add `NameFunctionExpr !NameFunction !Expr` to `Expr`.
   - Add public API functions with options-first, receiver-last order.

2. **Rust FFI** (`rust/polars-hs-ffi/src/expr.rs`)
   - Add `phs_expr_name_function` dispatching on opcode (0–5).
   - Validate required strings via `c_str_to_str`.
   - Add tests for all opcodes and error paths.

3. **Haskell FFI** (`src/Polars/Internal/Raw.hs`, `src/Polars/Internal/Expr.hs`)
   - Add `phs_expr_name_function` foreign import.
   - Add compile case for `NameFunctionExpr`.

4. **Test fixture** (`test/data/name_ops.csv`)
   - Two-row CSV with columns `Camel` and `score_value`.

5. **Spec test** (`test/Spec.hs`)
   - Schema-level test validating field names and types after name transforms.
   - Assert shape `(2, 6)`.

6. **Metadata**
   - Update `polars-hs.cabal` extra-source-files.
   - Update README and CHANGELOG.

## Validation

```bash
cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml
cargo clippy --manifest-path rust/polars-hs-ffi/Cargo.toml -- -D warnings
stack test --fast
hlint src test
```
