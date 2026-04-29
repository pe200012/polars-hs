# Plan: Expression DSL List Namespace Phase 4A

## Goal

Add a small collection/list expression batch that can be verified with current scalar and text column extraction.

## Scope

- Add `strSplit` and `strSplitInclusive` to produce `List[String]` expressions.
- Add scalar/Text list helpers: `listLen`, `listFirst`, `listLast`, `listGet`, `listJoin`, `listContains`, and `listCountMatches`.
- Verify by splitting phrase strings and extracting Int64, Bool, and Text outputs.

## Steps

1. Add `test/data/phrases.csv` and a RED Hspec test using `strSplit` plus list helpers.
2. Add public AST constructors and smart constructors in `src/Polars/Expr.hs`.
3. Extend Rust ABI:
   - string opcodes 24/25 for split variants;
   - new `phs_expr_list_function` opcodes 0-9.
4. Add raw FFI declarations and compiler mapping in Haskell internals.
5. Add Rust ABI tests for supported opcodes and error paths.
6. Update `README.md`, `CHANGELOG.md`, `polars-hs.cabal`, and the design log.
7. Run full verification and commit.

## Validation

```bash
cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml
cargo clippy --manifest-path rust/polars-hs-ffi/Cargo.toml -- -D warnings
stack clean polars-hs
stack test --fast
hlint src app test
```
