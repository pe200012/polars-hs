# polars-hs Expression DSL String Phase 2C — Design Log

**2026-04-29**

## Goal

Complete the remaining scalar and list-producing string helpers:
`strip_prefix`, `strip_suffix`, `escape_regex`, and `extract_all`.

## Architecture decisions

- Use the existing `phs_expr_string_function` ABI with opcodes 26‑29.
  These methods are `map_binary` or `map_unary` in Polars and fit into the
  existing `StringFunctionExpr` AST constructor with no type‑erasure changes.
- `extract_all` returns `List[String]`. Verify the list output through
  `listLen`/`listJoin` helpers; no list‑specific ABI needed.
- Preserving the `String` Polars datatype output guarantees `Vector (Maybe Text)`
  column extraction works without dtype mapping changes.

## Implementation

### Rust (`phs_expr_string_function`)

```rust
26 => expr.str().strip_prefix(args[0].clone())  // arity 1
27 => expr.str().strip_suffix(args[0].clone())  // arity 1
28 => expr.str().escape_regex()                 // arity 0
29 => expr.str().extract_all(args[0].clone())   // arity 1
```

### Haskell

- `StringFunction` constructors: `StrStripPrefix`, `StrStripSuffix`,
  `StrEscapeRegex`, `StrExtractAll`.
- `stringFunctionCode` maps them to 26‑29.
- Public API in `Polars.Expr`:
  - `strStripPrefix :: Expr -> Expr -> Expr`
  - `strStripSuffix :: Expr -> Expr -> Expr`
  - `strEscapeRegex :: Expr -> Expr`
  - `strExtractAll :: Expr -> Expr -> Expr`

### Test fixture

`test/data/string_more.csv` contains 5 rows with mixed whitespace, Unicode, and
regex‑significant characters.

## Verification results

```
cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml  → 85 passed, 0 failed
cargo clippy --manifest-path rust/polars-hs-ffi/Cargo.toml -- -D warnings  → clean
stack test --fast  → 75 examples, 0 failures
hlint src app test  → No hints
marker scan  → clean
git diff --check  → clean
```

## Review notes

- All four ops delegate to the generic `StringFunctionExpr` match arm;
  no new compile‑time dispatch.
- Opcodes 26‑29 slot into the existing `stringFunctionCode` exhaustive match.
- `strExtractAll` output is a `List[String]`; existing list helpers work without
  further changes.
