# Expression DSL Name Namespace – Design Log

**Branch:** `expression-dsl-string`  
**Date:** 2026-04-29  

## Background

Polars has a `name()` namespace on expressions that manipulates output column names.
The Rust API exposes `name().keep()`, `name().prefix()`, `name().suffix()`,
`name().replace()`, `name().to_lowercase()`, and `name().to_uppercase()`.

These operations modify the column name in the output schema without affecting
the underlying data.

## Problem

Expression DSL Phase 5C needs to expose these name manipulation operations in the
pure Haskell expression AST and compile them through the Rust FFI.

## Q&A

**Q:** Are `keep`, `prefix`, `suffix`, `replace`, `to_lowercase`, and `to_uppercase`
the only methods needed?

**A:** Yes. The Polars 0.53 `name()` namespace exposes these six methods. The
`map` and `map_fields` methods are not included because they require Rust
closures that cannot cross the FFI.

**Q:** Does `name().keep()` interact with `alias`?

**A:** Yes. `keep` restores the original root column name, undoing any
preceding `alias`. Our test exercises this.

**Q:** What's the argument order for `nameReplace`?

**A:** Options first, receiver last: `nameReplace True "score" "points" expr`,
where the first `Bool` is `literal`, then `pattern`, then `value`, followed by
the expression.

## Design

Add a new `NameFunction` type and a new `Expr` constructor `NameFunctionExpr`.

### Haskell API

```haskell
data NameFunction
    = NameKeep
    | NamePrefix !Text
    | NameSuffix !Text
    | NameReplace !Bool !Text !Text
    | NameToLowercase
    | NameToUppercase

nameKeep :: Expr -> Expr
namePrefix :: Text -> Expr -> Expr
nameSuffix :: Text -> Expr -> Expr
nameReplace :: Bool -> Text -> Text -> Expr -> Expr
nameToLowercase :: Expr -> Expr
nameToUppercase :: Expr -> Expr
```

### Rust ABI

A single FFI function dispatches on opcode:

```c
int phs_expr_name_function(int op, const char *first, const char *second,
    bool flag, const struct phs_expr *expr, struct phs_expr **out,
    struct phs_error **err);
```

Opcode mapping:
- 0: keep — ignores strings and flag
- 1: prefix — requires `first`
- 2: suffix — requires `first`
- 3: replace — requires `first` pattern and `second` value; `flag` is literal
- 4: to_lowercase — ignores strings and flag
- 5: to_uppercase — ignores strings and flag

Unknown opcodes return `PHS_INVALID_ARGUMENT`. Missing required strings return
`PHS_INVALID_ARGUMENT`.

## Implementation Plan

1. Add `NameFunction` type and `NameFunctionExpr` to `Expr.hs`
2. Add public API functions (`nameKeep`, `namePrefix`, `nameSuffix`,
   `nameReplace`, `nameToLowercase`, `nameToUppercase`)
3. Add Rust FFI function `phs_expr_name_function` to `expr.rs`
4. Add Rust ABI tests covering all opcodes and error paths
5. Add FFI import to `Internal/Raw.hs`
6. Add compilation case to `Internal/Expr.hs` with `nameFunctionCode` and
   `nameFunctionExpr`
7. Create test fixture `test/data/name_ops.csv`
8. Add hspec schema-level test validating field names and types
9. Update `polars-hs.cabal` extra-source-files
10. Update README and CHANGELOG

## Examples

```haskell
import qualified Polars as Pl

projected <- Pl.select
    [ Pl.nameKeep (Pl.alias "renamed" (Pl.col "Camel"))
    , Pl.namePrefix "pre_" (Pl.col "score_value")
    , Pl.nameSuffix "_suf" (Pl.col "Camel")
    , Pl.nameToLowercase (Pl.col "Camel")
    , Pl.nameToUppercase (Pl.col "score_value")
    , Pl.nameReplace True "score" "points" (Pl.col "score_value")
    ] lf0
```

Expected schema field names: `Camel`, `pre_score_value`, `Camel_suf`,
`camel`, `SCORE_VALUE`, `points_value`. All types `Int64`.

## Trade-offs

- **Pro:** Single FFI function minimizes ABI surface.
- **Pro:** Pure Haskell AST enables later inspection and rewriting.
- **Con:** The `map` and `map_fields` methods are omitted because they
  take closures that cannot cross the FFI.
- **Con:** `ns_safe` is not exposed in this MVP.

## Implementation Results

All Rust and Haskell tests pass:

```
rust tests: 79 passed; 0 failed
haskell tests: 71 examples, 0 failures
cargo clippy: clean (no warnings)
hlint: no hints
```

Verified 2026-04-29 with:

```bash
$ cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml
test result: ok. 79 passed; 0 failed

$ cargo clippy --manifest-path rust/polars-hs-ffi/Cargo.toml -- -D warnings
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.34s

$ stack test --fast
71 examples, 0 failures

$ hlint src test
No hints
```
