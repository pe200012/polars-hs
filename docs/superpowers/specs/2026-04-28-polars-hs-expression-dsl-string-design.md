# Design Log: Expression DSL String Namespace

## Background

Expression DSL Phase 1 Foundation/Core is implemented. `Polars.Expr` now uses a pure Haskell AST that compiles to short-lived Rust `Expr` handles through private `phs_expr_*` ABI helpers.

Upstream Polars 0.53 exposes a `str` namespace for string expressions. The first string phase should reuse the Phase 1 architecture and deliver high-frequency string predicates and transformations with result-level Hspec coverage.

## Problem

Users need common string operations in lazy `select`, `withColumns`, `filter`, and grouped contexts. The binding should expose these as pure Haskell `Expr` constructors while keeping opcode details internal to `Polars.Internal.Expr` and `rust/polars-hs-ffi/src/expr.rs`.

## Questions and Answers

### Q1. Which string functions belong in this phase?

Answer: Start with operations that produce scalar string, boolean, or integer values and avoid List/Struct output scope: literal contains, startsWith, endsWith, strip variants, lowercase, uppercase, lenBytes, lenChars, slice, head, and tail.

### Q2. Should regex functions be included?

Answer: Include literal contains because Polars gates it behind the `regex` feature. Defer regex contains, extract, replace, replaceAll, and split/list-producing functions to a follow-up string batch with dedicated regex and List dtype tests.

### Q3. What argument order should public Haskell APIs use?

Answer: Use receiver-first order for namespace-like helpers. Example: `strStartsWith (col "name") (litText "A")`. This mirrors Polars `expr.str().starts_with(prefix)` and keeps string helpers consistent with method-style reading.

### Q4. How should Rust ABI stay compact?

Answer: Add one string ABI family:

```c
int phs_expr_string_function(int op, const phs_expr *expr, const phs_expr *const *args, uintptr_t len, phs_expr **out, phs_error **err);
```

Haskell compiles the input expression and argument list, passes a private opcode, and Rust validates arity before calling `expr.str().*`.

## Design

### Public API

Add to `Polars.Expr`:

```haskell
data StringFunction
    = StrContainsLiteral
    | StrStartsWith
    | StrEndsWith
    | StrStrip
    | StrStripStart
    | StrStripEnd
    | StrToLowercase
    | StrToUppercase
    | StrLenBytes
    | StrLenChars
    | StrSlice
    | StrHead
    | StrTail

strContainsLiteral :: Expr -> Expr -> Expr
strStartsWith :: Expr -> Expr -> Expr
strEndsWith :: Expr -> Expr -> Expr
strStrip :: Expr -> Expr -> Expr
strStripStart :: Expr -> Expr -> Expr
strStripEnd :: Expr -> Expr -> Expr
strToLowercase :: Expr -> Expr
strToUppercase :: Expr -> Expr
strLenBytes :: Expr -> Expr
strLenChars :: Expr -> Expr
strSlice :: Expr -> Expr -> Expr -> Expr
strHead :: Expr -> Expr -> Expr
strTail :: Expr -> Expr -> Expr
```

Add an AST constructor:

```haskell
| StringFunctionExpr !StringFunction !Expr ![Expr]
```

### Opcode mapping

```text
0  StrContainsLiteral  arity 1
1  StrStartsWith       arity 1
2  StrEndsWith         arity 1
3  StrStrip            arity 1
4  StrStripStart       arity 1
5  StrStripEnd         arity 1
6  StrToLowercase      arity 0
7  StrToUppercase      arity 0
8  StrLenBytes         arity 0
9  StrLenChars         arity 0
10 StrSlice            arity 2
11 StrHead             arity 1
12 StrTail             arity 1
```

### Rust feature flags

Keep existing `strings` feature and add `regex` for `contains_literal`. The current dependency graph already includes the regex crate through Polars, so this should have small lockfile impact.

### Test fixture

Create `test/data/strings.csv`:

```csv
text
" Alice "
"βeta"
"CAROL"
"日本語"
```

Expected values:

```haskell
contains "li"      = [True, False, False, False]
startsWith " A"    = [True, False, False, False]
endsWith " "       = [True, False, False, False]
strip " "          = ["Alice", "βeta", "CAROL", "日本語"]
lowercase           = [" alice ", "βeta", "carol", "日本語"]
uppercase           = [" ALICE ", "ΒETA", "CAROL", "日本語"]
lenBytes            = [7, 5, 5, 9]
lenChars            = [7, 4, 5, 3]
slice 0 2           = [" A", "βe", "CA", "日本"]
head 2              = [" A", "βe", "CA", "日本"]
tail 2              = ["e ", "ta", "OL", "本語"]
```

## Implementation Plan

1. Add RED Hspec tests and fixture for string namespace result behavior.
2. Add `StringFunction` and public smart constructors in `Polars.Expr`.
3. Add Rust string ABI and feature flag.
4. Add Raw FFI and Haskell compiler support.
5. Update README, CHANGELOG, and this design log with implementation results.
6. Run full verification and merge through the normal review flow.

## Examples

✅ Receiver-first public API:

```haskell
Pl.select
  [ Pl.alias "starts_a" (Pl.strStartsWith (Pl.col "text") (Pl.litText " A"))
  , Pl.alias "clean" (Pl.strStrip (Pl.col "text") (Pl.litText " "))
  , Pl.alias "chars" (Pl.cast Pl.Int64 (Pl.strLenChars (Pl.col "text")))
  ]
```

✅ String slicing:

```haskell
Pl.alias "prefix" (Pl.strSlice (Pl.col "text") (Pl.litInt 0) (Pl.litInt 2))
```

## Trade-offs

- Receiver-first APIs mirror Polars namespace methods and make expressions read like method calls.
- A single string ABI family keeps the C ABI compact and centralizes arity validation in Rust.
- Deferring regex extract/replace and split/list-producing helpers keeps this phase focused on scalar outputs and current typed extraction support.
- Adding the `regex` feature enables literal contains through Polars 0.53 and should have limited dependency impact.

## Implementation Results

### Phase 2A: String namespace — Implemented

**Public APIs in `Polars.Expr`:** `strContainsLiteral`, `strStartsWith`, `strEndsWith`, `strStrip`, `strStripStart`, `strStripEnd`, `strToLowercase`, `strToUppercase`, `strLenBytes`, `strLenChars`, `strSlice`, `strHead`, and `strTail`.

**AST and compiler:** `StringFunctionExpr` AST constructor with private `StringFunction` enum and opcode mapping in `Polars.Internal.Expr`. The compiler case marshals the input expression and argument list through the `phs_expr_string_function` ABI.

**Rust ABI:** `phs_expr_string_function` in `rust/polars-hs-ffi/src/expr.rs` dispatches 13 opcodes with arity validation and delegates to `expr.str().*` Polars methods. Rust unit tests cover all opcodes and error paths (unknown opcode, wrong arity).

**Fixture and Hspec tests:** `test/data/strings.csv` provides ASCII, Greek, and Japanese text rows. Two Hspec examples test the full result pipeline for 13 string operations across all four rows, covering boolean predicates, text transformations, byte/char lengths, and character slicing.

#### Full verification — 2026-04-29

**Cargo tests (57 passed, 0 failed, 0 ignored):**
- `builds_string_namespace_expressions` — all 13 opcodes
- `string_namespace_errors_validate_opcode_and_arity` — unknown opcode + wrong arity
- All existing tests continue to pass.

**Cargo clippy:** `-D warnings` — clean, no warnings.

**Stack test (62 examples, 0 failures):**
- 2 new `Expression DSL string namespace` examples pass.
- All existing 60 examples pass.

**HLint:** `No hints` across `src`, `app`, `test`.

**Example scripts:** All six exit successfully:
- `examples/iris.hs` — OK
- `examples/groupby.hs` — OK
- `examples/join.hs` — OK
- `examples/columns.hs` — OK
- `examples/series.hs` — OK
- `examples/construction.hs` — OK

**Marker/whitespace scan:** Clean for incomplete-work markers. `git diff --check` reports no whitespace issues.

### Phase 2B: Regex/Find/Extract/Count/Replace — Implemented

**Public APIs in `Polars.Expr`:** `strContainsRegex`, `strFindLiteral`, `strFindRegex`, `strExtract`, `strCountMatches`, `strReplace`, and `strReplaceAll`.

**AST and compiler:** Phase 2B adds to the `StringFunction` type: `StrContainsRegex !Bool`, `StrFindLiteral`, `StrFindRegex !Bool`, `StrCountMatches !Bool`, `StrReplace !Bool`, `StrReplaceAll !Bool`, and `StrExtract !Int`. New opcodes 13–23 in `phs_expr_string_function` cover regex contains (strict/non-strict), find (literal/regex), count matches (literal/regex), and replace/replaceAll (literal/regex). `StrExtract` dispatches through the new `phs_expr_string_function_i64` ABI with negative group index validation.

**Rust ABI:** `phs_expr_string_function_i64` opcode 0 wraps `expr.str().extract(pattern, group_index)` with `arg` validated non-negative. Rust unit tests cover the extract path and error paths (unknown opcode, negative group index). Total Rust tests: 59 (one new extract test, one new error test).

**Hspec tests:** Two Phase 2B tests validate regex contains/find/extract/count/replace operations across all 4 string fixture rows, literal find behavior for regex metacharacters, and strict regex find error propagation. Total Hspec examples: 64 (4 string namespace tests).

#### Full verification — 2026-04-29

**Cargo tests (59 passed, 0 failed, 0 ignored):**
- `builds_string_i64_extract_expression` — extract opcode 0
- `string_i64_errors_validate_opcode_and_negative_arg` — unknown opcode + negative group index
- All existing 57 tests continue to pass.

**Cargo clippy:** `-D warnings` — clean, no warnings.

**Stack test (64 examples, 0 failures):**
- Phase 2B examples cover scalar regex/find/extract/count/replace results and strict regex find error propagation.
- All existing 62 examples pass.

**HLint:** `No hints` across `src`, `app`, `test`.

**Marker/whitespace scan:** Clean for incomplete-work markers.
