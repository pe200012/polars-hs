# Design: Expression DSL List Namespace (Phase 4A)

**Date** 2026-04-29
**Status** Implemented
**Bookmark** `expression-dsl-string`

## Background

The Polars Expression DSL previously supported Core, String, and Temporal
namespaces. The List/Collection namespace was missing, blocking operations
that produce `List[String]` from string splitting and downstream list
operations.

## Problem

Users needed:
1. String split operations returning `List[String]` (`str.split(by)`,
   `str.split_inclusive(by)`)
2. List namespace helpers returning extractable scalar outputs:
   `list.len()`, `list.first()`, `list.last()`, `list.get()`, `list.join()`,
   `list.contains()`, `list.count_matches()`

Direct typed extraction of `List` columns is not present, so tests generate
`List[String]` via `str.split()` and consume scalar outputs through list
helpers.

## Q&A

**Q: Why add split to `StringFunction` and not to `ListFunction`?**
A: `split` and `split_inclusive` are string namespace operations in Polars.
They produce `List[String]` outputs but are accessed through `expr.str().split(by)`.

**Q: Why use a shared expression-argument helper?**
A: String and list namespace ABI functions both marshal arrays of expression
handles. A shared `expr_args` helper keeps null pointer checks, slice
construction, and handle dereferencing consistent across namespaces.

**Q: Why use separate opcodes for boolean parameters?**
A: Rust's C ABI cannot encode `bool` in a match arm like `(op, bool)`.
Separate opcodes (e.g., op 3 for `Get(false)`, op 4 for `Get(true)`) are
the established pattern from the String namespace (compare `StrContains` opcodes
13/14).

## Design

### Haskell `Expr` additions

```haskell
data StringFunction
    = ...
    | StrSplit
    | StrSplitInclusive

data ListFunction
    = ListLen
    | ListFirst
    | ListLast
    | ListGet !Bool     -- null_on_oob
    | ListJoin !Bool    -- ignore_nulls
    | ListContains !Bool -- nulls_equal
    | ListCountMatches

data Expr = ... | ListFunctionExpr !ListFunction !Expr ![Expr]
```

### Smart constructors

```haskell
strSplit :: Expr -> Expr -> Expr          -- input -> by -> expr
strSplitInclusive :: Expr -> Expr -> Expr
listLen :: Expr -> Expr                    -- input -> expr
listFirst :: Expr -> Expr
listLast :: Expr -> Expr
listGet :: Bool -> Expr -> Expr -> Expr    -- null_on_oob -> input -> index -> expr
listJoin :: Bool -> Expr -> Expr -> Expr   -- ignore_nulls -> input -> separator -> expr
listContains :: Bool -> Expr -> Expr -> Expr -- nulls_equal -> input -> element -> expr
listCountMatches :: Expr -> Expr -> Expr   -- input -> element -> expr
```

### Rust ABI

```c
int phs_expr_list_function(int op,
                           const struct phs_expr *expr,
                           const struct phs_expr *const *args,
                           uintptr_t arg_len,
                           struct phs_expr **out,
                           struct phs_error **err);
```

Opcode table:

| Opcode | Function           | Arity | Bool parameter    |
|--------|--------------------|-------|--------------------|
| 0      | `list.len`         | 0     | - |
| 1      | `list.first`       | 0     | - |
| 2      | `list.last`        | 0     | - |
| 3      | `list.get`         | 1     | `null_on_oob=false` |
| 4      | `list.get`         | 1     | `null_on_oob=true`  |
| 5      | `list.join`        | 1     | `ignore_nulls=false`|
| 6      | `list.join`        | 1     | `ignore_nulls=true` |
| 7      | `list.contains`    | 1     | `nulls_equal=false` |
| 8      | `list.contains`    | 1     | `nulls_equal=true`  |
| 9      | `list.count_matches`| 1     | - |

String split opcodes added to `phs_expr_string_function`:

| Opcode | Function              | Arity |
|--------|-----------------------|-------|
| 24     | `str.split(by)`       | 1     |
| 25     | `str.split_inclusive(by)` | 1  |

### Cargo features

Added `is_in` (for `list.contains`) and `list_count` (for `list.count_matches`)
to the `polars` dependency. These are feature-gated in upstream Polars.

## Implementation Plan

1. Add `is_in` and `list_count` Cargo features.
2. Add string split opcodes 24/25 to `phs_expr_string_function`.
3. Add `phs_expr_list_function` with opcodes 0-9, reusing the shared `expr_args` helper.
4. Add C header declaration.
5. Add `ListFunction` type and `ListFunctionExpr` constructor to Haskell `Expr`.
6. Add `stringFunctionCode` entries for `StrSplit`/`StrSplitInclusive`.
7. Add `listFunctionCode` mapping.
8. Add FFI import `phs_expr_list_function`.
9. Create `test/data/phrases.csv` fixture.
10. Add Hspec test block asserting (4,7) shape and expected list outputs.
11. Add Rust tests for split opcodes, list ops, and error paths.
12. Regenerate `polars-hs.cabal`.

## Examples

```haskell
let split = Pl.strSplit (Pl.col "phrase") (Pl.litText " ")
projected <- Pl.select
  [ Pl.alias "len" (Pl.cast Pl.Int64 (Pl.listLen split))
  , Pl.alias "first" (Pl.listFirst split)
  , Pl.alias "last" (Pl.listLast split)
  , Pl.alias "get1" (Pl.listGet True split (Pl.litInt 1))
  , Pl.alias "joined" (Pl.listJoin True split (Pl.litText "-"))
  , Pl.alias "has_red" (Pl.listContains False split (Pl.litText "red"))
  , Pl.alias "red_count" (Pl.cast Pl.Int64 (Pl.listCountMatches split (Pl.litText "red")))
  ] lf0
```

## Trade-offs

- **Shared argument marshaling**: `expr_args` handles argument arrays for both
  string and list ABI functions, avoiding duplicated unsafe pointer handling.
- **No `List` typed extraction**: This phase relies on scalar outputs
  (`Int64`, `Bool`, `Text`). Direct `List[String]` extraction belongs in a
  separate Phase 4B.
- **Separate opcodes for bool params**: Adds 5 extra opcodes (3/4, 5/6, 7/8)
  but keeps the ABI simple and debuggable.

## Implementation Results

```bash
cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml
# 64 passed; 0 failed

cargo clippy --manifest-path rust/polars-hs-ffi/Cargo.toml -- -D warnings
# No warnings

stack test --fast
# 66 examples, 0 failures

hlint src app test
# No hints

python3 marker scan
# No incomplete-work markers found

Examples (iris, groupby, join, columns, series, construction):
# All pass
```
