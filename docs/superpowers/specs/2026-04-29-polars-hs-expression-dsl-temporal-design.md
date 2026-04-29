# Temporal Expression DSL Design Log

**Date**: 2026-04-29
**Bookmark**: `expression-dsl-string`

## Background

The `polars-hs` Expression DSL compiles a pure Haskell expression AST into
Rust Polars `Expr` handles via a small C ABI.  String namespace support
(Phase 2A/2B) already provides pattern matching and transformation helpers.
Now we extend the DSL with temporal (datetime) namespace support.

## Problem

Polars exposes rich datetime extraction and formatting helpers through
`DateLikeNameSpace` (`expr.dt()`) in its lazy evaluation plan.  Users need
the same helpers in pure Haskell to build expressions over datetime columns.

## Q&A

### Why three separate ABI functions?

Temporal ops fall into three categories:
1. **Simple unary extractors** like year/month/day/… — dispatched by a
   single opcode integer in `phs_expr_temporal_function`.
2. **Time-unit parameterized** ops like `timestamp` — require an extra
   integer for the time unit, dispatched in `phs_expr_temporal_time_unit`.
3. **String parameterized** ops like `to_string` — pass a C format string
   and dispatch in `phs_expr_temporal_string`.

Mixing these into one function would require union arguments, which the
C ABI makes awkward.  Three functions keep the intent clear.

### Why cast datetime components to Int64 in tests?

The extractors return `i32` (or other types) but the column extraction in
Haskell uses `Pl.column @Int64`.  Explicit casts prevent type mismatch
errors.

## Design

- `TemporalFunction` data type captures all 18 opcodes plus the two
  parameterized variants (`DtTimestamp TimeUnit`, `DtToString Text`).
- `TimeUnit` maps to Rust `TimeUnit::Milliseconds/Microseconds/Nanoseconds`.
- The Rust ABI dispatches to `DateLikeNameSpace` methods directly:
  `expr.dt().year()`, `expr.dt().month()`, etc.
- Error paths validate opcodes and time unit codes, rejecting unknowns
  with `PHS_INVALID_ARGUMENT`.

## Implementation Plan

1. Added `TemporalFunctionExpr` AST constructor to `Expr`.
2. Added `TimeUnit` and `TemporalFunction` data types to `Polars.Expr`.
3. Added smart constructors (`dtYear`, ..., `dtToString`).
4. Added three C ABI functions:
   - `phs_expr_temporal_function` (opcodes 0-17)
   - `phs_expr_temporal_time_unit` (opcode 0=timestamp)
   - `phs_expr_temporal_string` (opcode 0=to_string)
5. Updated `include/polars_hs.h`, `Polars.Internal.Raw`, and `Polars.Internal.Expr`.
6. Created test fixture `test/data/temporal.csv`.
7. Added Rust tests for building each new opcode and error paths.
8. Added Hspec tests validating all scalar components.

## Examples

```haskell
Pl.dtYear (Pl.col "ts")
Pl.dtMonth (Pl.col "ts")
Pl.dtTimestamp Pl.Milliseconds (Pl.col "ts")
Pl.dtToString "%Y-%m-%d" (Pl.col "ts")
Pl.dtIsLeapYear (Pl.col "ts")
```

## Trade-offs

- **Not implemented**: date/time/datetime-returning helpers (`dtDate`,
  `dtTime`, `dtDatetime`).  These return temporal types that are not yet
  extractable as scalar columns through the existing column extraction API.
- **Typed dispatch**: `DtTimestamp` and `DtToString` dispatch through dedicated
  ABI functions, while scalar component functions use a total opcode mapper
  returning `Either PolarsError CInt`.

## Implementation Results

### Rust tests
```
test expr::tests::builds_temporal_function_expressions ... ok
test expr::tests::temporal_function_errors_validate_opcodes ... ok
61 passed; 0 failed
```

### Clippy
Clean (no warnings with `-D warnings`).

### Haskell tests
```
65 examples, 0 failures
Expression DSL temporal namespace
  extracts datetime components from a temporal CSV [✔]
```

### HLint
No hints.

### Verification sweep
- `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml` — 61 passed
- `cargo clippy --manifest-path rust/polars-hs-ffi/Cargo.toml -- -D warnings` — clean
- `stack test --fast` — 65 passed
- `hlint src app test` — no hints
- Examples (iris, groupby, join, columns, series, construction) — all work
