# Temporal Expression DSL Plan

**Date**: 2026-04-29
**Scope**: Expression DSL temporal namespace (Phase 3A)

## Goal

Add temporal namespace helpers to the Haskell Expression DSL, matching
Polars' `DateLikeNameSpace` API for datetime extraction and formatting.

## Deliverables

1. **Rust C ABI**: `phs_expr_temporal_function`, `phs_expr_temporal_time_unit`,
   `phs_expr_temporal_string`.
2. **Haskell types**: `TemporalFunction`, `TimeUnit`, `TemporalFunctionExpr`
   constructor, and smart constructors.
3. **Tests**: Rust unit tests for all opcodes and error paths; Hspec
   integration test validating scalar component extraction.
4. **Docs**: Design log, updated README and CHANGELOG.

## Steps

- [x] Create `test/data/temporal.csv` fixture
- [x] Add Rust C ABI functions and tests
- [x] Add `phs_expr_temporal_function` dispatch with opcodes 0–17
- [x] Add `phs_expr_temporal_time_unit` dispatch for timestamp
- [x] Add `phs_expr_temporal_string` dispatch for to_string
- [x] Update `include/polars_hs.h`
- [x] Add FFI imports in `Polars.Internal.Raw`
- [x] Add `TemporalFunctionExpr` compilation in `Polars.Internal.Expr`
- [x] Add `temporalFunctionCode` and `timeUnitCode` in `Polars.Internal.Expr`
- [x] Add Hspec integration test
- [x] Validate with `cargo test`, `cargo clippy`, `stack test --fast`, `hlint`
- [x] Verify all examples still pass
- [x] Create design log
- [x] Update README and CHANGELOG
