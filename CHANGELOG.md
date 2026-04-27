# Changelog for `polars-hs`

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to the
[Haskell Package Versioning Policy](https://pvp.haskell.org/).

## Unreleased

### Added

- Rust Polars adapter crate with a stable `phs_*` C ABI.
- Managed Haskell `DataFrame` and `LazyFrame` handles.
- Typed `PolarsError` values copied from Rust error handles.
- Eager CSV and Parquet readers.
- DataFrame shape, schema, head, tail, and text rendering operations.
- Pure Haskell expression AST with comparison, boolean, and arithmetic operators.
- Lazy CSV and Parquet scans with filter, select, withColumns, sort, limit, and collect.
- Arrow IPC byte and file round-trip helpers.
- Hspec integration tests for eager, lazy, and IPC behavior.

## 0.1.0.0 - YYYY-MM-DD
