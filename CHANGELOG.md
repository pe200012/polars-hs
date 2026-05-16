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
- LazyFrame plan inspection, profiling, and common transforms: dropColumns, rename, slice, lazyHead, lazyTail, dropNulls, fillNulls, fillNans, nullCount, and unique.
- Arrow IPC byte and file round-trip helpers.
- Hspec integration tests for eager, lazy, and IPC behavior.
- Grouped lazy aggregation helpers.
- Lazy join helpers with join options and suffix handling.
- Typed DataFrame column extraction for bool, int64, double, and text columns.
- Unified `column @xxx` API with Series handles and `Vector (Maybe a)` typed value readers.
- Scalar dtype matrix coverage for typed Series construction, Series casts, Series extraction, column extraction, schema parsing, and Arrow RecordBatch/Series round trips across `Int8`, `Int16`, `Int32`, `UInt8`, `UInt16`, `UInt32`, `UInt64`, and `Float32`.
- Series transform helpers for type-application casts, rename, sort, unique, reverse, and dropNulls.
- Series append and shift helpers for owned Series handles.
- Series and DataFrame constructors from Haskell vectors with null preservation.
- Arrow C Data Interface RecordBatch import for managed DataFrame construction.
- Arrow C Data Interface RecordBatch export from managed DataFrames.
- Arrow C Data Interface single-array import/export for managed Series.
- Dataset-driven smoke fixtures generated from Polars public data and Metasyn synthetic data.
- Dataset-driven lazy query smoke tests for generated CSV fixtures.
- Opt-in NYC Taxi Parquet real-world test script with lazy grouped query coverage.
- Core Expression DSL coverage for casts, predicates, fills, conditionals, statistics, cumulative expressions, ranking, expression sorting/filtering/slicing, and window `over`.
- String Expression DSL helpers for literal contains, starts/ends predicates, strip variants (strip, stripStart, stripEnd, stripPrefix, stripSuffix), regex escape, extractAll, case conversion, byte/character lengths, character slicing/head/tail, regex contains/find/extract/count, and replace/replaceAll.
- Temporal Expression DSL helpers for datetime component extraction (year, month, day, hour, …), timestamp conversion, strftime formatting, millennium/century/days-in-month, and leap-year detection.
- String split helpers (`strSplit`, `strSplitInclusive`) returning `List[String]` expressions.
- List Expression DSL helpers for list length, first/last element, indexed get, string join, element contains, and count matches on string splits.
- Scalar predicate Expression DSL helpers for deduplication (`isDuplicated`, `isUnique`, `isFirstDistinct`, `isLastDistinct`), range checks (`isBetween` with interval variants), floating-point closeness (`isClose`), set membership (`isIn`), and value clipping (`clip`, `clipMin`, `clipMax`).
- Horizontal and coalesce Expression DSL helpers for row-wise operations (`sumHorizontal`, `meanHorizontal`, `maxHorizontal`, `minHorizontal`, `anyHorizontal`, `allHorizontal`, `coalesce`).

- Name Expression DSL helpers for column name manipulation (`nameKeep`, `namePrefix`, `nameSuffix`, `nameReplace`, `nameToLowercase`, `nameToUppercase`).
- String n-ary Expression DSL helpers for horizontal string concatenation (`concatStr`) with separator and null handling, and format-string interpolation (`formatStr`) with `{}` placeholders.

## 0.1.0.0 - YYYY-MM-DD
