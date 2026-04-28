# polars-hs Agent Notes

## Project goal

Evolve `polars-hs` into a safe Haskell binding over Rust Polars 0.53 through a stable Rust-owned `phs_*` C ABI and type-directed Haskell APIs.

## Current completed expansion milestones

- MVP eager/lazy DataFrame binding with typed errors and managed handles.
- GroupBy/Agg, lazy joins, typed column extraction, owned Series handles, Series transforms, append/shift, and Series/DataFrame construction.
- Arrow C Data Interface RecordBatch import/export and Series single-array import/export.
- Dataset-driven committed fixtures plus opt-in NYC Taxi coverage.
- Expression DSL Phase 1 Foundation/Core:
  - casts and strict casts;
  - null, NaN, finite, and infinite predicates;
  - fillNull/fillNan;
  - conditionals;
  - median/std/var/quantile/nUnique;
  - cumulative expressions;
  - rank;
  - expression filtering, slicing, sorting, and window `over`.

## Current strongest expansion priorities

1. **Expression DSL Phase 2: String namespace**
   - Add basic string expression helpers first: contains literal, startsWith, endsWith, strip, stripStart, stripEnd, lowercase, uppercase, lenBytes, lenChars, slice, head, and tail.
   - Add regex/extract/replace/split helpers as a second string batch after feature-flag and List dtype test scope is clear.
   - Keep public Haskell expressions pure and compile them into short-lived Rust expression handles at FFI boundaries.

2. **Data type coverage**
   - Add typed construction, extraction, casting, Arrow round-trip tests, and schema parsing for `Int8/Int16/Int32`, `UInt8/UInt16/UInt32/UInt64`, and `Float32` first.
   - Add temporal scalar support next: `Date`, `Datetime`, `Time`, and `Duration`.
   - Add `Binary`, `Decimal`, `Categorical`, `List`, `Array`, and `Struct` after scalar/temporal coverage stabilizes.

3. **LazyFrame coverage**
   - Add plan introspection and execution helpers: `explain`, `profile`, collect-all, and streaming collect where Polars 0.53 features allow.
   - Add common transformations: `drop`, `rename`, `fill_null`, `drop_nulls`, `slice`, `head`, `tail`, `unique`, `explode`, `unpivot`, and `null_count`.
   - Keep initial selector support narrow and explicit; expand selector-style APIs with dedicated tests later.

4. **IO coverage**
   - Add write CSV and write Parquet first, with temp-file round-trip tests.
   - Add reader/writer options for CSV, Parquet, and IPC after the first writer APIs land.
   - Add JSON/NDJSON, Avro, IPC stream, cloud/object-store, and SQL/catalog scan support in later IO phases.

5. **Join coverage**
   - Add semi, anti, and cross joins next.
   - Add asof and non-equi joins after temporal dtype support provides stronger fixtures.
   - Add join validation, null equality, coalescing, and parallel execution options after join-mode coverage.

6. **GroupBy and time-series coverage**
   - Add dynamic groupby and rolling groupby after temporal dtype support.
   - Add eager groupby and richer aggregations such as list aggregation in later phases.

7. **Arrow interop coverage**
   - Extend Arrow C Data Interface support from DataFrame RecordBatch and Series single-array interop to Arrow C Stream and chunked streaming.
   - Add broad dtype and nested dtype compatibility tests alongside dtype and collection namespace work.

8. **Eager Series/DataFrame API coverage**
   - Extend Series with filter, take, arithmetic, fill/null operations, stats, sampling, and explode.
   - Extend DataFrame with eager select, filter, drop, rename, sort, join, groupby, and concat operations.

## Next task queue

1. **Task A — Expression DSL String Namespace design and plan**
   - Design public APIs for basic string helpers.
   - Add a compact string opcode ABI family.
   - Add Hspec tests over committed text fixtures, including ASCII and non-ASCII cases.
   - Recommended first implementation batch: literal contains, startsWith, endsWith, lowercase, uppercase, lenBytes, lenChars, strip, slice/head/tail.

2. **Task B — Data Type Matrix Phase 1**
   - Add typed `series @xxx` and `column @xxx` for narrow signed/unsigned integers and Float32.
   - Add cast, schema, and Arrow round-trip tests for the same dtype set.

3. **Task C — LazyFrame explain/profile and simple transformations**
   - Add `explain`, `profile`, `drop`, `rename`, `slice`, `head`, `tail`, `dropNulls`, `fillNull`, `nullCount`, and `unique`.
   - Validate with result-level Hspec tests and plan-text assertions.

4. **Task D — CSV and Parquet writers**
   - Add safe writer APIs with minimal option records.
   - Verify write/read round-trips preserve shape, schema, nulls, and representative values.

5. **Task E — Join Modes Phase 2**
   - Add semi, anti, and cross joins.
   - Extend `JoinType` and fixture-driven result tests.

Detailed companion roadmap: `docs/superpowers/specs/2026-04-28-polars-hs-roadmap-next-tasks.md`.

Implementation preference: advance this roadmap through small design logs, TDD, focused Rust ABI additions, safe Haskell wrappers, dataset/Arrow-driven tests, and full verification before each commit.
