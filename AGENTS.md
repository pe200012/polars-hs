# polars-hs Agent Notes

## Roadmap from upstream Polars comparison

Project goal: evolve `polars-hs` into a safe Haskell binding over Rust Polars 0.53 through a stable `phs_*` C ABI and type-directed Haskell APIs.

Current strongest expansion priorities:

1. **Expression DSL completeness**
   - Extend `Polars.Expr` beyond `col`, literals, binary ops, `not_`, aliases, and basic aggregations.
   - Add casts, null predicates, NaN predicates, fill operations, string functions, temporal functions, conditional expressions, window expressions, ranking, cumulative operations, expression filtering, slicing, sorting, and richer aggregations.
   - Keep public Haskell expressions pure and compile them into short-lived Rust expression handles at FFI boundaries.

2. **Data type coverage**
   - Add typed construction, extraction, casting, Arrow round-trip tests, and schema parsing for `Int8/Int16/Int32`, `UInt8/UInt16/UInt32/UInt64`, `Float32`, `Date`, `Datetime`, `Time`, `Duration`, `Binary`, `Decimal`, `Categorical`, `List`, `Array`, and `Struct`.

3. **LazyFrame coverage**
   - Add plan introspection and execution helpers: `explain`, `profile`, `collect_all`, and streaming collect.
   - Add transformations: `drop`, `rename`, `cast`, `fill_null`, `drop_nulls`, `slice`, `head`, `tail`, `unique`, `explode`, `pivot`, `unpivot`, and `null_count`.

4. **Join coverage**
   - Add semi, anti, cross, asof, and non-equi joins.
   - Add join validation, null equality, coalescing, and parallel execution options.

5. **IO coverage**
   - Add reader and writer options for CSV, Parquet, and IPC.
   - Add write CSV/Parquet, JSON/NDJSON, Avro, IPC stream, cloud/object-store, and SQL/catalog scan support.

6. **GroupBy and time-series coverage**
   - Add dynamic groupby, rolling groupby, eager groupby, and richer aggregations such as median, quantile, std, var, and list aggregation.

7. **Arrow interop coverage**
   - Extend Arrow C Data Interface support from DataFrame RecordBatch and Series single-array interop to Arrow C Stream and chunked streaming.
   - Add broad dtype and nested dtype compatibility tests.

8. **Eager Series/DataFrame API coverage**
   - Extend Series with filter, take, arithmetic, fill/null operations, stats, sampling, and explode.
   - Extend DataFrame with eager select, filter, drop, rename, sort, join, groupby, and concat operations.

Implementation preference: advance this roadmap through small design logs, TDD, focused Rust ABI additions, safe Haskell wrappers, dataset/Arrow-driven tests, and full verification before each commit.
