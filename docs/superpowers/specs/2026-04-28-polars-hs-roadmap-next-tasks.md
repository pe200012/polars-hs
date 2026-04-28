# Design Log: Roadmap and Next Tasks After Expression DSL Core

## Background

`polars-hs` now has a safe Haskell binding over Rust Polars 0.53 using a stable `phs_*` C ABI. The current local `master` includes Expression DSL Phase 1 Foundation/Core: casts, null and NaN predicates, fill operations, conditionals, statistics, cumulative expressions, rank, expression filtering/slicing/sorting, and window `over`.

The upstream Polars 0.53 surface remains much larger than the binding. Recent upstream lookup confirms major feature families around string expressions, temporal expressions, list/array/struct namespaces, LazyFrame plan helpers, richer IO writers, and additional join modes.

## Problem

The roadmap should reflect completed Expression DSL Core work and give the next implementation session a small set of high-value tasks with clear acceptance criteria. The next tasks should preserve the current architecture:

- pure public Haskell values;
- Rust-owned `phs_*` ABI;
- safe `Either PolarsError a` public APIs;
- dataset-driven Hspec tests;
- focused Cargo feature expansion;
- full verification before merge.

## Questions and Answers

### Q1. What changed after Expression DSL Core?

Answer: The first Expression DSL phase is complete. The immediate expression gap moved from core primitives to namespaces, starting with strings.

### Q2. Which task should come next?

Answer: Expression DSL Phase 2 String namespace is the strongest next task. It builds directly on the new expression compiler pattern, uses existing string fixtures, and unlocks common dataframe workflows with a compact ABI family.

### Q3. Which tasks should stay near the front of the queue?

Answer: Data type coverage and LazyFrame coverage should stay close behind string expressions. Data type coverage improves safety across construction, extraction, casting, and Arrow interop. LazyFrame helpers improve day-to-day query ergonomics with small ABI additions.

### Q4. How should task size be controlled?

Answer: Each next task should add one coherent family with a design log, TDD tests, Rust ABI, Haskell wrapper, documentation, and verification. Feature flags should be added only when a delivered API needs them.

## Design

### Updated roadmap order

1. **Expression DSL Phase 2: String namespace**
   - Add literal predicates and transformations: contains literal, startsWith, endsWith, strip, stripStart, stripEnd, lower, upper, lenBytes, lenChars, slice, head, tail.
   - Add regex-backed functions in the same phase only if Cargo feature impact stays small: contains regex, extract, replace, replaceAll, countMatches.
   - Keep API names Haskell-friendly while following Polars `str` namespace semantics.

2. **Data type coverage: scalar and temporal base matrix**
   - Add typed construction/extraction for `Int8/Int16/Int32`, `UInt8/UInt16/UInt32/UInt64`, and `Float32`.
   - Add Date/Datetime/Time/Duration schema parsing and cast result tests.
   - Extend Arrow RecordBatch/Series round-trip tests across these dtypes.

3. **LazyFrame coverage: plan and common transformations**
   - Add `explain`, optimized/unoptimized plan text, and `profile`.
   - Add transformations: `drop`, `rename`, `slice`, `head`, `tail`, `dropNulls`, `fillNull`, `nullCount`, `unique`, `explode`.
   - Use existing Polars LazyFrame methods and keep selector scope simple for the first pass.

4. **IO coverage: writers and reader options**
   - Add write CSV, write Parquet, and writer options with deterministic round-trip tests.
   - Add CSV/Parquet reader options after writer basics land.

5. **Join coverage: additional modes and options**
   - Add semi, anti, cross, then asof joins.
   - Add validation/null-equality/coalescing options after mode coverage.

6. **GroupBy and time-series coverage**
   - Add dynamic groupby and rolling groupby after temporal dtype support gives stable test fixtures.
   - Add eager groupby and list aggregation later in the groupby track.

7. **Arrow streaming and nested dtype interop**
   - Add Arrow C Stream and chunked streaming after scalar/temporal dtype tests stabilize.
   - Add List/Array/Struct Arrow tests alongside collection namespace work.

8. **Eager Series/DataFrame expansion**
   - Add Series filter/take/arithmetic/fill/stats and DataFrame eager select/filter/drop/rename/sort/join/groupby/concat.
   - Reuse expression and dtype helpers from earlier tasks.

### Next task queue

#### Task A — Expression DSL String Namespace Design and Plan

Scope:
- `src/Polars/Expr.hs`
- `src/Polars/Internal/Expr.hs`
- `src/Polars/Internal/Raw.hs`
- `rust/polars-hs-ffi/src/expr.rs`
- `include/polars_hs.h`
- `test/Spec.hs`
- `README.md`
- `CHANGELOG.md`

Candidate public APIs:

```haskell
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

Acceptance:
- Hspec result tests over committed CSV fixtures cover ASCII and non-ASCII strings.
- Rust ABI tests cover unary/binary/ternary string opcodes and invalid opcode errors.
- `cargo test`, `cargo clippy -D warnings`, `stack test --fast`, and `hlint` pass.

#### Task B — Data Type Matrix Phase 1

Scope:
- typed Series/DataFrame construction and extraction;
- cast tests for scalar numeric dtypes;
- schema parsing assertions;
- Arrow round-trip for scalar numeric and Float32.

Acceptance:
- `series @Int32`, `column @Int32`, and corresponding unsigned/Float32 APIs work with `Vector (Maybe a)`.
- Casted lazy columns can be collected and extracted with the new typed APIs.
- Arrow RecordBatch and Series round-trip tests cover all added scalar dtypes.

#### Task C — LazyFrame Explain/Profile and Simple Transformations

Scope:
- `explain`, `profile`;
- `drop`, `rename`, `slice`, `head`, `tail`, `dropNulls`, `fillNull`, `nullCount`, `unique`.

Acceptance:
- Plan text contains expected column names and operation keywords.
- Transformations have result-level Hspec checks over committed fixtures.
- Error paths cover missing columns and invalid arguments.

#### Task D — CSV and Parquet Writers

Scope:
- write CSV and Parquet file functions;
- small writer option records;
- temp-file round-trip tests.

Acceptance:
- Writing then reading preserves shape, schema, nulls, and representative values.
- Writer failures return typed `PolarsError`.

#### Task E — Join Modes Phase 2

Scope:
- semi, anti, cross joins;
- API extension to `JoinType`;
- fixture-driven tests.

Acceptance:
- Result rows match Polars semantics for matched, unmatched, and cross-product cases.
- Existing join tests continue to pass.

## Implementation Plan

1. Update `AGENTS.md` with completed milestones, refreshed roadmap order, and the next task queue.
2. Keep this design log as the detailed companion reference for future planning.
3. Verify documentation-only changes with marker scan and whitespace checks.
4. Commit the roadmap update with Jujutsu.

## Examples

✅ Next task selection:

```text
Start with Task A: Expression DSL String Namespace Design and Plan.
Rationale: It reuses the new Expr compiler architecture and gives users high-frequency text operations.
```

✅ Task sizing:

```text
String namespace Phase 2A: literal predicates, case conversion, length, slicing.
String namespace Phase 2B: regex extract/replace and split/list-producing operations.
```

## Trade-offs

- Prioritizing string expressions continues the Expression DSL momentum and keeps changes localized.
- Prioritizing dtype coverage improves type safety across the whole binding and needs broader FFI/value-code work.
- Prioritizing LazyFrame helpers improves user ergonomics with smaller public API concepts and more Rust ABI functions.
- Splitting string namespace into literal/basic and regex/list-producing subsets keeps feature flags and tests easier to review.
