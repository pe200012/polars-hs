# Polars 0.53 Parity Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Expand `polars-hs` from the current MVP plus Expression DSL batches toward Rust Polars 0.53 parity through verified, reviewable feature families.

**Architecture:** Keep Rust Polars calls behind the stable `phs_*` C ABI. Public Haskell APIs use typed values, typed option records, and `Either PolarsError` for recoverable failures. Each batch adds tests before implementation and records verification output in the design log.

**Tech Stack:** Haskell GHC 9.12.2, Stack, Rust 2024, Polars 0.53, C ABI, Hspec, Cargo tests, HLint, Jujutsu.

---

### Task 1: Data Type Matrix Phase 1

Status: implemented and verified on 2026-05-16.

**Files:**
- Modify: `src/Polars/Internal/ColumnEncode.hs`
- Modify: `src/Polars/Internal/ColumnDecode.hs`
- Modify: `src/Polars/Series.hs`
- Modify: `src/Polars/Column.hs`
- Modify: `src/Polars/Internal/Raw.hs`
- Modify: `rust/polars-hs-ffi/src/series.rs`
- Modify: `rust/polars-hs-ffi/src/dataframe.rs`
- Modify: `include/polars_hs.h`
- Modify: `test/Spec.hs`
- Modify: `test/ArrowRecordBatch.hs`
- Modify: `README.md`
- Modify: `CHANGELOG.md`

**Step 1: Write failing Hspec tests**

Add constructor/extractor/cast/schema cases for:

```haskell
Int8, Int16, Int32, Word8, Word16, Word32, Word64, Float
```

Expected RED: missing instances and missing value reader functions.

**Step 2: Write failing Arrow tests**

Extend RecordBatch and Series Arrow round-trip tests to cover all new scalar
dtypes.

Expected RED: missing typed APIs or unsupported dtype handling.

**Step 3: Implement Haskell encoding/decoding**

Add fixed-width little-endian encoders/decoders matching the existing null/value
tag format.

**Step 4: Implement Rust FFI**

Add `phs_series_new_*`, `phs_series_values_*`, and dtype cast codes for the new
scalar dtypes.

**Step 5: Wire public APIs**

Add `SeriesFrom`, `SeriesCast`, and `Column` instances plus named helpers only
where existing naming conventions already provide them.

**Step 6: Verify**

Run:

```bash
cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml
stack test --fast
hlint src app test
git diff --check
```

Current verification:

```bash
cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml
# 85 passed

PATH="$HOME/.ghcup/bin:$PATH" stack --system-ghc test --fast
# 79 examples, 0 failures

hlint src app test
# No hints

git diff --check
# passed
```

**Step 7: Commit**

```bash
jj --config signing.behavior=drop describe -m "feat(dtype): add scalar dtype matrix phase 1"
jj --config signing.behavior=drop new
```

### Task 2: LazyFrame Plan and Transformations

**Files:**
- Modify: `src/Polars/LazyFrame.hs`
- Modify: `src/Polars/Internal/Raw.hs`
- Modify: `rust/polars-hs-ffi/src/lazyframe.rs`
- Modify: `include/polars_hs.h`
- Modify: `test/Spec.hs`
- Modify: `README.md`
- Modify: `CHANGELOG.md`

**Step 1:** Add RED tests for `explain`, `drop`, `rename`, `slice`, `head`,
`tail`, `dropNulls`, `fillNull`, `fillNan`, `nullCount`, and `unique`.

**Step 2:** Implement Rust ABI and Haskell wrappers.

**Step 3:** Add `profile` after the basic transformations pass.

**Step 4:** Verify with focused Hspec, then full suite.

### Task 3: CSV and Parquet Writers

**Files:**
- Modify: `src/Polars/DataFrame.hs`
- Modify: `src/Polars/Internal/Raw.hs`
- Modify: `rust/polars-hs-ffi/src/dataframe.rs`
- Modify: `include/polars_hs.h`
- Modify: `test/Spec.hs`
- Modify: `README.md`
- Modify: `CHANGELOG.md`

**Step 1:** Add temp-file round-trip tests for `writeCsv` and `writeParquet`.

**Step 2:** Add minimal writer option records.

**Step 3:** Implement Rust writers and Haskell wrappers.

**Step 4:** Verify round trips preserve shape, schema, nulls, text, integers,
and floats.

### Task 4: Join Modes Phase 2

**Files:**
- Modify: `src/Polars/Join.hs`
- Modify: `rust/polars-hs-ffi/src/lazyframe.rs`
- Modify: `test/Spec.hs`
- Modify: `README.md`
- Modify: `CHANGELOG.md`

**Step 1:** Add tests for semi, anti, and cross joins.

**Step 2:** Extend `JoinType` and Rust join mapping.

**Step 3:** Verify existing join tests and new result-level tests.

### Task 5: Eager DataFrame and Series Core Transforms

**Files:**
- Modify: `src/Polars/DataFrame.hs`
- Modify: `src/Polars/Series.hs`
- Modify: `src/Polars/Internal/Raw.hs`
- Modify: `rust/polars-hs-ffi/src/dataframe.rs`
- Modify: `rust/polars-hs-ffi/src/series.rs`
- Modify: `include/polars_hs.h`
- Modify: `test/Spec.hs`
- Modify: `README.md`
- Modify: `CHANGELOG.md`

**Step 1:** Add DataFrame tests for select/drop/rename/slice/filter/sort/reverse/nullCount/dropNulls/fill/unique.

**Step 2:** Add Series tests for filter/take/slice/fill/null predicates/arithmetic/stats.

**Step 3:** Implement one subfamily at a time with focused verification.

### Task 6: Nested and Temporal Design Follow-through

**Files:**
- Create: `docs/superpowers/specs/2026-05-16-polars-hs-nested-temporal-schema-design.md`
- Create: `docs/superpowers/plans/2026-05-16-polars-hs-nested-temporal-schema.md`

**Step 1:** Design structured schema ABI for parameterized and nested dtypes.

**Step 2:** Design temporal scalar Haskell newtypes and binary/list/array/struct transport.

**Step 3:** Implement after Task 1 establishes scalar dtype coverage.
