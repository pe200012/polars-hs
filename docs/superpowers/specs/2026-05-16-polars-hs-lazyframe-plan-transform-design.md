# LazyFrame Plan and Transform Design

## Background

`polars-hs` already wraps lazy scans, filters, projections, `withColumns`,
sorting, limits, group-by aggregations, and lazy joins. Rust Polars 0.53 exposes
additional `LazyFrame` plan introspection and common dataframe-level transforms
that can fit the current owned-handle ABI.

Sources checked:
- docs.rs `polars::prelude::LazyFrame` 0.53 method list.
- Local upstream source:
  `/home/pe200012/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/polars-lazy-0.53.0/src/frame/mod.rs`.
- Existing project wrappers:
  `src/Polars/LazyFrame.hs`,
  `src/Polars/Internal/Raw.hs`,
  `rust/polars-hs-ffi/src/lazyframe.rs`.

## Problem

The current lazy API cannot inspect plans, rename/drop columns, slice rows,
drop/fill nulls/NaNs, count nulls, deduplicate rows, or profile execution.
These features block common Polars workflows and make later parity work harder
to test.

## Questions and Answers

Q: How should `head` and `tail` be named?
A: Use `lazyHead` and `lazyTail` in `Polars.LazyFrame` to keep `Polars` re-export
usage clear alongside eager `Polars.DataFrame.head` and `Polars.DataFrame.tail`.

Q: How should column subsets cross the FFI boundary?
A: Reuse the existing CString array pattern. Rust converts names to
`by_name(names, strict, false)` selectors. `Nothing` subsets pass
`has_subset = false`.

Q: How should unique options be represented?
A: Add a small Haskell record:

```haskell
data UniqueKeepStrategy = KeepFirst | KeepLast | KeepNone | KeepAny

data UniqueOptions = UniqueOptions
  { uniqueSubset :: !(Maybe [Text])
  , uniqueKeepStrategy :: !UniqueKeepStrategy
  , uniqueMaintainOrder :: !Bool
  }
```

Q: How should `profile` return its two DataFrames?
A: Return `(DataFrame, DataFrame)` from Haskell. Rust writes two out pointers in
one FFI call and frees both through normal `ForeignPtr` finalizers.

## Design

### Public Haskell API

```haskell
explain :: Bool -> LazyFrame -> IO (Either PolarsError Text)
profile :: LazyFrame -> IO (Either PolarsError (DataFrame, DataFrame))

dropColumns :: [Text] -> LazyFrame -> IO (Either PolarsError LazyFrame)
rename :: RenameOptions -> [(Text, Text)] -> LazyFrame -> IO (Either PolarsError LazyFrame)

slice :: Int -> Int -> LazyFrame -> IO (Either PolarsError LazyFrame)
lazyHead :: Int -> LazyFrame -> IO (Either PolarsError LazyFrame)
lazyTail :: Int -> LazyFrame -> IO (Either PolarsError LazyFrame)

dropNulls :: Maybe [Text] -> LazyFrame -> IO (Either PolarsError LazyFrame)
fillNulls :: Expr -> LazyFrame -> IO (Either PolarsError LazyFrame)
fillNans :: Expr -> LazyFrame -> IO (Either PolarsError LazyFrame)
nullCount :: LazyFrame -> IO (Either PolarsError LazyFrame)
unique :: UniqueOptions -> LazyFrame -> IO (Either PolarsError LazyFrame)
```

Validation rules:
- `dropColumns []` returns `InvalidArgument`.
- `rename []` returns `InvalidArgument`.
- `rename` requires equal existing/new names because the Rust API truncates
  mismatched iterators.
- `slice _ len`, `lazyHead`, and `lazyTail` reject negative lengths.
- `Just []` subsets for `dropNulls` and `unique` return `InvalidArgument`.

### Rust ABI

```c
int phs_lazyframe_explain(const phs_lazyframe *, bool optimized, phs_bytes **, phs_error **);
int phs_lazyframe_profile(const phs_lazyframe *, phs_dataframe **result, phs_dataframe **profile, phs_error **);
int phs_lazyframe_drop(const phs_lazyframe *, const char *const *names, uintptr_t len, phs_lazyframe **, phs_error **);
int phs_lazyframe_rename(const phs_lazyframe *, const char *const *old, const char *const *new, uintptr_t len, bool strict, phs_lazyframe **, phs_error **);
int phs_lazyframe_slice(const phs_lazyframe *, int64_t offset, uint64_t len, phs_lazyframe **, phs_error **);
int phs_lazyframe_head(const phs_lazyframe *, uint64_t n, phs_lazyframe **, phs_error **);
int phs_lazyframe_tail(const phs_lazyframe *, uint64_t n, phs_lazyframe **, phs_error **);
int phs_lazyframe_drop_nulls(const phs_lazyframe *, const char *const *names, uintptr_t len, bool has_subset, phs_lazyframe **, phs_error **);
int phs_lazyframe_fill_null(const phs_lazyframe *, const phs_expr *, phs_lazyframe **, phs_error **);
int phs_lazyframe_fill_nan(const phs_lazyframe *, const phs_expr *, phs_lazyframe **, phs_error **);
int phs_lazyframe_null_count(const phs_lazyframe *, phs_lazyframe **, phs_error **);
int phs_lazyframe_unique(const phs_lazyframe *, const char *const *names, uintptr_t len, bool has_subset, int keep, bool maintain_order, phs_lazyframe **, phs_error **);
```

Keep strategy codes:
- `0 = First`
- `1 = Last`
- `2 = None`
- `3 = Any`

### Tests

Hspec:
- `explain True` and `explain False` return plan text containing scan and
  projection/filter markers.
- `dropColumns` and `rename` preserve expected schema and values.
- `slice`, `lazyHead`, and `lazyTail` return expected row subsets.
- `dropNulls Nothing`, `fillNulls`, `fillNans`, `nullCount`, and `unique` produce
  value-level assertions over committed fixtures.
- Invalid argument tests cover empty drop/rename/subset and negative lengths.

Rust:
- Cargo tests compile the new ABI surface and keep existing lazy filter, groupby,
  and join collect smoke tests passing.
- Additional Rust-side ABI shape tests can be added with the next LazyFrame
  transform batch.

## Implementation Plan

1. Add Hspec RED tests for the public LazyFrame API.
2. Add Raw imports and Haskell wrappers.
3. Add Rust ABI functions and helper conversion routines.
4. Let cbindgen refresh `include/polars_hs.h`.
5. Update README and CHANGELOG.
6. Verify with Cargo, Stack, HLint, and `git diff --check`.

## Examples

```haskell
Right lf0 <- scanCsv valuesCsv
Right lf1 <- rename defaultRenameOptions [("age", "years")] lf0
Right df <- collect lf1
column @Int64 df "years" `shouldReturn` Right (V.fromList [Just 34, Nothing, Just 29])
```

```haskell
Right lf0 <- scanCsv salesCsv
Right lf1 <- unique defaultUniqueOptions { uniqueSubset = Just ["department"] } lf0
Right df <- collect lf1
shape df `shouldReturn` Right (2, 4)
```

## Trade-offs

- Domain-prefixed `lazyHead` and `lazyTail` keep the aggregate `Polars` import
  practical while still exposing the Rust operations.
- Domain-prefixed `fillNulls` and `fillNans` keep LazyFrame fill operations
  distinct from expression-level `fillNull` and `fillNan` in aggregate imports.
- `Maybe [Text]` subsets keep selector support narrow and explicit. Broader
  selectors can build on the same ABI after a selector design log.
- `profile` executes the query. Rust Polars 0.53 can return `no data to time`
  when a plan produces no executor timings, so the ABI returns the collected
  result plus an empty `node/start/end` profile frame in that case.

## Implementation Results

Implemented public LazyFrame helpers in `src/Polars/LazyFrame.hs`, Raw imports
in `src/Polars/Internal/Raw.hs`, Rust ABI functions in
`rust/polars-hs-ffi/src/lazyframe.rs`, cbindgen header updates in
`include/polars_hs.h`, and Hspec coverage in `test/Spec.hs`.

API deviation from the initial draft: LazyFrame fill helpers are named
`fillNulls` and `fillNans` to keep aggregate `Polars` imports unambiguous with
expression-level `fillNull` and `fillNan`.

Implementation deviation from the initial draft: `profile` preserves successful
query execution even when Polars returns no timer rows by falling back to
`collect` plus an empty profile frame with the standard profile schema.

Verification on 2026-05-17:

```bash
cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml
# 85 passed

PATH="$HOME/.ghcup/bin:$PATH" stack --system-ghc test --fast
# 86 examples, 0 failures

hlint src app test
# No hints

git diff --check
# passed
```
