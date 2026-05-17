# Polars HS Eager Sort Unique Design

## Background

`polars-hs` already exposes eager DataFrame column selection, dropping, renaming,
slicing, reversing, null dropping, null counting, and boolean mask filtering.
The next high-value eager parity step is DataFrame sorting and unique row
selection.

Upstream Rust Polars 0.53 exposes:

```rust
DataFrame::sort(by, SortMultipleOptions)
DataFrame::unique(subset, UniqueKeepStrategy, slice)
DataFrame::unique_stable(subset, UniqueKeepStrategy, slice)
```

`SortMultipleOptions` carries per-column descending/null placement, threading,
stable tie handling, and an optional `IdxSize` limit. `UniqueKeepStrategy`
contains `First`, `Last`, `None`, and `Any`.

## Problem

The binding has lazy sorting and lazy unique, but eager DataFrame users still
need to round-trip through lazy execution for common row-ordering and distinct
workflows. This leaves a gap in the eager API surface and complicates tests that
want to compare eager transforms directly.

## Questions and Answers

Q: Should the eager API reuse LazyFrame `UniqueOptions`?

A: Use a DataFrame-prefixed option record for now. It avoids import cycles and
public-name conflicts in the umbrella `Polars` module while keeping the API
clear.

Q: Should sort support per-column descending and null placement in the first
batch?

A: Yes. Rust `SortMultipleOptions` supports a length of one or exactly one
value per sort column. Haskell validates that shape before crossing the FFI.

Q: Should unique expose Rust's optional slice parameter now?

A: No. This batch keeps unique focused on subset, keep strategy, and stable
order. Slice can be added later with the same non-negative validation pattern
used for DataFrame and Series slices.

## Design

Public API:

```haskell
data DataFrameSortOptions = DataFrameSortOptions
    { dataFrameSortDescending :: ![Bool]
    , dataFrameSortNullsLast :: ![Bool]
    , dataFrameSortMultithreaded :: !Bool
    , dataFrameSortMaintainOrder :: !Bool
    , dataFrameSortLimit :: !(Maybe Int)
    }

defaultDataFrameSortOptions :: DataFrameSortOptions
dataFrameSort :: DataFrameSortOptions -> [Text] -> DataFrame -> IO (Either PolarsError DataFrame)

data DataFrameUniqueKeepStrategy
    = DataFrameKeepFirst
    | DataFrameKeepLast
    | DataFrameKeepNone
    | DataFrameKeepAny

data DataFrameUniqueOptions = DataFrameUniqueOptions
    { dataFrameUniqueSubset :: !(Maybe [Text])
    , dataFrameUniqueKeepStrategy :: !DataFrameUniqueKeepStrategy
    , dataFrameUniqueMaintainOrder :: !Bool
    }

defaultDataFrameUniqueOptions :: DataFrameUniqueOptions
dataFrameUnique :: DataFrameUniqueOptions -> DataFrame -> IO (Either PolarsError DataFrame)
```

FFI shape:

```c
int phs_dataframe_sort(
  const phs_dataframe *dataframe,
  const char *const *names,
  uintptr_t names_len,
  const uint8_t *descending,
  uintptr_t descending_len,
  const uint8_t *nulls_last,
  uintptr_t nulls_last_len,
  bool multithreaded,
  bool maintain_order,
  bool has_limit,
  uint64_t limit,
  phs_dataframe **out,
  phs_error **err);

int phs_dataframe_unique(
  const phs_dataframe *dataframe,
  const char *const *subset,
  uintptr_t subset_len,
  bool has_subset,
  int keep_strategy,
  bool maintain_order,
  phs_dataframe **out,
  phs_error **err);
```

Rust mapping:

```rust
let options = SortMultipleOptions::default()
    .with_order_descending_multi(descending)
    .with_nulls_last_multi(nulls_last)
    .with_multithreaded(multithreaded)
    .with_maintain_order(maintain_order);
options.limit = Some(idx_size_from_u64(limit, "dataframe sort limit")?);

df.sort(names, options)?;

if maintain_order {
    df.unique_stable(subset.as_deref(), keep, None)?
} else {
    df.unique(subset.as_deref(), keep, None)?
}
```

## Implementation Plan

1. Add RED Hspec tests for eager DataFrame multi-column sort with per-column
   descending/null placement and optional limit.
2. Add RED Hspec tests for eager DataFrame unique with subset, keep first/last,
   keep none, and stable ordering.
3. Add Haskell option records, defaults, validation, and exports.
4. Add Raw.hs imports and C header declarations.
5. Add Rust ABI functions and option decoding.
6. Run focused tests, Rust release tests, full Stack/Hspec, HLint, and
   `git diff --check`.
7. Append implementation results to this design log.

## Examples

Good:

```haskell
sorted <-
    Pl.dataFrameSort
        Pl.defaultDataFrameSortOptions
            { Pl.dataFrameSortDescending = [False, True]
            , Pl.dataFrameSortNullsLast = [False]
            }
        ["department", "salary"]
        df
```

Good:

```haskell
uniqueDepartments <-
    Pl.dataFrameUnique
        Pl.defaultDataFrameUniqueOptions
            { Pl.dataFrameUniqueSubset = Just ["department"]
            , Pl.dataFrameUniqueKeepStrategy = Pl.DataFrameKeepFirst
            , Pl.dataFrameUniqueMaintainOrder = True
            }
        df
```

Bad:

```haskell
Pl.dataFrameSort Pl.defaultDataFrameSortOptions { Pl.dataFrameSortDescending = [] } ["department"] df
```

## Trade-offs

DataFrame-specific option names are slightly longer, but they avoid ambiguity in
the current public module graph. The unique API omits slice in this batch to
keep the first eager distinct implementation small and well tested. Sort option
arrays use `uint8_t` across the C ABI so per-column booleans have explicit,
portable representation.

## Implementation Results

Implemented files:

- `src/Polars/DataFrame.hs`
- `src/Polars/Internal/Raw.hs`
- `include/polars_hs.h`
- `rust/polars-hs-ffi/src/dataframe.rs`
- `test/Spec.hs`

Delivered APIs:

```haskell
defaultDataFrameSortOptions :: DataFrameSortOptions
dataFrameSort :: DataFrameSortOptions -> [Text] -> DataFrame -> IO (Either PolarsError DataFrame)
defaultDataFrameUniqueOptions :: DataFrameUniqueOptions
dataFrameUnique :: DataFrameUniqueOptions -> DataFrame -> IO (Either PolarsError DataFrame)
```

Test coverage added:

- Multi-column eager DataFrame sort with per-column descending.
- Null placement controls for eager DataFrame sort.
- Sort validation for empty columns, empty/mismatched option vectors, negative
  limit, and Polars index-size overflow.
- Eager DataFrame unique by subset with keep first, keep last, and keep none.
- Unique validation for empty subset.

Verification:

- Focused Hspec sort/unique tests: 5 examples, 0 failures.
- Rust FFI release tests: 87 passed, 0 failed.
- Full Stack/Hspec suite: 123 examples, 0 failures.
- HLint: no hints.
- `git diff --check`: clean.

Notes:

- `SortMultipleOptions.limit` is passed through and validated, but Polars 0.53
  documents it as an optimization hint that may be ignored. Tests validate
  input handling and sort order rather than output truncation.
- The unstable unique branch requires explicit `unique::<String, String>` type
  parameters because Polars 0.53 keeps unused generic parameters on
  `DataFrame::unique`.
