# DataFrame Eager Transforms Phase 1 Design

## Background

`polars-hs` exposes eager `DataFrame` readers, writers, construction, metadata,
head/tail, and typed column extraction. The next eager API step is a narrow set
of structural transforms that map directly to Rust Polars 0.53 `DataFrame`
methods.

Upstream references:

- <https://docs.rs/polars/latest/polars/frame/struct.DataFrame.html>
- <https://docs.rs/polars/latest/polars/prelude/struct.DataFrame.html>

Rust Polars 0.53 has eager methods for `select`, `drop`, `rename`, `slice`,
`reverse`, `drop_nulls`, and `null_count`.

## Problem

Users need common eager transforms without converting to `LazyFrame`. The API
should preserve Rust-owned handle semantics, return `Either PolarsError`, and
validate empty name lists or negative row counts at the Haskell boundary.

## Questions and Answers

Q: What public names avoid conflict with lazy helpers?

A: Use `dataFrame*` prefixes for eager transforms:
`dataFrameSelect`, `dataFrameDropColumns`, `dataFrameRename`,
`dataFrameSlice`, `dataFrameReverse`, `dataFrameDropNulls`, and
`dataFrameNullCount`.

Q: Should this batch include sort, filter, fill, and unique?

A: Keep those for phase 2. Sort and unique need option records; filter needs a
mask transport design; fill needs scalar/strategy design.

Q: How should rename work?

A: Clone the Rust `DataFrame`, call `rename_many`, and return the clone. This
keeps the Haskell handle immutable.

## Design

Public API:

```haskell
dataFrameSelect :: [Text] -> DataFrame -> IO (Either PolarsError DataFrame)
dataFrameDropColumns :: [Text] -> DataFrame -> IO (Either PolarsError DataFrame)
dataFrameRename :: [(Text, Text)] -> DataFrame -> IO (Either PolarsError DataFrame)
dataFrameSlice :: Int -> Int -> DataFrame -> IO (Either PolarsError DataFrame)
dataFrameReverse :: DataFrame -> IO (Either PolarsError DataFrame)
dataFrameDropNulls :: Maybe [Text] -> DataFrame -> IO (Either PolarsError DataFrame)
dataFrameNullCount :: DataFrame -> IO (Either PolarsError DataFrame)
```

Rust ABI:

```c
int phs_dataframe_select(const struct phs_dataframe *dataframe, const char *const *names, uintptr_t len, struct phs_dataframe **out, struct phs_error **err);
int phs_dataframe_drop(const struct phs_dataframe *dataframe, const char *const *names, uintptr_t len, struct phs_dataframe **out, struct phs_error **err);
int phs_dataframe_rename(const struct phs_dataframe *dataframe, const char *const *existing, const char *const *new_names, uintptr_t len, struct phs_dataframe **out, struct phs_error **err);
int phs_dataframe_slice(const struct phs_dataframe *dataframe, int64_t offset, uint64_t len, struct phs_dataframe **out, struct phs_error **err);
int phs_dataframe_reverse(const struct phs_dataframe *dataframe, struct phs_dataframe **out, struct phs_error **err);
int phs_dataframe_drop_nulls(const struct phs_dataframe *dataframe, const char *const *names, uintptr_t len, bool has_subset, struct phs_dataframe **out, struct phs_error **err);
int phs_dataframe_null_count(const struct phs_dataframe *dataframe, struct phs_dataframe **out, struct phs_error **err);
```

## Implementation Plan

1. Add Hspec RED tests over `values.csv`.
2. Add Haskell wrappers and Raw imports.
3. Add Rust ABI helpers and functions.
4. Let cbindgen refresh `include/polars_hs.h`.
5. Update README, CHANGELOG, and parity plan.
6. Verify with Cargo, Stack, HLint, and `git diff --check`.

## Examples

```haskell
Right df <- readCsv "test/data/values.csv"
Right selected <- dataFrameSelect ["name", "age"] df
shape selected `shouldReturn` Right (3, 2)
```

```haskell
Right df <- readCsv "test/data/values.csv"
Right counts <- dataFrameNullCount df
column @Word32 counts "age" `shouldReturn` Right (V.fromList [Just 1])
```

## Trade-offs

- Prefixed eager names keep the umbrella `Polars` module unambiguous.
- This batch covers structural transforms first and leaves option-heavy
  transforms for dedicated design logs.

## Implementation Results

Implemented on 2026-05-17.

Files changed:

- `src/Polars/DataFrame.hs`
- `src/Polars/Internal/Raw.hs`
- `rust/polars-hs-ffi/src/dataframe.rs`
- `include/polars_hs.h`
- `test/Spec.hs`
- `README.md`
- `CHANGELOG.md`
- `docs/superpowers/plans/2026-05-16-polars-hs-polars-053-parity.md`

Tests added:

- select/drop/rename over `test/data/values.csv`
- slice/reverse/dropNulls/nullCount over `test/data/values.csv`
- validation for empty names and negative slice length

Verification:

```bash
cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml
# 85 passed

PATH="$HOME/.ghcup/bin:$PATH" stack --system-ghc test --fast
# 96 examples, 0 failures

hlint src app test
# No hints

git diff --check
# passed
```

Deviation notes:

- Rust rename uses repeated `DataFrame::rename` calls because it keeps schema
  and column lookup names aligned for typed extraction.
