# DataFrame Partition By Design

## Background

Rust Polars 0.53 exposes eager DataFrame partitioning behind the `partition_by` feature. It splits a DataFrame into one DataFrame per group and supports stable first-seen group order plus optional removal of the key columns.

Upstream references:

- `DataFrame::partition_by<I, S>(&self, cols: I, include_key: bool) -> PolarsResult<Vec<DataFrame>>`
- `DataFrame::partition_by_stable<I, S>(&self, cols: I, include_key: bool) -> PolarsResult<Vec<DataFrame>>`
- docs.rs lists `partition_by` and `partition_by_stable`: https://docs.rs/polars/latest/polars/frame/struct.DataFrame.html

## Problem

`polars-hs` has lazy group-by aggregation and eager joins, but no eager API for splitting a DataFrame into grouped subframes. This leaves common downstream workflows, such as writing one file per group or applying Haskell-side actions per group, without a direct Polars-backed primitive.

## Questions And Answers

Q: Does this need a Rust feature flag?

A: Yes. Add `partition_by` to the pinned `polars = 0.53.0` feature list.

Q: How should Rust return `Vec<DataFrame>` over C FFI?

A: Return an opaque temporary `phs_dataframe_array` handle. Haskell queries length, clones each frame into an owned DataFrame handle, and frees the temporary array.

Q: Should stable order be configurable?

A: Yes. A `maintainOrder` Boolean chooses `partition_by_stable` when true and `partition_by` when false.

## Design

Public Haskell API:

```haskell
data DataFramePartitionOptions = DataFramePartitionOptions
    { dataFramePartitionColumns :: ![Text]
    , dataFramePartitionIncludeKey :: !Bool
    , dataFramePartitionMaintainOrder :: !Bool
    }

defaultDataFramePartitionOptions :: DataFramePartitionOptions
dataFramePartitionBy :: DataFramePartitionOptions -> DataFrame -> IO (Either PolarsError [DataFrame])
```

ABI:

```c
struct phs_dataframe_array;

int phs_dataframe_partition_by(const struct phs_dataframe *dataframe,
                               const char *const *names,
                               uintptr_t names_len,
                               bool include_key,
                               bool maintain_order,
                               struct phs_dataframe_array **out,
                               struct phs_error **err);

uintptr_t phs_dataframe_array_len(const struct phs_dataframe_array *array);

int phs_dataframe_array_get(const struct phs_dataframe_array *array,
                            uintptr_t index,
                            struct phs_dataframe **out,
                            struct phs_error **err);

void phs_dataframe_array_free(struct phs_dataframe_array *array);
```

Ownership:

- `phs_dataframe_partition_by` returns a Rust-owned temporary array of DataFrame values.
- `phs_dataframe_array_get` clones one DataFrame into a normal owned `phs_dataframe` handle.
- Haskell wraps each cloned handle with the existing DataFrame finalizer.
- Haskell frees the temporary array after cloning all frames.

## Implementation Plan

1. Add RED Rust tests for stable partition order, include-key false, missing keys, empty key validation, null output pointers, and array get/free behavior.
2. Add RED Hspec tests for public options, stable group order, include-key false, empty columns, and missing columns.
3. Add the Rust `partition_by` feature.
4. Add opaque array ABI helpers.
5. Add Haskell raw imports and wrapper.
6. Run focused RED/GREEN checks and full verification.

## Examples

Good:

```haskell
parts <- dataFramePartitionBy defaultDataFramePartitionOptions
    { dataFramePartitionColumns = ["department"]
    , dataFramePartitionMaintainOrder = True
    } employees
```

Bad:

```haskell
parts <- dataFramePartitionBy defaultDataFramePartitionOptions employees
```

At least one partition column is required.

## Trade-offs

- `phs_dataframe_array_get` clones DataFrames instead of transferring child ownership from the array. This keeps temporary array freeing simple and avoids double-free ambiguity.
- Non-stable partition order is delegated to Polars and should not be asserted in Haskell tests.

## Implementation Results

Implemented on 2026-05-18:

- Added `partition_by` to the Rust Polars feature list.
- Added `phs_dataframe_array` as an opaque temporary result handle.
- Added `phs_dataframe_partition_by`, `phs_dataframe_array_len`, `phs_dataframe_array_get`, and `phs_dataframe_array_free`.
- Added `DataFramePartitionOptions`, `defaultDataFramePartitionOptions`, and `dataFramePartitionBy`.
- Added Rust ABI tests for stable grouping, key-column removal, null pointers, array bounds, null array handles, and child-frame validity after array free.
- Added Hspec coverage for stable grouping, key-column removal, empty column validation, and missing-column Polars errors.

Focused verification:

- `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml dataframe_partition_by_returns_group_frames`: 1/1 passed.
- `stack test --fast --test-arguments='--match=partitions'`: 1/1 passed.

Full verification:

- `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml`: 137/137 passed.
- `stack test --fast`: 184/184 passed.
- `hlint src test app`: no hints.
- `jj diff --git --color=never | git apply --cached --check --whitespace=error -`: passed.

Deviations:

- None.
