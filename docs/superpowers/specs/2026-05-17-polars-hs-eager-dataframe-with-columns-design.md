# Polars HS Eager DataFrame With Columns Design

## Background

Rust Polars 0.53 exposes eager column insertion/replacement through
`DataFrame::with_column`. It adds a new column, replaces an existing column with
the same name, and broadcasts unit-length columns to the frame height.

Docs checked:

- https://docs.rs/polars/latest/polars/frame/struct.DataFrame.html

Local source checked:

- `polars-core-0.53.0/src/frame/mod.rs`

## Problem

`dataFrameHStack` adds columns and rejects duplicate names through Polars
`hstack`. Users also need the Polars-style "add or replace columns" operation
for eager DataFrames, including the useful scalar-broadcast semantics for
single-value Series.

## Questions and Answers

Q: Should this use expressions like lazy `withColumns`?

A: Use Series inputs for the eager API. The current eager surface has Series
handles as concrete columns; expression evaluation remains in LazyFrame.

Q: Should this accept multiple Series?

A: Yes. The Haskell wrapper accepts `[Series]` and Rust applies
`with_column` sequentially, matching the common `with_columns` user workflow.

Q: Should empty input be accepted?

A: Reject empty inputs at the Haskell boundary. Existing eager transform helpers
prefer explicit validation for empty lists.

## Design

Public API:

```haskell
dataFrameWithColumns :: [Series] -> DataFrame -> IO (Either PolarsError DataFrame)
```

C ABI:

```c
int phs_dataframe_with_columns(const phs_dataframe *dataframe,
                               const phs_series *const *series,
                               uintptr_t len,
                               phs_dataframe **out,
                               phs_error **err);
```

Rust mapping:

```rust
let mut df = handle.value.clone();
for column in columns {
    df.with_column(column)?;
}
```

## Implementation Plan

1. Add RED Hspec tests for add, replace, unit-length broadcast, and errors.
2. Add Haskell export and wrapper.
3. Add Raw.hs import and C header declaration.
4. Add Rust `phs_dataframe_with_columns`.
5. Add Rust FFI tests for replacement/broadcast and invalid arrays.
6. Run focused Hspec and Rust tests.
7. Run full Rust, full Stack/Hspec, HLint, and `git diff --check`.
8. Append implementation results.

## Examples

Good:

```haskell
updated <- dataFrameWithColumns [newAge, city] df
```

Bad:

```haskell
dataFrameWithColumns [] df
```

## Trade-offs

This API overlaps with `dataFrameHStack` for adding new columns, but mirrors
Polars' replacement and broadcast behavior. Keeping both operations explicit
makes duplicate-name semantics visible: `hstack` rejects duplicates, while
`withColumns` replaces them.

## Implementation Results

Implemented:

```haskell
dataFrameWithColumns :: [Series] -> DataFrame -> IO (Either PolarsError DataFrame)
```

Added C ABI:

```c
int phs_dataframe_with_columns(const phs_dataframe *dataframe,
                               const phs_series *const *series,
                               uintptr_t len,
                               phs_dataframe **out,
                               phs_error **err);
```

Rust behavior:

- Clones the input DataFrame.
- Converts each input Series handle to a Polars column.
- Applies `DataFrame::with_column` in order.
- Returns a new owned DataFrame handle.

Semantics covered:

- Adds new columns.
- Replaces same-name columns.
- Broadcasts unit-length Series to DataFrame height.
- Returns Polars errors for mismatched non-unit Series length.
- Rejects empty Haskell input with `InvalidArgument`.
- Rejects null or empty direct Rust ABI arrays.

Verification on 2026-05-17:

- RED Hspec failed on missing `dataFrameWithColumns`.
- Focused Hspec: 3/3 examples passing.
- Focused Rust FFI: 2/2 tests passing.
- Full Rust FFI: 104/104 tests passing.
- Full Stack/Hspec: 154/154 examples passing.
- HLint: no hints.
- `git diff --check`: clean.
