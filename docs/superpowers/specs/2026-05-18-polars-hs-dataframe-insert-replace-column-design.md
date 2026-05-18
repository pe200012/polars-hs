# DataFrame Insert And Replace Column Design

## Background

`polars-hs` already exposes `dataFrameWithColumns` for name-based add/replace behavior. Rust Polars 0.53 also exposes position-aware eager column mutation through `DataFrame::insert_column` and `DataFrame::replace_column`.

Upstream references:

- `DataFrame::insert_column(&mut self, index: usize, column: Column) -> PolarsResult<&mut Self>`
- `DataFrame::replace_column(&mut self, index: usize, new_column: Column) -> PolarsResult<&mut Self>`
- docs.rs lists both methods on Polars 0.53 DataFrame: https://docs.rs/polars/latest/polars/frame/struct.DataFrame.html

## Problem

Current Haskell users can append or name-replace columns, yet they cannot insert a column at a specific position or replace a column by numeric index. These operations matter when column order is part of an interchange contract or report output.

## Questions And Answers

Q: Should Haskell mutate the original DataFrame handle?

A: No. Existing eager wrappers return new owned DataFrame handles. Rust should clone the DataFrame, apply the mutable Polars method, and return the clone.

Q: Should unit-length Series broadcast?

A: No for this batch. Polars `insert_column` and `replace_column` require column length to match DataFrame height; broadcast behavior belongs to `with_column` and is already covered by `dataFrameWithColumns`.

Q: Should index validation happen in Haskell?

A: Haskell rejects negative indexes. Rust converts to `usize` and delegates width bounds to Polars.

## Design

Public Haskell API:

```haskell
dataFrameInsertColumn :: Int -> Series -> DataFrame -> IO (Either PolarsError DataFrame)
dataFrameReplaceColumn :: Int -> Series -> DataFrame -> IO (Either PolarsError DataFrame)
```

ABI:

```c
int phs_dataframe_insert_column(const struct phs_dataframe *dataframe,
                                uint64_t index,
                                const struct phs_series *series,
                                struct phs_dataframe **out,
                                struct phs_error **err);

int phs_dataframe_replace_column(const struct phs_dataframe *dataframe,
                                 uint64_t index,
                                 const struct phs_series *series,
                                 struct phs_dataframe **out,
                                 struct phs_error **err);
```

Validation:

- Haskell rejects negative indexes.
- Rust rejects `u64 -> usize` conversion overflow.
- Rust rejects insert indexes greater than DataFrame width before calling Polars, because upstream `insert_column` panics on that input.
- Rust rejects replacement names that duplicate another existing column name, because upstream `replace_column` can create duplicate names.
- Polars handles duplicate insert names, replacement index bounds, and length mismatch errors.
- Input Series handles are cloned into Rust Polars `Column` values.

## Implementation Plan

1. Add RED Rust tests for insert success, append-at-width success, duplicate insert name, replace success, replace out-of-bounds, length mismatch, and null output pointers.
2. Add RED Hspec tests for public APIs, column order, values, negative index validation, duplicate names, and length mismatch.
3. Add C header declarations and raw Haskell imports.
4. Implement Rust wrappers by cloning the DataFrame and Series.
5. Add Haskell wrappers and exports.
6. Run focused RED/GREEN checks and full verification.

## Examples

Good:

```haskell
df2 <- dataFrameInsertColumn 1 newColumn df
df3 <- dataFrameReplaceColumn 0 replacement df
```

Bad:

```haskell
df2 <- dataFrameInsertColumn (-1) newColumn df
```

Indexes must be non-negative.

## Trade-offs

- APIs use `Int` for consistency with existing Haskell row/column count arguments while Rust still checks `usize` conversion.
- Name-based replacement remains `dataFrameWithColumns`; index-based replacement is explicit through `dataFrameReplaceColumn`.

## Implementation Results

- Added `dataFrameInsertColumn` and `dataFrameReplaceColumn` to the public eager DataFrame API.
- Added `phs_dataframe_insert_column` and `phs_dataframe_replace_column` to the C header, raw Haskell imports, and Rust FFI.
- Rust clones the input DataFrame and Series, performs the Polars mutation on the clone, and returns a new owned DataFrame handle.
- Haskell rejects negative indexes before FFI.
- Rust rejects insert indexes greater than width before calling Polars, preventing upstream `Vec::insert` panic.
- Rust rejects replacement names that duplicate another existing column.
- Polars handles duplicate insert names, length mismatch, and replacement index bounds.

Test coverage:

- Insert in the middle preserves order and values.
- Insert at `index == width` appends.
- Insert `index > width` returns `InvalidArgument`.
- Duplicate insert names fail.
- Negative insert/replace indexes return `InvalidArgument`.
- Short and unit-length insert/replace columns fail on height mismatch.
- Replacement by index changes the column name and values.
- Replacement with a name already present at another index returns `InvalidArgument`.
- Rust FFI rejects null output pointers for both functions.

Verification so far:

- RED Rust: missing `phs_dataframe_insert_column` and `phs_dataframe_replace_column`.
- RED Hspec: missing public Haskell exports.
- Focused Rust: `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml dataframe_insert_and_replace_column_work` passed, 1/1.
- Focused Hspec: `stack test --fast --test-arguments='--match=inserts'` passed, 1/1.
- Full Rust: `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml` passed, 136/136.
- Full Hspec: `stack test --fast` passed, 183/183.
- HLint: `hlint src test app` returned `No hints`.
- Whitespace: `jj diff --git --color=never | git apply --cached --check --whitespace=error -` returned exit 0.

Deviations:

- Rust validates insert index upper bounds before calling Polars, because upstream `insert_column` panics for `index > width`.
- Rust validates replace-column duplicate names at a different index to preserve safe DataFrame schema invariants.
