# Polars HS DataFrame Take Design

## Background

`polars-hs` now exposes eager DataFrame filtering, slicing, sorting, unique,
fill-null, drop-null, reverse, and null-count operations. Rust Polars 0.53 also
exposes row gather for eager DataFrames:

```rust
DataFrame::take(&IdxCa) -> PolarsResult<DataFrame>
```

The Series take batch already established a `Vector Word64` Haskell API with a
Rust-side `IdxSize` conversion. DataFrame take should use the same convention so
row-index APIs behave consistently.

## Problem

Users need eager DataFrame row selection by explicit positions. The API must
preserve duplicate and reordered rows, keep null values intact, support empty
index lists, surface Polars out-of-bounds errors, and reject indices that exceed
the compiled Polars index width.

## Questions and Answers

Q: Should DataFrame take reuse the Series take index payload design?

A: Yes. `Vector Word64` gives a stable Haskell surface, and Rust validates each
index against the compiled `IdxSize`.

Q: Should empty indices return a zero-row DataFrame with the same columns?

A: Yes. Polars returns a DataFrame with `indices.len()` rows and the original
schema.

Q: Should this expose nullable index arrays?

A: Keep nullable indices for a later ABI. This batch handles non-null row
positions and preserves null values in the selected rows.

## Design

Public API:

```haskell
dataFrameTake :: Vector Word64 -> DataFrame -> IO (Either PolarsError DataFrame)
```

C ABI:

```c
int phs_dataframe_take(const phs_dataframe *dataframe,
                       const uint64_t *indices,
                       uintptr_t len,
                       phs_dataframe **out,
                       phs_error **err);
```

Rust implementation:

```rust
let indices = idx_ca_from_u64_slice(indices, "dataframe take index")?;
let output = dataframe.take(&indices)?;
```

Validation rules:

- `indices == NULL` is accepted only when `len == 0`.
- Index-width overflow returns `InvalidArgument`.
- Row bounds are checked by Polars.

## Implementation Plan

1. Add RED Hspec tests for duplicate/reordered DataFrame take and empty take.
2. Add RED Hspec tests for null preservation, out-of-bounds, and overflow.
3. Add `dataFrameTake` export and Haskell wrapper.
4. Add Raw.hs import and C header declaration.
5. Add Rust index-slice conversion helper and `phs_dataframe_take`.
6. Add Rust FFI tests for success and pointer/overflow validation.
7. Run focused tests, Rust release tests, full Stack/Hspec, HLint, and
   `git diff --check`.
8. Append implementation results to this design log.

## Examples

Good:

```haskell
taken <- Pl.dataFrameTake (V.fromList [2, 0, 2]) df
```

Bad:

```haskell
Pl.dataFrameTake (V.fromList [fromIntegral (maxBound :: Word64)]) df
```

## Trade-offs

The Haskell wrapper uses a simple copied array payload. This keeps the ABI
compact and consistent with the Series take batch. A future large-index batch
can optimize the payload transfer without changing the public API.

## Implementation Results

Implemented files:

- `src/Polars/DataFrame.hs`
- `src/Polars/Internal/Raw.hs`
- `include/polars_hs.h`
- `rust/polars-hs-ffi/src/dataframe.rs`
- `test/Spec.hs`

Delivered API:

```haskell
dataFrameTake :: Vector Word64 -> DataFrame -> IO (Either PolarsError DataFrame)
```

Rust/C ABI:

```c
int phs_dataframe_take(const phs_dataframe *dataframe,
                       const uint64_t *indices,
                       uintptr_t len,
                       phs_dataframe **out,
                       phs_error **err);
```

Validation and behavior:

- `Vector Word64` crosses the Haskell boundary as a copied `uint64_t` array.
- Rust accepts `NULL` indices only with `len == 0`.
- Rust converts each index through `IdxSize::try_from` and reports
  `dataframe take index exceeds Polars index size` on overflow.
- Polars checks row bounds and reports out-of-bounds indices through
  `PolarsFailure`.
- Duplicate, reordered, empty, and nullable-row gather cases are covered.

Tests added:

- Hspec duplicate/reordered row gather over `[2, 0, 2]`.
- Hspec empty gather returning a zero-row, four-column DataFrame.
- Hspec null preservation over `[1, 2, 1, 0]`.
- Hspec out-of-bounds and index-width overflow errors.
- Rust FFI tests for empty/reordered indices, `NULL` pointer with positive
  length, and index overflow.

Verification:

- RED: focused Hspec failed on missing `Pl.dataFrameTake` export.
- Focused Hspec: 3 examples, 0 failures.
- Focused Rust `dataframe_take`: 3 tests, 0 failures.
- Rust release tests: 93 passed, 0 failed.
- Full Stack/Hspec: 137 examples, 0 failures.
- HLint: no hints.
- `git diff --check`: clean.

Deviations:

- The Rust helper mirrors the Series take helper locally in `dataframe.rs`.
  A shared FFI index helper can be extracted once more index-array APIs land.
