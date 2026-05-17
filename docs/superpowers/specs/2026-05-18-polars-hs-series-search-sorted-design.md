# Series search sorted design

## Background

Rust Polars 0.53 exposes `polars_ops::series::search_sorted` behind the `search_sorted` feature. The function takes a sorted input `Series`, a `Series` of search values, a `SearchSortedSide`, and a descending flag, then returns insertion indexes as `IdxCa`.

Current `polars-hs` already has eager Series transforms such as `seriesGatherEvery`, `seriesPctChange`, `seriesZipWith`, and typed extraction for `Word32`/`Word64`. This batch adds the eager search-sorted operation through the same Rust-owned `phs_*` ABI pattern.

## Problem

Haskell callers need a typed way to ask Polars for insertion positions into an already sorted Series. The binding should preserve Polars 0.53 semantics for duplicate values, descending sorted inputs, null search values, dtype mismatches, and empty inputs.

## Questions and Answers

1. What is the public Haskell API?

   Answer: expose `SearchSortedSide` and `seriesSearchSorted`.

   ```haskell
   data SearchSortedSide
       = SearchSortedAny
       | SearchSortedLeft
       | SearchSortedRight

   seriesSearchSorted :: SearchSortedSide -> Bool -> Series -> Series -> IO (Either PolarsError Series)
   ```

2. How are sides encoded across the C ABI?

   Answer: use compact `int` opcodes matching the public Haskell constructors.

   ```text
   0 = Any
   1 = Left
   2 = Right
   ```

3. Does this API verify sortedness?

   Answer: it delegates sortedness to upstream Polars 0.53. Tests use explicitly sorted inputs. Documentation states that the first Series must be sorted in the direction specified by the descending flag.

4. What output dtype should Haskell users decode?

   Answer: the Rust return type is `IdxCa`. With the current non-bigidx build this is `UInt32`, so tests decode through `seriesWord32`. A future bigidx build can widen the decode surface.

## Design

The Rust ABI adds:

```c
int phs_series_search_sorted(const struct phs_series *series,
                             const struct phs_series *search_values,
                             int side,
                             bool descending,
                             struct phs_series **out,
                             struct phs_error **err);
```

Rust maps the `side` opcode to `SearchSortedSide`, calls `polars_series_search_sorted(&series.value, &search_values.value, side, descending)`, converts the returned `IdxCa` to a `Series`, and stores the result in `out`.

Haskell maps `SearchSortedSide` to the same `CInt` opcodes and calls the safe FFI import:

```haskell
seriesSearchSorted side descending sorted values =
    withSeries sorted $ \sortedPtr ->
        withSeries values $ \valuesPtr ->
            seriesOut (phs_series_search_sorted sortedPtr valuesPtr (searchSortedSideCode side) (CBool ...))
```

The FFI import is `safe` because the operation can scan and binary-search large chunks.

```mermaid
flowchart LR
    Haskell[seriesSearchSorted] --> Raw[phs_series_search_sorted]
    Raw --> Rust[polars_ops::series::search_sorted]
    Rust --> IdxCa[IdxCa]
    IdxCa --> Series[Series handle]
```

## Implementation Plan

1. Add RED Rust FFI tests for duplicate side behavior, descending inputs, null search values, empty inputs, string inputs, dtype mismatch, and invalid side opcode.
2. Add RED Hspec tests for the public Haskell API over the same visible behaviors.
3. Enable the `search_sorted` feature on `polars` and `polars-ops`.
4. Implement Rust ABI, C header, raw Haskell import, public Haskell data type, and wrapper.
5. Run focused Rust and Hspec tests, then full Rust tests, full Hspec, HLint, whitespace check, jj describe/bookmark move/export/push.

## Examples

Good duplicate-aware insertion:

```haskell
seriesSearchSorted SearchSortedLeft False sorted needles
-- sorted:  [1, 2, 2, 4]
-- needles: [0, 2, 3, 5]
-- result:  [0, 1, 3, 4]
```

Good descending insertion:

```haskell
seriesSearchSorted SearchSortedRight True sorted needles
-- sorted:  [9, 7, 7, 3]
-- needles: [8, 7, 2]
-- result:  [1, 3, 4]
```

Good null search values follow upstream null insertion rules:

```haskell
seriesSearchSorted SearchSortedLeft False sorted needles
-- sorted:  [null, null, 1, 3]
-- needles: [null, 2]
-- result:  [0, 3]
```

Dtype mismatch:

```haskell
seriesSearchSorted SearchSortedLeft False intSeries textNeedles
-- returns a Polars error from upstream search_sorted
```

## Trade-offs

The public API exposes `Bool` for descending to match current `SeriesSortOptions` style and the upstream Rust function. A dedicated order type would be clearer for future APIs, but this small wrapper stays consistent with nearby Series functions.

The return value remains a `Series` so the ABI mirrors Polars and composes with existing Series transforms. Callers can decode with `seriesWord32` in the current build.

## Implementation Results

Implemented files:

- `rust/polars-hs-ffi/Cargo.toml`: enabled `search_sorted` for `polars` and `polars-ops`.
- `rust/polars-hs-ffi/src/series.rs`: added side opcode decoding, `phs_series_search_sorted`, and FFI tests.
- `include/polars_hs.h`: added the C ABI declaration.
- `src/Polars/Internal/Raw.hs`: added a safe FFI import.
- `src/Polars/Series.hs`: added `SearchSortedSide` and `seriesSearchSorted`.
- `test/Spec.hs`: added public Hspec coverage.

Focused verification:

- RED Rust: `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml series_search_sorted_returns_insertion_indexes` failed because `phs_series_search_sorted` was absent.
- RED Hspec: `stack test --fast --ta --match=searches` failed because `Polars` exported neither `seriesSearchSorted` nor `SearchSortedSide`.
- GREEN Rust: focused Rust test passed, 1/1.
- GREEN Hspec: focused Hspec test passed, 1/1.
- Full Rust: `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml` passed, 124/124.
- Full Haskell: `stack test --fast` passed, 171/171.
- HLint: `hlint src test app` returned `No hints`.
- Whitespace: `git diff --check` returned exit 0.

Deviations:

- The Rust invalid side error includes the numeric opcode: `unknown search sorted side code {value}`. This is more diagnostic than a generic invalid-side message.
