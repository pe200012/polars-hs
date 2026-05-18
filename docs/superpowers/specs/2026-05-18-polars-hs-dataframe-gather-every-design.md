# DataFrame Gather Every Design

## Background

Python Polars exposes `DataFrame.gather_every(n, offset=0)` for selecting every nth row from a DataFrame. Rust Polars 0.53 exposes the same primitive on `Series` and `Column`; the DataFrame behavior can be implemented by constructing row indexes and calling `DataFrame::take`.

Upstream references:

- Python docs: `gather_every(n: int, offset: int = 0) -> DataFrame`
- Rust `Series::gather_every(&self, n: usize, offset: usize) -> PolarsResult<Series>`
- Rust `Column::gather_every(&self, n: usize, offset: usize) -> PolarsResult<Column>`
- Rust `DataFrame::take(&self, indices: &IdxCa) -> PolarsResult<DataFrame>`

## Problem

`polars-hs` has eager row selection by explicit indexes through `dataFrameTake`, but no convenience API for selecting a regular stride of rows. Series already has `seriesGatherEvery`, so DataFrame users need the matching eager row helper.

## Questions And Answers

Q: Is there a Rust `DataFrame::gather_every` method in Polars 0.53?

A: No direct Rust DataFrame method appears in local source. Build the row index range from offset to height with step `n`, convert to `IdxCa`, and call `DataFrame::take`.

Q: How should invalid values be handled?

A: Haskell validates negative `step` and `offset`. Haskell rejects `step == 0`. Rust also rejects `step == 0`, offset overflow, and step overflow. Offset beyond height returns an empty DataFrame with the same schema.

Q: Should this reuse `DataFrameTake`?

A: Rust can reuse the same `idx_size_from_u64` conversion pattern and `DataFrame::take`, while the public API stays compact.

## Design

Public Haskell API:

```haskell
dataFrameGatherEvery :: Int -> Int -> DataFrame -> IO (Either PolarsError DataFrame)
```

Parameter order follows Polars: step first, offset second.

ABI:

```c
int phs_dataframe_gather_every(const struct phs_dataframe *dataframe,
                               uint64_t step,
                               uint64_t offset,
                               struct phs_dataframe **out,
                               struct phs_error **err);
```

Rust implementation:

```rust
let step = usize_from_u64(step, "dataframe gather-every step")?;
let offset = idx_size_from_u64(offset, "dataframe gather-every offset")?;
if step == 0 {
    return Err(PhsError::invalid_argument("dataframe gather-every step must be positive"));
}
let height = handle.value.height() as IdxSize;
let indexes = (offset..height).step_by(step).collect::<Vec<_>>();
let indexes = IdxCa::from_vec(PlSmallStr::EMPTY, indexes);
let output = handle.value.take(&indexes)?;
```

## Implementation Plan

1. Add RED Rust tests for stride selection, offset selection, offset beyond height, zero step, null output pointer, and step/offset conversion.
2. Add RED Hspec tests over `values.csv`.
3. Add Rust ABI function and C header declaration.
4. Add Raw import and Haskell wrapper/export.
5. Run focused GREEN and full verification.

## Examples

Good:

```haskell
rows <- dataFrameGatherEvery 2 1 df
```

This returns rows at indexes 1, 3, 5, and so on.

Bad:

```haskell
rows <- dataFrameGatherEvery 0 0 df
```

The step must be positive.

## Trade-offs

- The Rust wrapper uses `take` rather than a direct DataFrame method because Polars 0.53 only exposes the public eager helper on Series/Column.
- Returning an empty DataFrame for offset beyond height matches range-based selection and preserves schema.

## Implementation Results

Implemented as designed.

Files changed:

- `include/polars_hs.h`: added `phs_dataframe_gather_every`.
- `rust/polars-hs-ffi/src/dataframe.rs`: added the Rust ABI function using checked `usize`/`IdxSize` conversions, `IdxCa`, and `DataFrame::take`.
- `src/Polars/Internal/Raw.hs`: added the safe FFI import.
- `src/Polars/DataFrame.hs`: exported `dataFrameGatherEvery` and added Haskell-side validation for zero/negative step and negative offset.
- `test/Spec.hs`: added Hspec coverage for stride selection, offset selection, offset beyond height, and invalid arguments.

Behavior covered:

- Step `2`, offset `0` over `values.csv` returns rows `Alice` and `Carol`.
- Step `2`, offset `1` returns row `Bob`.
- Step larger than height returns the first row when offset is in bounds.
- Offset beyond height returns an empty DataFrame with the original schema.
- `u64::MAX` offset at the Rust ABI returns an empty DataFrame when it is beyond height.
- Nulls in selected rows are preserved.
- Step `0`, negative step, and negative offset return `InvalidArgument`.
- Rust ABI rejects a null output pointer and clears the output pointer before work.

Verification:

- RED Rust FFI failed on missing `phs_dataframe_gather_every`.
- RED Hspec failed on missing `Pl.dataFrameGatherEvery`.
- Focused Rust FFI: 1/1 passing.
- Focused Hspec: 2/2 matching examples passing.
- Full Rust FFI: 139/139 passing.
- Full Stack/Hspec: 186/186 passing.
- HLint: no hints.
- Whitespace gate: clean.

Deviation from design:

- The Rust implementation checks `offset >= height` before any `IdxSize` conversion and returns `DataFrame::clear()` in that case. This follows the upstream `Column::gather_every` empty-result semantics for huge offsets.
- Real row indexes are converted one-by-one to `IdxSize` before calling `DataFrame::take`, so non-bigidx builds still reject indexes that would be materialized outside Polars index capacity.

Readonly review:

- Agent guidance in `research/api/dataframe-gather-every-readonly-guidance-2026-05-18` recommended the early `offset >= height` check and additional huge-offset coverage.
