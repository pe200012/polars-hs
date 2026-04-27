# Design Log: Polars-Haskell Series Transform API

## Background

Phase 5 introduced owned `Series` handles and unified column access:

```haskell
{-# LANGUAGE TypeApplications #-}

column @Series df "age"    :: IO (Either PolarsError Series)
column @Int64  df "age"    :: IO (Either PolarsError (Vector (Maybe Int64)))
column @Text   df "name"   :: IO (Either PolarsError (Vector (Maybe Text)))
```

Users can read Series metadata, slices, one-column DataFrames, and typed values. The next step is a read-oriented transform layer over existing Series handles.

Polars Rust 0.53 exposes these relevant APIs:

```rust
Series::rename(&mut self, name: PlSmallStr) -> &mut Series
Series::with_name(self, name: PlSmallStr) -> Series
Series::cast(&self, dtype: &DataType) -> PolarsResult<Series>
Series::sort(&self, sort_options: SortOptions) -> PolarsResult<Series>
Series::unique(&self) -> PolarsResult<Series>
Series::unique_stable(&self) -> PolarsResult<Series>
SeriesTrait::reverse(&self) -> Series
SeriesTrait::drop_nulls(&self) -> Series
```

`SortOptions` has stable fields for descending order, null placement, multithreading, equal-value order, and limit.

## Problem

Series handles need common transforms that return new owned handles while preserving the current safety model:

- Rust owns Polars values behind opaque handles.
- Haskell owns `ForeignPtr` finalizers.
- Recoverable failures return `Either PolarsError a`.
- Public APIs stay type-directed and consistent with `column @xxx`.

## Questions and Answers

### Q1. What transform scope should this phase cover?

Answer: `rename`, `cast`, `sort`, `unique`, `reverse`, and `dropNulls`.

Selected API:

```haskell
seriesRename       :: Text -> Series -> IO (Either PolarsError Series)
seriesSort         :: SeriesSortOptions -> Series -> IO (Either PolarsError Series)
seriesUnique       :: Series -> IO (Either PolarsError Series)
seriesUniqueStable :: Series -> IO (Either PolarsError Series)
seriesReverse      :: Series -> IO (Either PolarsError Series)
seriesDropNulls    :: Series -> IO (Either PolarsError Series)
```

### Q2. What API shape should `seriesCast` use?

Answer: Use visible type applications, aligned with `column @xxx`.

Selected API:

```haskell
class SeriesCast a where
    seriesCast :: Series -> IO (Either PolarsError Series)

instance SeriesCast Bool
instance SeriesCast Int64
instance SeriesCast Double
instance SeriesCast Text
```

Usage:

```haskell
seriesCast @Double age
seriesCast @Text age
```

## Design

### Public module changes

Extend `Polars.Series` exports:

```haskell
module Polars.Series
    ( Series
    , SeriesCast (..)
    , SeriesSortOptions (..)
    , defaultSeriesSortOptions
    , seriesBool
    , seriesCast
    , seriesDataType
    , seriesDouble
    , seriesDropNulls
    , seriesHead
    , seriesInt64
    , seriesLength
    , seriesName
    , seriesNullCount
    , seriesRename
    , seriesReverse
    , seriesSort
    , seriesTail
    , seriesText
    , seriesToFrame
    , seriesUnique
    , seriesUniqueStable
    ) where
```

Add sorting options:

```haskell
data SeriesSortOptions = SeriesSortOptions
    { seriesSortDescending :: !Bool
    , seriesSortNullsLast :: !Bool
    , seriesSortMultithreaded :: !Bool
    , seriesSortMaintainOrder :: !Bool
    , seriesSortLimit :: !(Maybe Int)
    }
    deriving stock (Eq, Show)

defaultSeriesSortOptions :: SeriesSortOptions
defaultSeriesSortOptions =
    SeriesSortOptions
        { seriesSortDescending = False
        , seriesSortNullsLast = False
        , seriesSortMultithreaded = True
        , seriesSortMaintainOrder = False
        , seriesSortLimit = Nothing
        }
```

### Cast target mapping

`seriesCast @xxx` maps Haskell types to Rust Polars datatypes:

| Haskell type application | Rust `DataType` |
| --- | --- |
| `seriesCast @Bool` | `DataType::Boolean` |
| `seriesCast @Int64` | `DataType::Int64` |
| `seriesCast @Double` | `DataType::Float64` |
| `seriesCast @Text` | `DataType::String` |

Internal class:

```haskell
class SeriesCast a where
    seriesCast :: Series -> IO (Either PolarsError Series)

class SeriesCastCode a where
    seriesCastCode :: CInt
```

The implementation can use one public class with hidden code selection, or one class with an internal helper:

```haskell
seriesCastWithCode :: CInt -> Series -> IO (Either PolarsError Series)
```

### Sort semantics

`seriesSort` maps Haskell options directly into Rust `SortOptions`:

```rust
SortOptions::default()
    .with_order_descending(descending)
    .with_nulls_last(nulls_last)
    .with_multithreaded(multithreaded)
    .with_maintain_order(maintain_order)
```

`seriesSortLimit = Just n` sets `SortOptions.limit = Some(n as IdxSize)`. Negative limits return `InvalidArgument` in Haskell before FFI.

### Transform ownership

Every transform returns a new Rust-owned `phs_series` handle. The original input Series remains valid and reusable.

```mermaid
flowchart LR
  HS[Haskell Series ForeignPtr] --> Raw[phs_series]
  Raw --> Rust[Polars Series]
  Rust --> Transform[rename/cast/sort/unique/reverse/drop_nulls]
  Transform --> NewRaw[new phs_series]
  NewRaw --> NewHS[new Haskell Series ForeignPtr]
```

### Rust ABI

Add functions to `rust/polars-hs-ffi/src/series.rs`:

```c
int phs_series_rename(
  const struct phs_series *series,
  const char *name,
  struct phs_series **out,
  struct phs_error **err
);

int phs_series_cast(
  const struct phs_series *series,
  int32_t dtype_code,
  struct phs_series **out,
  struct phs_error **err
);

int phs_series_sort(
  const struct phs_series *series,
  bool descending,
  bool nulls_last,
  bool multithreaded,
  bool maintain_order,
  bool has_limit,
  uint64_t limit,
  struct phs_series **out,
  struct phs_error **err
);

int phs_series_unique(const struct phs_series *series, struct phs_series **out, struct phs_error **err);
int phs_series_unique_stable(const struct phs_series *series, struct phs_series **out, struct phs_error **err);
int phs_series_reverse(const struct phs_series *series, struct phs_series **out, struct phs_error **err);
int phs_series_drop_nulls(const struct phs_series *series, struct phs_series **out, struct phs_error **err);
```

Cast dtype codes:

| Code | Rust DataType |
| --- | --- |
| `0` | `Boolean` |
| `1` | `Int64` |
| `2` | `Float64` |
| `3` | `String` |

Unknown dtype codes return `InvalidArgument`.

### Haskell internals

Extend `Polars.Internal.Raw` with the new FFI imports.

Reuse `seriesOut` from `Polars.Internal.Series` for all Series-returning calls.

Add helpers in `Polars.Series`:

```haskell
seriesUnaryOut :: Series -> (Ptr RawSeries -> Ptr (Ptr RawSeries) -> Ptr (Ptr RawError) -> IO CInt) -> IO (Either PolarsError Series)
seriesCastWithCode :: CInt -> Series -> IO (Either PolarsError Series)
```

`seriesRename` uses `withTextCString`. `seriesSort` passes `CBool` flags and a `Word64` limit after validating `Maybe Int`.

## Examples

### Rename and cast

```haskell
{-# LANGUAGE TypeApplications #-}

Right age <- Pl.column @Pl.Series df "age"
Right ageDouble <- Pl.seriesCast @Double age
Right renamed <- Pl.seriesRename "age_f64" ageDouble
print =<< Pl.seriesName renamed
print =<< Pl.seriesDouble renamed
```

### Sort, reverse, and drop nulls

```haskell
let options =
      Pl.defaultSeriesSortOptions
        { Pl.seriesSortDescending = True
        , Pl.seriesSortNullsLast = True
        }
Right sorted <- Pl.seriesSort options age
Right reversed <- Pl.seriesReverse age
Right dense <- Pl.seriesDropNulls age
```

### Unique values

```haskell
Right department <- Pl.column @Pl.Series df "department"
Right stable <- Pl.seriesUniqueStable department
print =<< Pl.seriesText stable
```

## Testing Plan

- `seriesRename` changes `seriesName` and `seriesToFrame` schema field.
- `seriesCast @Double` converts an Int64 Series to Float64 and `seriesDouble` reads expected values.
- `seriesCast @Text` converts an Int64 Series to String and `seriesText` reads expected text values.
- `seriesSort` covers descending order and `nulls_last`.
- `seriesSort` rejects negative `seriesSortLimit` with `InvalidArgument`.
- `seriesUnique` returns the expected number of unique values for duplicate columns.
- `seriesUniqueStable` preserves first-seen order on a duplicate text Series.
- `seriesReverse` reverses value order while preserving nulls.
- `seriesDropNulls` removes null rows and preserves dtype.
- Existing Series read APIs remain green.

## Trade-offs

### Benefits

- Keeps transform API consistent with `column @xxx` through `seriesCast @xxx`.
- Preserves immutable handle semantics: each transform returns a fresh `Series` handle.
- Covers high-frequency Series operations with small C ABI additions.
- Leaves richer two-Series operations for a focused later phase.

### Costs

- `seriesCast @xxx` requires visible type applications for explicit target selection.
- Cast MVP covers Bool, Int64, Double, and Text targets.
- Sort limit uses `Maybe Int` in Haskell and `uint64_t` across the ABI.

### Future extensions

- `seriesCast @Int32`, `seriesCast @Word64`, and typed constructors for more cast targets.
- `seriesShift` with explicit period semantics.
- `seriesAppend` with dtype compatibility tests.
- Arithmetic and comparison transforms that return Series handles.

## Implementation Plan

1. Write RED Hspec tests for cast, rename, sort, unique, reverse, and dropNulls.
2. Add Rust ABI functions and Rust unit tests in `series.rs`.
3. Add raw Haskell imports and public `Polars.Series` functions/classes.
4. Update docs and examples.
5. Verify with Rust tests, Clippy, Stack tests, HLint, and examples.

## Implementation Results

Implementation starts after design approval. Verification results, deviations, and final commit information are recorded during implementation.
