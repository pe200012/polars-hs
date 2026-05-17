# Polars-HS Parquet Options Phase 2 Design

## Background

`polars-hs` already exposes default Parquet eager read/write and lazy scan
helpers. Phase 1 added scalar row limits for eager reads, compression and row
group size for writes, and basic lazy scan booleans.

Polars 0.53 exposes additional scalar Parquet knobs that fit the current FFI
style:

- eager read: `ParquetReader::read_parallel`, `set_low_memory`, `set_rechunk`;
- lazy scan: `ScanArgsParquet.parallel` in addition to existing
  `n_rows/use_statistics/low_memory/rechunk/cache`;
- write: `ParquetWriter::set_parallel`, `with_data_page_size`,
  `with_statistics`.

## Problem

The current Haskell option records cannot express Parquet parallel strategy,
eager read memory/rechunk behavior, or writer statistics/data-page/parallel
settings. Those are all scalar values and can be added without introducing
list-valued projection, schema, cloud, row-index, or metadata transport.

## Questions and Answers

Q: Should this batch include column projection and row index?

A: No. Projection and row index require list/string or structured metadata
transport. This phase keeps the ABI scalar and testable.

Q: Should writer statistics be a single bool?

A: Use a record matching Rust `StatisticsOptions`: min value, max value,
distinct count, and null count. This preserves Polars defaults and avoids a
later breaking change.

Q: Should eager and lazy use the same parallel enum?

A: Yes. Both map to `polars_io::parquet::read::ParallelStrategy`.

## Design

Add:

```haskell
data ParquetParallelStrategy
    = ParquetParallelAuto
    | ParquetParallelNone
    | ParquetParallelColumns
    | ParquetParallelRowGroups
    | ParquetParallelPrefiltered

data ParquetStatisticsOptions = ParquetStatisticsOptions
    { parquetStatisticsMinValue :: !Bool
    , parquetStatisticsMaxValue :: !Bool
    , parquetStatisticsDistinctCount :: !Bool
    , parquetStatisticsNullCount :: !Bool
    }
```

Extend:

```haskell
data ParquetReadOptions = ParquetReadOptions
    { parquetReadNRows :: !(Maybe Int)
    , parquetReadParallel :: !ParquetParallelStrategy
    , parquetReadLowMemory :: !Bool
    , parquetReadRechunk :: !Bool
    }

data ParquetWriteOptions = ParquetWriteOptions
    { parquetWriteCompression :: !ParquetCompression
    , parquetWriteRowGroupSize :: !(Maybe Int)
    , parquetWriteDataPageSize :: !(Maybe Int)
    , parquetWriteStatistics :: !ParquetStatisticsOptions
    , parquetWriteParallel :: !Bool
    }

data ParquetScanOptions = ParquetScanOptions
    { parquetScanNRows :: !(Maybe Int)
    , parquetScanParallel :: !ParquetParallelStrategy
    , parquetScanUseStatistics :: !Bool
    , parquetScanLowMemory :: !Bool
    , parquetScanRechunk :: !Bool
    , parquetScanCache :: !Bool
    }
```

Defaults mirror Rust Polars 0.53:

- parallel: `Auto`
- read low memory: `False`
- read rechunk: `False`
- write data page size: `Nothing`
- writer statistics: min/max/null enabled, distinct count disabled
- write parallel: `True`

ABI additions stay scalar:

```c
int parquet_parallel_code;
bool low_memory;
bool rechunk;
bool has_data_page_size;
uint64_t data_page_size;
bool statistics_min_value;
bool statistics_max_value;
bool statistics_distinct_count;
bool statistics_null_count;
bool parallel;
```

Rust maps parallel codes:

```rust
0 -> ParallelStrategy::Auto
1 -> ParallelStrategy::None
2 -> ParallelStrategy::Columns
3 -> ParallelStrategy::RowGroups
4 -> ParallelStrategy::Prefiltered
```

## Implementation Plan

1. Add RED Hspec tests for eager read options, writer scalar options, lazy scan
   parallel options, negative `parquetWriteDataPageSize`, and Rust oracle
   parity for eager read parallel options.
2. Extend IO records and defaults.
3. Validate `parquetWriteDataPageSize` with existing non-negative helpers.
4. Extend raw Haskell imports, C header, Rust FFI signatures, and Rust builder
   mappings.
5. Extend `polars_hs_oracle` with a matching Parquet read command.
6. Run focused tests and full verification.

## Examples

Good pattern:

```haskell
Pl.defaultParquetReadOptions
    { Pl.parquetReadNRows = Just 2
    , Pl.parquetReadParallel = Pl.ParquetParallelRowGroups
    , Pl.parquetReadLowMemory = True
    }
```

Bad pattern:

```haskell
Pl.readParquetLowMemoryRowGroups path
```

## Trade-offs

Adding fields to exported records affects direct record construction. Existing
call sites using `defaultParquet*Options` with record updates remain the intended
style. The added fields match scalar Rust options and defer structured
projection, row-index, cloud, and metadata APIs to later designs.

## Implementation Results

Implemented:

- `ParquetParallelStrategy` with Auto, None, Columns, RowGroups, and Prefiltered
  mappings.
- `ParquetStatisticsOptions` with min, max, distinct, and null statistic flags.
- `ParquetReadOptions` fields for parallel strategy, low-memory mode, and
  rechunking.
- `ParquetWriteOptions` fields for data page size, statistics, and writer
  parallelism.
- `ParquetScanOptions` parallel strategy.

Mapped Rust FFI:

- eager read uses `ParquetReader::read_parallel`, `set_low_memory`,
  `set_rechunk`, and existing `with_slice`;
- lazy scan sets `ScanArgsParquet.parallel`;
- writer uses `ParquetWriter::with_data_page_size`, `with_statistics`, and
  `set_parallel`.

Added tests for Parquet read/write option round-trips, lazy scan parallel
options, writer data-page validation, and Rust oracle parity through
`parquet-read-options-phase2`.

Verification:

- `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml --release`: 87/87
  Rust tests passing.
- `PATH="$HOME/.ghcup/bin:$PATH" stack --system-ghc test --fast`: 112/112 Hspec
  examples passing.
- `PATH="$HOME/.ghcup/bin:$PATH" hlint src app test`: no hints.
- `git diff --check`: clean.

Deviation from design: the plain debug-profile `cargo test` was interrupted
after spending over ten minutes in the Rust debug linker. The release Rust test
run completed and is aligned with the Stack build path used by this package.
