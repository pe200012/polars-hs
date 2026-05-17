# Design Log: CSV Read Options Phase 2

## Background

IO options phase 1 added shared `CsvReadOptions` for eager CSV reads and lazy
CSV scans with header, separator, and all-column null token controls. Rust
Polars 0.53 exposes additional CSV read controls on `CsvReadOptions` and
`LazyCsvReader`.

## Problem

Common CSV workflows still need row limiting, pre-header row skipping,
post-header row skipping, schema inference length, parser error recovery,
ragged-line truncation, missing-field handling, rechunking, and low-memory
reading. These are scalar option fields that fit the existing FFI style.

## Questions and Answers

### Q1. Should eager reads and lazy scans share the same record?

Answer: Yes for this batch. The selected fields are present on both eager
`CsvReadOptions` and lazy `LazyCsvReader`, or map to the lazy reader's embedded
read options.

### Q2. How should full-schema inference be represented?

Answer: Use `csvReadInferSchemaLength :: Maybe Int`. The default is `Just 100`,
matching Polars. `Nothing` maps to Rust `None`, which asks Polars to scan all
rows for inference.

### Q3. Which fields need Haskell-side validation?

Answer: `csvReadNRows`, `csvReadSkipRows`, `csvReadSkipRowsAfterHeader`, and
`csvReadInferSchemaLength` are public `Int` values and must be non-negative
before crossing FFI.

## Design

Extend `CsvReadOptions`:

```haskell
data CsvReadOptions = CsvReadOptions
    { csvReadHasHeader :: Bool
    , csvReadSeparator :: Word8
    , csvReadNullValue :: Maybe Text
    , csvReadNRows :: Maybe Int
    , csvReadSkipRows :: Int
    , csvReadSkipRowsAfterHeader :: Int
    , csvReadInferSchemaLength :: Maybe Int
    , csvReadIgnoreErrors :: Bool
    , csvReadTruncateRaggedLines :: Bool
    , csvReadMissingIsNull :: Bool
    , csvReadLowMemory :: Bool
    , csvReadRechunk :: Bool
    }
```

Rust eager mapping:

```rust
CsvReadOptions::default()
    .with_has_header(has_header)
    .with_n_rows(...)
    .with_skip_rows(...)
    .with_skip_rows_after_header(...)
    .with_infer_schema_length(...)
    .with_ignore_errors(...)
    .with_low_memory(...)
    .with_rechunk(...)
    .map_parse_options(|opts| opts
        .with_separator(...)
        .with_null_values(...)
        .with_truncate_ragged_lines(...)
        .with_missing_is_null(...))
```

Rust lazy mapping uses `LazyCsvReader` builder methods where available and
`map_parse_options` for parse-only fields.

## Implementation Plan

1. Add RED Hspec tests for eager row controls, eager parser controls, lazy row
   controls, and Rust oracle row-control parity.
2. Extend `CsvReadOptions` and default values in `Polars.IO`.
3. Validate signed fields in `DataFrame.readCsvWith` and `LazyFrame.scanCsvWith`.
4. Extend `phs_read_csv_options` and `phs_scan_csv_options` signatures in Haskell
   raw imports, C header, and Rust FFI.
5. Extend `polars_hs_oracle` with a matching `csv-read-row-options` command.
6. Run focused tests and full verification.

## Examples

Good pattern:

```haskell
Pl.defaultCsvReadOptions
    { Pl.csvReadSkipRows = 1
    , Pl.csvReadSkipRowsAfterHeader = 1
    , Pl.csvReadNRows = Just 2
    }
```

Bad pattern:

```haskell
Pl.readCsvSkippingOneHeaderAndTwoRows path
```

## Trade-offs

Adding fields to an exported record constructor can affect code that constructs
the record directly. The project already encourages use of `defaultCsvReadOptions`
with record updates, and full option parity requires this record to evolve.

## Implementation Results

Implemented fields:

- `csvReadNRows`
- `csvReadSkipRows`
- `csvReadSkipRowsAfterHeader`
- `csvReadInferSchemaLength`
- `csvReadIgnoreErrors`
- `csvReadTruncateRaggedLines`
- `csvReadMissingIsNull`
- `csvReadLowMemory`
- `csvReadRechunk`

Mapped eager `readCsvWith` through `CsvReadOptions` and lazy `scanCsvWith`
through `LazyCsvReader` using Polars 0.53 builder methods. The C ABI and raw
Haskell imports now carry the same scalar option set for eager and lazy CSV
reads.

Added tests for eager row controls, lazy row controls, ragged-line parsing,
missing-field null handling, negative option validation, and Rust oracle parity
for CSV row controls.

Verification:

- `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml`: 87/87 Rust tests
  passing.
- `PATH="$HOME/.ghcup/bin:$PATH" stack --system-ghc test --fast`: 111/111 Hspec
  examples passing.
- `PATH="$HOME/.ghcup/bin:$PATH" hlint src app test`: no hints.
- `git diff --check`: clean.

Deviation from design: lazy `truncate_ragged_lines` and `missing_is_null` used
dedicated `LazyCsvReader` methods from Polars 0.53. That keeps the lazy mapping
more direct than the planned `map_parse_options` route.
