# CSV and Parquet Writers Design

## Background

`polars-hs` already exposes eager `readCsv` and `readParquet` through Rust
Polars 0.53. The next IO parity step is writing managed `DataFrame` handles back
to disk through the same Rust-owned `phs_*` ABI boundary.

Upstream Rust Polars 0.53 writer APIs require a mutable `DataFrame` reference:
`CsvWriter::finish(&mut DataFrame)` and `ParquetWriter::finish(&mut DataFrame)`.
The Rust ABI must clone the owned handle value before writing.

## Problem

Users need safe default writer APIs that preserve shape, schema, nulls, and
typed values across write/read round trips. Writer failures such as missing
directories should return `Either PolarsError ()`.

## Questions and Answers

Q: Should Task 3 include writer option records?

A: Deliver default writers first. CSV separator/header and Parquet compression
need their own option tests and ABI fields, so they fit the next IO options
batch.

Q: Where should the public APIs live?

A: Put `writeCsv` and `writeParquet` in `Polars.DataFrame`, next to
`readCsv` and `readParquet`; `Polars` re-exports that module.

Q: How does the writer satisfy Polars' mutable `DataFrame` requirement?

A: Rust clones the Polars `DataFrame` stored inside the handle, then passes the
clone as `&mut DataFrame` to Polars writers.

## Design

Public API:

```haskell
writeCsv :: FilePath -> DataFrame -> IO (Either PolarsError ())
writeParquet :: FilePath -> DataFrame -> IO (Either PolarsError ())
```

Raw FFI:

```haskell
phs_write_csv :: CString -> Ptr RawDataFrame -> Ptr (Ptr RawError) -> IO CInt
phs_write_parquet :: CString -> Ptr RawDataFrame -> Ptr (Ptr RawError) -> IO CInt
```

Rust ABI:

```c
int phs_write_csv(const char *path, const struct phs_dataframe *dataframe, struct phs_error **err);
int phs_write_parquet(const char *path, const struct phs_dataframe *dataframe, struct phs_error **err);
```

Rust implementation:

```rust
let path = unsafe { c_path(path) }?;
let handle = unsafe { dataframe_ref(dataframe) }?;
let mut df = handle.value.clone();
let mut file = File::create(path)?;
CsvWriter::new(&mut file).finish(&mut df)?;
```

Parquet uses `ParquetWriter::new(file).finish(&mut df)?`; the returned byte
count is intentionally discarded by the C ABI.

## Implementation Plan

1. Add Hspec RED tests for CSV and Parquet temp-file round trips plus missing
   directory errors.
2. Add public Haskell writer wrappers and unit-output FFI helper.
3. Add Raw imports.
4. Add Rust `phs_write_csv` and `phs_write_parquet`.
5. Let cbindgen refresh `include/polars_hs.h`.
6. Update README and CHANGELOG.
7. Verify with Cargo, Stack, HLint, and `git diff --check`.

## Examples

```haskell
Right df0 <- readCsv "test/data/values.csv"
Right () <- writeCsv "/tmp/values.csv" df0
Right df1 <- readCsv "/tmp/values.csv"
shape df1 `shouldReturn` Right (3, 4)
```

```haskell
Right df0 <- readCsv "test/data/values.csv"
Right () <- writeParquet "/tmp/values.parquet" df0
Right df1 <- readParquet "/tmp/values.parquet"
column @Int64 df1 "age" `shouldReturn` Right (V.fromList [Just 34, Nothing, Just 29])
```

## Trade-offs

- Default writers keep the first IO write ABI narrow and testable.
- Option records remain additive public API work for a later batch.
- Writers clone the Rust `DataFrame` to satisfy upstream mutability while
  preserving the Haskell handle semantics.

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

Public API:

```haskell
writeCsv :: FilePath -> DataFrame -> IO (Either PolarsError ())
writeParquet :: FilePath -> DataFrame -> IO (Either PolarsError ())
```

Tests added:

- CSV temp-file write/read round trip over `test/data/values.csv`
- Parquet temp-file write/read round trip over `test/data/values.csv`
- Missing-directory writer failures for CSV and Parquet with `PolarsFailure`

Verification:

```bash
cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml
# 85 passed

PATH="$HOME/.ghcup/bin:$PATH" stack --system-ghc test --fast
# 89 examples, 0 failures

hlint src app test
# No hints

git diff --check
# passed
```

Deviation notes:

- Writer option records moved to the IO options batch so each option gets
  dedicated ABI fields and round-trip tests.
