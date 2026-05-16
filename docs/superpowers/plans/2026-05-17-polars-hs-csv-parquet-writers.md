# CSV and Parquet Writers Implementation Plan

## Scope

Implement default eager file writers from the design log:
`docs/superpowers/specs/2026-05-17-polars-hs-csv-parquet-writers-design.md`.

## Files

- `src/Polars/DataFrame.hs`
- `src/Polars/Internal/Raw.hs`
- `rust/polars-hs-ffi/src/dataframe.rs`
- `include/polars_hs.h`
- `test/Spec.hs`
- `README.md`
- `CHANGELOG.md`
- `docs/superpowers/plans/2026-05-16-polars-hs-polars-053-parity.md`

## Steps

1. Add Hspec tests:
   - `writeCsv` round-trips `values.csv` through a temp file.
   - `writeParquet` round-trips `values.csv` through a temp file.
   - missing directory writes return `PolarsFailure`.
2. Add public Haskell wrappers:
   - `writeCsv :: FilePath -> DataFrame -> IO (Either PolarsError ())`
   - `writeParquet :: FilePath -> DataFrame -> IO (Either PolarsError ())`
3. Add Raw FFI imports for `phs_write_csv` and `phs_write_parquet`.
4. Implement Rust ABI functions using `CsvWriter` and `ParquetWriter`.
5. Update README, CHANGELOG, and parity plan status.
6. Run:

```bash
cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml
PATH="$HOME/.ghcup/bin:$PATH" stack --system-ghc test --fast
hlint src app test
git diff --check
```

7. Describe the jj change:

```bash
jj --config signing.behavior=drop describe -m "feat(io): add csv and parquet writers"
jj --config signing.behavior=drop new
```
