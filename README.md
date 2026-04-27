# polars-hs

![CI](https://github.com/pe200012/polars-hs/actions/workflows/ci.yml/badge.svg?branch=master)

`polars-hs` is a Haskell binding to the Rust Polars dataframe engine. The current MVP exposes eager CSV/Parquet readers, lazy CSV/Parquet scans, expression-based lazy filters and projections, grouped aggregations, lazy joins, typed errors, and Arrow IPC byte round-trips.

The Haskell package uses a small Rust adapter crate in `rust/polars-hs-ffi`. The adapter owns direct calls into Polars and exposes a stable `phs_*` C ABI. Haskell wraps returned handles in `ForeignPtr` finalizers and returns `Either PolarsError a` for recoverable failures.

## Build

The project is pinned to Stack with GHC 9.12.2:

```yaml
resolver: nightly-2026-04-26
compiler: ghc-9.12.2
```

Build and test:

```bash
stack test --fast
```

The custom `Setup.hs` runs:

```bash
cargo build --release --manifest-path rust/polars-hs-ffi/Cargo.toml
```

The Rust adapter links as a static library during the Haskell build.

## Example

```haskell
{-# LANGUAGE OverloadedStrings #-}

import qualified Polars as Pl

main :: IO ()
main = do
    scanResult <- Pl.scanCsv "test/data/people.csv"
    result <- case scanResult of
        Left err -> pure (Left err)
        Right lf0 -> do
            filtered <- Pl.filter (Pl.col "age" Pl..> Pl.litInt 35) lf0
            case filtered of
                Left err -> pure (Left err)
                Right lf1 -> do
                    selected <- Pl.select [Pl.col "name"] lf1
                    case selected of
                        Left err -> pure (Left err)
                        Right lf2 -> Pl.collect lf2
    case result of
        Left err -> print err
        Right df -> print =<< Pl.shape df
```

Run the included example with:

```bash
stack runghc examples/iris.hs
```

## Grouped aggregation

```haskell
{-# LANGUAGE OverloadedStrings #-}

import qualified Polars as Pl

main :: IO ()
main = do
  scanResult <- Pl.scanCsv "test/data/sales.csv"
  case scanResult of
    Left err -> print err
    Right lf -> do
      grouped <-
        Pl.agg
          [ Pl.alias "salary_sum" (Pl.sum_ (Pl.col "salary"))
          , Pl.alias "age_mean" (Pl.mean_ (Pl.col "age"))
          , Pl.alias "people" (Pl.count_ (Pl.col "name"))
          ]
          (Pl.groupByStable [Pl.col "department"] lf)
      case grouped of
        Left err -> print err
        Right result -> Pl.collect result >>= print
```

Run the grouped aggregation example with:

```bash
stack runghc examples/groupby.hs
```

## Lazy joins

```haskell
{-# LANGUAGE OverloadedStrings #-}

import qualified Polars as Pl

main :: IO ()
main = do
  employeesResult <- Pl.scanCsv "test/data/employees.csv"
  departmentsResult <- Pl.scanCsv "test/data/departments.csv"
  case (employeesResult, departmentsResult) of
    (Right employees, Right departments) -> do
      let options =
            Pl.defaultJoinOptions
              { Pl.joinType = Pl.JoinLeft
              , Pl.leftOn = [Pl.col "department"]
              , Pl.rightOn = [Pl.col "department"]
              , Pl.suffix = Just "_dept"
              }
      joined <- Pl.joinWith options employees departments
      case joined of
        Left err -> print err
        Right lf -> Pl.collect lf >>= print
    (Left err, _) -> print err
    (_, Left err) -> print err
```

Run the join example with:

```bash
stack runghc examples/join.hs
```

## Public modules

- `Polars` re-exports the MVP API.
- `Polars.DataFrame` provides eager readers, shape/schema queries, head/tail, text rendering, and IPC byte conversion.
- `Polars.LazyFrame` provides scan, filter, select, withColumns, sort, limit, and collect.
- `Polars.GroupBy` provides grouped lazy aggregation through groupBy, groupByStable, and agg.
- `Polars.Join` provides lazy inner, left, right, and full joins with optional suffix configuration.
- `Polars.Expr` and `Polars.Operators` build a pure Haskell expression AST, including aggregation expressions.
- `Polars.Error` defines `PolarsError` and `PolarsErrorCode`.
- `Polars.Schema` defines schema field and datatype values.
- `Polars.IPC` provides IPC byte and file helpers.

## Verification

Use these commands before committing implementation changes:

```bash
cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml
cargo clippy --manifest-path rust/polars-hs-ffi/Cargo.toml -- -D warnings
stack test --fast
hlint src app test
```
