# polars-hs

![CI](https://github.com/pe200012/polars-hs/actions/workflows/ci.yml/badge.svg?branch=master)

`polars-hs` is a Haskell binding to the Rust Polars dataframe engine. The current MVP exposes eager CSV/Parquet readers, lazy CSV/Parquet scans, expression-based lazy filters and projections, grouped aggregations, lazy joins, typed column extraction, typed errors, Arrow C Data Interface import, and Arrow IPC byte round-trips.

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

## Typed column extraction and Series handles

```haskell
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

import Control.Monad ((<=<))
import Data.Int (Int64)
import qualified Data.Text as T
import qualified Polars as Pl

main :: IO ()
main = do
  result <- Pl.readCsv "test/data/values.csv"
  case result of
    Left err -> print err
    Right df -> do
      names <- Pl.column @T.Text df "name"
      ages <- Pl.column @Int64 df "age"
      scores <- Pl.column @Double df "score"
      active <- Pl.column @Bool df "active"
      ageSeries <- Pl.column @Pl.Series df "age"
      print names
      print ages
      print scores
      print active
      either print (print <=< Pl.seriesName) ageSeries
```

Typed extraction returns one value per row in `Vector (Maybe a)` and preserves Polars null values as `Nothing`:

```haskell
Right [Just "Alice",Just "Bob",Just "Carol"]
Right [Just 34,Nothing,Just 29]
Right [Just 9.5,Just 8.25,Nothing]
Right [Just True,Just False,Nothing]
```

The named helpers remain available as aliases: `columnText`, `columnInt64`, `columnDouble`, and `columnBool`.

## Series and DataFrame construction

```haskell
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

import Data.Int (Int64)
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Polars as Pl

main :: IO ()
main = do
  Right name <- Pl.series @T.Text "name" (V.fromList [Just "Alice", Just "Bob", Just "Carol"])
  Right age <- Pl.series @Int64 "age" (V.fromList [Just 34, Nothing, Just 29])
  Right score <- Pl.series @Double "score" (V.fromList [Just 9.5, Just 8.25, Nothing])
  Right active <- Pl.series @Bool "active" (V.fromList [Just True, Just False, Nothing])
  Right df <- Pl.dataFrame [name, age, score, active]
  print =<< Pl.shape df
  print =<< Pl.column @Int64 df "age"
```

Run the construction example with:

```bash
stack runghc examples/construction.hs
```

## Arrow C Data Interface import and export

`fromArrowRecordBatch` imports a standard Arrow C Data Interface RecordBatch into a managed Polars `DataFrame`:

```haskell
(schemaPtr, arrayPtr) <- dataframeToArrow sourceDf
result <- Pl.fromArrowRecordBatch (Pl.unsafeArrowRecordBatch schemaPtr arrayPtr)
```

`withArrowRecordBatch` exports a Polars `DataFrame` as callback-scoped Arrow C Data Interface pointers:

```haskell
roundTrip <- Pl.withArrowRecordBatch df $ \schemaPtr arrayPtr ->
  Pl.fromArrowRecordBatch (Pl.unsafeArrowRecordBatch schemaPtr arrayPtr)
```

The batch is represented by a top-level struct `ArrowSchema` and top-level struct `ArrowArray`. Import consumes producer pointers after validation. Export pointers stay live during the callback.

## Series transforms

```haskell
{-# LANGUAGE TypeApplications #-}

Right age <- Pl.column @Pl.Series df "age"
Right ageDouble <- Pl.seriesCast @Double age
Right renamed <- Pl.seriesRename "age_years" ageDouble

let sortOptions =
      Pl.defaultSeriesSortOptions
        { Pl.seriesSortDescending = True
        , Pl.seriesSortNullsLast = True
        }
Right sorted <- Pl.seriesSort sortOptions age
Right shifted <- Pl.seriesShift 1 age
Right firstAge <- Pl.seriesHead 1 age
Right appended <- Pl.seriesAppend age firstAge
Right reversed <- Pl.seriesReverse age
Right dense <- Pl.seriesDropNulls age
```

`seriesUnique` and `seriesUniqueStable` return fresh Series handles for distinct values:

```haskell
Right department <- Pl.column @Pl.Series employees "department"
Right stableDepartments <- Pl.seriesUniqueStable department
```

Run the column and Series examples with:

```bash
stack runghc examples/columns.hs
stack runghc examples/series.hs
stack runghc examples/construction.hs
```

## Public modules

- `Polars` re-exports the MVP API.
- `Polars.Arrow` provides Arrow C Data Interface RecordBatch import and export.
- `Polars.DataFrame` provides `dataFrame`, eager readers, shape/schema queries, head/tail, text rendering, and IPC byte conversion.
- `Polars.Column` provides `column @Series` and typed `column @Bool/@Int64/@Double/@Text` extraction with null preservation.
- `Polars.Series` provides `series @xxx`, Series metadata, slicing, DataFrame conversion, typed value readers, and transforms.
- `Polars.LazyFrame` provides scan, filter, select, withColumns, sort, limit, and collect.
- `Polars.GroupBy` provides grouped lazy aggregation through groupBy, groupByStable, and agg.
- `Polars.Join` provides lazy inner, left, right, and full joins with optional suffix configuration.
- `Polars.Expr` and `Polars.Operators` build a pure Haskell expression AST, including aggregation expressions.
- `Polars.Error` defines `PolarsError` and `PolarsErrorCode`.
- `Polars.Schema` defines schema field and datatype values.
- `Polars.IPC` provides IPC byte and file helpers.

## Dataset-driven tests

Generate committed smoke-test fixtures with:

```bash
uv run --with polars --with metasyn --with pyarrow python scripts/generate_dataset_fixtures.py
```

Default `stack test --fast` reads generated Polars/Metasyn CSV fixtures from `test/data/generated/` and runs lazy query smoke tests over those fixtures.

Run the opt-in NYC Taxi Parquet test with:

```bash
scripts/run-nyc-taxi-test.sh
```

The NYC sample is generated under `test/data/external/`, which is ignored by version control.

## Verification

Use these commands before committing implementation changes:

```bash
cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml
cargo clippy --manifest-path rust/polars-hs-ffi/Cargo.toml -- -D warnings
stack test --fast
hlint src app test
```
