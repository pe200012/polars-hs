# Dataset-Driven Testing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add deterministic dataset-driven smoke tests using Polars public data and Metasyn-generated data, plus an opt-in NYC Taxi real-world test.

**Architecture:** A Python fixture generator runs through `uv` with Polars, Metasyn, and PyArrow. Small generated fixtures are committed for normal Hspec tests. NYC Taxi generation and validation live behind an explicit opt-in script so normal CI remains stable.

**Tech Stack:** Haskell Stack/GHC 9.12.2, Hspec, Python 3 via `uv`, Polars Python, Metasyn, PyArrow, Rust Polars 0.53.0.

---

## File Structure

- Create `scripts/generate_dataset_fixtures.py`: generates small CSV/manifest fixtures and optional NYC Taxi Parquet sample.
- Create `scripts/run-nyc-taxi-test.sh`: opt-in real-world test runner.
- Create `test/NYCTaxi.hs`: Haskell real-world Parquet smoke test driver.
- Create `test/data/generated/polars_iris.csv`: committed Polars public dataset fixture.
- Create `test/data/generated/metasyn_people.csv`: committed Metasyn fixture.
- Create `test/data/generated/manifest.json`: fixture provenance and schema summary.
- Modify `test/Spec.hs`: add default Hspec tests for generated fixtures.
- Modify `package.yaml`: include generated CSV/JSON fixtures and scripts in package metadata.
- Modify `README.md` and `CHANGELOG.md`: document generation and opt-in test.
- Modify design and plan docs with implementation results.

---

### Task 1: Fixture generator

**Files:**
- Create: `scripts/generate_dataset_fixtures.py`

- [ ] **Step 1: Create the generator script**

Create `scripts/generate_dataset_fixtures.py`:

```python
#!/usr/bin/env python3
"""Generate dataset-driven fixtures for polars-hs.

Default mode writes small deterministic fixtures under test/data/generated.
The optional --nyc-taxi mode writes a sampled Parquet file under test/data/external.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import polars as pl
from metasyn import MetaFrame

ROOT = Path(__file__).resolve().parents[1]
GENERATED = ROOT / "test" / "data" / "generated"
EXTERNAL = ROOT / "test" / "data" / "external"
IRIS_URL = "https://huggingface.co/datasets/nameexhaustion/polars-docs/resolve/main/iris.csv"
NYC_TAXI_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"


def load_polars_dataset(name: str) -> pl.DataFrame:
    if hasattr(pl, "datasets") and hasattr(pl.datasets, "load"):
        return pl.datasets.load(name)
    if name == "iris":
        return pl.read_csv(IRIS_URL)
    raise ValueError(f"unsupported fallback dataset: {name}")


def normalize_iris(df: pl.DataFrame) -> pl.DataFrame:
    rename = {
        "sepal.length": "sepal_length",
        "sepal_length": "sepal_length",
        "sepal width": "sepal_width",
        "sepal.width": "sepal_width",
        "petal.length": "petal_length",
        "petal_length": "petal_length",
        "petal.width": "petal_width",
        "petal_width": "petal_width",
        "variety": "species",
        "Species": "species",
    }
    selected = []
    for source, target in rename.items():
        if source in df.columns and target not in selected:
            selected.append(target)
    renamed = df.rename({name: rename[name] for name in df.columns if name in rename})
    return renamed.select(["sepal_length", "sepal_width", "petal_length", "petal_width", "species"])


def metasyn_people() -> pl.DataFrame:
    base = pl.DataFrame(
        {
            "city": ["Kyoto", "Tokyo", "Osaka", "Kyoto", "Sapporo", "Tokyo", "Nara", "Osaka"],
            "age": [34, 41, 29, 52, 23, 37, 46, 31],
            "score": [9.5, 8.25, 7.75, 8.8, 6.5, 9.1, 7.2, 8.0],
        }
    )
    meta = MetaFrame.fit_dataframe(base)
    synth = meta.synthesize(16)
    return synth.select(
        pl.col("city").cast(pl.String),
        pl.col("age").cast(pl.Int64),
        pl.col("score").cast(pl.Float64),
    )


def write_default_fixtures() -> None:
    GENERATED.mkdir(parents=True, exist_ok=True)
    iris = normalize_iris(load_polars_dataset("iris"))
    people = metasyn_people()
    iris_path = GENERATED / "polars_iris.csv"
    people_path = GENERATED / "metasyn_people.csv"
    iris.write_csv(iris_path)
    people.write_csv(people_path)
    manifest = {
        "polars_iris": {
            "path": str(iris_path.relative_to(ROOT)),
            "rows": iris.height,
            "columns": iris.columns,
            "dtypes": [str(dtype) for dtype in iris.dtypes],
            "source": "pl.datasets.load('iris') when available, otherwise Hugging Face Polars docs iris CSV",
        },
        "metasyn_people": {
            "path": str(people_path.relative_to(ROOT)),
            "rows": people.height,
            "columns": people.columns,
            "dtypes": [str(dtype) for dtype in people.dtypes],
            "source": "metasyn.MetaFrame fitted to a small Polars DataFrame",
        },
    }
    (GENERATED / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_nyc_taxi_sample(rows: int) -> None:
    EXTERNAL.mkdir(parents=True, exist_ok=True)
    out = EXTERNAL / "nyc_taxi_sample.parquet"
    df = (
        pl.scan_parquet(NYC_TAXI_URL)
        .select(
            "passenger_count",
            "trip_distance",
            "fare_amount",
            "payment_type",
        )
        .filter(pl.col("fare_amount").is_not_null())
        .limit(rows)
        .collect()
    )
    df.write_parquet(out)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--nyc-taxi", action="store_true", help="generate the opt-in NYC Taxi sample")
    parser.add_argument("--nyc-rows", type=int, default=5000)
    args = parser.parse_args()
    write_default_fixtures()
    if args.nyc_taxi:
        write_nyc_taxi_sample(args.nyc_rows)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Make the script executable**

Run:

```bash
chmod +x scripts/generate_dataset_fixtures.py
```

- [ ] **Step 3: Generate default fixtures**

Run:

```bash
uv run --with polars --with metasyn --with pyarrow python scripts/generate_dataset_fixtures.py
```

Expected result:

```text
test/data/generated/polars_iris.csv exists
test/data/generated/metasyn_people.csv exists
test/data/generated/manifest.json exists
```

---

### Task 2: Default Hspec tests

**Files:**
- Modify: `test/Spec.hs`
- Modify: `package.yaml`

- [ ] **Step 1: Add generated fixture paths**

Near the other fixture paths in `test/Spec.hs`, add:

```haskell
polarsIrisCsv :: FilePath
polarsIrisCsv = "test/data/generated/polars_iris.csv"

metasynPeopleCsv :: FilePath
metasynPeopleCsv = "test/data/generated/metasyn_people.csv"
```

- [ ] **Step 2: Add dataset smoke tests**

Add this block before `describe "Polars.IPC"`:

```haskell
    describe "Dataset-driven fixtures" $ do
        it "reads a Polars public iris fixture" $ do
            result <- Pl.readCsv polarsIrisCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    Pl.shape df `shouldReturn` Right (150, 5)
                    fields <- Pl.schema df
                    fmap (map Pl.fieldName) fields `shouldBe` Right ["sepal_length", "sepal_width", "petal_length", "petal_width", "species"]
                    lengths <- Pl.column @Double df "sepal_length"
                    case lengths of
                        Left err -> expectationFailure (show err)
                        Right values -> V.length values `shouldBe` 150
                    species <- Pl.column @T.Text df "species"
                    case species of
                        Left err -> expectationFailure (show err)
                        Right values -> V.length values `shouldBe` 150

        it "reads a Metasyn synthetic people fixture" $ do
            result <- Pl.readCsv metasynPeopleCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    Pl.shape df `shouldReturn` Right (16, 3)
                    cities <- Pl.column @T.Text df "city"
                    case cities of
                        Left err -> expectationFailure (show err)
                        Right values -> V.length values `shouldBe` 16
                    ages <- Pl.column @Int64 df "age"
                    case ages of
                        Left err -> expectationFailure (show err)
                        Right values -> V.length values `shouldBe` 16
                    scores <- Pl.column @Double df "score"
                    case scores of
                        Left err -> expectationFailure (show err)
                        Right values -> V.length values `shouldBe` 16
```

- [ ] **Step 3: Update package metadata**

In `package.yaml`, add generated data and scripts to `extra-source-files`:

```yaml
- scripts/*.py
- scripts/*.sh
- test/data/generated/*.csv
- test/data/generated/*.json
```

- [ ] **Step 4: Run Hspec**

Run:

```bash
stack test --fast
```

Expected result: dataset tests pass, or RED failure identifies a concrete missing binding feature.

---

### Task 3: Opt-in NYC Taxi test

**Files:**
- Create: `test/NYCTaxi.hs`
- Create: `scripts/run-nyc-taxi-test.sh`

- [ ] **Step 1: Create the Haskell NYC Taxi test driver**

Create `test/NYCTaxi.hs`:

```haskell
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

import Data.Int (Int64)
import qualified Data.Vector as V
import qualified Polars as Pl

nycTaxiParquet :: FilePath
nycTaxiParquet = "test/data/external/nyc_taxi_sample.parquet"

main :: IO ()
main = do
    eager <- Pl.readParquet nycTaxiParquet
    case eager of
        Left err -> fail (show err)
        Right df -> do
            shape <- Pl.shape df
            case shape of
                Right (rows, 4) | rows > 0 -> pure ()
                other -> fail ("unexpected NYC Taxi shape: " <> show other)
            passengerCounts <- Pl.column @Int64 df "passenger_count"
            case passengerCounts of
                Left err -> fail (show err)
                Right values | V.length values > 0 -> pure ()
                Right _ -> fail "empty passenger_count column"

    lazy <- Pl.scanParquet nycTaxiParquet
    case lazy of
        Left err -> fail (show err)
        Right lf -> do
            filtered <- Pl.filter (Pl.col "fare_amount" Pl..> Pl.litDouble 0) lf
            case filtered of
                Left err -> fail (show err)
                Right lf1 -> do
                    limited <- Pl.limit 10 lf1
                    case limited of
                        Left err -> fail (show err)
                        Right lf2 -> do
                            collected <- Pl.collect lf2
                            case collected of
                                Left err -> fail (show err)
                                Right out -> do
                                    resultShape <- Pl.shape out
                                    case resultShape of
                                        Right (rows, 4) | rows >= 0 && rows <= 10 -> pure ()
                                        other -> fail ("unexpected filtered shape: " <> show other)
```

- [ ] **Step 2: Create the opt-in shell runner**

Create `scripts/run-nyc-taxi-test.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ ! -f test/data/external/nyc_taxi_sample.parquet ]]; then
  uv run --with polars --with metasyn --with pyarrow python scripts/generate_dataset_fixtures.py --nyc-taxi
fi

stack runghc test/NYCTaxi.hs
```

- [ ] **Step 3: Make the runner executable**

Run:

```bash
chmod +x scripts/run-nyc-taxi-test.sh
```

- [ ] **Step 4: Run the opt-in test when network is available**

Run:

```bash
scripts/run-nyc-taxi-test.sh
```

Expected result: NYC Taxi sample is generated and the Haskell test exits successfully. If download fails due network or upstream availability, record the failure message in Implementation Results and keep the script runnable.

---

### Task 4: Documentation and verification

**Files:**
- Modify: `README.md`
- Modify: `CHANGELOG.md`
- Modify: `docs/superpowers/specs/2026-04-28-polars-hs-dataset-driven-testing-design.md`
- Modify: `docs/superpowers/plans/2026-04-28-polars-hs-dataset-driven-testing.md`

- [ ] **Step 1: Update README**

Add a section near Verification:

````markdown
## Dataset-driven tests

Generate committed smoke-test fixtures with:

```bash
uv run --with polars --with metasyn --with pyarrow python scripts/generate_dataset_fixtures.py
```

Default `stack test --fast` reads generated Polars/Metasyn CSV fixtures from `test/data/generated/`.

Run the opt-in NYC Taxi Parquet test with:

```bash
scripts/run-nyc-taxi-test.sh
```
````

- [ ] **Step 2: Update CHANGELOG**

Add:

```markdown
- Dataset-driven smoke fixtures generated from Polars public data and Metasyn synthetic data.
- Opt-in NYC Taxi Parquet real-world test script.
```

- [ ] **Step 3: Full verification**

Run:

```bash
cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml
cargo clippy --manifest-path rust/polars-hs-ffi/Cargo.toml -- -D warnings
stack test --fast
hlint src app test
stack runghc examples/iris.hs
stack runghc examples/groupby.hs
stack runghc examples/join.hs
stack runghc examples/columns.hs
stack runghc examples/series.hs
stack runghc examples/construction.hs
```

Expected result: all commands pass.

- [ ] **Step 4: Record implementation results**

Append verification output, generated fixture provenance, NYC Taxi opt-in result, and any implemented feature gaps to the design doc.

- [ ] **Step 5: Commit and push**

Run:

```bash
jj commit -m "test: add dataset-driven smoke tests"
jj bookmark move master --to @-
jj git push --bookmark master
```
