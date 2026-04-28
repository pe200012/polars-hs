#!/usr/bin/env python3
"""Generate dataset-driven fixtures for polars-hs.

Default mode writes small deterministic fixtures under test/data/generated.
The optional --nyc-taxi mode writes a sampled Parquet file under test/data/external.

Requires: polars, metasyn, pyarrow (available via uv run --with ...).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import polars as pl
from metasyn import MetaFrame

ROOT = Path(__file__).resolve().parents[1]
GENERATED = ROOT / "test" / "data" / "generated"
EXTERNAL = ROOT / "test" / "data" / "external"
IRIS_URL = "https://huggingface.co/datasets/nameexhaustion/polars-docs/resolve/main/iris.csv"
NYC_TAXI_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
)


def load_polars_dataset(name: str) -> pl.DataFrame:
    """Load a Polars public dataset, with fallback to Hugging Face CSV."""
    if hasattr(pl, "datasets") and hasattr(pl.datasets, "load"):
        try:
            return pl.datasets.load(name)  # type: ignore[attr-defined]
        except Exception as exc:
            print(f"pl.datasets.load({name!r}) failed: {exc}", file=sys.stderr)
            print("Falling back to Hugging Face URL.", file=sys.stderr)
    if name == "iris":
        print(f"Loading iris from {IRIS_URL}", file=sys.stderr)
        return pl.read_csv(IRIS_URL)
    raise ValueError(
        f"unsupported dataset: {name!r} (expected 'iris')"
    )


def normalize_iris(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize iris columns to the canonical names.

    Hugging Face mirror already uses canonical names, but older Polars
    versions and other mirrors use dotted or abbreviated forms.
    """
    rename_map = {
        "sepal.length": "sepal_length",
        "sepal_length": "sepal_length",
        "sepal width": "sepal_width",
        "sepal.width": "sepal_width",
        "sepal_width": "sepal_width",
        "petal.length": "petal_length",
        "petal_length": "petal_length",
        "petal.width": "petal_width",
        "petal_width": "petal_width",
        "variety": "species",
        "Species": "species",
        "species": "species",
    }
    # Build a rename dict only for columns that actually exist and need renaming.
    actual_renames = {}
    for col in df.columns:
        if col in rename_map:
            target = rename_map[col]
            if col != target:
                actual_renames[col] = target
        else:
            print(
                f"Warning: unexpected iris column {col!r}, keeping as-is.",
                file=sys.stderr,
            )
    if actual_renames:
        df = df.rename(actual_renames)
    # Select canonical columns in order.
    canonical = ["sepal_length", "sepal_width", "petal_length", "petal_width", "species"]
    missing = [c for c in canonical if c not in df.columns]
    if missing:
        raise ValueError(
            f"iris DataFrame is missing canonical columns: {missing}. "
            f"Available columns: {df.columns}"
        )
    return df.select(canonical)


def metasyn_people() -> pl.DataFrame:
    """Generate synthetic people data via Metasyn.

    Fits a MetaFrame to a small Polars DataFrame and synthesizes 16 seeded rows.
    Columns: city (String), age (Int64), score (Float64).
    """
    base = pl.DataFrame(
        {
            "city": [
                "Kyoto", "Tokyo", "Osaka", "Kyoto",
                "Sapporo", "Tokyo", "Nara", "Osaka",
            ],
            "age": [34, 41, 29, 52, 23, 37, 46, 31],
            "score": [9.5, 8.25, 7.75, 8.8, 6.5, 9.1, 7.2, 8.0],
        }
    )
    meta = MetaFrame.fit_dataframe(base, progress_bar=False)
    synth = meta.synthesize(16, seed=20260428, progress_bar=False)
    # Ensure deterministic types regardless of Metasyn version.
    return synth.select(
        pl.col("city").cast(pl.String),
        pl.col("age").cast(pl.Int64),
        pl.col("score").cast(pl.Float64),
    )


def write_default_fixtures() -> None:
    """Generate and write the default Polars iris + Metasyn people fixtures."""
    GENERATED.mkdir(parents=True, exist_ok=True)

    print("Generating Polars iris fixture ...", file=sys.stderr)
    iris = normalize_iris(load_polars_dataset("iris"))
    iris_path = GENERATED / "polars_iris.csv"
    iris.write_csv(iris_path)
    print(f"  Wrote {iris_path} ({iris.height} rows x {len(iris.columns)} cols)", file=sys.stderr)

    print("Generating Metasyn people fixture ...", file=sys.stderr)
    people = metasyn_people()
    people_path = GENERATED / "metasyn_people.csv"
    people.write_csv(people_path)
    print(f"  Wrote {people_path} ({people.height} rows x {len(people.columns)} cols)", file=sys.stderr)

    print("Writing manifest ...", file=sys.stderr)
    manifest = {
        "polars_iris": {
            "path": str(iris_path.relative_to(ROOT)),
            "rows": iris.height,
            "columns": iris.columns,
            "dtypes": [str(dt) for dt in iris.dtypes],
            "source": (
                "pl.datasets.load('iris') when available, "
                "otherwise Hugging Face Polars docs iris CSV"
            ),
        },
        "metasyn_people": {
            "path": str(people_path.relative_to(ROOT)),
            "rows": people.height,
            "columns": people.columns,
            "dtypes": [str(dt) for dt in people.dtypes],
            "source": "metasyn.MetaFrame fitted to a small Polars DataFrame",
        },
    }
    manifest_path = GENERATED / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(f"  Wrote {manifest_path}", file=sys.stderr)


def write_nyc_taxi_sample(rows: int) -> None:
    """Download and sample the NYC Yellow Taxi Jan 2024 Parquet file."""
    EXTERNAL.mkdir(parents=True, exist_ok=True)
    out = EXTERNAL / "nyc_taxi_sample.parquet"

    print(f"Scanning NYC Taxi from {NYC_TAXI_URL} ...", file=sys.stderr)
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
    print(
        f"  Wrote {out} ({df.height} rows x {len(df.columns)} cols)",
        file=sys.stderr,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate dataset-driven fixtures for polars-hs."
    )
    parser.add_argument(
        "--nyc-taxi",
        action="store_true",
        help="generate the opt-in NYC Taxi sample",
    )
    parser.add_argument(
        "--nyc-rows",
        type=int,
        default=5000,
        help="number of rows for NYC Taxi sample (default: 5000)",
    )
    args = parser.parse_args()

    try:
        write_default_fixtures()
    except Exception as exc:
        print(f"Error generating default fixtures: {exc}", file=sys.stderr)
        sys.exit(1)

    if args.nyc_taxi:
        try:
            write_nyc_taxi_sample(args.nyc_rows)
        except Exception as exc:
            print(f"Error generating NYC Taxi sample: {exc}", file=sys.stderr)
            sys.exit(1)

    print("Done.", file=sys.stderr)


if __name__ == "__main__":
    main()
