#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ ! -f test/data/external/nyc_taxi_sample.parquet ]]; then
  uv run --with polars --with metasyn --with pyarrow python scripts/generate_dataset_fixtures.py --nyc-taxi
fi

stack runghc test/NYCTaxi.hs
