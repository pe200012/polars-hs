# Polars HS Series Distinct Predicates Design

## Background

Rust Polars 0.53 exposes Series-level distinct predicate helpers in
`polars-ops`:

```rust
is_duplicated(&Series)
is_unique(&Series)
is_first_distinct(&Series)
is_last_distinct(&Series)
```

Docs and source checked:

- https://docs.rs/crate/polars/latest/features
- `polars-ops-0.53.0/src/series/ops/is_unique.rs`
- `polars-ops-0.53.0/src/series/ops/is_first_distinct.rs`
- `polars-ops-0.53.0/src/series/ops/is_last_distinct.rs`

## Problem

The expression DSL already exposes distinct predicates, but eager Series users
cannot produce these boolean masks directly. Adding the Series variants makes
mask construction symmetric with `seriesIsNull`, float predicates, and
`seriesFilter`.

## Questions and Answers

Q: Should the return type be `Series` or `Vector (Maybe Bool)`?

A: Return `Series`, matching the existing eager predicate style and preserving
mask composability.

Q: Should these functions live in `Polars.Series`?

A: Yes. They operate on an owned Series handle and return a boolean Series
handle.

Q: Should the Rust adapter add a direct `polars-ops` dependency?

A: Yes. The helper functions are exported from `polars-ops::series::ops`, while
the existing `polars` dependency already enables the same feature flags.

## Design

Public API:

```haskell
seriesIsDuplicated :: Series -> IO (Either PolarsError Series)
seriesIsUnique :: Series -> IO (Either PolarsError Series)
seriesIsFirstDistinct :: Series -> IO (Either PolarsError Series)
seriesIsLastDistinct :: Series -> IO (Either PolarsError Series)
```

C ABI:

```c
int phs_series_is_duplicated(const phs_series *series, phs_series **out, phs_error **err);
int phs_series_is_unique(const phs_series *series, phs_series **out, phs_error **err);
int phs_series_is_first_distinct(const phs_series *series, phs_series **out, phs_error **err);
int phs_series_is_last_distinct(const phs_series *series, phs_series **out, phs_error **err);
```

Rust mapping:

```rust
is_duplicated(value)?.into_series()
is_unique(value)?.into_series()
is_first_distinct(value)?.into_series()
is_last_distinct(value)?.into_series()
```

## Implementation Plan

1. Add RED Hspec tests over text values with duplicates.
2. Add Haskell exports and wrappers.
3. Add Raw.hs imports and C header declarations.
4. Add direct `polars-ops` dependency and Rust ABI functions.
5. Add Rust FFI tests for expected boolean masks.
6. Run focused Hspec and Rust tests.
7. Run full Rust, full Stack/Hspec, HLint, and `git diff --check`.
8. Append implementation results.

## Examples

Good:

```haskell
mask <- seriesIsUnique names
uniqueOnly <- seriesFilter mask names
```

Bad:

```haskell
values <- seriesBool =<< seriesIsUnique names
```

## Trade-offs

Adding `polars-ops` as a direct dependency expands the Rust adapter dependency
surface slightly, but keeps the eager Series implementation aligned with Polars
0.53 internals and avoids reimplementing distinct logic across dtypes.

## Implementation Results

Implemented:

- `seriesIsDuplicated`
- `seriesIsUnique`
- `seriesIsFirstDistinct`
- `seriesIsLastDistinct`

Changed files:

- `src/Polars/Series.hs`
- `src/Polars/Internal/Raw.hs`
- `include/polars_hs.h`
- `rust/polars-hs-ffi/Cargo.toml`
- `rust/polars-hs-ffi/Cargo.lock`
- `rust/polars-hs-ffi/src/series.rs`
- `test/Spec.hs`

Behavior verified over text values:

```haskell
[Just "a", Just "b", Just "a", Nothing, Nothing, Just "c"]
```

Expected masks:

- duplicated: `[True, False, True, True, True, False]`
- unique: `[False, True, False, False, False, True]`
- first distinct: `[True, True, False, True, False, True]`
- last distinct: `[False, True, True, False, True, True]`

Additional edge coverage:

- Int64 values with duplicates and a single null.
- Bool values with duplicates.
- Empty Int64 Series.
- Singleton Bool Series.

Deviation:

- The design text mentioned `polars_ops::series::ops`, but Rust keeps the
  `ops` module private. The implementation imports the public re-exports from
  `polars_ops::series`.

Verification:

- RED Hspec failed on missing `Pl.seriesIsDuplicated`, `Pl.seriesIsUnique`,
  `Pl.seriesIsFirstDistinct`, and `Pl.seriesIsLastDistinct`.
- Focused Rust FFI:
  `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml --release series_distinct -- --nocapture`
  passed: 2/2.
- Focused Hspec:
  `PATH="$HOME/.ghcup/bin:$PATH" stack --system-ghc test --fast --test-arguments '--match "Series distinct predicates"'`
  passed: 2/2.
- Full Rust FFI:
  `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml --release`
  passed: 106/106.
- HLint:
  `PATH="$HOME/.ghcup/bin:$PATH" hlint src test`
  returned `No hints`.
- Whitespace:
  `git diff --check`
  returned no output.
- Full Stack/Hspec:
  `PATH="$HOME/.ghcup/bin:$PATH" stack --system-ghc test --fast`
  passed: 156/156.

Review follow-up:

- A read-only subagent found API/ABI alignment intact.
- The reported design-log evidence gap was closed in this section.
- The reported single-input test gap was reduced with numeric, boolean, empty,
  and singleton coverage in both Hspec and Rust FFI tests.
