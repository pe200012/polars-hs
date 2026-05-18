# DataFrame Sampling Design

## Background

Series sampling is already exposed in `polars-hs`. Rust Polars 0.53 also provides eager DataFrame sampling:

- `DataFrame::sample_n_literal(n, with_replacement, shuffle, seed)`
- `DataFrame::sample_frac(frac_series, with_replacement, shuffle, seed)`

The pinned Rust dependency enables the Polars `random` feature. Upstream docs.rs confirms these methods on Polars
0.53 DataFrame, and local source shows the same seeded sampling controls as Series sampling.

## Problem

Haskell users can sample Series values but cannot sample DataFrame rows. Adding row sampling improves eager DataFrame
parity and reuses the same validation and seed model already present for Series.

## Questions And Answers

### Should DataFrame sampling reuse Series options?

Answer: use a DataFrame-specific option record. The fields mirror Series sampling while keeping DataFrame API names
clear under the umbrella `Polars` module.

### Should invalid fractions be rejected in Haskell?

Answer: yes. Match Series sampling validation: finite, non-negative, and at most 1.0 without replacement.

### Should fractional sampling accept a scalar Series across the ABI?

Answer: no. The public API should accept `Double`; Rust can construct the single-value Polars Series needed by
`DataFrame::sample_frac`.

## Design

Public Haskell API:

```haskell
data DataFrameSampleOptions = DataFrameSampleOptions
    { dataFrameSampleWithReplacement :: !Bool
    , dataFrameSampleShuffle :: !Bool
    , dataFrameSampleSeed :: !(Maybe Word64)
    }

defaultDataFrameSampleOptions :: DataFrameSampleOptions
dataFrameSampleN :: DataFrameSampleOptions -> Int -> DataFrame -> IO (Either PolarsError DataFrame)
dataFrameSampleFrac :: DataFrameSampleOptions -> Double -> DataFrame -> IO (Either PolarsError DataFrame)
```

Rust ABI:

```c
int phs_dataframe_sample_n(const struct phs_dataframe *dataframe,
                           uint64_t n,
                           bool with_replacement,
                           bool shuffle,
                           bool has_seed,
                           uint64_t seed,
                           struct phs_dataframe **out,
                           struct phs_error **err);

int phs_dataframe_sample_frac(const struct phs_dataframe *dataframe,
                              double frac,
                              bool with_replacement,
                              bool shuffle,
                              bool has_seed,
                              uint64_t seed,
                              struct phs_dataframe **out,
                              struct phs_error **err);
```

## Implementation Plan

1. Add RED Rust tests for seeded `sample_n`, `sample_frac`, replacement sampling, and error paths.
2. Add RED Hspec tests for public API, deterministic seeded output, replacement, text/null row preservation, and
   invalid argument validation.
3. Add ABI declarations/imports.
4. Implement Rust wrappers over Polars DataFrame sampling.
5. Add Haskell option record, validation, wrappers, and exports.
6. Run focused RED/GREEN checks and full verification.

## Examples

Good:

```haskell
sampled <- dataFrameSampleN defaultDataFrameSampleOptions {dataFrameSampleSeed = Just 0} 2 df
```

Bad:

```haskell
sampled <- dataFrameSampleFrac defaultDataFrameSampleOptions 2.0 df
```

Without replacement, a fraction above 1.0 is rejected at the Haskell boundary.

## Trade-offs

- Deterministic seeded test outputs are tied to the pinned Polars 0.53 implementation and lockfile.
- The first API keeps sampling controls small; stratified/group-aware sampling belongs in a later groupby/lazy batch.

## Implementation Results

- Added `DataFrameSampleOptions`, `defaultDataFrameSampleOptions`, `dataFrameSampleN`, and `dataFrameSampleFrac`.
- Added `phs_dataframe_sample_n` and `phs_dataframe_sample_frac` to the C header, raw Haskell imports, and Rust FFI.
- Rust `sample_n` uses Polars `DataFrame::sample_n_literal`; Rust `sample_frac` builds a one-value fraction Series and calls `DataFrame::sample_frac`.
- Haskell rejects negative sample sizes, non-finite fractions, negative fractions, and no-replacement fractions above 1.0 before crossing FFI.
- Tests cover seeded deterministic rows, text/null row alignment, shuffle order, replacement sampling, replacement fraction above 1.0, empty sampling, oversampling failures, invalid arguments, and null output pointers.

Verification so far:

- RED Rust: missing `phs_dataframe_sample_n` and `phs_dataframe_sample_frac`.
- RED Hspec: missing public DataFrame sampling API.
- Focused Rust: `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml dataframe_sampling_uses_seeded_options` passed, 1/1.
- Focused Hspec: `stack test --fast --test-arguments='--match=samples'` passed, 2/2.
- Full Rust: `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml` passed, 134/134.
- Full Hspec: `stack test --fast` passed, 181/181.
- HLint: `hlint src test app` returned `No hints`.
- Whitespace: `jj diff --git --color=never | git apply --cached --check --whitespace=error -` returned exit 0.

Deviations:

- None.
