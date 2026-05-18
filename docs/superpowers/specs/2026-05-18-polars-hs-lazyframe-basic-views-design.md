# LazyFrame Basic Views Design

## Background

Rust Polars 0.53 exposes these LazyFrame helpers:

```rust
pub fn cache(self) -> Self
pub fn first(self) -> LazyFrame
pub fn last(self) -> LazyFrame
```

`first` is equivalent to `slice(0, 1)`, and `last` is equivalent to
`slice(-1, 1)`. Python Polars also exposes `LazyFrame.clear()` for an empty
copy of the lazy frame. The local Rust 0.53 LazyFrame source does not expose a
direct `clear` method, so the binding can compose the same result as
`limit(0)`.

## Problem

`polars-hs` already has `lazyHead`, `lazyTail`, `slice`, and eager clear
helpers. It lacks the common one-row and cache helpers, plus a lazy clear
helper for schema-preserving empty results.

## Questions And Answers

Q: What public names should avoid Prelude conflicts?

A: Use `lazyFirst` and `lazyLast`, matching `lazyHead` and `lazyTail`. Use
`lazyClear` for parity with `dataFrameClear` and `seriesClear`. Use `cache`
because the name has no Prelude conflict.

Q: How should lazy clear be implemented?

A: Implement the Rust ABI as `lf.limit(0)`. This keeps lazy execution and
returns the same schema with zero rows.

Q: Does `cache` require an extra feature?

A: No. The method is available with the existing `lazy` feature.

## Design

Public Haskell API:

```haskell
lazyClear :: LazyFrame -> IO (Either PolarsError LazyFrame)
cache :: LazyFrame -> IO (Either PolarsError LazyFrame)
lazyFirst :: LazyFrame -> IO (Either PolarsError LazyFrame)
lazyLast :: LazyFrame -> IO (Either PolarsError LazyFrame)
```

C ABI:

```c
int phs_lazyframe_clear(const struct phs_lazyframe *lazyframe,
                        struct phs_lazyframe **out,
                        struct phs_error **err);

int phs_lazyframe_cache(const struct phs_lazyframe *lazyframe,
                        struct phs_lazyframe **out,
                        struct phs_error **err);

int phs_lazyframe_first(const struct phs_lazyframe *lazyframe,
                        struct phs_lazyframe **out,
                        struct phs_error **err);

int phs_lazyframe_last(const struct phs_lazyframe *lazyframe,
                       struct phs_lazyframe **out,
                       struct phs_error **err);
```

## Implementation Plan

1. Add RED Rust ABI tests for clear/cache/first/last outputs and null pointer
   validation.
2. Add RED Hspec coverage for public wrappers over `values.csv`.
3. Add Rust ABI declarations and implementations.
4. Add Raw imports, public Haskell wrappers, and Haddock comments.
5. Run focused GREEN and full verification.

## Examples

```haskell
empty <- lazyClear lf
one <- lazyFirst lf
final <- lazyLast lf
cached <- cache lf
```

## Trade-offs

- `lazyClear` composes `limit(0)` because the pinned Rust LazyFrame API has no
  direct clear helper.
- The one-row APIs use `lazyFirst` and `lazyLast` to keep unqualified imports
  practical.

## Implementation Results

Implemented on 2026-05-18.

- Added `lazyClear`, `cache`, `lazyFirst`, and `lazyLast` to
  `Polars.LazyFrame`.
- Added `phs_lazyframe_clear`, `phs_lazyframe_cache`, `phs_lazyframe_first`,
  and `phs_lazyframe_last` to the Rust-owned C ABI and public header.
- Implemented `lazyClear` as `lf.limit(0)`.
- Implemented `cache`, `lazyFirst`, and `lazyLast` through Rust Polars
  `LazyFrame::cache`, `LazyFrame::first`, and `LazyFrame::last`.
- Added Rust ABI and Hspec coverage for schema-preserving clear, cache result
  preservation, first row, last row, null lazyframe pointer, and null output
  pointer.

RED verification:

- `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml lazy_basic_views_cache_clear_first_last_work`
  failed on missing `phs_lazyframe_clear/cache/first/last`.
- `stack test --fast --test-arguments '--match "clears caches and selects boundary lazy rows"'`
  failed on missing `Pl.lazyClear`, `Pl.cache`, `Pl.lazyFirst`, and
  `Pl.lazyLast`.

GREEN verification:

- Focused Rust: 1/1 passed.
- Focused Hspec: 1/1 passed.
- `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml`: 151/151 passed.
- `stack test --fast`: 198/198 passed.
- `hlint src test app`: no hints.
- `jj diff --git --color=never | git apply --cached --check --whitespace=error -`: passed.

Deviations:

- The Rust ABI for clear composes `limit(0)` because the pinned Rust source does
  not expose `LazyFrame::clear`.
