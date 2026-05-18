# LazyFrame Top And Bottom K Design

## Background

Rust Polars 0.53 exposes lazy top/bottom row selection:

```rust
pub fn top_k<E: AsRef<[Expr]>>(
    self,
    k: IdxSize,
    by_exprs: E,
    sort_options: SortMultipleOptions,
) -> Self

pub fn bottom_k<E: AsRef<[Expr]>>(
    self,
    k: IdxSize,
    by_exprs: E,
    sort_options: SortMultipleOptions,
) -> Self
```

`top_k` sorts by the provided expressions with order reversed and then slices to
`k`. `bottom_k` sorts by the provided expressions and slices to `k`. Both force
nulls last through `SortMultipleOptions::with_nulls_last(true)`.

Upstream references:

- https://docs.rs/polars/0.53.0/polars/prelude/struct.LazyFrame.html
- Local `polars-lazy-0.53.0/src/frame/mod.rs`.
- Python docs for `DataFrame.top_k` / LazyFrame top/bottom behavior.

## Problem

`polars-hs` exposes lazy `sort` and `limit`, but it lacks direct `top_k` and
`bottom_k` helpers. Direct helpers close a Polars query API gap and give callers
the same top/bottom row intent as upstream Polars.

## Questions And Answers

Q: Should `by` be column names or expressions?

A: Use `[Expr]`. Rust accepts expressions, and the Haskell lazy API already
compiles pure `Expr` values at each FFI boundary.

Q: How should reverse ordering be represented?

A: Use a bool list with one value or one value per `by` expression. For
`topK`, `False` returns largest rows and `True` reverses to smallest rows. For
`bottomK`, `False` returns smallest rows and `True` reverses to largest rows.

Q: Should empty `by` be accepted?

A: Yes. Rust Polars 0.53 routes empty `by` through `sort_by_exprs`, which returns
the original lazy plan, followed by `slice(0, k)`. Haskell should preserve that
Rust behavior and treat empty `by` as head-like selection.

## Design

Public Haskell API:

```haskell
data LazyFrameTopKOptions = LazyFrameTopKOptions
    { lazyFrameTopKBy :: ![Expr]
    , lazyFrameTopKReverse :: ![Bool]
    , lazyFrameTopKMaintainOrder :: !Bool
    }

defaultLazyFrameTopKOptions :: LazyFrameTopKOptions

topK :: LazyFrameTopKOptions -> Int -> LazyFrame -> IO (Either PolarsError LazyFrame)
bottomK :: LazyFrameTopKOptions -> Int -> LazyFrame -> IO (Either PolarsError LazyFrame)
```

C ABI:

```c
int phs_lazyframe_top_k(const struct phs_lazyframe *lazyframe,
                        uint64_t k,
                        const struct phs_expr *const *by,
                        uintptr_t by_len,
                        const uint8_t *reverse,
                        uintptr_t reverse_len,
                        bool maintain_order,
                        struct phs_lazyframe **out,
                        struct phs_error **err);

int phs_lazyframe_bottom_k(...same arguments...);
```

Rust implementation:

```rust
let k = idx_size_from_u64(k, "lazyframe top_k count")?;
let by = expr_vec(by, by_len)?;
let reverse = bool_vec(reverse, reverse_len, "reverse")?;
validate_sort_bool_options("reverse", by.len(), &reverse)?;
let options = SortMultipleOptions::default()
    .with_order_descending_multi(reverse)
    .with_maintain_order(maintain_order);
*out = lazyframe_into_raw(lf.top_k(k, by, options));
```

## Implementation Plan

1. Enable Rust Polars `top_k` feature for the pinned dependency set.
2. Add RED Rust ABI tests for top and bottom selection, reverse options, empty
   `by`, reverse length validation, k overflow, null `by` pointer with positive
   length, null reverse pointer, and null output pointer.
3. Add RED Hspec tests over committed lazy CSV fixtures for top, bottom,
   reverse, maintain-order option transport, and invalid Haskell arguments.
4. Add Rust ABI declarations and implementations.
5. Add Raw imports, Haskell option record/default, validation, wrappers, and
   exports.
6. Run focused GREEN and full verification.

## Examples

Good:

```haskell
topK
    defaultLazyFrameTopKOptions {lazyFrameTopKBy = [col "salary"]}
    2
    lf
```

Good:

```haskell
bottomK
    defaultLazyFrameTopKOptions
        { lazyFrameTopKBy = [col "salary"]
        , lazyFrameTopKReverse = [True]
        }
    2
    lf
```

## Trade-offs

- The initial API mirrors Rust expression-based `by` and keeps selector-style
  inputs out of scope.
- Haskell validates `k` and reverse-option shape before FFI to keep
  user-facing errors consistent with other lazy helpers.
- Null placement stays fixed to upstream `top_k`/`bottom_k` behavior.

## Implementation Results

Implemented as designed, with the empty-`by` parity correction above.

Files changed:

- `rust/polars-hs-ffi/Cargo.toml`: enabled `top_k` on `polars`, `polars-ops`, and `polars-plan`.
- `include/polars_hs.h`: added `phs_lazyframe_top_k` and `phs_lazyframe_bottom_k`.
- `rust/polars-hs-ffi/src/lazyframe.rs`: added bool-list option parsing, ABI wrappers, and Rust unit coverage.
- `src/Polars/Internal/Raw.hs`: added safe FFI imports.
- `src/Polars/LazyFrame.hs`: exported and implemented `LazyFrameTopKOptions`, `defaultLazyFrameTopKOptions`, `topK`, and `bottomK`.
- `test/Spec.hs`: added Hspec coverage for top, bottom, reverse, maintain-order option transport, empty-`by` head-like behavior, and invalid Haskell arguments.
- `docs/superpowers/specs/2026-05-18-polars-hs-lazyframe-top-bottom-k-design.md`: design log plus implementation results.

Behavior covered:

- `topK` with `reverse = [False]` returns largest rows by the given expression.
- `bottomK` with `reverse = [False]` returns smallest rows by the given expression.
- `topK` with `reverse = [True]` returns the smallest rows by the given expression.
- Empty `by` is accepted and acts like lazy head for Rust Polars parity.
- Negative Haskell counts and invalid reverse option lengths return `InvalidArgument`.
- Rust ABI rejects k overflow, null `by` with positive length, null reverse with positive length, reverse length mismatch, and null output pointer.

Verification so far:

- RED Rust failed on missing `phs_lazyframe_top_k` / `phs_lazyframe_bottom_k`.
- RED Hspec failed on missing `Pl.defaultLazyFrameTopKOptions`, option fields, and wrappers.
- First focused Hspec run surfaced the empty-`by` parity correction; tests and Haskell validation were updated.
- Focused Rust FFI: 1/1 passing.
- Focused Hspec top/bottom behavior: 1/1 passing.
- Focused Hspec argument validation: 1/1 passing.
