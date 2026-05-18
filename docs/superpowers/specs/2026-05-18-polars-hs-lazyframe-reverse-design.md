# LazyFrame Reverse Design

## Background

Rust Polars 0.53 exposes:

```rust
pub fn reverse(self) -> Self
```

The implementation rewrites the plan as:

```rust
self.select(vec![col(PlSmallStr::from_static("*")).reverse()])
```

## Problem

`polars-hs` already exposes eager `dataFrameReverse` and `seriesReverse`, while
lazy users must collect before reversing row order.

## Questions And Answers

Q: Should the Haskell API be named `reverse`?

A: Yes. `Polars.LazyFrame` already exports Polars-native names such as `filter`,
and callers commonly use the qualified `Polars` module. The module will hide
Prelude `reverse`.

Q: Are options needed?

A: No. Rust Polars 0.53 `LazyFrame::reverse` has no arguments.

Q: Is a new Cargo feature required?

A: No. This uses existing lazy expression planning.

## Design

Public Haskell API:

```haskell
reverse :: LazyFrame -> IO (Either PolarsError LazyFrame)
```

C ABI:

```c
int phs_lazyframe_reverse(const struct phs_lazyframe *lazyframe,
                          struct phs_lazyframe **out,
                          struct phs_error **err);
```

Rust implementation:

```rust
let lf = lazyframe_ref(lazyframe)?.value.clone();
*out = lazyframe_into_raw(lf.reverse());
```

## Implementation Plan

1. Add RED Rust ABI tests for reversing a lazy CSV scan, null lazy frame pointer,
   and null output pointer.
2. Add RED Hspec test that scans `values.csv`, reverses the lazy frame, collects,
   and verifies row order.
3. Add Rust ABI declaration and implementation.
4. Add Raw import, Haskell wrapper, and export.
5. Run focused GREEN and full verification.

## Examples

```haskell
scanResult <- scanCsv "test/data/values.csv"
reversed <- reverse lf
```

## Trade-offs

- The public name follows Polars terminology and accepts the same unqualified
  import ambiguity already present for `filter`.
- The wrapper stays minimal because all semantic work is in the Rust Polars
  logical plan.

## Implementation Results

- Added `reverse` to `src/Polars/LazyFrame.hs`, exported from the public module.
- Added `phs_lazyframe_reverse` to `include/polars_hs.h`,
  `src/Polars/Internal/Raw.hs`, and `rust/polars-hs-ffi/src/lazyframe.rs`.
- Added Rust ABI coverage for reversed row order, null lazy frame pointer, and
  null output pointer.
- Added Hspec coverage over `values.csv`, asserting reversed text and nullable
  integer columns.

Verification:

- `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml`: 147/147 passed.
- `stack test --fast`: 194/194 passed.
- `hlint src test app`: no hints.
- `jj diff --git --color=never | git apply --cached --check --whitespace=error -`:
  passed.

Deviation from plan:

- The public `reverse` name required internal uses of list reversal inside
  `Polars.LazyFrame` to qualify `Prelude.reverse` as `P.reverse`.
