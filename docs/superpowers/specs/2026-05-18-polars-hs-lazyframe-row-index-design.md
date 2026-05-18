# LazyFrame Row Index Design

## Background

Rust Polars 0.53 exposes:

```rust
pub fn with_row_index<S>(self, name: S, offset: Option<IdxSize>) -> LazyFrame
where
    S: Into<PlSmallStr>
```

The new column is inserted at index 0. `None` starts at `0`; `Some offset`
starts at that offset. The row-index dtype follows Polars `IdxSize`.

## Problem

`polars-hs` already exposes eager `dataFrameWithRowIndex`. Lazy users must
collect before adding a row index, which loses Polars scan-level planning support.

## Questions And Answers

Q: Should the public name be `withRowIndex`?

A: Yes. It matches the existing `withColumns` style in `Polars.LazyFrame` and
avoids colliding with eager `dataFrameWithRowIndex`.

Q: How should offsets be transported?

A: Use `Maybe Int` in Haskell, reject negative values before FFI, transport
`has_offset` plus `Word64`, and convert to `IdxSize` in Rust with overflow
checking.

Q: Where do duplicate-name errors surface?

A: Let Polars surface them. Depending on whether the plan remains a scan or map,
errors may surface during plan resolution or collect.

## Design

Public Haskell API:

```haskell
withRowIndex :: Text -> Maybe Int -> LazyFrame -> IO (Either PolarsError LazyFrame)
```

C ABI:

```c
int phs_lazyframe_with_row_index(const struct phs_lazyframe *lazyframe,
                                 const char *name,
                                 bool has_offset,
                                 uint64_t offset,
                                 struct phs_lazyframe **out,
                                 struct phs_error **err);
```

Rust implementation:

```rust
let name = PlSmallStr::from_str(c_str_to_str(name, "name")?);
let offset = if has_offset { Some(idx_size_from_u64(offset, "row index offset")?) } else { None };
*out = lazyframe_into_raw(lf.with_row_index(name, offset));
```

## Implementation Plan

1. Add RED Rust ABI tests for default and offset row indexes, null name pointer,
   null lazy frame pointer, null output pointer, and offset overflow.
2. Add RED Hspec test for default and offset row-index columns, negative offset,
   and duplicate-name collect failure.
3. Add Rust ABI declaration and implementation.
4. Add Raw import, Haskell wrapper, and export.
5. Run focused GREEN and full verification.

## Examples

```haskell
indexed <- withRowIndex "row_nr" (Just 10) lf
defaultIndexed <- withRowIndex "row_nr" Nothing lf
```

## Trade-offs

- The Haskell API keeps the same offset model as eager DataFrame row-index
  support.
- Name validation uses the shared CString transport and Polars schema resolver.

## Implementation Results

Implemented on 2026-05-18.

- Added `withRowIndex` to `Polars.LazyFrame`.
- Added `phs_lazyframe_with_row_index` to the Rust-owned C ABI and public header.
- Added Haskell Raw import and managed-handle wrapper.
- Added Rust ABI tests for default offset, explicit offset, null pointer
  validation, and `IdxSize` overflow.
- Added Hspec coverage for default offset, explicit offset, schema position,
  duplicate-name collect failure, and negative Haskell offset validation.

Verification:

- `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml`: 148/148 passed.
- `stack test --fast`: 195/195 passed.
- `hlint src test app`: no hints.
- `jj diff --git --color=never | git apply --cached --check --whitespace=error -`: passed.

Deviations:

- The public wrapper uses existing `optionalNonNegativeWord64` validation for
  Haskell-side offset handling, keeping behavior aligned with other row-count
  options.
- Duplicate-name validation remains collect/schema-resolution behavior because
  upstream `LazyFrame::with_row_index` returns a `LazyFrame`.
