# LazyFrame Explode Design

## Background

Rust Polars 0.53 exposes lazy explode:

```rust
pub fn explode(self, columns: Selector, options: ExplodeOptions) -> LazyFrame
```

`ExplodeOptions` has `empty_as_null` and `keep_nulls`, matching eager
`DataFrame::explode`.

Upstream references:

- https://docs.rs/polars/0.53.0/polars/prelude/struct.LazyFrame.html
- Local `polars-lazy-0.53.0/src/frame/mod.rs`.
- Local `polars-plan-0.53.0/src/dsl/builder_dsl.rs`.
- Python docs for `polars.LazyFrame.explode`.

## Problem

`polars-hs` exposes eager `dataFrameExplode`, and list columns can already be
created lazily through string split expressions. The lazy query API lacks direct
`LazyFrame::explode`, so callers must collect to eager before exploding list
columns.

## Questions And Answers

Q: Should columns be names or expressions?

A: Use explicit column names for this batch. The existing lazy drop/unique
wrappers already map name lists to `Selector` through `by_name`.

Q: Should option names match eager DataFrame explode?

A: Yes. Use `LazyFrameExplodeOptions` with `lazyFrameExplodeColumns`,
`lazyFrameExplodeEmptyAsNull`, and `lazyFrameExplodeKeepNulls`.

Q: Should empty columns be accepted?

A: No. The public Haskell API should return `InvalidArgument`, matching eager
`dataFrameExplode` and the existing `selector_from_names` helper behavior.

## Design

Public Haskell API:

```haskell
data LazyFrameExplodeOptions = LazyFrameExplodeOptions
    { lazyFrameExplodeColumns :: ![Text]
    , lazyFrameExplodeEmptyAsNull :: !Bool
    , lazyFrameExplodeKeepNulls :: !Bool
    }

defaultLazyFrameExplodeOptions :: LazyFrameExplodeOptions

explode :: LazyFrameExplodeOptions -> LazyFrame -> IO (Either PolarsError LazyFrame)
```

C ABI:

```c
int phs_lazyframe_explode(const struct phs_lazyframe *lazyframe,
                          const char *const *names,
                          uintptr_t names_len,
                          bool empty_as_null,
                          bool keep_nulls,
                          struct phs_lazyframe **out,
                          struct phs_error **err);
```

Rust implementation:

```rust
let names = name_vec(names, names_len)?;
let selector = selector_from_names(names, "explode")?;
let options = ExplodeOptions { empty_as_null, keep_nulls };
*out = lazyframe_into_raw(lf.explode(selector, options));
```

## Implementation Plan

1. Add RED Rust ABI tests for list-column explode, empty column list, null name
   pointer with positive length, missing column, scalar column, and null output
   pointer.
2. Add RED Hspec tests that create a list column with `strSplit`, call lazy
   `explode`, collect, and assert exploded values plus invalid arguments.
3. Add Rust ABI declaration and implementation.
4. Add Raw import, Haskell option record/default, wrapper, and export.
5. Run focused GREEN and full verification.

## Examples

Good:

```haskell
parts <- withColumns [alias "parts" (strSplit (col "phrase") (litText " "))] lf
exploded <- explode defaultLazyFrameExplodeOptions {lazyFrameExplodeColumns = ["parts"]} parts
```

## Trade-offs

- Selector expressions stay out of scope for this batch; name selectors match
  current lazy wrapper patterns.
- `empty_as_null` and `keep_nulls` are transported even when tests focus on
  normal list values, keeping option parity with eager explode.

## Implementation Results

- Added `LazyFrameExplodeOptions`, `defaultLazyFrameExplodeOptions`, and
  `explode` in `src/Polars/LazyFrame.hs`.
- Added `phs_lazyframe_explode` to `include/polars_hs.h`,
  `src/Polars/Internal/Raw.hs`, and `rust/polars-hs-ffi/src/lazyframe.rs`.
- Added Rust ABI tests over a native `List[String]` lazy frame, including empty
  column list, null names pointer with positive length, single null name,
  missing column collect failure, scalar column collect failure, null output
  pointer, and null lazy frame pointer.
- Added Hspec coverage using `phrases.csv` plus `strSplit`, verifying exploded
  `phrase` and `parts` values and lazy collect failures for missing/scalar
  columns.

Verification:

- `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml`: 146/146 passed.
- `stack test --fast`: 193/193 passed.
- `hlint src test app`: no hints.
- `jj diff --git --color=never | git apply --cached --check --whitespace=error -`:
  passed.

Deviation from plan:

- The Rust success-path fixture uses native Polars list columns directly. Hspec
  keeps the CSV plus `strSplit` path for end-to-end Haskell coverage.
