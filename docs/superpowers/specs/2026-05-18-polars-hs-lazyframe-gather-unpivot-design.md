# LazyFrame Gather-Every And Unpivot Design

## Background

Polars Python exposes `LazyFrame.gather_every(n, offset=0)` for row-strided
lazy selection. Rust Polars 0.53 exposes the same physical operation through
`Expr::gather_every(self, n: usize, offset: usize) -> Expr`, so a frame-level
binding can apply it to `all()` columns.

Rust Polars 0.53 also exposes lazy unpivot behind the `pivot` feature:

```rust
pub fn unpivot(self, args: UnpivotArgsDSL) -> LazyFrame
```

`UnpivotArgsDSL` carries `on: Option<Selector>`, `index: Selector`,
`variable_name: Option<PlSmallStr>`, and `value_name: Option<PlSmallStr>`.

## Problem

`polars-hs` has eager DataFrame gather-every and unpivot plus Series
gather-every. Lazy users currently collect before using those reshaping
operations, which loses lazy planning and optimization.

## Questions And Answers

Q: Should the LazyFrame gather API be named `gatherEvery`?

A: Yes. `Polars.LazyFrame` uses concise operation names, and the eager modules
already use `dataFrameGatherEvery` and `seriesGatherEvery`.

Q: How should gather-every be implemented without a direct Rust LazyFrame method?

A: Compile it as `lf.select([all().as_expr().gather_every(step, offset)])`, which
applies the same row-stride expression to every column.

Q: How should `on = Just []` be represented for lazy unpivot?

A: Use `Selector::Empty` via Polars `empty()` so it remains distinct from
`Nothing`, which lets Polars choose all non-index columns.

Q: Are extra Cargo features required?

A: Add `pivot` to the `polars` dependency. The existing eager unpivot already
uses `polars-ops/pivot`; lazy `LazyFrame::unpivot` is gated on the `polars`
feature.

## Design

Public Haskell API:

```haskell
gatherEvery :: Int -> Int -> LazyFrame -> IO (Either PolarsError LazyFrame)

data LazyFrameUnpivotOptions = LazyFrameUnpivotOptions
    { lazyFrameUnpivotOn :: Maybe [Text]
    , lazyFrameUnpivotIndex :: [Text]
    , lazyFrameUnpivotVariableName :: Maybe Text
    , lazyFrameUnpivotValueName :: Maybe Text
    }

defaultLazyFrameUnpivotOptions :: LazyFrameUnpivotOptions

unpivot :: LazyFrameUnpivotOptions -> LazyFrame -> IO (Either PolarsError LazyFrame)
```

C ABI:

```c
int phs_lazyframe_gather_every(const struct phs_lazyframe *lazyframe,
                               uint64_t step,
                               uint64_t offset,
                               struct phs_lazyframe **out,
                               struct phs_error **err);

int phs_lazyframe_unpivot(const struct phs_lazyframe *lazyframe,
                          bool has_on,
                          const char *const *on,
                          uintptr_t on_len,
                          const char *const *index,
                          uintptr_t index_len,
                          const char *variable_name,
                          const char *value_name,
                          struct phs_lazyframe **out,
                          struct phs_error **err);
```

## Implementation Plan

1. Add RED Rust ABI tests for lazy gather-every success, offset success, empty
   offset result, zero step, null lazyframe, and null output.
2. Add RED Hspec tests for `gatherEvery` success, offset success, empty offset,
   zero step, negative step, and negative offset.
3. Add RED Rust ABI tests for lazy unpivot explicit/default/empty `on`, custom
   names, null pointer validation, and missing-column collect failure.
4. Add RED Hspec tests for lazy unpivot explicit/default/empty `on` and missing
   column collect failure.
5. Add Cargo feature, header declarations, Rust ABI implementations, Raw imports,
   public Haskell wrappers, and Haddock comments.
6. Run focused GREEN and full verification.

## Examples

```haskell
sampled <- gatherEvery 2 1 lf

long <-
    unpivot
        defaultLazyFrameUnpivotOptions
            { lazyFrameUnpivotIndex = ["department"]
            , lazyFrameUnpivotOn = Just ["salary"]
            , lazyFrameUnpivotVariableName = Just "metric"
            , lazyFrameUnpivotValueName = Just "amount"
            }
        lf
```

## Trade-offs

- Gather-every uses a selector expression instead of a direct Rust LazyFrame
  method because Polars 0.53 exposes the primitive at expression level.
- Lazy unpivot uses selectors internally while the first public API remains
  explicit column-name based, matching the eager DataFrame API.

## Implementation Results

Implemented on 2026-05-18.

- Added `gatherEvery` to `Polars.LazyFrame`.
- Added `LazyFrameUnpivotOptions`, `defaultLazyFrameUnpivotOptions`, and
  `unpivot` to `Polars.LazyFrame`.
- Added `phs_lazyframe_gather_every` and `phs_lazyframe_unpivot` to the
  Rust-owned C ABI and public header.
- Enabled Polars `pivot` feature for lazy unpivot.
- Added Rust ABI tests for gather-every row stride, offset, empty offset,
  zero step, null lazyframe, and null output.
- Added Rust ABI tests for unpivot explicit/default/empty `on`, custom names,
  missing-column collect failure, null pointers, and null output.
- Added Hspec coverage for public lazy gather-every and lazy unpivot behavior.

RED verification:

- `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml lazy_gather_every_returns_strided_rows`
  failed on missing `phs_lazyframe_gather_every` and `phs_lazyframe_unpivot`.
- `stack test --fast --test-arguments '--match "gathers every nth lazy row"'`
  failed on missing `Pl.gatherEvery`, `Pl.unpivot`, and lazy unpivot exports.

GREEN verification:

- Focused Rust gather-every: 1/1 passed.
- Focused Hspec gather-every: 1/1 passed.
- Focused Rust unpivot: 1/1 passed.
- Focused Hspec unpivot: 1/1 passed.
- `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml`: 150/150 passed.
- `stack test --fast`: 197/197 passed.
- `hlint src test app`: no hints.
- `jj diff --git --color=never | git apply --cached --check --whitespace=error -`: passed.

Deviations:

- The public gather name is `gatherEvery`, matching other concise
  `Polars.LazyFrame` exports.
- Lazy unpivot allows empty `index` and empty `on` through an internal
  selector helper that maps empty lists to `Selector::Empty`.
