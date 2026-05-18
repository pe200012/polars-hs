# LazyFrame Shift Design Log

## Background

Rust Polars 0.53 exposes whole-frame lazy shift helpers:

```rust
pub fn shift<E: Into<Expr>>(self, n: E) -> Self
pub fn shift_and_fill<E: Into<Expr>, IE: Into<Expr>>(self, n: E, fill_value: IE) -> Self
```

These helpers select all columns and apply expression-level `shift` or
`shift_and_fill`. The binding already has eager `dataFrameShift` and
`seriesShift`.

## Problem

Haskell callers can shift eager DataFrames and Series, but LazyFrame lacks the
direct whole-frame shift helpers. Users can spell the behavior manually through
expressions, yet the upstream LazyFrame methods remain absent from the binding.

## Questions and Answers

Q: What should the public names be?

A: Use `lazyShift` and `lazyShiftAndFill`. The names match existing
`lazyHead`/`lazyTail` style and keep the aggregate `Polars` module clear beside
`dataFrameShift` and `seriesShift`.

Q: Should period be an `Int` or an `Expr`?

A: Use `Expr`, matching upstream. This supports literals and computed shift
periods through the existing expression compiler.

Q: How should mixed dtype fill be handled?

A: Let Polars validate it at collect time. Tests use a one-column numeric lazy
frame for `lazyShiftAndFill` so the intended fill semantics are clear.

## Design

Public API:

```haskell
lazyShift :: Expr -> LazyFrame -> IO (Either PolarsError LazyFrame)
lazyShiftAndFill :: Expr -> Expr -> LazyFrame -> IO (Either PolarsError LazyFrame)
```

C ABI:

```c
int phs_lazyframe_shift(const struct phs_lazyframe *lazyframe,
                        const struct phs_expr *n,
                        struct phs_lazyframe **out,
                        struct phs_error **err);

int phs_lazyframe_shift_and_fill(const struct phs_lazyframe *lazyframe,
                                 const struct phs_expr *n,
                                 const struct phs_expr *fill_value,
                                 struct phs_lazyframe **out,
                                 struct phs_error **err);
```

Validation rules:

- Null lazyframe pointer returns `InvalidArgument`.
- Null output pointer returns `InvalidArgument`.
- Null shift expression pointer returns `InvalidArgument`.
- Null fill expression pointer returns `InvalidArgument`.

## Implementation Plan

1. Add RED Rust ABI tests for positive shift, negative shift with fill, null
   lazyframe, null output, null shift expression, and null fill expression.
2. Add RED Hspec tests over `values.csv`: `lazyShift (litInt 1)` on the full
   frame and `lazyShiftAndFill (litInt 1) (litInt 0)` on the `age` projection.
3. Add header declarations, Rust ABI functions, Raw imports, public Haskell
   exports, and wrappers.
4. Run focused GREEN and full verification.

## Examples

```haskell
shifted <- lazyShift (litInt 1) lf
filled <- lazyShiftAndFill (litInt 1) (litInt 0) ageOnlyLf
```

Good pattern:

```haskell
lazyShiftAndFill (litInt (-1)) (litInt 0) lf
```

Bad pattern:

```haskell
withColumns [col "age"] lf
```

## Trade-offs

- Expression period arguments preserve upstream flexibility.
- The `lazy*` prefix avoids public name ambiguity in the aggregate re-export.
- Mixed dtype fill errors remain Polars errors, preserving upstream semantics.

## Implementation Results

Implemented LazyFrame whole-frame shift and shift-with-fill helpers.

Changed files:

- `docs/superpowers/specs/2026-05-18-polars-hs-lazyframe-shift-design.md`
- `include/polars_hs.h`
- `rust/polars-hs-ffi/src/lazyframe.rs`
- `src/Polars/Internal/Raw.hs`
- `src/Polars/LazyFrame.hs`
- `test/Spec.hs`

Public Haskell API:

```haskell
lazyShift :: Expr -> LazyFrame -> IO (Either PolarsError LazyFrame)
lazyShiftAndFill :: Expr -> Expr -> LazyFrame -> IO (Either PolarsError LazyFrame)
```

Rust ABI:

```c
int phs_lazyframe_shift(const struct phs_lazyframe *lazyframe,
                        const struct phs_expr *n,
                        struct phs_lazyframe **out,
                        struct phs_error **err);

int phs_lazyframe_shift_and_fill(const struct phs_lazyframe *lazyframe,
                                 const struct phs_expr *n,
                                 const struct phs_expr *fill_value,
                                 struct phs_lazyframe **out,
                                 struct phs_error **err);
```

Implementation notes:

- Rust uses `LazyFrame::shift` and `LazyFrame::shift_and_fill`.
- Haskell `lazyShift` reuses the single-expression wrapper path.
- Haskell `lazyShiftAndFill` compiles and holds both expression handles across
  the FFI call.

Verification:

- RED Rust focused test failed on missing `phs_lazyframe_shift` and
  `phs_lazyframe_shift_and_fill`.
- RED Hspec focused test failed on missing public `Pl.lazyShift` and
  `Pl.lazyShiftAndFill`.
- Focused Rust shift test: 1/1 passed.
- Focused Hspec shift test: 1/1 passed.
- `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml`: 160/160 passed.
- `stack test --fast`: 205/205 passed.
- `hlint src test app`: no hints.
- `jj diff --git --color=never | git apply --cached --check --whitespace=error -`: passed.

Deviation notes:

- Public names use the `lazy` prefix for consistency with existing LazyFrame
  count-limited helpers.
