# LazyFrame Remove Design Log

## Background

Rust Polars 0.53 exposes row removal through:

```rust
pub fn remove(self, predicate: Expr) -> Self
```

Upstream implements it as the complement of filter semantics while retaining
rows where the predicate evaluates to null.

## Problem

The binding exposes `filter`, so callers can keep rows matching a predicate.
It lacks the direct `remove` companion that drops rows matching a predicate and
retains false or null predicate rows.

## Questions and Answers

Q: Should the API be called `remove`?

A: Use `removeRows`. The name describes row removal and avoids confusing it
with column dropping helpers in the aggregate API surface.

Q: How should null predicates behave?

A: Preserve upstream semantics. Rows where the predicate is null remain in the
output.

Q: Does this need a new expression transport?

A: No. It has the same shape as `filter`: one compiled predicate expression,
one input LazyFrame, and one output LazyFrame.

## Design

Public API:

```haskell
removeRows :: Expr -> LazyFrame -> IO (Either PolarsError LazyFrame)
```

C ABI:

```c
int phs_lazyframe_remove(const struct phs_lazyframe *lazyframe,
                         const struct phs_expr *predicate,
                         struct phs_lazyframe **out,
                         struct phs_error **err);
```

Validation rules:

- Null lazyframe pointer returns `InvalidArgument`.
- Null predicate pointer returns `InvalidArgument`.
- Null output pointer returns `InvalidArgument`.

## Implementation Plan

1. Add RED Rust ABI tests for dropping true predicate rows, retaining null
   predicate rows, and null pointer validation.
2. Add RED Hspec tests over `values.csv` for `age > 30` and `isNull age`.
3. Add header declaration, Rust ABI function, Raw import, public Haskell export,
   and wrapper.
4. Run focused GREEN and full verification.

## Examples

```haskell
kept <- removeRows (col "age" .> litInt 30) lf
withoutNullAges <- removeRows (isNull (col "age")) lf
```

Good pattern:

```haskell
removeRows (isNull (col "age")) lf
```

Bad pattern:

```haskell
filter (isNotNull (col "age")) lf
```

## Trade-offs

- `removeRows` is explicit at the Haskell API boundary.
- The wrapper stays aligned with `filter` and shares expression compilation.
- Null predicate behavior is delegated to Polars and pinned with tests.

## Implementation Results

Implemented LazyFrame row removal by predicate.

Changed files:

- `docs/superpowers/specs/2026-05-18-polars-hs-lazyframe-remove-design.md`
- `include/polars_hs.h`
- `rust/polars-hs-ffi/src/lazyframe.rs`
- `src/Polars/Internal/Raw.hs`
- `src/Polars/LazyFrame.hs`
- `test/Spec.hs`

Public Haskell API:

```haskell
removeRows :: Expr -> LazyFrame -> IO (Either PolarsError LazyFrame)
```

Rust ABI:

```c
int phs_lazyframe_remove(const struct phs_lazyframe *lazyframe,
                         const struct phs_expr *predicate,
                         struct phs_lazyframe **out,
                         struct phs_error **err);
```

Implementation notes:

- Rust uses `LazyFrame::remove`.
- Haskell `removeRows` reuses the single-expression LazyFrame wrapper path.
- `filter` now also uses the same wrapper helper.

Verification:

- RED Rust focused test failed on missing `phs_lazyframe_remove`.
- RED Hspec focused test failed on missing public `Pl.removeRows`.
- Focused Rust remove test: 1/1 passed.
- Focused Hspec remove test: 1/1 passed.
- `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml`: 161/161 passed.
- `stack test --fast`: 206/206 passed.
- `hlint src test app`: no hints.
- `jj diff --git --color=never | git apply --cached --check --whitespace=error -`: passed.

Deviation notes:

- Public name is `removeRows`; raw ABI keeps upstream `remove`.
