# LazyFrame With Context Design Log

## Background

Rust Polars 0.53 exposes:

```rust
pub fn with_context<C: AsRef<[LazyFrame]>>(self, contexts: C) -> LazyFrame
```

The method adds external lazy plans to the computation graph so expressions can
refer to columns from those plans. The Python docs describe this as external
query context and recommend horizontal concat in newer Python releases, while
the pinned Rust 0.53 API still exposes `LazyFrame::with_context`.

## Problem

`polars-hs` can collect multiple lazy plans together, yet a single lazy query
cannot refer to columns provided by another lazy plan. This leaves a direct
LazyFrame parity gap for context-dependent expressions such as filling values
from a training-frame statistic or selecting a scalar aggregate from another
frame.

## Questions and Answers

Q: What should the public name be?

A: Use `withContext`. It mirrors upstream `with_context` and fits existing
camelCase lazy APIs.

Q: What argument order should Haskell use?

A: Use `withContext :: [LazyFrame] -> LazyFrame -> IO (Either PolarsError LazyFrame)`.
Options/context arguments come before the target frame in the existing module.

Q: Should an empty context list be rejected?

A: Preserve Polars behavior. Empty context lists are passed through.

Q: Does context transport need new ownership rules?

A: No. The ABI borrows all input handles for the dynamic extent of the call and
returns a new Rust-owned `LazyFrame` handle.

## Design

Public API:

```haskell
withContext :: [LazyFrame] -> LazyFrame -> IO (Either PolarsError LazyFrame)
```

C ABI:

```c
int phs_lazyframe_with_context(const struct phs_lazyframe *lazyframe,
                               const struct phs_lazyframe *const *contexts,
                               uintptr_t len,
                               struct phs_lazyframe **out,
                               struct phs_error **err);
```

Validation rules:

- Null `lazyframe` returns `InvalidArgument`.
- Null `contexts` with positive length returns `InvalidArgument`.
- Null context element returns `InvalidArgument`.
- Null output pointer returns `InvalidArgument`.

## Implementation Plan

1. Add RED Rust ABI tests for context-dependent selection and pointer
   validation.
2. Add RED Hspec coverage over committed CSV fixtures.
3. Add header declaration, Rust ABI, Raw import, public export, and wrapper.
4. Run focused GREEN, full Rust/Haskell tests, HLint, and whitespace check.

## Examples

```haskell
context <- select [alias "salary_ref" (col "salary")] employees
withCtx <- withContext [context] values
select [alias "age_plus_salary" (col "age" .+ first_ (col "salary_ref"))] withCtx
```

✅ Good pattern:

```haskell
withContext [trainingStats] scoringFrame
```

❌ Problem pattern:

```haskell
withColumns [alias "filled" (fillNull (median_ (col "train_value")) (col "value"))] scoringFrame
```

The second query references `train_value` without adding a context frame.

## Trade-offs

- The API exposes raw lazy contexts directly, matching Rust and avoiding a new
  context object type.
- Context column name collisions remain delegated to Polars.
- Empty contexts retain upstream behavior and avoid an extra Haskell-only rule.

## Implementation Results

Implemented `withContext` for LazyFrame.

Changed files:

- `docs/superpowers/specs/2026-05-18-polars-hs-lazyframe-with-context-design.md`
- `include/polars_hs.h`
- `rust/polars-hs-ffi/src/lazyframe.rs`
- `src/Polars/Internal/Raw.hs`
- `src/Polars/LazyFrame.hs`
- `test/Spec.hs`

Public Haskell API:

```haskell
withContext :: [LazyFrame] -> LazyFrame -> IO (Either PolarsError LazyFrame)
```

Rust ABI:

```c
int phs_lazyframe_with_context(const struct phs_lazyframe *lazyframe,
                               const struct phs_lazyframe *const *contexts,
                               uintptr_t len,
                               struct phs_lazyframe **out,
                               struct phs_error **err);
```

Implementation notes:

- Rust clones borrowed `LazyFrame` handles into a short-lived context vector and
  calls `LazyFrame::with_context`.
- Haskell reuses `withLazyFrameList`, matching `collectAll` and `explainAll`
  handle-array transport.

Verification:

- RED Rust focused test failed on missing `phs_lazyframe_with_context`.
- RED Hspec focused test failed on missing public `Pl.withContext`.
- Focused Rust `lazy_with_context_allows_external_columns`: 1/1 passed.
- Focused Hspec `adds external lazy context columns`: 1/1 passed.
- `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml`: 163/163 passed.
- `stack test --fast`: 208/208 passed.
- `hlint src test app`: no hints.
- `jj diff --git --color=never | git apply --cached --check --whitespace=error -`: passed.

Deviations:

- No deviations from the design.
