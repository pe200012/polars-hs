# LazyFrame Collect All Design Log

## Background

Rust Polars 0.53 exposes multi-plan lazy execution through
`LazyFrame::collect_all_with_engine(plans, engine, opt_state)` and plan
inspection through `LazyFrame::explain_all(plans, opt_state)`.

`polars-hs` currently collects one `LazyFrame` at a time. It already has a
managed DataFrame-array ABI for eager `partitionBy`, so multi-collect can reuse
that ownership pattern.

## Problem

Haskell callers cannot collect several independent lazy plans through Polars'
multi-plan executor. This leaves a query-engine parity gap for workflows that
build several lazy plans and materialize them together.

## Questions and Answers

Q: How should per-plan optimizer settings map to Rust Polars 0.53?

A: Rust `collect_all_with_engine` accepts one `OptFlags` value for the whole
multi-plan execution. The ABI will use the first input LazyFrame's flags when
the list is non-empty and Polars defaults for an empty list.

Q: Should `collectAll` expose an engine argument?

A: Provide both `collectAllWithEngine` and `collectAll`. `collectAll` uses
`LazyInMemory` to match the current single-frame `collect` default.

Q: How should empty lists behave?

A: `collectAllWithEngine engine []` returns `Right []`, matching upstream's
empty-plan handling. `explainAll []` returns the upstream Polars plan text or a
Polars error if the upstream implementation rejects the empty plan.

## Design

Public API:

```haskell
collectAll :: [LazyFrame] -> IO (Either PolarsError [DataFrame])
collectAllWithEngine :: LazyExecutionEngine -> [LazyFrame] -> IO (Either PolarsError [DataFrame])
explainAll :: [LazyFrame] -> IO (Either PolarsError Text)
```

C ABI:

```c
int phs_lazyframe_collect_all_with_engine(const struct phs_lazyframe *const *lazyframes,
                                          uintptr_t len,
                                          int engine,
                                          struct phs_dataframe_array **out,
                                          struct phs_error **err);

int phs_lazyframe_explain_all(const struct phs_lazyframe *const *lazyframes,
                              uintptr_t len,
                              struct phs_bytes **out,
                              struct phs_error **err);
```

Validation rules:

- Null `lazyframes` pointer with positive length returns `InvalidArgument`.
- Null output pointer returns `InvalidArgument`.
- Null item pointer returns `InvalidArgument`.
- Unknown engine code returns `InvalidArgument`.

## Implementation Plan

1. Add RED Rust tests for multi-collect shapes, streaming engine collection,
   empty list, unknown engine, null array pointer, null item pointer, null output
   pointer, and explain-all plan text.
2. Add RED Hspec tests for `collectAll`, `collectAllWithEngine LazyStreaming`,
   `collectAllWithEngine LazyInMemory`, empty lists, and `explainAll`.
3. Make Rust dataframe-array construction available inside the crate, add the
   lazyframe ABI functions, add header declarations, Raw imports, and Haskell
   wrappers.
4. Run focused GREEN and full verification.

## Examples

```haskell
frames <- collectAll [lfA, lfB]
streamed <- collectAllWithEngine LazyStreaming [lfA, lfB]
plan <- explainAll [lfA, lfB]
```

Good pattern:

```haskell
collectAllWithEngine LazyStreaming [queryA, queryB]
```

Bad pattern:

```haskell
mapM collect [queryA, queryB]
```

## Trade-offs

- Returning `[DataFrame]` reuses the existing managed DataFrame-array ownership
  model.
- First-frame optimizer flags keep the binding faithful to Rust 0.53's single
  `OptFlags` multi-plan API.
- Richer per-query optimizer policy would require a higher-level Haskell design
  on top of the Rust API.

## Implementation Results

Implemented multi-plan lazy collection and plan explanation.

Changed files:

- `docs/superpowers/specs/2026-05-18-polars-hs-lazyframe-collect-all-design.md`
- `include/polars_hs.h`
- `rust/polars-hs-ffi/src/dataframe.rs`
- `rust/polars-hs-ffi/src/lazyframe.rs`
- `src/Polars/Internal/Raw.hs`
- `src/Polars/LazyFrame.hs`
- `test/Spec.hs`

Public Haskell API:

```haskell
collectAll :: [LazyFrame] -> IO (Either PolarsError [DataFrame])
collectAllWithEngine :: LazyExecutionEngine -> [LazyFrame] -> IO (Either PolarsError [DataFrame])
explainAll :: [LazyFrame] -> IO (Either PolarsError Text)
```

Rust ABI:

```c
int phs_lazyframe_collect_all_with_engine(const struct phs_lazyframe *const *lazyframes,
                                          uintptr_t len,
                                          int engine,
                                          struct phs_dataframe_array **out,
                                          struct phs_error **err);

int phs_lazyframe_explain_all(const struct phs_lazyframe *const *lazyframes,
                              uintptr_t len,
                              struct phs_bytes **out,
                              struct phs_error **err);
```

Implementation notes:

- Rust extracts `DslPlan` values from input handles and uses the first frame's
  optimizer flags, or `OptFlags::default()` for an empty list.
- `collectAll` is `collectAllWithEngine LazyInMemory`.
- Multi-frame results reuse the existing `phs_dataframe_array` ownership model.
- Haskell wrappers keep every `LazyFrame` handle alive while the raw pointer
  array crosses the FFI boundary.

Verification:

- Focused Rust collect-all test: 1/1 passed.
- Focused Hspec collect-all test: 1/1 passed.
- `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml`: 156/156 passed.
- `stack test --fast`: 203/203 passed.
- `hlint src test app`: no hints.
- `jj diff --git --color=never | git apply --cached --check --whitespace=error -`: passed.

Deviation notes:

- The implementation follows first-frame optimizer flags directly; mixed flag
  validation is left out because Rust Polars 0.53 accepts one global flag set.
- `explainAll []` is left to upstream behavior; tests cover non-empty multi-plan
  explanation and empty-list collection.
