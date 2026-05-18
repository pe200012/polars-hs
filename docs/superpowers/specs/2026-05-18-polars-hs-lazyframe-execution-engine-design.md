# LazyFrame Execution Engine Design Log

## Background

`polars-hs` currently exposes `collect`, which calls Rust Polars
`LazyFrame::collect()` and therefore uses Polars 0.53 in-memory execution.
Upstream Polars 0.53 also exposes `collect_with_engine(Engine)` with `Auto`,
`Streaming`, `InMemory`, and `Gpu` variants.

The parity review listed streaming execution as a core query-engine gap because
larger-than-RAM workloads need an explicit streaming path.

## Problem

Haskell users can build lazy plans, inspect optimized plans, and collect with the
default in-memory engine, but they cannot choose the Polars execution engine.
This prevents use of the streaming engine for supported lazy queries.

## Questions and Answers

Q: Which upstream API should be bound first?

A: Bind `LazyFrame::collect_with_engine(Engine)` first. It is the smallest
stable Rust 0.53 API surface for selecting in-memory, streaming, auto, or gpu
execution.

Q: Should callback/batch collection and sink APIs be included here?

A: Keep this batch focused on DataFrame collection. `collect_batches` is behind
the Rust `async` feature and sink APIs need file/output option records, so they
belong in later streaming IO batches.

Q: Should `Gpu` be public?

A: Include it because upstream `Engine` includes it. In Polars 0.53
`collect_with_engine` currently maps GPU collection to in-memory execution when
the GPU path is unavailable.

## Design

Public Haskell type:

```haskell
data LazyExecutionEngine
    = LazyAuto
    | LazyStreaming
    | LazyInMemory
    | LazyGpu
```

Public Haskell functions:

```haskell
collectWithEngine :: LazyExecutionEngine -> LazyFrame -> IO (Either PolarsError DataFrame)
collectStreaming :: LazyFrame -> IO (Either PolarsError DataFrame)
```

Engine code mapping:

```text
0 Auto
1 Streaming
2 InMemory
3 Gpu
```

C ABI:

```c
int phs_lazyframe_collect_with_engine(const struct phs_lazyframe *lazyframe,
                                      int engine,
                                      struct phs_dataframe **out,
                                      struct phs_error **err);
```

Validation rules:

- Null lazyframe pointer returns `InvalidArgument`.
- Null output pointer returns `InvalidArgument`.
- Unknown engine code returns `InvalidArgument`.
- Rust Polars execution errors are propagated as Polars errors.

## Implementation Plan

1. Add RED Rust ABI tests for Auto, Streaming, InMemory, and GPU engine codes,
   plus unknown engine, null lazyframe pointer, and null output pointer.
2. Add RED Hspec tests for `collectWithEngine LazyStreaming`,
   `collectWithEngine LazyAuto`, `collectWithEngine LazyInMemory`, and
   `collectStreaming`.
3. Add header declaration, Rust engine decoder and ABI function, Raw import,
   Haskell `LazyExecutionEngine` type, wrappers, and exports.
4. Run focused GREEN and full verification.

## Examples

```haskell
streamed <- collectWithEngine LazyStreaming lf
same <- collectStreaming lf
```

Good pattern:

```haskell
result <- collectStreaming =<< select [col "name"] lf
```

Bad pattern:

```haskell
result <- collect lf
```

## Trade-offs

- A Haskell enum avoids integerly typed public APIs while keeping the ABI compact.
- `collectStreaming` is a convenience wrapper over `collectWithEngine`.
- Sinks and batch callbacks need richer lifetime and option design, so they stay
  outside this batch.

## Implementation Results

Implemented explicit lazy execution engine selection as planned.

Changed files:

- `docs/superpowers/specs/2026-05-18-polars-hs-lazyframe-execution-engine-design.md`
- `include/polars_hs.h`
- `rust/polars-hs-ffi/src/lazyframe.rs`
- `src/Polars/Internal/Raw.hs`
- `src/Polars/LazyFrame.hs`
- `test/Spec.hs`

Public Haskell API:

```haskell
data LazyExecutionEngine
    = LazyAuto
    | LazyStreaming
    | LazyInMemory
    | LazyGpu

collectWithEngine :: LazyExecutionEngine -> LazyFrame -> IO (Either PolarsError DataFrame)
collectStreaming :: LazyFrame -> IO (Either PolarsError DataFrame)
```

Rust ABI:

```c
int phs_lazyframe_collect_with_engine(const struct phs_lazyframe *lazyframe,
                                      int engine,
                                      struct phs_dataframe **out,
                                      struct phs_error **err);
```

Engine code mapping:

```text
0 Auto
1 Streaming
2 InMemory
3 Gpu
```

Validation:

- Unknown engine values return `InvalidArgument`.
- Null lazyframe pointer returns `InvalidArgument`.
- Null output pointer returns `InvalidArgument`.

Verification:

- Focused Rust execution-engine test: 1/1 passed.
- Focused Hspec execution-engine test: 1/1 passed.
- `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml`: 155/155 passed.
- `stack test --fast`: 202/202 passed.
- `hlint src test app`: no hints.
- `jj diff --git --color=never | git apply --cached --check --whitespace=error -`: passed.

Deviation notes:

- The shipped API includes all Rust Polars 0.53 engine variants rather than only
  a streaming convenience wrapper. The extra public surface is a small enum and
  gives callers direct parity with `collect_with_engine`.
- `collect_batches` and file sinks remain future batches because they need
  async and IO option design.
