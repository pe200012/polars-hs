# LazyFrame Optimizer Toggles Design

## Background

Rust Polars 0.53 exposes LazyFrame optimizer controls:

```rust
pub fn without_optimizations(self) -> Self
pub fn with_projection_pushdown(self, toggle: bool) -> Self
pub fn with_cluster_with_columns(self, toggle: bool) -> Self
pub fn with_check_order(self, toggle: bool) -> Self
pub fn with_predicate_pushdown(self, toggle: bool) -> Self
pub fn with_type_coercion(self, toggle: bool) -> Self
pub fn with_type_check(self, toggle: bool) -> Self
pub fn with_simplify_expr(self, toggle: bool) -> Self
pub fn with_slice_pushdown(self, toggle: bool) -> Self
pub fn with_row_estimate(self, toggle: bool) -> Self
```

`with_comm_subplan_elim`, `with_comm_subexpr_elim`, and
`with_new_streaming` are gated by features the current crate does not enable.

## Problem

`polars-hs` can describe optimized plans but cannot alter optimizer flags. This
limits parity with Rust/Python Polars debugging workflows where users compare
optimized plans with selected optimizations disabled.

## Questions And Answers

Q: Which toggles should this first batch expose?

A: Expose the ungated Rust 0.53 toggles listed above. Leave CSE and streaming
toggles for feature-specific batches.

Q: Should Haskell use one option record or separate functions?

A: Use separate functions mirroring Rust names. They compose naturally with
`LazyFrame -> IO (Either PolarsError LazyFrame)` wrappers and keep tests small.

Q: How should the C ABI avoid many near-identical functions?

A: Use one opcode function for boolean toggles and one direct function for
`without_optimizations`.

## Design

Public Haskell API:

```haskell
withoutOptimizations :: LazyFrame -> IO (Either PolarsError LazyFrame)
withProjectionPushdown :: Bool -> LazyFrame -> IO (Either PolarsError LazyFrame)
withPredicatePushdown :: Bool -> LazyFrame -> IO (Either PolarsError LazyFrame)
withTypeCoercion :: Bool -> LazyFrame -> IO (Either PolarsError LazyFrame)
withTypeCheck :: Bool -> LazyFrame -> IO (Either PolarsError LazyFrame)
withSimplifyExpr :: Bool -> LazyFrame -> IO (Either PolarsError LazyFrame)
withSlicePushdown :: Bool -> LazyFrame -> IO (Either PolarsError LazyFrame)
withClusterWithColumns :: Bool -> LazyFrame -> IO (Either PolarsError LazyFrame)
withCheckOrder :: Bool -> LazyFrame -> IO (Either PolarsError LazyFrame)
withRowEstimate :: Bool -> LazyFrame -> IO (Either PolarsError LazyFrame)
```

C ABI:

```c
int phs_lazyframe_without_optimizations(const struct phs_lazyframe *lazyframe,
                                        struct phs_lazyframe **out,
                                        struct phs_error **err);

int phs_lazyframe_with_optimization(const struct phs_lazyframe *lazyframe,
                                    int optimization,
                                    bool toggle,
                                    struct phs_lazyframe **out,
                                    struct phs_error **err);
```

Opcode mapping:

```text
0 projection pushdown
1 predicate pushdown
2 type coercion
3 type check
4 simplify expression
5 slice pushdown
6 cluster with columns
7 check order
8 row estimate
```

## Implementation Plan

1. Add RED Rust ABI tests for predicate pushdown plan differences, all opcode
   toggles preserving collection, unknown opcode, null lazyframe pointer, and
   null output pointer.
2. Add RED Hspec tests for `withoutOptimizations`, predicate pushdown plan
   differences, all public toggle wrappers preserving collection, and
   missing-column propagation through optimized plan description.
3. Add header declarations, Rust ABI implementations, Raw imports, and Haskell
   wrappers.
4. Run focused GREEN and full verification.

## Examples

```haskell
plain <- withoutOptimizations lf
noPredicate <- withPredicatePushdown False lf
noProjection <- withProjectionPushdown False lf
```

Good pattern:

```haskell
plan <- describeOptimizedPlan =<< withPredicatePushdown False lf
```

Bad pattern:

```haskell
plan <- describePlan lf
```

## Trade-offs

- Opcode-based ABI keeps the header compact while public Haskell wrappers stay
  named and type-directed.
- `withoutOptimizations` follows upstream semantics; Rust keeps type coercion
  enabled internally even after disabling the other optimizer flags.

## Implementation Results

Implemented the ungated LazyFrame optimizer controls as planned.

Changed files:

- `include/polars_hs.h`
- `rust/polars-hs-ffi/src/lazyframe.rs`
- `src/Polars/Internal/Raw.hs`
- `src/Polars/LazyFrame.hs`
- `test/Spec.hs`

Public Haskell wrappers:

```haskell
withoutOptimizations :: LazyFrame -> IO (Either PolarsError LazyFrame)
withProjectionPushdown :: Bool -> LazyFrame -> IO (Either PolarsError LazyFrame)
withPredicatePushdown :: Bool -> LazyFrame -> IO (Either PolarsError LazyFrame)
withTypeCoercion :: Bool -> LazyFrame -> IO (Either PolarsError LazyFrame)
withTypeCheck :: Bool -> LazyFrame -> IO (Either PolarsError LazyFrame)
withSimplifyExpr :: Bool -> LazyFrame -> IO (Either PolarsError LazyFrame)
withSlicePushdown :: Bool -> LazyFrame -> IO (Either PolarsError LazyFrame)
withClusterWithColumns :: Bool -> LazyFrame -> IO (Either PolarsError LazyFrame)
withCheckOrder :: Bool -> LazyFrame -> IO (Either PolarsError LazyFrame)
withRowEstimate :: Bool -> LazyFrame -> IO (Either PolarsError LazyFrame)
```

Rust maps ABI opcodes 0 through 8 to the matching Polars 0.53 methods:
projection pushdown, predicate pushdown, type coercion, type check, simplify
expression, slice pushdown, cluster with-columns, check order, and row
estimate. Unknown opcode values return `InvalidArgument`.

Verification:

- Focused Rust optimizer test: 1/1 passed.
- Focused Hspec optimizer test: 1/1 passed.
- `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml`: 154/154 passed.
- `stack test --fast`: 201/201 passed.
- `hlint src test app`: no hints.
- `jj diff --git --color=never | git apply --cached --check --whitespace=error -`: passed.

Deviation notes:

- `with_new_streaming` is left for the streaming execution batch.
- CSE optimizer toggles are left for a feature-enablement batch that can add the
  `cse` feature and tests together.
