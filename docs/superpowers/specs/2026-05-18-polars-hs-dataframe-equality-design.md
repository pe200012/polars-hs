# DataFrame Equality Design

## Background

Rust Polars 0.53 exposes eager DataFrame equality helpers in `polars-core/src/testing.rs`:

- `DataFrame::equals(&self, other: &DataFrame) -> bool`
- `DataFrame::equals_missing(&self, other: &DataFrame) -> bool`

`equals` requires matching shape, column names, and values, with `None == None` evaluated as false. `equals_missing` applies the same shape and column-name checks while treating matching nulls as equal.

Upstream references:

- docs.rs `polars::frame::DataFrame` method list for `equals` and `equals_missing`.
- Local vendored source `polars-core-0.53.0/src/testing.rs`.

## Problem

`polars-hs` can compare extracted columns in Haskell, but it has no direct eager DataFrame equality API. Users need a binding-level equality predicate for round-trip tests, fixture comparisons, and parity harness checks.

## Questions And Answers

Q: Should the Haskell API expose both null-sensitive and null-equal behavior?

A: Yes. Polars has two distinct methods and both are useful for tests and data validation.

Q: Should unequal shapes or schemas produce errors?

A: No. Upstream equality returns `False` for shape mismatch and column name mismatch. The Haskell API should return `Right False` when both handles are valid.

Q: How should null pointer and released-handle failures behave?

A: The Rust ABI should use existing `dataframe_ref` and `required_mut` helpers, returning typed FFI errors.

## Design

Public Haskell API:

```haskell
dataFrameEquals :: DataFrame -> DataFrame -> IO (Either PolarsError Bool)
dataFrameEqualsMissing :: DataFrame -> DataFrame -> IO (Either PolarsError Bool)
```

C ABI:

```c
int phs_dataframe_equals(const struct phs_dataframe *left,
                         const struct phs_dataframe *right,
                         bool missing_equal,
                         bool *out,
                         struct phs_error **err);
```

Rust implementation:

```rust
let left = unsafe { dataframe_ref(left) }?;
let right = unsafe { dataframe_ref(right) }?;
*out = if missing_equal {
    left.value.equals_missing(&right.value)
} else {
    left.value.equals(&right.value)
};
```

## Implementation Plan

1. Add RED Rust ABI tests for equality, null-sensitive equality, column-order mismatch, shape mismatch, and null out pointer.
2. Add RED Hspec tests over `values.csv` for same handle, copied/equivalent frame, null-sensitive result, null-equal result, and row-count mismatch.
3. Add C header declaration and Rust ABI implementation.
4. Add Raw import and Haskell wrappers/exports.
5. Run focused GREEN and full verification.

## Examples

Good:

```haskell
same <- dataFrameEqualsMissing expected actual
```

This treats matching nulls as equal.

Good:

```haskell
strict <- dataFrameEquals left right
```

This follows Polars `equals`, where matching nulls compare false.

## Trade-offs

- A single ABI function with a `missing_equal` flag keeps the C surface small while preserving two clear Haskell names.
- The API returns `Either PolarsError Bool` for consistency with the rest of the handle-based eager API.

## Implementation Results

Implemented as designed.

Files changed:

- `include/polars_hs.h`: added `phs_dataframe_equals`.
- `rust/polars-hs-ffi/src/dataframe.rs`: added the Rust ABI function and Rust unit coverage.
- `src/Polars/Internal/Raw.hs`: added a safe FFI import for `phs_dataframe_equals`.
- `src/Polars/DataFrame.hs`: exported and implemented `dataFrameEquals` and `dataFrameEqualsMissing`.
- `test/Spec.hs`: added Hspec coverage for null-sensitive, null-equal, no-null, shape mismatch, column order mismatch, and same-schema value mismatch cases.

Behavior covered:

- `dataFrameEqualsMissing values values` returns `Right True`.
- `dataFrameEquals values values` returns `Right False` because matching nulls compare false.
- No-null `employees.csv` frames return `Right True` for both equality modes.
- Equal one-row heads return `Right True`.
- Same-schema different one-row frames return `Right False`.
- Shape mismatch returns `Right False`.
- Column order mismatch returns `Right False`.
- Rust ABI rejects null output, left DataFrame, and right DataFrame pointers.
- Rust ABI initializes the output bool to `false` before handle lookup.

Readonly review:

- Agent guidance in `research/api/dataframe-equality-readonly-guidance-2026-05-18` confirmed the API names and ABI shape.
- The review also recommended safe FFI import and output initialization before handle lookup; both were applied.

Verification:

- RED Rust failed on missing `phs_dataframe_equals`.
- RED Hspec failed on missing `Pl.dataFrameEquals` and `Pl.dataFrameEqualsMissing`.
- Focused Rust FFI: 1/1 passing.
- Focused Hspec: 11/11 matching examples passing.
- Full Rust FFI: 140/140 passing.
- Full Stack/Hspec: 187/187 passing.
- HLint: no hints.
- Whitespace gate: clean.

Deviation from design:

- The Rust ABI sets `*out = false` immediately after `required_mut(out, "out")` so pointer-validation failures leave a deterministic output value for callers that inspect it.
