# DataFrame Set Column Names Design

## Background

Rust Polars 0.53 exposes:

```rust
pub fn set_column_names<T>(&mut self, new_names: &[T]) -> PolarsResult<()>
where
    T: AsRef<str>
```

The method replaces all column names at once. Existing `polars-hs` users can rename selected columns, but cannot provide a complete replacement name vector directly.

Upstream references:

- docs.rs `polars::frame::DataFrame::set_column_names`
- Local vendored Rust source for `DataFrame::set_column_names`

## Problem

`dataFrameRename` requires old/new name pairs. It is verbose for schema replacement after CSV import, transpose, or generated-column workflows. A direct `dataFrameSetColumnNames` wrapper should mirror Rust Polars and keep the source handle reusable.

## Questions And Answers

Q: Should the wrapper mutate the existing Haskell DataFrame handle?

A: No. The FFI should clone the Rust DataFrame, call `set_column_names` on the clone, and return a new handle.

Q: Should Haskell validate the name count?

A: Let Rust Polars validate count and duplicate names. This preserves upstream error behavior and supports any future zero-width behavior.

Q: Should empty names be allowed?

A: Defer to Polars. The binding should transport text faithfully through the existing CString path.

## Design

Public Haskell API:

```haskell
dataFrameSetColumnNames :: [Text] -> DataFrame -> IO (Either PolarsError DataFrame)
```

C ABI:

```c
int phs_dataframe_set_column_names(const struct phs_dataframe *dataframe,
                                   const char *const *names,
                                   uintptr_t names_len,
                                   struct phs_dataframe **out,
                                   struct phs_error **err);
```

Rust implementation:

```rust
let names = name_vec(names, names_len, "names")?;
let mut value = handle.value.clone();
value.set_column_names(&names)?;
*out = dataframe_into_raw(value);
```

## Implementation Plan

1. Add RED Rust ABI tests for replacing names, source handle reuse, length mismatch, duplicate names, null names pointer, null individual name, and null out pointer.
2. Add RED Hspec tests for renamed schema/value preservation, source schema preservation, length mismatch, and duplicate-name errors.
3. Add C header declaration and Rust ABI function.
4. Add Raw import and Haskell wrapper/export.
5. Run focused GREEN and full verification.

## Examples

Good:

```haskell
renamed <- dataFrameSetColumnNames ["person", "years", "points", "enabled"] df
```

The returned DataFrame has the same data with the provided names.

## Trade-offs

- Cloning costs memory but keeps Haskell handle behavior immutable.
- Rust owns validation for exact name-count and duplicate-name semantics.

## Implementation Results

Implemented as designed.

Files changed:

- `include/polars_hs.h`: added `phs_dataframe_set_column_names`.
- `rust/polars-hs-ffi/src/dataframe.rs`: added clone-based Rust ABI wrapper and Rust unit coverage.
- `src/Polars/Internal/Raw.hs`: added safe FFI import.
- `src/Polars/DataFrame.hs`: exported and implemented `dataFrameSetColumnNames`.
- `test/Spec.hs`: extended eager select/drop/rename coverage for full schema replacement.
- `docs/superpowers/specs/2026-05-18-polars-hs-dataframe-set-column-names-design.md`: design log plus implementation results.

Behavior covered:

- Replaces all column names while preserving shape, values, and order.
- Source DataFrame keeps its original names.
- Non-ASCII names round-trip through schema and column extraction.
- Short name lists and duplicate names surface Polars failures.
- Rust ABI rejects null names pointer, null individual name pointer, and null output pointer.

Readonly review:

- Agent guidance in `research/api/dataframe-set-column-names-readonly-guidance-2026-05-18` confirmed the clone-based wrapper, API name, ABI shape, and non-ASCII test recommendation.

Verification:

- RED Rust failed on missing `phs_dataframe_set_column_names`.
- RED Hspec failed on missing `Pl.dataFrameSetColumnNames`.
- Focused Rust FFI: 1/1 passing.
- Focused Hspec: 3/3 matching examples passing.
- Full Rust FFI: 142/142 passing.
- Full Stack/Hspec: 188/188 passing.
- HLint: no hints.
- Whitespace gate: clean.

Deviation from design:

- Haskell keeps Rust-owned validation for all name-list lengths, including empty lists.
