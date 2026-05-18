# DataFrame Unpivot Design

## Background

Rust Polars 0.53 exposes eager unpivot through `polars_ops::frame::unpivot::UnpivotDF`:

```rust
fn unpivot<I, J>(&self, on: Option<I>, index: J) -> PolarsResult<DataFrame>
fn unpivot2(&self, args: UnpivotArgsIR) -> PolarsResult<DataFrame>
```

`UnpivotArgsIR` carries `on`, `index`, `variable_name`, and `value_name`. Defaults are `"variable"` and `"value"`. When `on` is `None`, Polars uses all columns outside `index`.

Upstream references:

- docs.rs eager unpivot cookbook.
- Local `polars-ops-0.53.0/src/frame/unpivot.rs`.
- Local `polars-core-0.53.0/src/frame/explode.rs` for `UnpivotArgsIR`.

## Problem

`polars-hs` has eager column selection and reshape helpers such as explode and transpose, but it lacks wide-to-long unpivot. This is a common Polars DataFrame operation and pairs naturally with the recently added transpose support.

## Questions And Answers

Q: Should `on` be optional or just a list?

A: Haskell should preserve Polars' distinction: `Nothing` means all non-index columns, while `Just []` means no value columns.

Q: Should variable/value output names be configurable?

A: Yes. Rust `UnpivotArgsIR` supports both and defaults to `"variable"`/`"value"`.

Q: Are extra Cargo features required?

A: `polars-ops` gates `UnpivotDF` behind the `pivot` feature. Enable `pivot` on the existing `polars-ops` dependency.

## Design

Public Haskell API:

```haskell
data DataFrameUnpivotOptions = DataFrameUnpivotOptions
    { dataFrameUnpivotOn :: Maybe [Text]
    , dataFrameUnpivotIndex :: [Text]
    , dataFrameUnpivotVariableName :: Maybe Text
    , dataFrameUnpivotValueName :: Maybe Text
    }

defaultDataFrameUnpivotOptions :: DataFrameUnpivotOptions

dataFrameUnpivot :: DataFrameUnpivotOptions -> DataFrame -> IO (Either PolarsError DataFrame)
```

C ABI:

```c
int phs_dataframe_unpivot(const struct phs_dataframe *dataframe,
                          bool has_on,
                          const char *const *on,
                          uintptr_t on_len,
                          const char *const *index,
                          uintptr_t index_len,
                          const char *variable_name,
                          const char *value_name,
                          struct phs_dataframe **out,
                          struct phs_error **err);
```

Rust implementation:

```rust
let on = if has_on { Some(names_to_plsmall(name_vec(...)?)) } else { None };
let index = names_to_plsmall(name_vec(...)?);
let args = UnpivotArgsIR::new(
    handle.value.get_column_names_owned(),
    on,
    index,
    value_name.map(PlSmallStr::from_str),
    variable_name.map(PlSmallStr::from_str),
);
*out = dataframe_into_raw(handle.value.unpivot2(args)?);
```

## Implementation Plan

1. Add RED Rust ABI tests for default `on`, explicit `on`, custom output names, empty `on`, missing column, null pointers, and null output pointer.
2. Add RED Hspec tests over `employees.csv` for default and explicit modes.
3. Add Rust ABI declaration/implementation and import `UnpivotDF`.
4. Add Raw import, Haskell options/default, and wrapper/export.
5. Run focused GREEN and full verification.

## Examples

Good:

```haskell
dataFrameUnpivot
    defaultDataFrameUnpivotOptions
        { dataFrameUnpivotIndex = ["department"]
        , dataFrameUnpivotOn = Just ["salary"]
        }
    df
```

Good:

```haskell
dataFrameUnpivot
    defaultDataFrameUnpivotOptions
        { dataFrameUnpivotIndex = ["department"]
        , dataFrameUnpivotVariableName = Just "metric"
        , dataFrameUnpivotValueName = Just "amount"
        }
    df
```

## Trade-offs

- The ABI passes `has_on` separately so `Nothing` and `Just []` remain distinct.
- Rust owns dtype-supertype, missing-column, and unsupported-dtype validation.

## Implementation Results

Implemented as designed, with the feature correction above.

Files changed:

- `rust/polars-hs-ffi/Cargo.toml`: enabled `polars-ops/pivot` for `UnpivotDF`.
- `include/polars_hs.h`: added `phs_dataframe_unpivot`.
- `rust/polars-hs-ffi/src/dataframe.rs`: added ABI wrapper, `UnpivotDF` import, name conversion helper, and Rust unit coverage.
- `src/Polars/Internal/Raw.hs`: added safe FFI import.
- `src/Polars/DataFrame.hs`: exported `DataFrameUnpivotOptions`, `defaultDataFrameUnpivotOptions`, and `dataFrameUnpivot`.
- `test/Spec.hs`: added Hspec coverage for explicit `on`, default `on`, empty `on`, custom output names, and missing-column errors.
- `docs/superpowers/specs/2026-05-18-polars-hs-dataframe-unpivot-design.md`: design log plus implementation results.

Behavior covered:

- Explicit `on = Just ["salary"]` and `index = ["department"]` returns long-form columns `department`, custom variable name, and custom value name.
- `on = Nothing` uses all non-index columns.
- `on = Just []` returns an empty long-form frame with standard schema.
- Missing index column surfaces as a Polars failure.
- Rust ABI rejects null `on` pointer with positive length and null output pointer.

Verification:

- RED Rust failed on missing `phs_dataframe_unpivot`.
- RED Hspec failed on missing `Pl.dataFrameUnpivot` and `defaultDataFrameUnpivotOptions`.
- Focused Rust FFI: 1/1 passing.
- Focused Hspec: 1/1 passing.
- Full Rust FFI: 143/143 passing.
- Full Stack/Hspec: 189/189 passing.
- HLint: no hints.
- Whitespace gate: clean.

Deviation from design:

- The implementation enables `polars-ops/pivot` because the `UnpivotDF` prelude export is feature-gated there.
