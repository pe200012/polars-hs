# DataFrame Eager Transforms Phase 1 Implementation Plan

## Scope

Implement structural eager `DataFrame` transforms from:
`docs/superpowers/specs/2026-05-17-polars-hs-dataframe-eager-transforms-phase1-design.md`.

## Files

- `src/Polars/DataFrame.hs`
- `src/Polars/Internal/Raw.hs`
- `rust/polars-hs-ffi/src/dataframe.rs`
- `include/polars_hs.h`
- `test/Spec.hs`
- `README.md`
- `CHANGELOG.md`
- `docs/superpowers/plans/2026-05-16-polars-hs-polars-053-parity.md`

## Steps

1. Add Hspec tests for:
   - `dataFrameSelect`
   - `dataFrameDropColumns`
   - `dataFrameRename`
   - `dataFrameSlice`
   - `dataFrameReverse`
   - `dataFrameDropNulls`
   - `dataFrameNullCount`
   - invalid empty names and negative slice length
2. Add public Haskell wrappers and validation.
3. Add Raw FFI imports.
4. Implement Rust ABI functions.
5. Update generated header and docs.
6. Run:

```bash
cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml
PATH="$HOME/.ghcup/bin:$PATH" stack --system-ghc test --fast
hlint src app test
git diff --check
```

7. Describe the jj change:

```bash
jj --config signing.behavior=drop describe -m "feat(dataframe): add eager structural transforms"
jj --config signing.behavior=drop new
```
