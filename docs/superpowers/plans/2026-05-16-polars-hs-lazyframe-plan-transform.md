# LazyFrame Plan and Transform Implementation Plan

## Scope

Implement plan introspection, profiling, and common `LazyFrame` transformations
from the design log:
`docs/superpowers/specs/2026-05-16-polars-hs-lazyframe-plan-transform-design.md`.

## Files

- `src/Polars/LazyFrame.hs`
- `src/Polars/Internal/Raw.hs`
- `rust/polars-hs-ffi/src/lazyframe.rs`
- `include/polars_hs.h`
- `test/Spec.hs`
- `README.md`
- `CHANGELOG.md`

## Steps

1. Add Hspec tests for:
   - `explain`
   - `profile`
   - `dropColumns`
   - `rename`
   - `slice`
   - `lazyHead`
   - `lazyTail`
   - `dropNulls`
   - `fillNulls`
   - `fillNans`
   - `nullCount`
   - `unique`
2. Add public Haskell options and wrappers.
3. Add Raw FFI imports.
4. Implement Rust ABI functions and selector/name helpers.
5. Let the Cargo build regenerate `include/polars_hs.h`.
6. Update README and CHANGELOG.
7. Run:

```bash
cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml
PATH="$HOME/.ghcup/bin:$PATH" stack --system-ghc test --fast
hlint src app test
git diff --check
```

8. Describe the jj change:

```bash
jj describe -m "feat(lazyframe): add plan and transform helpers"
jj new
```
