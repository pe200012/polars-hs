# Join Modes Phase 2 Implementation Plan

## Scope

Implement semi, anti, and cross joins from:
`docs/superpowers/specs/2026-05-17-polars-hs-join-modes-phase2-design.md`.

## Files

- `src/Polars/Join.hs`
- `rust/polars-hs-ffi/Cargo.toml`
- `rust/polars-hs-ffi/src/lazyframe.rs`
- `test/Spec.hs`
- `README.md`
- `CHANGELOG.md`
- `docs/superpowers/plans/2026-05-16-polars-hs-polars-053-parity.md`

## Steps

1. Add Hspec tests:
   - `semiJoin` keeps matching left rows and left columns.
   - `antiJoin` keeps unmatched left rows and left columns.
   - `crossJoin` returns the Cartesian product with suffixed right columns.
   - keyed `JoinCross` returns `InvalidArgument`.
2. Extend public Haskell join API:
   - `JoinSemi`
   - `JoinAnti`
   - `JoinCross`
   - `semiJoin`
   - `antiJoin`
   - `crossJoin`
3. Add Rust Polars features:
   - `semi_anti_join`
   - `cross_join`
4. Extend Rust join type mapping and cross join validation.
5. Update README, CHANGELOG, and parity plan status.
6. Run:

```bash
cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml
PATH="$HOME/.ghcup/bin:$PATH" stack --system-ghc test --fast
hlint src app test
git diff --check
```

7. Describe the jj change:

```bash
jj --config signing.behavior=drop describe -m "feat(join): add semi anti and cross joins"
jj --config signing.behavior=drop new
```
