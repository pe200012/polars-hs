# Plan: Expression DSL String Phase 2C — strip_prefix, strip_suffix, escape_regex, extract_all

**2026-04-29**

## Steps

1. Create `test/data/string_more.csv` fixture (5 rows).
2. Extend `phs_expr_string_function` with opcodes 26‑29 in Rust.
3. Add `StrStripPrefix`, `StrStripSuffix`, `StrEscapeRegex`, `StrExtractAll`
   to the `StringFunction` ADT.
4. Map the new constructors to opcodes in `stringFunctionCode`.
5. Add public API functions: `strStripPrefix`, `strStripSuffix`,
   `strEscapeRegex`, `strExtractAll`.
6. Add Rust tests that exercise each new opcode and its arity.
7. Add hspec test verifying shape, output values, and list‑producing
   `extract_all` through `listJoin`/`listLen`.
8. Update `package.yaml` (glob already covers `*.csv`), README, and CHANGELOG.
9. Run full validation suite:
   - `cargo test` + `cargo clippy`
   - `stack test --fast`
   - `hlint src app test`
   - Todo/placeholder scan
   - `git diff --check`
10. Commit with `jj` and place bookmark `expression-dsl-string`.
