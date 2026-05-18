# Lazy Asof Join Design Log

## Background

Rust Polars 0.53 exposes asof joins through `JoinType::AsOf(Box<AsOfOptions>)`.
The feature flag is `asof_join`. Asof joins match each left row with a nearest
right row by sorted key, using backward, forward, or nearest strategy.

## Problem

`polars-hs` can perform equi joins and non-equi predicate joins, yet it lacks
the sorted nearest-key join used for trades/quotes, time-series lookups, and
last-known-value enrichment.

## Questions and Answers

Q: What should the public API be?

A: Add `asofJoin :: AsofJoinOptions -> LazyFrame -> LazyFrame -> IO (Either PolarsError LazyFrame)`.

Q: How should required key expressions be represented?

A: Store `asofLeftOn :: Expr` and `asofRightOn :: Expr` in the options record.
Use `defaultAsofJoinOptions :: Expr -> Expr -> AsofJoinOptions` as the
constructor helper.

Q: Which tolerance forms are needed?

A: Support integer tolerance for numeric keys and duration-text tolerance for
temporal keys. This covers the Rust `AsOfOptions` fields `tolerance` and
`tolerance_str`.

Q: Which tests should land first?

A: Use sorted Int64 fixtures for deterministic backward, forward, nearest,
by-group, tolerance, and allow-equal behavior. Temporal dtype round-trips can
come after temporal scalar extraction expands.

## Design

Public API:

```haskell
data AsofStrategy = AsofBackward | AsofForward | AsofNearest

data AsofTolerance
    = AsofToleranceInt !Int64
    | AsofToleranceDuration !Text

data AsofJoinOptions = AsofJoinOptions
    { asofLeftOn :: !Expr
    , asofRightOn :: !Expr
    , asofLeftBy :: ![Text]
    , asofRightBy :: ![Text]
    , asofStrategy :: !AsofStrategy
    , asofTolerance :: !(Maybe AsofTolerance)
    , asofAllowEqual :: !Bool
    , asofCheckSortedness :: !Bool
    , asofSuffix :: !(Maybe Text)
    , asofCoalesce :: !JoinCoalesce
    , asofAllowParallel :: !Bool
    , asofForceParallel :: !Bool
    }

defaultAsofJoinOptions :: Expr -> Expr -> AsofJoinOptions
asofJoin :: AsofJoinOptions -> LazyFrame -> LazyFrame -> IO (Either PolarsError LazyFrame)
```

C ABI:

```c
int phs_lazyframe_join_asof(const struct phs_lazyframe *left,
                            const struct phs_lazyframe *right,
                            const struct phs_expr *left_on,
                            const struct phs_expr *right_on,
                            const char *const *left_by,
                            uintptr_t left_by_len,
                            const char *const *right_by,
                            uintptr_t right_by_len,
                            int strategy,
                            bool has_tolerance_int,
                            int64_t tolerance_int,
                            const char *tolerance_duration,
                            bool allow_eq,
                            bool check_sortedness,
                            const char *suffix,
                            int coalesce,
                            bool allow_parallel,
                            bool force_parallel,
                            struct phs_lazyframe **out,
                            struct phs_error **err);
```

Validation rules:

- `left_by` and `right_by` lengths must match.
- Rust rejects unknown strategy and coalesce codes.
- Haskell passes a single tolerance representation.
- Sortedness errors remain Polars collect-time errors when checking is enabled.

## Implementation Plan

1. Add RED Rust ABI tests for backward/forward/nearest, tolerance,
   `allow_eq`, by groups, and invalid by lengths.
2. Add RED Hspec tests over temporary sorted CSV fixtures.
3. Enable `asof_join`, add ABI, Raw import, Haskell options, and wrapper.
4. Run focused GREEN and full verification.

## Examples

```haskell
asofJoin
    (defaultAsofJoinOptions (col "trade_ts") (col "quote_ts"))
        { asofStrategy = AsofBackward
        , asofLeftBy = ["symbol"]
        , asofRightBy = ["symbol"]
        , asofTolerance = Just (AsofToleranceInt 3)
        }
    trades
    quotes
```

✅ Good pattern:

```haskell
asofJoin options sortedLeft sortedRight
```

❌ Problem pattern:

```haskell
joinWhere defaultJoinWhereOptions [col "quote_ts" .<= col "trade_ts"] trades quotes
```

The second form lacks nearest-key selection semantics.

## Trade-offs

- The first batch focuses on Int64 keys with typed extraction.
- Duration strings are exposed now so temporal asof joins can be used once
  callers can shape temporal data.
- Build-side controls remain part of the existing extended equi join roadmap.

## Implementation Results

Implemented files:

- `rust/polars-hs-ffi/Cargo.toml` enables the Polars `asof_join` feature.
- `rust/polars-hs-ffi/src/lazyframe.rs` adds `phs_lazyframe_join_asof`,
  strategy decoding, tolerance transport, by-column validation, and Rust tests.
- `include/polars_hs.h` declares the new ABI.
- `src/Polars/Internal/Raw.hs` imports the new ABI.
- `src/Polars/Join.hs` exposes `AsofStrategy`, `AsofTolerance`,
  `AsofJoinOptions`, `defaultAsofJoinOptions`, and `asofJoin`.
- `test/Spec.hs` adds Hspec coverage for backward, forward, nearest, grouped
  by-column asof joins, and mismatched by-column validation.

Verification:

- `cargo test --manifest-path rust/polars-hs-ffi/Cargo.toml`
  passed: 166/166 Rust tests.
- `stack test --fast` passed: 211/211 Hspec examples.
- `hlint src test app` passed with no hints.
- `jj diff --git --color=never | git apply --cached --check --whitespace=error -`
  passed.

Deviations:

- The first Rust test covers backward tolerance and grouped by behavior
  directly; Hspec covers forward and nearest behavior through the public API.
