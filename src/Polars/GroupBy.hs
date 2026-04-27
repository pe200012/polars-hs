{-# LANGUAGE OverloadedStrings #-}

{- |
Module      : Polars.GroupBy
Description : Grouped aggregation operations for lazy Polars queries.

A GroupBy value is a pure Haskell descriptor. Aggregation compiles keys and
aggregation expressions into temporary Rust handles, calls Rust Polars once, and
returns a managed LazyFrame.
-}
module Polars.GroupBy
    ( GroupBy
    , agg
    , groupBy
    , groupByStable
    ) where

import Foreign.C.Types (CBool (..), CInt)
import Foreign.Marshal.Alloc (alloca)
import Foreign.Ptr (Ptr, nullPtr)
import Foreign.Storable (peek, poke)

import Polars.Error (PolarsError (..), PolarsErrorCode (InvalidArgument))
import Polars.Expr (Expr)
import Polars.Internal.Expr (withCompiledExprs)
import Polars.Internal.Managed (LazyFrame, mkLazyFrame, withLazyFrame)
import Polars.Internal.Raw (RawError, RawLazyFrame, phs_lazyframe_group_by_agg)
import Polars.Internal.Result (consumeError, nullPointerError)

-- | Pure descriptor for a grouped lazy query.
data GroupBy = GroupBy !LazyFrame ![Expr] !Bool

groupBy :: [Expr] -> LazyFrame -> GroupBy
groupBy keys input = GroupBy input keys False

groupByStable :: [Expr] -> LazyFrame -> GroupBy
groupByStable keys input = GroupBy input keys True

agg :: [Expr] -> GroupBy -> IO (Either PolarsError LazyFrame)
agg [] _ = pure (Left (PolarsError InvalidArgument "aggregation list must contain at least one expression"))
agg aggregations (GroupBy input keys maintainOrder) =
    withLazyFrame input $ \lfPtr ->
        withCompiledExprs keys $ \keyArray keyLen ->
            withCompiledExprs aggregations $ \aggArray aggLen ->
                lazyFrameOut (phs_lazyframe_group_by_agg lfPtr keyArray keyLen aggArray aggLen (toCBool maintainOrder))

toCBool :: Bool -> CBool
toCBool False = CBool 0
toCBool True = CBool 1

lazyFrameOut :: (Ptr (Ptr RawLazyFrame) -> Ptr (Ptr RawError) -> IO CInt) -> IO (Either PolarsError LazyFrame)
lazyFrameOut action =
    alloca $ \outPtr ->
        alloca $ \errPtr -> do
            poke outPtr nullPtr
            poke errPtr nullPtr
            status <- action outPtr errPtr
            if fromIntegralStatus status == 0
                then do
                    ptr <- peek outPtr
                    if ptr == nullPtr
                        then pure (Left (nullPointerError "lazyframe output"))
                        else Right <$> mkLazyFrame ptr
                else Left <$> (consumeError (fromIntegralStatus status) =<< peek errPtr)

fromIntegralStatus :: (Integral a) => a -> CInt
fromIntegralStatus = fromIntegral
