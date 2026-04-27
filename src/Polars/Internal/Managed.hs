{- |
Module      : Polars.Internal.Managed
Description : Managed Haskell wrappers around Rust-owned Polars handles.

This module attaches Rust finalizers to opaque pointers returned by the adapter.
Public modules re-export the DataFrame and LazyFrame newtypes through their own
APIs.
-}
module Polars.Internal.Managed
    ( DataFrame (..)
    , LazyFrame (..)
    , ManagedExpr (..)
    , mkDataFrame
    , mkLazyFrame
    , mkManagedExpr
    , withDataFrame
    , withLazyFrame
    , withManagedExpr
    ) where

import Foreign.ForeignPtr (ForeignPtr, newForeignPtr, withForeignPtr)
import Foreign.Ptr (Ptr)

import Polars.Internal.Raw
    ( RawDataFrame
    , RawExpr
    , RawLazyFrame
    , phs_dataframe_free_finalizer
    , phs_expr_free_finalizer
    , phs_lazyframe_free_finalizer
    )

newtype DataFrame = DataFrame (ForeignPtr RawDataFrame)
newtype LazyFrame = LazyFrame (ForeignPtr RawLazyFrame)
newtype ManagedExpr = ManagedExpr (ForeignPtr RawExpr)

mkDataFrame :: Ptr RawDataFrame -> IO DataFrame
mkDataFrame ptr = DataFrame <$> newForeignPtr phs_dataframe_free_finalizer ptr

mkLazyFrame :: Ptr RawLazyFrame -> IO LazyFrame
mkLazyFrame ptr = LazyFrame <$> newForeignPtr phs_lazyframe_free_finalizer ptr

mkManagedExpr :: Ptr RawExpr -> IO ManagedExpr
mkManagedExpr ptr = ManagedExpr <$> newForeignPtr phs_expr_free_finalizer ptr

withDataFrame :: DataFrame -> (Ptr RawDataFrame -> IO a) -> IO a
withDataFrame (DataFrame foreignPtr) = withForeignPtr foreignPtr

withLazyFrame :: LazyFrame -> (Ptr RawLazyFrame -> IO a) -> IO a
withLazyFrame (LazyFrame foreignPtr) = withForeignPtr foreignPtr

withManagedExpr :: ManagedExpr -> (Ptr RawExpr -> IO a) -> IO a
withManagedExpr (ManagedExpr foreignPtr) = withForeignPtr foreignPtr
