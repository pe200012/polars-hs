{-# LANGUAGE OverloadedStrings #-}

{- |
Module      : Polars.LazyFrame
Description : Safe lazy query operations backed by Rust Polars LazyFrame handles.

Lazy operations clone Rust logical plans and return new managed LazyFrame values.
Expression inputs are compiled from pure Haskell AST nodes at each FFI boundary.
-}
module Polars.LazyFrame
    ( LazyFrame
    , collect
    , filter
    , limit
    , scanCsv
    , scanParquet
    , select
    , sort
    , withColumns
    ) where

import Prelude hiding (filter)

import Data.Text (Text)
import Foreign.C.String (CString)
import Foreign.C.Types (CInt, CSize)
import Foreign.Marshal.Alloc (alloca)
import Foreign.Marshal.Array (withArray)
import Foreign.Ptr (Ptr, nullPtr)
import Foreign.Storable (peek, poke)

import Polars.Error (PolarsError)
import Polars.Expr (Expr)
import Polars.Internal.CString (withFilePathCString, withTextCString)
import Polars.Internal.Expr (compileExpr, withCompiledExprs)
import Polars.Internal.Managed (DataFrame, LazyFrame, mkDataFrame, mkLazyFrame, withLazyFrame, withManagedExpr)
import Polars.Internal.Raw
    ( RawDataFrame
    , RawError
    , RawLazyFrame
    , phs_lazyframe_collect
    , phs_lazyframe_filter
    , phs_lazyframe_limit
    , phs_lazyframe_select
    , phs_lazyframe_sort
    , phs_lazyframe_with_columns
    , phs_scan_csv
    , phs_scan_parquet
    )
import Polars.Internal.Result (consumeError, nullPointerError)

scanCsv :: FilePath -> IO (Either PolarsError LazyFrame)
scanCsv path = withFilePathCString path $ \cPath -> lazyFrameOut (phs_scan_csv cPath)

scanParquet :: FilePath -> IO (Either PolarsError LazyFrame)
scanParquet path = withFilePathCString path $ \cPath -> lazyFrameOut (phs_scan_parquet cPath)

collect :: LazyFrame -> IO (Either PolarsError DataFrame)
collect lf = withLazyFrame lf $ \ptr -> dataframeOut (phs_lazyframe_collect ptr)

filter :: Expr -> LazyFrame -> IO (Either PolarsError LazyFrame)
filter predicate lf = do
    compiled <- compileExpr predicate
    case compiled of
        Left err -> pure (Left err)
        Right managed -> withLazyFrame lf $ \lfPtr ->
            withManagedExpr managed $ \exprPtr -> lazyFrameOut (phs_lazyframe_filter lfPtr exprPtr)

select :: [Expr] -> LazyFrame -> IO (Either PolarsError LazyFrame)
select exprs lf = withLazyFrame lf $ \lfPtr ->
    withCompiledExprs exprs $ \exprArray len -> lazyFrameOut (phs_lazyframe_select lfPtr exprArray len)

withColumns :: [Expr] -> LazyFrame -> IO (Either PolarsError LazyFrame)
withColumns exprs lf = withLazyFrame lf $ \lfPtr ->
    withCompiledExprs exprs $ \exprArray len -> lazyFrameOut (phs_lazyframe_with_columns lfPtr exprArray len)

sort :: [Text] -> LazyFrame -> IO (Either PolarsError LazyFrame)
sort names lf = withLazyFrame lf $ \lfPtr -> withCStringList names $ \nameArray len ->
    lazyFrameOut (phs_lazyframe_sort lfPtr nameArray len)

limit :: Word -> LazyFrame -> IO (Either PolarsError LazyFrame)
limit n lf = withLazyFrame lf $ \lfPtr -> lazyFrameOut (phs_lazyframe_limit lfPtr (fromIntegral n))

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

dataframeOut :: (Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt) -> IO (Either PolarsError DataFrame)
dataframeOut action =
    alloca $ \outPtr ->
        alloca $ \errPtr -> do
            poke outPtr nullPtr
            poke errPtr nullPtr
            status <- action outPtr errPtr
            if fromIntegralStatus status == 0
                then do
                    ptr <- peek outPtr
                    if ptr == nullPtr
                        then pure (Left (nullPointerError "dataframe output"))
                        else Right <$> mkDataFrame ptr
                else Left <$> (consumeError (fromIntegralStatus status) =<< peek errPtr)

withCStringList :: [Text] -> (Ptr CString -> CSize -> IO a) -> IO a
withCStringList values action = go values []
  where
    go [] acc = withArray (reverse acc) $ \ptr -> action ptr (fromIntegral (length acc))
    go (value : rest) acc = withTextCString value $ \ptr -> go rest (ptr : acc)

fromIntegralStatus :: (Integral a) => a -> CInt
fromIntegralStatus = fromIntegral
