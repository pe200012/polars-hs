{- |
Module      : Polars
Description : Convenience re-export for the Polars Haskell binding MVP.

Import this module for the first binding surface: eager DataFrames, lazy query
construction, expressions, operators, schema values, typed errors, and IPC byte
helpers.
-}
module Polars
    ( module Polars.DataFrame
    , module Polars.Error
    , module Polars.Expr
    , module Polars.GroupBy
    , module Polars.IPC
    , module Polars.LazyFrame
    , module Polars.Operators
    , module Polars.Schema
    ) where

import Polars.DataFrame
import Polars.Error
import Polars.Expr
import Polars.GroupBy
import Polars.IPC
import Polars.LazyFrame
import Polars.Operators
import Polars.Schema
