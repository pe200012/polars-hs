{- |
Module      : Polars
Description : Convenience re-export for the Polars Haskell binding MVP.

Import this module for the first binding surface: eager DataFrames, lazy query
construction, expressions, operators, typed column extraction, Series handles,
Series/DataFrame construction, schema values, typed errors, Arrow C Data Interface import, and IPC byte helpers.
-}
module Polars
    ( module Polars.Arrow
    , module Polars.Column
    , module Polars.DataFrame
    , module Polars.Error
    , module Polars.Expr
    , module Polars.GroupBy
    , module Polars.IPC
    , module Polars.Join
    , module Polars.LazyFrame
    , module Polars.Operators
    , module Polars.Schema
    , module Polars.Series
    ) where

import Polars.Arrow
import Polars.Column
import Polars.DataFrame
import Polars.Error
import Polars.Expr
import Polars.GroupBy
import Polars.IPC
import Polars.Join
import Polars.LazyFrame
import Polars.Operators
import Polars.Schema
import Polars.Series
