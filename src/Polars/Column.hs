{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}

{- |
Module      : Polars.Column
Description : Unified typed DataFrame column selection helpers.

This module exposes `column @a` for selecting a named DataFrame column either as
an owned Series handle or as typed Haskell values. Named helpers remain as
stable aliases for common typed value extraction.
-}
module Polars.Column
    ( Column (..)
    , columnBool
    , columnDouble
    , columnInt64
    , columnText
    ) where

import Data.Int (Int64)
import Data.Text (Text)
import Data.Vector (Vector)
import Polars.DataFrame (DataFrame)
import Polars.Error (PolarsError)
import Polars.Internal.CString (withTextCString)
import Polars.Internal.Managed (Series, withDataFrame)
import Polars.Internal.Raw (phs_dataframe_column)
import Polars.Internal.Series (seriesOut)
import Polars.Series
    ( seriesBool
    , seriesDouble
    , seriesInt64
    , seriesText
    )

class Column a where
    type ColumnResult a
    column :: DataFrame -> Text -> IO (Either PolarsError (ColumnResult a))

instance Column Series where
    type ColumnResult Series = Series
    column df name = withDataFrame df $ \ptr ->
        withTextCString name $ \cName ->
            seriesOut (phs_dataframe_column ptr cName)

instance Column Bool where
    type ColumnResult Bool = Vector (Maybe Bool)
    column df name = column @Series df name >>= either (pure . Left) seriesBool

instance Column Int64 where
    type ColumnResult Int64 = Vector (Maybe Int64)
    column df name = column @Series df name >>= either (pure . Left) seriesInt64

instance Column Double where
    type ColumnResult Double = Vector (Maybe Double)
    column df name = column @Series df name >>= either (pure . Left) seriesDouble

instance Column Text where
    type ColumnResult Text = Vector (Maybe Text)
    column df name = column @Series df name >>= either (pure . Left) seriesText

columnBool :: DataFrame -> Text -> IO (Either PolarsError (Vector (Maybe Bool)))
columnBool = column @Bool

columnInt64 :: DataFrame -> Text -> IO (Either PolarsError (Vector (Maybe Int64)))
columnInt64 = column @Int64

columnDouble :: DataFrame -> Text -> IO (Either PolarsError (Vector (Maybe Double)))
columnDouble = column @Double

columnText :: DataFrame -> Text -> IO (Either PolarsError (Vector (Maybe Text)))
columnText = column @Text
