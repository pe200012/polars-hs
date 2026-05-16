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
    , columnFloat
    , columnInt8
    , columnInt16
    , columnInt32
    , columnInt64
    , columnText
    , columnWord8
    , columnWord16
    , columnWord32
    , columnWord64
    ) where

import Data.Int (Int16, Int32, Int64, Int8)
import Data.Text (Text)
import Data.Vector (Vector)
import Data.Word (Word16, Word32, Word64, Word8)
import Polars.DataFrame (DataFrame)
import Polars.Error (PolarsError)
import Polars.Internal.CString (withTextCString)
import Polars.Internal.Managed (Series, withDataFrame)
import Polars.Internal.Raw (phs_dataframe_column)
import Polars.Internal.Series (seriesOut)
import Polars.Series
    ( seriesBool
    , seriesDouble
    , seriesFloat
    , seriesInt8
    , seriesInt16
    , seriesInt32
    , seriesInt64
    , seriesText
    , seriesWord8
    , seriesWord16
    , seriesWord32
    , seriesWord64
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

instance Column Int8 where
    type ColumnResult Int8 = Vector (Maybe Int8)
    column df name = column @Series df name >>= either (pure . Left) seriesInt8

instance Column Int16 where
    type ColumnResult Int16 = Vector (Maybe Int16)
    column df name = column @Series df name >>= either (pure . Left) seriesInt16

instance Column Int32 where
    type ColumnResult Int32 = Vector (Maybe Int32)
    column df name = column @Series df name >>= either (pure . Left) seriesInt32

instance Column Word8 where
    type ColumnResult Word8 = Vector (Maybe Word8)
    column df name = column @Series df name >>= either (pure . Left) seriesWord8

instance Column Word16 where
    type ColumnResult Word16 = Vector (Maybe Word16)
    column df name = column @Series df name >>= either (pure . Left) seriesWord16

instance Column Word32 where
    type ColumnResult Word32 = Vector (Maybe Word32)
    column df name = column @Series df name >>= either (pure . Left) seriesWord32

instance Column Word64 where
    type ColumnResult Word64 = Vector (Maybe Word64)
    column df name = column @Series df name >>= either (pure . Left) seriesWord64

instance Column Float where
    type ColumnResult Float = Vector (Maybe Float)
    column df name = column @Series df name >>= either (pure . Left) seriesFloat

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

columnInt8 :: DataFrame -> Text -> IO (Either PolarsError (Vector (Maybe Int8)))
columnInt8 = column @Int8

columnInt16 :: DataFrame -> Text -> IO (Either PolarsError (Vector (Maybe Int16)))
columnInt16 = column @Int16

columnInt32 :: DataFrame -> Text -> IO (Either PolarsError (Vector (Maybe Int32)))
columnInt32 = column @Int32

columnWord8 :: DataFrame -> Text -> IO (Either PolarsError (Vector (Maybe Word8)))
columnWord8 = column @Word8

columnWord16 :: DataFrame -> Text -> IO (Either PolarsError (Vector (Maybe Word16)))
columnWord16 = column @Word16

columnWord32 :: DataFrame -> Text -> IO (Either PolarsError (Vector (Maybe Word32)))
columnWord32 = column @Word32

columnWord64 :: DataFrame -> Text -> IO (Either PolarsError (Vector (Maybe Word64)))
columnWord64 = column @Word64

columnDouble :: DataFrame -> Text -> IO (Either PolarsError (Vector (Maybe Double)))
columnDouble = column @Double

columnFloat :: DataFrame -> Text -> IO (Either PolarsError (Vector (Maybe Float)))
columnFloat = column @Float

columnText :: DataFrame -> Text -> IO (Either PolarsError (Vector (Maybe Text)))
columnText = column @Text
