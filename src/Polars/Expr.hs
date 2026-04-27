{-# LANGUAGE DerivingStrategies #-}

{- |
Module      : Polars.Expr
Description : Pure expression AST for Polars lazy queries.

Expressions are ordinary Haskell values. Internal modules compile them into
short-lived Rust expression handles when a lazy operation crosses the FFI
boundary.
-}
module Polars.Expr
    ( Expr (..)
    , BinaryOperator (..)
    , alias
    , col
    , litBool
    , litDouble
    , litInt
    , litText
    , not_
    ) where

import Data.Int (Int64)
import Data.Text (Text)

-- | Pure Haskell representation of a Polars expression.
data Expr
    = Column !Text
    | LiteralBool !Bool
    | LiteralInt !Int64
    | LiteralDouble !Double
    | LiteralText !Text
    | Alias !Text !Expr
    | BinaryExpr !BinaryOperator !Expr !Expr
    | Not !Expr
    deriving stock (Eq, Show)

-- | Binary operators supported by the MVP expression compiler.
data BinaryOperator
    = Eq
    | NotEq
    | Gt
    | GtEq
    | Lt
    | LtEq
    | And
    | Or
    | Add
    | Subtract
    | Multiply
    | Divide
    deriving stock (Eq, Show)

col :: Text -> Expr
col = Column

litBool :: Bool -> Expr
litBool = LiteralBool

litInt :: Int64 -> Expr
litInt = LiteralInt

litDouble :: Double -> Expr
litDouble = LiteralDouble

litText :: Text -> Expr
litText = LiteralText

alias :: Text -> Expr -> Expr
alias = Alias

not_ :: Expr -> Expr
not_ = Not
