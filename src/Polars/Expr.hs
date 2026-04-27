{-# LANGUAGE DerivingStrategies #-}

{- |
Module      : Polars.Expr
Description : Pure expression AST for Polars lazy queries.

Expressions are ordinary Haskell values. Internal modules compile them into
short-lived Rust expression handles when a lazy operation crosses the FFI
boundary.
-}
module Polars.Expr
    ( AggFunction (..)
    , BinaryOperator (..)
    , Expr (..)
    , alias
    , col
    , count_
    , first_
    , last_
    , len_
    , litBool
    , litDouble
    , litInt
    , litText
    , max_
    , mean_
    , min_
    , not_
    , sum_
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
    | Aggregate !AggFunction !Expr
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

-- | Aggregation functions supported by grouped aggregation expressions.
data AggFunction
    = AggSum
    | AggMean
    | AggMin
    | AggMax
    | AggCount
    | AggLen
    | AggFirst
    | AggLast
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

sum_ :: Expr -> Expr
sum_ = Aggregate AggSum

mean_ :: Expr -> Expr
mean_ = Aggregate AggMean

min_ :: Expr -> Expr
min_ = Aggregate AggMin

max_ :: Expr -> Expr
max_ = Aggregate AggMax

count_ :: Expr -> Expr
count_ = Aggregate AggCount

len_ :: Expr -> Expr
len_ = Aggregate AggLen

first_ :: Expr -> Expr
first_ = Aggregate AggFirst

last_ :: Expr -> Expr
last_ = Aggregate AggLast
