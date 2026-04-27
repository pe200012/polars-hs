{- |
Module      : Polars.Operators
Description : Infix expression operators for lazy Polars queries.

These operators build pure expression values. Evaluation happens after a
LazyFrame is collected by Rust Polars.
-}
module Polars.Operators
    ( (.!=)
    , (.&&)
    , (.*)
    , (.+)
    , (.-)
    , (./)
    , (.<)
    , (.<=)
    , (.==)
    , (.>)
    , (.>=)
    , (.||)
    ) where

import Polars.Expr (BinaryOperator (..), Expr (BinaryExpr))

infix 4 .==, .!=, .>, .>=, .<, .<=
infixr 3 .&&
infixr 2 .||
infixl 6 .+, .-
infixl 7 .*, ./

(.==) :: Expr -> Expr -> Expr
(.==) = BinaryExpr Eq

(.!=) :: Expr -> Expr -> Expr
(.!=) = BinaryExpr NotEq

(.>) :: Expr -> Expr -> Expr
(.>) = BinaryExpr Gt

(.>=) :: Expr -> Expr -> Expr
(.>=) = BinaryExpr GtEq

(.<) :: Expr -> Expr -> Expr
(.<) = BinaryExpr Lt

(.<=) :: Expr -> Expr -> Expr
(.<=) = BinaryExpr LtEq

(.&&) :: Expr -> Expr -> Expr
(.&&) = BinaryExpr And

(.||) :: Expr -> Expr -> Expr
(.||) = BinaryExpr Or

(.+) :: Expr -> Expr -> Expr
(.+) = BinaryExpr Add

(.-) :: Expr -> Expr -> Expr
(.-) = BinaryExpr Subtract

(.*) :: Expr -> Expr -> Expr
(.*) = BinaryExpr Multiply

(./) :: Expr -> Expr -> Expr
(./) = BinaryExpr Divide
