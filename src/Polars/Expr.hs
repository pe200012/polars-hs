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
    , BinaryFunction (..)
    , BinaryOperator (..)
    , Expr (..)
    , ExprSortOptions (..)
    , QuantileMethod (..)
    , RankMethod (..)
    , RankOptions (..)
    , StringFunction (..)
    , UnaryFunction (..)
    , alias
    , cast
    , col
    , count_
    , cumCount
    , cumMax
    , cumMin
    , cumProd
    , cumSum
    , defaultExprSortOptions
    , defaultRankOptions
    , exprFilter
    , exprSlice
    , exprSortBy
    , fillNan
    , fillNull
    , first_
    , isFinite
    , isInfinite
    , isNan
    , isNotNan
    , isNotNull
    , isNull
    , last_
    , len_
    , litBool
    , litDouble
    , litInt
    , litText
    , max_
    , mean_
    , median_
    , min_
    , nUnique_
    , not_
    , over
    , quantile_
    , rank
    , std_
    , strContainsLiteral
    , strEndsWith
    , strHead
    , strLenBytes
    , strLenChars
    , strSlice
    , strStartsWith
    , strStrip
    , strStripEnd
    , strStripStart
    , strTail
    , strToLowercase
    , strToUppercase
    , strictCast
    , sum_
    , var_
    , whenThenOtherwise
    ) where

import Prelude hiding (isInfinite)
import Data.Int (Int64)
import Data.Text (Text)
import Polars.Schema (DataType)

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
    | Cast !Bool !DataType !Expr
    | UnaryExpr !UnaryFunction !Expr
    | BinaryFunctionExpr !BinaryFunction !Expr !Expr
    | TernaryExpr !Expr !Expr !Expr
    | StdExpr !Int !Expr
    | VarExpr !Int !Expr
    | QuantileExpr !QuantileMethod !Expr !Expr
    | RankExpr !RankOptions !Expr
    | SliceExpr !Expr !Expr !Expr
    | SortByExpr !ExprSortOptions ![Expr] !Expr
    | OverExpr ![Expr] !Expr
    | StringFunctionExpr !StringFunction !Expr ![Expr]
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

-- | Unary expression functions.
data UnaryFunction
    = IsNull
    | IsNotNull
    | IsNan
    | IsNotNan
    | IsFinite
    | IsInfinite
    | Median
    | NUnique
    | CumCount !Bool
    | CumSum !Bool
    | CumProd !Bool
    | CumMin !Bool
    | CumMax !Bool
    deriving stock (Eq, Show)

-- | Binary expression functions.
data BinaryFunction
    = FillNull
    | FillNan
    | ExprFilter
    deriving stock (Eq, Show)

-- | String namespace expression functions.
data StringFunction
    = StrContainsLiteral
    | StrStartsWith
    | StrEndsWith
    | StrStrip
    | StrStripStart
    | StrStripEnd
    | StrToLowercase
    | StrToUppercase
    | StrLenBytes
    | StrLenChars
    | StrSlice
    | StrHead
    | StrTail
    deriving stock (Eq, Show)

-- | Method for computing quantiles.
data QuantileMethod
    = QuantileNearest
    | QuantileLower
    | QuantileHigher
    | QuantileMidpoint
    | QuantileLinear
    | QuantileEquiprobable
    deriving stock (Eq, Show)

-- | Method for ranking.
data RankMethod = RankAverage | RankMin | RankMax | RankDense | RankOrdinal
    deriving stock (Eq, Show)

-- | Options for rank expressions.
data RankOptions = RankOptions
    { rankMethod :: !RankMethod
    , rankDescending :: !Bool
    }
    deriving stock (Eq, Show)

defaultRankOptions :: RankOptions
defaultRankOptions = RankOptions {rankMethod = RankDense, rankDescending = False}

-- | Options for expression sorting.
data ExprSortOptions = ExprSortOptions
    { exprSortDescending :: !Bool
    , exprSortNullsLast :: !Bool
    , exprSortMultithreaded :: !Bool
    , exprSortMaintainOrder :: !Bool
    }
    deriving stock (Eq, Show)

defaultExprSortOptions :: ExprSortOptions
defaultExprSortOptions =
    ExprSortOptions
        { exprSortDescending = False
        , exprSortNullsLast = False
        , exprSortMultithreaded = True
        , exprSortMaintainOrder = False
        }

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

cast :: DataType -> Expr -> Expr
cast = Cast False

strictCast :: DataType -> Expr -> Expr
strictCast = Cast True

isNull, isNotNull, isNan, isNotNan, isFinite, isInfinite :: Expr -> Expr
isNull = UnaryExpr IsNull
isNotNull = UnaryExpr IsNotNull
isNan = UnaryExpr IsNan
isNotNan = UnaryExpr IsNotNan
isFinite = UnaryExpr IsFinite
isInfinite = UnaryExpr IsInfinite

fillNull, fillNan, exprFilter :: Expr -> Expr -> Expr
fillNull fillValue input = BinaryFunctionExpr FillNull input fillValue
fillNan fillValue input = BinaryFunctionExpr FillNan input fillValue
exprFilter = BinaryFunctionExpr ExprFilter

whenThenOtherwise :: Expr -> Expr -> Expr -> Expr
whenThenOtherwise = TernaryExpr

median_, nUnique_ :: Expr -> Expr
median_ = UnaryExpr Median
nUnique_ = UnaryExpr NUnique

std_ :: Int -> Expr -> Expr
std_ = StdExpr

var_ :: Int -> Expr -> Expr
var_ = VarExpr

quantile_ :: QuantileMethod -> Expr -> Expr -> Expr
quantile_ = QuantileExpr

cumCount, cumSum, cumProd, cumMin, cumMax :: Bool -> Expr -> Expr
cumCount = UnaryExpr . CumCount
cumSum = UnaryExpr . CumSum
cumProd = UnaryExpr . CumProd
cumMin = UnaryExpr . CumMin
cumMax = UnaryExpr . CumMax

rank :: RankOptions -> Expr -> Expr
rank = RankExpr

exprSlice :: Expr -> Expr -> Expr -> Expr
exprSlice = SliceExpr

exprSortBy :: ExprSortOptions -> [Expr] -> Expr -> Expr
exprSortBy = SortByExpr

over :: [Expr] -> Expr -> Expr
over = OverExpr

strContainsLiteral, strStartsWith, strEndsWith, strStrip, strStripStart, strStripEnd, strHead, strTail :: Expr -> Expr -> Expr
strContainsLiteral input patternExpr = StringFunctionExpr StrContainsLiteral input [patternExpr]
strStartsWith input prefix = StringFunctionExpr StrStartsWith input [prefix]
strEndsWith input suffix = StringFunctionExpr StrEndsWith input [suffix]
strStrip input matches = StringFunctionExpr StrStrip input [matches]
strStripStart input matches = StringFunctionExpr StrStripStart input [matches]
strStripEnd input matches = StringFunctionExpr StrStripEnd input [matches]
strHead input n = StringFunctionExpr StrHead input [n]
strTail input n = StringFunctionExpr StrTail input [n]

strToLowercase, strToUppercase, strLenBytes, strLenChars :: Expr -> Expr
strToLowercase input = StringFunctionExpr StrToLowercase input []
strToUppercase input = StringFunctionExpr StrToUppercase input []
strLenBytes input = StringFunctionExpr StrLenBytes input []
strLenChars input = StringFunctionExpr StrLenChars input []

strSlice :: Expr -> Expr -> Expr -> Expr
strSlice input offset len = StringFunctionExpr StrSlice input [offset, len]
