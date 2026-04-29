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
    , ClosedInterval (..)
    , Expr (..)
    , ExprSortOptions (..)
    , HorizontalFunction (..)
    , ListFunction (..)
    , NameFunction (..)
    , QuantileMethod (..)
    , RankMethod (..)
    , RankOptions (..)
    , ScalarFunction (..)
    , StringFunction (..)
    , StringNaryFunction (..)
    , TemporalFunction (..)
    , TimeUnit (..)
    , UnaryFunction (..)
    , alias
    , cast
    , clip
    , clipMax
    , clipMin
    , col
    , concatStr
    , count_
    , cumCount
    , cumMax
    , cumMin
    , cumProd
    , cumSum
    , defaultExprSortOptions
    , defaultRankOptions
    , dtYear
    , dtIsoYear
    , dtQuarter
    , dtMonth
    , dtWeek
    , dtWeekday
    , dtDay
    , dtOrdinalDay
    , dtHour
    , dtMinute
    , dtSecond
    , dtMillisecond
    , dtMicrosecond
    , dtNanosecond
    , dtMillennium
    , dtCentury
    , dtDaysInMonth
    , dtIsLeapYear
    , dtTimestamp
    , dtToString
    , exprFilter
    , exprSlice
    , exprSortBy
    , fillNan
    , fillNull
    , first_
    , formatStr
    , allHorizontal
    , anyHorizontal
    , coalesce
    , isBetween
    , isClose
    , isDuplicated
    , isFinite
    , isIn
    , isInfinite
    , isFirstDistinct
    , isLastDistinct
    , isNan
    , isNotNan
    , isNotNull
    , isNull
    , isUnique
    , last_
    , len_
    , listContains
    , listCountMatches
    , listFirst
    , listGet
    , listJoin
    , listLast
    , listLen
    , litBool
    , litDouble
    , litInt
    , litText
    , max_
    , mean_
    , maxHorizontal
    , meanHorizontal
    , median_
    , min_
    , minHorizontal
    , nameKeep
    , namePrefix
    , nameSuffix
    , nameReplace
    , nameToLowercase
    , nameToUppercase
    , nUnique_
    , not_
    , over
    , quantile_
    , rank
    , std_
    , strContainsLiteral
    , strEndsWith
    , strContainsRegex
    , strCountMatches
    , strExtract
    , strFindLiteral
    , strFindRegex
    , strHead
    , strReplace
    , strReplaceAll
    , strLenBytes
    , strLenChars
    , strSlice
    , strEscapeRegex
    , strExtractAll
    , strSplit
    , strSplitInclusive
    , strStartsWith
    , strStrip
    , strStripEnd
    , strStripPrefix
    , strStripStart
    , strStripSuffix
    , strTail
    , strToLowercase
    , strToUppercase
    , strictCast
    , sum_
    , sumHorizontal
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
    | ListFunctionExpr !ListFunction !Expr ![Expr]
    | TemporalFunctionExpr !TemporalFunction !Expr
    | ScalarFunctionExpr !ScalarFunction !Expr ![Expr]
    | HorizontalFunctionExpr !HorizontalFunction ![Expr]
    | NameFunctionExpr !NameFunction !Expr
    | StringNaryFunctionExpr !StringNaryFunction ![Expr]
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
    | StrContainsRegex !Bool
    | StrCountMatches !Bool
    | StrExtract !Int
    | StrFindLiteral
    | StrFindRegex !Bool
    | StrReplace !Bool
    | StrReplaceAll !Bool
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
    | StrSplit
    | StrSplitInclusive
    | StrStripPrefix
    | StrStripSuffix
    | StrEscapeRegex
    | StrExtractAll
    deriving stock (Eq, Show)

-- | List namespace expression functions.
data ListFunction
    = ListLen
    | ListFirst
    | ListLast
    | ListGet !Bool
    | ListJoin !Bool
    | ListContains !Bool
    | ListCountMatches
    deriving stock (Eq, Show)

-- | Closed interval boundary specification for is_between.
data ClosedInterval = ClosedBoth | ClosedLeft | ClosedRight | ClosedNone
    deriving stock (Eq, Show)

-- | Scalar predicate and clip expression functions.
data ScalarFunction
    = IsDuplicated
    | IsUnique
    | IsFirstDistinct
    | IsLastDistinct
    | IsBetween !ClosedInterval
    | IsClose !Double !Double !Bool
    | IsIn !Bool
    | Clip
    | ClipMin
    | ClipMax
    deriving stock (Eq, Show)

-- | Horizontal expression functions (row-wise across columns).
data HorizontalFunction
    = HorizontalSum !Bool      -- ^ ignore_nulls
    | HorizontalMean !Bool     -- ^ ignore_nulls
    | HorizontalMax
    | HorizontalMin
    | HorizontalAny
    | HorizontalAll
    | HorizontalCoalesce
    deriving stock (Eq, Show)

-- | Name namespace expression functions.
data NameFunction
    = NameKeep
    | NamePrefix !Text
    | NameSuffix !Text
    | NameReplace !Bool !Text !Text  -- ^ literal -> pattern -> value
    | NameToLowercase
    | NameToUppercase
    deriving stock (Eq, Show)

-- | String n-ary expression functions (concat_str, format_str).
data StringNaryFunction
    = ConcatStr !Bool !Text  -- ^ ignore_nulls, separator
    | FormatStr !Text        -- ^ format string with `{}` placeholders
    deriving stock (Eq, Show)

-- | Time unit for temporal functions.
data TimeUnit = Milliseconds | Microseconds | Nanoseconds
    deriving stock (Eq, Show)

-- | Temporal namespace expression functions.
data TemporalFunction
    = DtYear | DtIsoYear | DtQuarter | DtMonth | DtWeek | DtWeekday
    | DtDay | DtOrdinalDay | DtHour | DtMinute | DtSecond
    | DtMillisecond | DtMicrosecond | DtNanosecond
    | DtMillennium | DtCentury | DtDaysInMonth | DtIsLeapYear
    | DtTimestamp !TimeUnit
    | DtToString !Text
    deriving stock (Eq, Show)

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

strContainsRegex :: Bool -> Expr -> Expr -> Expr
strContainsRegex strict input pat = StringFunctionExpr (StrContainsRegex strict) input [pat]

strFindLiteral :: Expr -> Expr -> Expr
strFindLiteral input pat = StringFunctionExpr StrFindLiteral input [pat]

strFindRegex :: Bool -> Expr -> Expr -> Expr
strFindRegex strict input pat = StringFunctionExpr (StrFindRegex strict) input [pat]

strExtract :: Int -> Expr -> Expr -> Expr
strExtract groupIndex input pat = StringFunctionExpr (StrExtract groupIndex) input [pat]

strCountMatches :: Bool -> Expr -> Expr -> Expr
strCountMatches literal input pat = StringFunctionExpr (StrCountMatches literal) input [pat]

strReplace :: Bool -> Expr -> Expr -> Expr -> Expr
strReplace literal input pat value = StringFunctionExpr (StrReplace literal) input [pat, value]

strReplaceAll :: Bool -> Expr -> Expr -> Expr -> Expr
strReplaceAll literal input pat value = StringFunctionExpr (StrReplaceAll literal) input [pat, value]

strSplit, strSplitInclusive :: Expr -> Expr -> Expr
strSplit input by = StringFunctionExpr StrSplit input [by]
strSplitInclusive input by = StringFunctionExpr StrSplitInclusive input [by]

strStripPrefix, strStripSuffix :: Expr -> Expr -> Expr
strStripPrefix input prefix = StringFunctionExpr StrStripPrefix input [prefix]
strStripSuffix input suffix = StringFunctionExpr StrStripSuffix input [suffix]

strEscapeRegex :: Expr -> Expr
strEscapeRegex input = StringFunctionExpr StrEscapeRegex input []

strExtractAll :: Expr -> Expr -> Expr
strExtractAll input pat = StringFunctionExpr StrExtractAll input [pat]

listLen, listFirst, listLast :: Expr -> Expr
listLen input = ListFunctionExpr ListLen input []
listFirst input = ListFunctionExpr ListFirst input []
listLast input = ListFunctionExpr ListLast input []

listGet :: Bool -> Expr -> Expr -> Expr
listGet nullOnOob input index = ListFunctionExpr (ListGet nullOnOob) input [index]

listJoin :: Bool -> Expr -> Expr -> Expr
listJoin ignoreNulls input separator = ListFunctionExpr (ListJoin ignoreNulls) input [separator]

listContains :: Bool -> Expr -> Expr -> Expr
listContains nullsEqual input element = ListFunctionExpr (ListContains nullsEqual) input [element]

listCountMatches :: Expr -> Expr -> Expr
listCountMatches input element = ListFunctionExpr ListCountMatches input [element]

dtYear, dtIsoYear, dtQuarter, dtMonth, dtWeek, dtWeekday, dtDay, dtOrdinalDay, dtHour, dtMinute, dtSecond :: Expr -> Expr
dtYear = TemporalFunctionExpr DtYear
dtIsoYear = TemporalFunctionExpr DtIsoYear
dtQuarter = TemporalFunctionExpr DtQuarter
dtMonth = TemporalFunctionExpr DtMonth
dtWeek = TemporalFunctionExpr DtWeek
dtWeekday = TemporalFunctionExpr DtWeekday
dtDay = TemporalFunctionExpr DtDay
dtOrdinalDay = TemporalFunctionExpr DtOrdinalDay
dtHour = TemporalFunctionExpr DtHour
dtMinute = TemporalFunctionExpr DtMinute
dtSecond = TemporalFunctionExpr DtSecond

dtMillisecond, dtMicrosecond, dtNanosecond :: Expr -> Expr
dtMillisecond = TemporalFunctionExpr DtMillisecond
dtMicrosecond = TemporalFunctionExpr DtMicrosecond
dtNanosecond = TemporalFunctionExpr DtNanosecond

dtMillennium, dtCentury, dtDaysInMonth, dtIsLeapYear :: Expr -> Expr
dtMillennium = TemporalFunctionExpr DtMillennium
dtCentury = TemporalFunctionExpr DtCentury
dtDaysInMonth = TemporalFunctionExpr DtDaysInMonth
dtIsLeapYear = TemporalFunctionExpr DtIsLeapYear

dtTimestamp :: TimeUnit -> Expr -> Expr
dtTimestamp unit = TemporalFunctionExpr (DtTimestamp unit)

dtToString :: Text -> Expr -> Expr
dtToString format = TemporalFunctionExpr (DtToString format)

isDuplicated, isUnique, isFirstDistinct, isLastDistinct :: Expr -> Expr
isDuplicated input = ScalarFunctionExpr IsDuplicated input []
isUnique input = ScalarFunctionExpr IsUnique input []
isFirstDistinct input = ScalarFunctionExpr IsFirstDistinct input []
isLastDistinct input = ScalarFunctionExpr IsLastDistinct input []

isBetween :: ClosedInterval -> Expr -> Expr -> Expr -> Expr
isBetween closed input lower upper =
    ScalarFunctionExpr (IsBetween closed) input [lower, upper]

isClose :: Double -> Double -> Bool -> Expr -> Expr -> Expr
isClose absTol relTol nansEqual input other =
    ScalarFunctionExpr (IsClose absTol relTol nansEqual) input [other]

isIn :: Bool -> Expr -> Expr -> Expr
isIn nullsEqual input listExpr =
    ScalarFunctionExpr (IsIn nullsEqual) input [listExpr]

clip :: Expr -> Expr -> Expr -> Expr
clip input lower upper =
    ScalarFunctionExpr Clip input [lower, upper]

clipMin :: Expr -> Expr -> Expr
clipMin input lower = ScalarFunctionExpr ClipMin input [lower]

clipMax :: Expr -> Expr -> Expr
clipMax input upper = ScalarFunctionExpr ClipMax input [upper]

sumHorizontal :: Bool -> [Expr] -> Expr
sumHorizontal ignore_nulls = HorizontalFunctionExpr (HorizontalSum ignore_nulls)

meanHorizontal :: Bool -> [Expr] -> Expr
meanHorizontal ignore_nulls = HorizontalFunctionExpr (HorizontalMean ignore_nulls)

maxHorizontal :: [Expr] -> Expr
maxHorizontal = HorizontalFunctionExpr HorizontalMax

minHorizontal :: [Expr] -> Expr
minHorizontal = HorizontalFunctionExpr HorizontalMin

anyHorizontal :: [Expr] -> Expr
anyHorizontal = HorizontalFunctionExpr HorizontalAny

allHorizontal :: [Expr] -> Expr
allHorizontal = HorizontalFunctionExpr HorizontalAll

coalesce :: [Expr] -> Expr
coalesce = HorizontalFunctionExpr HorizontalCoalesce

-- String n-ary

concatStr :: Bool -> Text -> [Expr] -> Expr
concatStr ignore_nulls separator = StringNaryFunctionExpr (ConcatStr ignore_nulls separator)

formatStr :: Text -> [Expr] -> Expr
formatStr = StringNaryFunctionExpr . FormatStr

-- Name namespace

nameKeep :: Expr -> Expr
nameKeep = NameFunctionExpr NameKeep

namePrefix :: Text -> Expr -> Expr
namePrefix prefix = NameFunctionExpr (NamePrefix prefix)

nameSuffix :: Text -> Expr -> Expr
nameSuffix suffix = NameFunctionExpr (NameSuffix suffix)

nameReplace :: Bool -> Text -> Text -> Expr -> Expr
nameReplace literal pat value = NameFunctionExpr (NameReplace literal pat value)

nameToLowercase :: Expr -> Expr
nameToLowercase = NameFunctionExpr NameToLowercase

nameToUppercase :: Expr -> Expr
nameToUppercase = NameFunctionExpr NameToUppercase
