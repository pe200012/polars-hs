{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

{- |
Module      : Polars.Internal.Expr
Description : Compile pure Haskell expressions into temporary Rust handles.

The public expression API is pure. This module allocates Rust expression handles
only while building lazy Polars plans, then relies on ForeignPtr finalizers to
release them.
-}
module Polars.Internal.Expr
    ( compileExpr
    , withCompiledExprs
    ) where

import Foreign.C.Types (CBool (..), CDouble (..), CInt (..), CLLong (..), CSize (..))
import Foreign.Marshal.Array (withArray)
import Foreign.Marshal.Alloc (alloca)
import Foreign.Ptr (Ptr, nullPtr)
import Foreign.Storable (peek, poke)

import qualified Data.Text as T
import Polars.Error (PolarsError (..), PolarsErrorCode (..))
import Polars.Expr (AggFunction (..), BinaryFunction (..), BinaryOperator (..), Expr (..), ExprSortOptions (..), QuantileMethod (..), RankMethod (..), RankOptions (..), UnaryFunction (..))
import Polars.Schema (DataType (..))
import Polars.Internal.CString (withTextCString)
import Polars.Internal.Managed (ManagedExpr, mkManagedExpr, withManagedExpr)
import Polars.Internal.Raw
    ( RawError
    , RawExpr
    , phs_expr_agg
    , phs_expr_alias
    , phs_expr_binary
    , phs_expr_binary_function
    , phs_expr_cast
    , phs_expr_col
    , phs_expr_lit_bool
    , phs_expr_lit_double
    , phs_expr_lit_int
    , phs_expr_lit_text
    , phs_expr_not
    , phs_expr_over
    , phs_expr_quantile
    , phs_expr_rank
    , phs_expr_slice
    , phs_expr_sort_by
    , phs_expr_ternary
    , phs_expr_unary
    , phs_expr_unary_i64
    )
import Polars.Internal.Result (consumeError, nullPointerError)

compileExpr :: Expr -> IO (Either PolarsError ManagedExpr)
compileExpr = \case
    Column name -> withTextCString name $ \cName -> exprOut (phs_expr_col cName)
    LiteralBool value -> exprOut (phs_expr_lit_bool (CBool (if value then 1 else 0)))
    LiteralInt value -> exprOut (phs_expr_lit_int (fromIntegral value :: CLLong))
    LiteralDouble value -> exprOut (phs_expr_lit_double (CDouble value))
    LiteralText value -> withTextCString value $ \cValue -> exprOut (phs_expr_lit_text cValue)
    Alias name expr -> do
        compiled <- compileExpr expr
        case compiled of
            Left err -> pure (Left err)
            Right managed -> withManagedExpr managed $ \ptr ->
                withTextCString name $ \cName -> exprOut (phs_expr_alias ptr cName)
    Not expr -> do
        compiled <- compileExpr expr
        case compiled of
            Left err -> pure (Left err)
            Right managed -> withManagedExpr managed $ \ptr -> exprOut (phs_expr_not ptr)
    Aggregate function expr -> do
        compiled <- compileExpr expr
        case compiled of
            Left err -> pure (Left err)
            Right managed -> withManagedExpr managed $ \ptr -> exprOut (phs_expr_agg (aggregationCode function) ptr)
    BinaryExpr op left right -> do
        leftCompiled <- compileExpr left
        case leftCompiled of
            Left err -> pure (Left err)
            Right leftManaged -> do
                rightCompiled <- compileExpr right
                case rightCompiled of
                    Left err -> pure (Left err)
                    Right rightManaged ->
                        withManagedExpr leftManaged $ \leftPtr ->
                            withManagedExpr rightManaged $ \rightPtr ->
                                exprOut (phs_expr_binary (operatorCode op) leftPtr rightPtr)
    Cast strict dtype expr -> 
        case dtypeCode dtype of
            Left err -> pure (Left err)
            Right dtypeC -> do
                compiled <- compileExpr expr
                case compiled of
                    Left err -> pure (Left err)
                    Right managed -> withManagedExpr managed $ \ptr ->
                        exprOut (phs_expr_cast (toCBool strict) dtypeC ptr)
    UnaryExpr fn expr -> do
        compiled <- compileExpr expr
        case compiled of
            Left err -> pure (Left err)
            Right managed -> withManagedExpr managed $ \ptr ->
                exprOut (phs_expr_unary (unaryFunctionCode fn) ptr)
    BinaryFunctionExpr fn left right -> do
        leftCompiled <- compileExpr left
        case leftCompiled of
            Left err -> pure (Left err)
            Right leftManaged -> do
                rightCompiled <- compileExpr right
                case rightCompiled of
                    Left err -> pure (Left err)
                    Right rightManaged ->
                        withManagedExpr leftManaged $ \leftPtr ->
                            withManagedExpr rightManaged $ \rightPtr ->
                                exprOut (phs_expr_binary_function (binaryFunctionCode fn) leftPtr rightPtr)
    TernaryExpr predicate truthy falsy -> do
        predCompiled <- compileExpr predicate
        case predCompiled of
            Left err -> pure (Left err)
            Right predManaged -> do
                truthyCompiled <- compileExpr truthy
                case truthyCompiled of
                    Left err -> pure (Left err)
                    Right truthyManaged -> do
                        falsyCompiled <- compileExpr falsy
                        case falsyCompiled of
                            Left err -> pure (Left err)
                            Right falsyManaged ->
                                withManagedExpr predManaged $ \predPtr ->
                                    withManagedExpr truthyManaged $ \truthyPtr ->
                                        withManagedExpr falsyManaged $ \falsyPtr ->
                                            exprOut (phs_expr_ternary predPtr truthyPtr falsyPtr)
    StdExpr ddof expr
        | ddof < 0 || ddof > 255 -> pure (Left (PolarsError InvalidArgument (T.pack ("ddof " <> show ddof <> " is out of range, must be 0..=255"))))
        | otherwise -> do
            compiled <- compileExpr expr
            case compiled of
                Left err -> pure (Left err)
                Right managed -> withManagedExpr managed $ \ptr ->
                    exprOut (phs_expr_unary_i64 0 (fromIntegral ddof :: CLLong) ptr)
    VarExpr ddof expr
        | ddof < 0 || ddof > 255 -> pure (Left (PolarsError InvalidArgument (T.pack ("ddof " <> show ddof <> " is out of range, must be 0..=255"))))
        | otherwise -> do
            compiled <- compileExpr expr
            case compiled of
                Left err -> pure (Left err)
                Right managed -> withManagedExpr managed $ \ptr ->
                    exprOut (phs_expr_unary_i64 1 (fromIntegral ddof :: CLLong) ptr)
    QuantileExpr method quantile expr -> do
        quantileCompiled <- compileExpr quantile
        case quantileCompiled of
            Left err -> pure (Left err)
            Right quantileManaged -> do
                exprCompiled <- compileExpr expr
                case exprCompiled of
                    Left err -> pure (Left err)
                    Right exprManaged ->
                        withManagedExpr quantileManaged $ \qPtr ->
                            withManagedExpr exprManaged $ \ePtr ->
                                exprOut (phs_expr_quantile (quantileMethodCode method) qPtr ePtr)
    RankExpr options expr -> do
        compiled <- compileExpr expr
        case compiled of
            Left err -> pure (Left err)
            Right managed -> withManagedExpr managed $ \ptr ->
                exprOut (phs_expr_rank (rankMethodCode (rankMethod options)) (toCBool (rankDescending options)) ptr)
    SliceExpr input offset len -> do
        inputCompiled <- compileExpr input
        case inputCompiled of
            Left err -> pure (Left err)
            Right inputManaged -> do
                offsetCompiled <- compileExpr offset
                case offsetCompiled of
                    Left err -> pure (Left err)
                    Right offsetManaged -> do
                        lenCompiled <- compileExpr len
                        case lenCompiled of
                            Left err -> pure (Left err)
                            Right lenManaged ->
                                withManagedExpr inputManaged $ \inPtr ->
                                    withManagedExpr offsetManaged $ \offPtr ->
                                        withManagedExpr lenManaged $ \lenPtr ->
                                            exprOut (phs_expr_slice inPtr offPtr lenPtr)
    SortByExpr options by expr -> do
        exprCompiled <- compileExpr expr
        case exprCompiled of
            Left err -> pure (Left err)
            Right exprManaged ->
                withCompiledExprs by $ \byPtrs byLen ->
                    withManagedExpr exprManaged $ \ePtr ->
                        exprOut (phs_expr_sort_by ePtr byPtrs byLen
                            (toCBool (exprSortDescending options))
                            (toCBool (exprSortNullsLast options))
                            (toCBool (exprSortMultithreaded options))
                            (toCBool (exprSortMaintainOrder options)))
    OverExpr partition expr -> do
        exprCompiled <- compileExpr expr
        case exprCompiled of
            Left err -> pure (Left err)
            Right exprManaged ->
                withCompiledExprs partition $ \partPtrs partLen ->
                    withManagedExpr exprManaged $ \ePtr ->
                        exprOut (phs_expr_over ePtr partPtrs partLen)

withCompiledExprs :: [Expr] -> (Ptr (Ptr RawExpr) -> CSize -> IO (Either PolarsError a)) -> IO (Either PolarsError a)
withCompiledExprs exprs action = do
    compiled <- traverse compileExpr exprs
    case sequence compiled of
        Left err -> pure (Left err)
        Right managed -> withManagedExprList managed $ \ptrs ->
            withArray ptrs $ \arrayPtr -> action arrayPtr (fromIntegral (length ptrs))

withManagedExprList :: [ManagedExpr] -> ([Ptr RawExpr] -> IO a) -> IO a
withManagedExprList exprs action = go exprs []
  where
    go [] acc = action (reverse acc)
    go (managed : rest) acc = withManagedExpr managed $ \ptr -> go rest (ptr : acc)

exprOut :: (Ptr (Ptr RawExpr) -> Ptr (Ptr RawError) -> IO CInt) -> IO (Either PolarsError ManagedExpr)
exprOut action =
    alloca $ \outPtr ->
        alloca $ \errPtr -> do
            poke outPtr nullPtr
            poke errPtr nullPtr
            status <- action outPtr errPtr
            if status == 0
                then do
                    ptr <- peek outPtr
                    if ptr == nullPtr
                        then pure (Left (nullPointerError "expr output"))
                        else Right <$> mkManagedExpr ptr
                else do
                    err <- peek errPtr
                    Left <$> consumeError status err

operatorCode :: BinaryOperator -> CInt
operatorCode Eq = 0
operatorCode NotEq = 1
operatorCode Gt = 2
operatorCode GtEq = 3
operatorCode Lt = 4
operatorCode LtEq = 5
operatorCode And = 6
operatorCode Or = 7
operatorCode Add = 8
operatorCode Subtract = 9
operatorCode Multiply = 10
operatorCode Divide = 11

aggregationCode :: AggFunction -> CInt
aggregationCode AggSum = 0
aggregationCode AggMean = 1
aggregationCode AggMin = 2
aggregationCode AggMax = 3
aggregationCode AggCount = 4
aggregationCode AggLen = 5
aggregationCode AggFirst = 6
aggregationCode AggLast = 7

toCBool :: Bool -> CBool
toCBool b = CBool (if b then 1 else 0)

dtypeCode :: DataType -> Either PolarsError CInt
dtypeCode Boolean = Right 0
dtypeCode Int8 = Right 1
dtypeCode Int16 = Right 2
dtypeCode Int32 = Right 3
dtypeCode Int64 = Right 4
dtypeCode UInt8 = Right 5
dtypeCode UInt16 = Right 6
dtypeCode UInt32 = Right 7
dtypeCode UInt64 = Right 8
dtypeCode Float32 = Right 9
dtypeCode Float64 = Right 10
dtypeCode Utf8 = Right 11
dtypeCode Date = Right 12
dtypeCode Datetime = Right 13
dtypeCode Duration = Right 14
dtypeCode Time = Right 15
dtypeCode Binary = Right 16
dtypeCode Null = Right 17
dtypeCode Categorical = Left (PolarsError InvalidArgument "Categorical datatype is not supported for cast expressions")
dtypeCode (UnknownType _) = Left (PolarsError InvalidArgument "Unknown datatype is not supported for cast expressions")

quantileMethodCode :: QuantileMethod -> CInt
quantileMethodCode QuantileNearest = 0
quantileMethodCode QuantileLower = 1
quantileMethodCode QuantileHigher = 2
quantileMethodCode QuantileMidpoint = 3
quantileMethodCode QuantileLinear = 4
quantileMethodCode QuantileEquiprobable = 5

rankMethodCode :: RankMethod -> CInt
rankMethodCode RankAverage = 0
rankMethodCode RankMin = 1
rankMethodCode RankMax = 2
rankMethodCode RankDense = 3
rankMethodCode RankOrdinal = 4

unaryFunctionCode :: UnaryFunction -> CInt
unaryFunctionCode IsNull = 0
unaryFunctionCode IsNotNull = 1
unaryFunctionCode IsNan = 2
unaryFunctionCode IsNotNan = 3
unaryFunctionCode IsFinite = 4
unaryFunctionCode IsInfinite = 5
unaryFunctionCode Median = 6
unaryFunctionCode NUnique = 7
unaryFunctionCode (CumCount False) = 8
unaryFunctionCode (CumCount True) = 9
unaryFunctionCode (CumSum False) = 10
unaryFunctionCode (CumSum True) = 11
unaryFunctionCode (CumProd False) = 12
unaryFunctionCode (CumProd True) = 13
unaryFunctionCode (CumMin False) = 14
unaryFunctionCode (CumMin True) = 15
unaryFunctionCode (CumMax False) = 16
unaryFunctionCode (CumMax True) = 17

binaryFunctionCode :: BinaryFunction -> CInt
binaryFunctionCode FillNull = 0
binaryFunctionCode FillNan = 1
binaryFunctionCode ExprFilter = 2
