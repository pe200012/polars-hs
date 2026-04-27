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

import Polars.Error (PolarsError)
import Polars.Expr (BinaryOperator (..), Expr (..))
import Polars.Internal.CString (withTextCString)
import Polars.Internal.Managed (ManagedExpr, mkManagedExpr, withManagedExpr)
import Polars.Internal.Raw
    ( RawError
    , RawExpr
    , phs_expr_alias
    , phs_expr_binary
    , phs_expr_col
    , phs_expr_lit_bool
    , phs_expr_lit_double
    , phs_expr_lit_int
    , phs_expr_lit_text
    , phs_expr_not
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
