{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE OverloadedStrings #-}

{- |
Module      : Polars.Join
Description : Safe lazy join operations backed by Rust Polars LazyFrame joins.

Join functions compile pure Haskell expression keys into temporary Rust handles,
call the Rust adapter once, and return a managed LazyFrame.
-}
module Polars.Join
    ( antiJoin
    , crossJoin
    , ExtendedJoinOptions (..)
    , JoinOptions (..)
    , JoinCoalesce (..)
    , JoinMaintainOrder (..)
    , JoinType (..)
    , JoinValidation (..)
    , defaultExtendedJoinOptions
    , defaultJoinOptions
    , fullJoin
    , innerJoin
    , joinWith
    , joinWithExtended
    , leftJoin
    , rightJoin
    , semiJoin
    ) where

import Data.Text (Text)
import Foreign.C.String (CString)
import Foreign.C.Types (CBool (..), CInt)
import Foreign.Marshal.Alloc (alloca)
import Foreign.Ptr (Ptr, nullPtr)
import Foreign.Storable (peek, poke)

import Polars.Error (PolarsError (..), PolarsErrorCode (InvalidArgument))
import Polars.Expr (Expr)
import Polars.Internal.CString (withTextCString)
import Polars.Internal.Expr (withCompiledExprs)
import Polars.Internal.Managed (LazyFrame, mkLazyFrame, withLazyFrame)
import Polars.Internal.Raw (RawError, RawLazyFrame, phs_lazyframe_join, phs_lazyframe_join_ex)
import Polars.Internal.Result (consumeError, nullPointerError)

-- | Join variants supported by the core join MVP.
data JoinType
    = JoinInner
    | JoinLeft
    | JoinRight
    | JoinFull
    | JoinSemi
    | JoinAnti
    | JoinCross
    deriving stock (Eq, Show)

-- | Options for joining two lazy frames.
data JoinOptions = JoinOptions
    { joinType :: !JoinType
    , leftOn :: ![Expr]
    , rightOn :: ![Expr]
    , suffix :: !(Maybe Text)
    }
    deriving stock (Eq, Show)

defaultJoinOptions :: JoinOptions
defaultJoinOptions =
    JoinOptions
        { joinType = JoinInner
        , leftOn = []
        , rightOn = []
        , suffix = Nothing
        }

-- | Join validation modes for Polars lazy joins.
data JoinValidation
    = JoinManyToMany
    | JoinManyToOne
    | JoinOneToMany
    | JoinOneToOne
    deriving stock (Eq, Show)

-- | Join key coalescing behavior.
data JoinCoalesce
    = JoinCoalesceDefault
    | JoinCoalesceColumns
    | JoinKeepColumns
    deriving stock (Eq, Show)

-- | Output row-order preservation controls.
data JoinMaintainOrder
    = JoinMaintainOrderNone
    | JoinMaintainOrderLeft
    | JoinMaintainOrderRight
    | JoinMaintainOrderLeftRight
    | JoinMaintainOrderRightLeft
    deriving stock (Eq, Show)

-- | Extended options for Polars lazy joins.
data ExtendedJoinOptions = ExtendedJoinOptions
    { extendedJoinBase :: !JoinOptions
    , extendedJoinValidation :: !JoinValidation
    , extendedJoinNullsEqual :: !Bool
    , extendedJoinCoalesce :: !JoinCoalesce
    , extendedJoinMaintainOrder :: !JoinMaintainOrder
    , extendedJoinAllowParallel :: !Bool
    , extendedJoinForceParallel :: !Bool
    }
    deriving stock (Eq, Show)

defaultExtendedJoinOptions :: ExtendedJoinOptions
defaultExtendedJoinOptions =
    ExtendedJoinOptions
        { extendedJoinBase = defaultJoinOptions
        , extendedJoinValidation = JoinManyToMany
        , extendedJoinNullsEqual = False
        , extendedJoinCoalesce = JoinCoalesceDefault
        , extendedJoinMaintainOrder = JoinMaintainOrderNone
        , extendedJoinAllowParallel = True
        , extendedJoinForceParallel = False
        }

joinWith :: JoinOptions -> LazyFrame -> LazyFrame -> IO (Either PolarsError LazyFrame)
joinWith options leftFrame rightFrame =
    case validateJoinOptions options of
        Left err -> pure (Left err)
        Right () ->
            withLazyFrame leftFrame $ \leftPtr ->
                withLazyFrame rightFrame $ \rightPtr ->
                    withCompiledExprs (leftOn options) $ \leftArray leftLen ->
                        withCompiledExprs (rightOn options) $ \rightArray rightLen ->
                            withOptionalTextCString (suffix options) $ \suffixPtr ->
                                lazyFrameOut
                                    ( phs_lazyframe_join
                                        leftPtr
                                        rightPtr
                                        leftArray
                                        leftLen
                                        rightArray
                                        rightLen
                                        (joinTypeCode (joinType options))
                                        suffixPtr
                                    )

-- | Join two lazy frames with extended Polars join controls.
joinWithExtended :: ExtendedJoinOptions -> LazyFrame -> LazyFrame -> IO (Either PolarsError LazyFrame)
joinWithExtended options leftFrame rightFrame =
    case validateJoinOptions baseOptions of
        Left err -> pure (Left err)
        Right () ->
            withLazyFrame leftFrame $ \leftPtr ->
                withLazyFrame rightFrame $ \rightPtr ->
                    withCompiledExprs (leftOn baseOptions) $ \leftArray leftLen ->
                        withCompiledExprs (rightOn baseOptions) $ \rightArray rightLen ->
                            withOptionalTextCString (suffix baseOptions) $ \suffixPtr ->
                                lazyFrameOut
                                    ( phs_lazyframe_join_ex
                                        leftPtr
                                        rightPtr
                                        leftArray
                                        leftLen
                                        rightArray
                                        rightLen
                                        (joinTypeCode (joinType baseOptions))
                                        suffixPtr
                                        (joinValidationCode (extendedJoinValidation options))
                                        (toCBool (extendedJoinNullsEqual options))
                                        (joinCoalesceCode (extendedJoinCoalesce options))
                                        (joinMaintainOrderCode (extendedJoinMaintainOrder options))
                                        (toCBool (extendedJoinAllowParallel options))
                                        (toCBool (extendedJoinForceParallel options))
                                    )
  where
    baseOptions = extendedJoinBase options

innerJoin :: [Expr] -> [Expr] -> LazyFrame -> LazyFrame -> IO (Either PolarsError LazyFrame)
innerJoin = joinUsing JoinInner

leftJoin :: [Expr] -> [Expr] -> LazyFrame -> LazyFrame -> IO (Either PolarsError LazyFrame)
leftJoin = joinUsing JoinLeft

rightJoin :: [Expr] -> [Expr] -> LazyFrame -> LazyFrame -> IO (Either PolarsError LazyFrame)
rightJoin = joinUsing JoinRight

fullJoin :: [Expr] -> [Expr] -> LazyFrame -> LazyFrame -> IO (Either PolarsError LazyFrame)
fullJoin = joinUsing JoinFull

semiJoin :: [Expr] -> [Expr] -> LazyFrame -> LazyFrame -> IO (Either PolarsError LazyFrame)
semiJoin = joinUsing JoinSemi

antiJoin :: [Expr] -> [Expr] -> LazyFrame -> LazyFrame -> IO (Either PolarsError LazyFrame)
antiJoin = joinUsing JoinAnti

crossJoin :: LazyFrame -> LazyFrame -> IO (Either PolarsError LazyFrame)
crossJoin =
    joinWith
        defaultJoinOptions
            { joinType = JoinCross
            , leftOn = []
            , rightOn = []
            }

joinUsing :: JoinType -> [Expr] -> [Expr] -> LazyFrame -> LazyFrame -> IO (Either PolarsError LazyFrame)
joinUsing kind leftKeys rightKeys =
    joinWith
        defaultJoinOptions
            { joinType = kind
            , leftOn = leftKeys
            , rightOn = rightKeys
            }

validateJoinOptions :: JoinOptions -> Either PolarsError ()
validateJoinOptions options
    | joinType options == JoinCross && (not (null leftKeys) || not (null rightKeys)) =
        Left (invalidArgument "cross join requires empty join key lists")
    | joinType options == JoinCross = Right ()
    | null leftKeys = Left (invalidArgument "left join keys must contain at least one expression")
    | null rightKeys = Left (invalidArgument "right join keys must contain at least one expression")
    | length leftKeys /= length rightKeys = Left (invalidArgument "left and right join key counts must match")
    | otherwise = Right ()
  where
    leftKeys = leftOn options
    rightKeys = rightOn options

invalidArgument :: Text -> PolarsError
invalidArgument = PolarsError InvalidArgument

withOptionalTextCString :: Maybe Text -> (CString -> IO a) -> IO a
withOptionalTextCString Nothing action = action nullPtr
withOptionalTextCString (Just value) action = withTextCString value action

joinTypeCode :: JoinType -> CInt
joinTypeCode JoinInner = 0
joinTypeCode JoinLeft = 1
joinTypeCode JoinRight = 2
joinTypeCode JoinFull = 3
joinTypeCode JoinSemi = 4
joinTypeCode JoinAnti = 5
joinTypeCode JoinCross = 6

joinValidationCode :: JoinValidation -> CInt
joinValidationCode JoinManyToMany = 0
joinValidationCode JoinManyToOne = 1
joinValidationCode JoinOneToMany = 2
joinValidationCode JoinOneToOne = 3

joinCoalesceCode :: JoinCoalesce -> CInt
joinCoalesceCode JoinCoalesceDefault = 0
joinCoalesceCode JoinCoalesceColumns = 1
joinCoalesceCode JoinKeepColumns = 2

joinMaintainOrderCode :: JoinMaintainOrder -> CInt
joinMaintainOrderCode JoinMaintainOrderNone = 0
joinMaintainOrderCode JoinMaintainOrderLeft = 1
joinMaintainOrderCode JoinMaintainOrderRight = 2
joinMaintainOrderCode JoinMaintainOrderLeftRight = 3
joinMaintainOrderCode JoinMaintainOrderRightLeft = 4

toCBool :: Bool -> CBool
toCBool value = CBool (if value then 1 else 0)

lazyFrameOut :: (Ptr (Ptr RawLazyFrame) -> Ptr (Ptr RawError) -> IO CInt) -> IO (Either PolarsError LazyFrame)
lazyFrameOut action =
    alloca $ \outPtr ->
        alloca $ \errPtr -> do
            poke outPtr nullPtr
            poke errPtr nullPtr
            status <- action outPtr errPtr
            if status == 0
                then do
                    ptr <- peek outPtr
                    if ptr == nullPtr
                        then pure (Left (nullPointerError "lazyframe output"))
                        else Right <$> mkLazyFrame ptr
                else Left <$> (consumeError status =<< peek errPtr)
