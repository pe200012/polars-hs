{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE OverloadedStrings #-}

{- |
Module      : Polars.Join
Description : Safe lazy join operations backed by Rust Polars LazyFrame joins.

Join functions compile pure Haskell expression keys into temporary Rust handles,
call the Rust adapter once, and return a managed LazyFrame.
-}
module Polars.Join
    ( JoinOptions (..)
    , JoinType (..)
    , defaultJoinOptions
    , fullJoin
    , innerJoin
    , joinWith
    , leftJoin
    , rightJoin
    ) where

import Data.Text (Text)
import Foreign.C.String (CString)
import Foreign.C.Types (CInt)
import Foreign.Marshal.Alloc (alloca)
import Foreign.Ptr (Ptr, nullPtr)
import Foreign.Storable (peek, poke)

import Polars.Error (PolarsError (..), PolarsErrorCode (InvalidArgument))
import Polars.Expr (Expr)
import Polars.Internal.CString (withTextCString)
import Polars.Internal.Expr (withCompiledExprs)
import Polars.Internal.Managed (LazyFrame, mkLazyFrame, withLazyFrame)
import Polars.Internal.Raw (RawError, RawLazyFrame, phs_lazyframe_join)
import Polars.Internal.Result (consumeError, nullPointerError)

-- | Join variants supported by the core join MVP.
data JoinType
    = JoinInner
    | JoinLeft
    | JoinRight
    | JoinFull
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

innerJoin :: [Expr] -> [Expr] -> LazyFrame -> LazyFrame -> IO (Either PolarsError LazyFrame)
innerJoin = joinUsing JoinInner

leftJoin :: [Expr] -> [Expr] -> LazyFrame -> LazyFrame -> IO (Either PolarsError LazyFrame)
leftJoin = joinUsing JoinLeft

rightJoin :: [Expr] -> [Expr] -> LazyFrame -> LazyFrame -> IO (Either PolarsError LazyFrame)
rightJoin = joinUsing JoinRight

fullJoin :: [Expr] -> [Expr] -> LazyFrame -> LazyFrame -> IO (Either PolarsError LazyFrame)
fullJoin = joinUsing JoinFull

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
