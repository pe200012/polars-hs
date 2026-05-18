{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE OverloadedStrings #-}

{- |
Module      : Polars.GroupBy
Description : Grouped aggregation operations for lazy Polars queries.

A GroupBy value is a pure Haskell descriptor. Aggregation compiles keys and
aggregation expressions into temporary Rust handles, calls Rust Polars once, and
returns a managed LazyFrame.
-}
module Polars.GroupBy
    ( DurationSpec (..)
    , DynamicGroupByOptions (..)
    , DynamicLabel (..)
    , GroupBy
    , RollingGroupByOptions (..)
    , StartBy (..)
    , agg
    , defaultDynamicGroupByOptions
    , defaultRollingGroupByOptions
    , groupBy
    , groupByDynamic
    , groupByRolling
    , groupByStable
    ) where

import Data.Text (Text)
import Foreign.C.String (CString)
import Foreign.C.Types (CBool (..), CInt)
import Foreign.Marshal.Alloc (alloca)
import Foreign.Ptr (Ptr, nullPtr)
import Foreign.Storable (peek, poke)

import Polars.Error (PolarsError (..), PolarsErrorCode (InvalidArgument))
import Polars.Expr (ClosedInterval (..), Expr)
import Polars.Internal.CString (withTextCString)
import Polars.Internal.Expr (compileExpr, withCompiledExprs)
import Polars.Internal.Managed (LazyFrame, mkLazyFrame, withLazyFrame, withManagedExpr)
import Polars.Internal.Raw (RawError, RawExpr, RawLazyFrame, phs_lazyframe_group_by_agg, phs_lazyframe_group_by_dynamic_agg, phs_lazyframe_group_by_rolling_agg)
import Polars.Internal.Result (consumeError, nullPointerError)

-- | Polars duration string wrapper for dynamic and rolling windows.
newtype DurationSpec = DurationSpec {durationSpecText :: Text}
    deriving stock (Eq, Show)

-- | Dynamic window output label placement.
data DynamicLabel
    = LabelLeft
    | LabelRight
    | LabelDataPoint
    deriving stock (Eq, Show)

-- | Dynamic window anchor.
data StartBy
    = StartByWindowBound
    | StartByDataPoint
    | StartByMonday
    | StartByTuesday
    | StartByWednesday
    | StartByThursday
    | StartByFriday
    | StartBySaturday
    | StartBySunday
    deriving stock (Eq, Show)

-- | Options for Polars dynamic groupby windows.
data DynamicGroupByOptions = DynamicGroupByOptions
    { dynamicEvery :: !DurationSpec
    , dynamicPeriod :: !DurationSpec
    , dynamicOffset :: !DurationSpec
    , dynamicLabel :: !DynamicLabel
    , dynamicIncludeBoundaries :: !Bool
    , dynamicClosedWindow :: !ClosedInterval
    , dynamicStartBy :: !StartBy
    }
    deriving stock (Eq, Show)

-- | Options for Polars rolling groupby windows.
data RollingGroupByOptions = RollingGroupByOptions
    { rollingPeriod :: !DurationSpec
    , rollingOffset :: !DurationSpec
    , rollingClosedWindow :: !ClosedInterval
    }
    deriving stock (Eq, Show)

-- | Pure descriptor for a grouped lazy query.
data GroupBy
    = PlainGroupBy !LazyFrame ![Expr] !Bool
    | DynamicGroupBy !LazyFrame !Expr ![Expr] !DynamicGroupByOptions
    | RollingGroupBy !LazyFrame !Expr ![Expr] !RollingGroupByOptions

-- | Default dynamic window options using the same duration for every and period.
defaultDynamicGroupByOptions :: DurationSpec -> DynamicGroupByOptions
defaultDynamicGroupByOptions duration =
    DynamicGroupByOptions
        { dynamicEvery = duration
        , dynamicPeriod = duration
        , dynamicOffset = DurationSpec "0i"
        , dynamicLabel = LabelLeft
        , dynamicIncludeBoundaries = False
        , dynamicClosedWindow = ClosedLeft
        , dynamicStartBy = StartByWindowBound
        }

-- | Default rolling window options using a trailing right-closed window.
defaultRollingGroupByOptions :: DurationSpec -> RollingGroupByOptions
defaultRollingGroupByOptions duration =
    RollingGroupByOptions
        { rollingPeriod = duration
        , rollingOffset = DurationSpec ("-" <> durationSpecText duration)
        , rollingClosedWindow = ClosedRight
        }

groupBy :: [Expr] -> LazyFrame -> GroupBy
groupBy keys input = PlainGroupBy input keys False

groupByStable :: [Expr] -> LazyFrame -> GroupBy
groupByStable keys input = PlainGroupBy input keys True

groupByDynamic :: DynamicGroupByOptions -> Expr -> [Expr] -> LazyFrame -> GroupBy
groupByDynamic options indexColumn keys input = DynamicGroupBy input indexColumn keys options

groupByRolling :: RollingGroupByOptions -> Expr -> [Expr] -> LazyFrame -> GroupBy
groupByRolling options indexColumn keys input = RollingGroupBy input indexColumn keys options

agg :: [Expr] -> GroupBy -> IO (Either PolarsError LazyFrame)
agg [] _ = pure (Left (PolarsError InvalidArgument "aggregation list must contain at least one expression"))
agg aggregations (PlainGroupBy input keys maintainOrder) =
    withLazyFrame input $ \lfPtr ->
        withCompiledExprs keys $ \keyArray keyLen ->
            withCompiledExprs aggregations $ \aggArray aggLen ->
                lazyFrameOut (phs_lazyframe_group_by_agg lfPtr keyArray keyLen aggArray aggLen (toCBool maintainOrder))
agg aggregations (DynamicGroupBy input indexColumn keys options) =
    withLazyFrame input $ \lfPtr ->
        withCompiledExpr indexColumn $ \indexPtr ->
            withCompiledExprs keys $ \keyArray keyLen ->
                withCompiledExprs aggregations $ \aggArray aggLen ->
                    withDurationCString (dynamicEvery options) $ \everyPtr ->
                        withDurationCString (dynamicPeriod options) $ \periodPtr ->
                            withDurationCString (dynamicOffset options) $ \offsetPtr ->
                                lazyFrameOut
                                    ( phs_lazyframe_group_by_dynamic_agg
                                        lfPtr
                                        indexPtr
                                        keyArray
                                        keyLen
                                        aggArray
                                        aggLen
                                        everyPtr
                                        periodPtr
                                        offsetPtr
                                        (dynamicLabelCode (dynamicLabel options))
                                        (toCBool (dynamicIncludeBoundaries options))
                                        (closedWindowCode (dynamicClosedWindow options))
                                        (startByCode (dynamicStartBy options))
                                    )
agg aggregations (RollingGroupBy input indexColumn keys options) =
    withLazyFrame input $ \lfPtr ->
        withCompiledExpr indexColumn $ \indexPtr ->
            withCompiledExprs keys $ \keyArray keyLen ->
                withCompiledExprs aggregations $ \aggArray aggLen ->
                    withDurationCString (rollingPeriod options) $ \periodPtr ->
                        withDurationCString (rollingOffset options) $ \offsetPtr ->
                            lazyFrameOut
                                ( phs_lazyframe_group_by_rolling_agg
                                    lfPtr
                                    indexPtr
                                    keyArray
                                    keyLen
                                    aggArray
                                    aggLen
                                    periodPtr
                                    offsetPtr
                                    (closedWindowCode (rollingClosedWindow options))
                                )

withCompiledExpr :: Expr -> (Ptr RawExpr -> IO (Either PolarsError a)) -> IO (Either PolarsError a)
withCompiledExpr expr action = do
    compiled <- compileExpr expr
    case compiled of
        Left err -> pure (Left err)
        Right managed -> withManagedExpr managed action

withDurationCString :: DurationSpec -> (CString -> IO a) -> IO a
withDurationCString = withTextCString . durationSpecText

closedWindowCode :: ClosedInterval -> CInt
closedWindowCode ClosedLeft = 0
closedWindowCode ClosedRight = 1
closedWindowCode ClosedBoth = 2
closedWindowCode ClosedNone = 3

dynamicLabelCode :: DynamicLabel -> CInt
dynamicLabelCode LabelLeft = 0
dynamicLabelCode LabelRight = 1
dynamicLabelCode LabelDataPoint = 2

startByCode :: StartBy -> CInt
startByCode StartByWindowBound = 0
startByCode StartByDataPoint = 1
startByCode StartByMonday = 2
startByCode StartByTuesday = 3
startByCode StartByWednesday = 4
startByCode StartByThursday = 5
startByCode StartByFriday = 6
startByCode StartBySaturday = 7
startByCode StartBySunday = 8

toCBool :: Bool -> CBool
toCBool False = CBool 0
toCBool True = CBool 1

lazyFrameOut :: (Ptr (Ptr RawLazyFrame) -> Ptr (Ptr RawError) -> IO CInt) -> IO (Either PolarsError LazyFrame)
lazyFrameOut action =
    alloca $ \outPtr ->
        alloca $ \errPtr -> do
            poke outPtr nullPtr
            poke errPtr nullPtr
            status <- action outPtr errPtr
            if fromIntegralStatus status == 0
                then do
                    ptr <- peek outPtr
                    if ptr == nullPtr
                        then pure (Left (nullPointerError "lazyframe output"))
                        else Right <$> mkLazyFrame ptr
                else Left <$> (consumeError (fromIntegralStatus status) =<< peek errPtr)

fromIntegralStatus :: (Integral a) => a -> CInt
fromIntegralStatus = fromIntegral
