{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DerivingStrategies #-}

{- |
Module      : Polars.LazyFrame
Description : Safe lazy query operations backed by Rust Polars LazyFrame handles.

Lazy operations clone Rust logical plans and return new managed LazyFrame values.
Expression inputs are compiled from pure Haskell AST nodes at each FFI boundary.
Schema helpers resolve lazy logical-plan metadata without materializing frames.
-}
module Polars.LazyFrame
    ( CsvReadOptions (..)
    , LazyFrame
    , LazyExecutionEngine (..)
    , LazyFrameExplodeOptions (..)
    , LazyFrameTopKOptions (..)
    , LazyFrameUnpivotOptions (..)
    , ParquetParallelStrategy (..)
    , ParquetScanOptions (..)
    , RenameOptions (..)
    , UniqueKeepStrategy (..)
    , UniqueOptions (..)
    , cache
    , collect
    , collectAll
    , collectAllWithEngine
    , collectStreaming
    , collectSchema
    , collectWithEngine
    , castAllColumns
    , castColumns
    , count
    , defaultCsvReadOptions
    , defaultLazyFrameExplodeOptions
    , defaultLazyFrameTopKOptions
    , defaultLazyFrameUnpivotOptions
    , defaultParquetScanOptions
    , defaultRenameOptions
    , defaultUniqueOptions
    , describeOptimizedPlan
    , describeOptimizedPlanTree
    , describePlan
    , describePlanTree
    , dropColumns
    , dropNans
    , dropNulls
    , explode
    , explain
    , explainAll
    , fillNans
    , fillNulls
    , filter
    , gatherEvery
    , lazyClear
    , lazyFirst
    , lazyHead
    , lazyLast
    , lazyShift
    , lazyShiftAndFill
    , lazyTail
    , limit
    , nullCount
    , profile
    , rename
    , removeRows
    , reverse
    , scanCsv
    , scanCsvWith
    , scanParquet
    , scanParquetWith
    , select
    , slice
    , sort
    , toDot
    , topK
    , bottomK
    , unique
    , unpivot
    , withCheckOrder
    , withClusterWithColumns
    , withColumns
    , withPredicatePushdown
    , withProjectionPushdown
    , withRowEstimate
    , withRowIndex
    , withSimplifyExpr
    , withSlicePushdown
    , withTypeCheck
    , withTypeCoercion
    , withoutOptimizations
    ) where

import Prelude hiding (filter, reverse)

import Control.Exception (bracket)
import Data.Text (Text)
import qualified Data.Text.Encoding as TE
import Data.Word (Word8, Word64)
import Foreign.C.String (CString)
import Foreign.C.Types (CBool (..), CInt, CSize, CUChar (..))
import Foreign.Marshal.Alloc (alloca)
import Foreign.Marshal.Array (withArray)
import Foreign.Ptr (Ptr, nullPtr)
import Foreign.Storable (peek, poke)
import qualified Prelude as P

import Polars.Error (PolarsError (..), PolarsErrorCode (InvalidArgument))
import Polars.Expr (Expr)
import Polars.Internal.Bytes (copyAndFreeBytes)
import Polars.Internal.CString (withFilePathCString, withMaybeTextCString, withTextCString)
import Polars.Internal.Expr (compileExpr, dtypeCode, withCompiledExprs)
import Polars.Internal.Managed (DataFrame, LazyFrame, mkDataFrame, mkLazyFrame, withLazyFrame, withManagedExpr)
import Polars.Internal.Raw
    ( RawBytes
    , RawDataFrame
    , RawDataFrameArray
    , RawError
    , RawExpr
    , RawLazyFrame
    , phs_dataframe_array_free
    , phs_dataframe_array_get
    , phs_dataframe_array_len
    , phs_lazyframe_collect
    , phs_lazyframe_collect_all_with_engine
    , phs_lazyframe_collect_schema
    , phs_lazyframe_collect_with_engine
    , phs_lazyframe_count
    , phs_lazyframe_cache
    , phs_lazyframe_cast
    , phs_lazyframe_cast_all
    , phs_lazyframe_clear
    , phs_lazyframe_describe_plan
    , phs_lazyframe_drop
    , phs_lazyframe_drop_nans
    , phs_lazyframe_drop_nulls
    , phs_lazyframe_explode
    , phs_lazyframe_explain
    , phs_lazyframe_explain_all
    , phs_lazyframe_fill_nan
    , phs_lazyframe_fill_null
    , phs_lazyframe_filter
    , phs_lazyframe_first
    , phs_lazyframe_gather_every
    , phs_lazyframe_head
    , phs_lazyframe_last
    , phs_lazyframe_limit
    , phs_lazyframe_bottom_k
    , phs_lazyframe_null_count
    , phs_lazyframe_profile
    , phs_lazyframe_rename
    , phs_lazyframe_remove
    , phs_lazyframe_reverse
    , phs_lazyframe_select
    , phs_lazyframe_shift
    , phs_lazyframe_shift_and_fill
    , phs_lazyframe_slice
    , phs_lazyframe_sort
    , phs_lazyframe_tail
    , phs_lazyframe_to_dot
    , phs_lazyframe_top_k
    , phs_lazyframe_unique
    , phs_lazyframe_unpivot
    , phs_lazyframe_with_optimization
    , phs_lazyframe_with_columns
    , phs_lazyframe_with_row_index
    , phs_lazyframe_without_optimizations
    , phs_scan_csv_options
    , phs_scan_parquet_options
    )
import Polars.IO
    ( CsvReadOptions (..)
    , ParquetParallelStrategy (..)
    , ParquetScanOptions (..)
    , defaultCsvReadOptions
    , defaultParquetScanOptions
    )
import Polars.Internal.Result (consumeError, nullPointerError)
import Polars.Schema (DataType, Field, parseSchemaBytes)

newtype RenameOptions = RenameOptions
    { renameStrict :: Bool
    }
    deriving stock (Eq, Show)

defaultRenameOptions :: RenameOptions
defaultRenameOptions = RenameOptions {renameStrict = True}

data UniqueKeepStrategy
    = KeepFirst
    | KeepLast
    | KeepNone
    | KeepAny
    deriving stock (Eq, Show)

data UniqueOptions = UniqueOptions
    { uniqueSubset :: !(Maybe [Text])
    , uniqueKeepStrategy :: !UniqueKeepStrategy
    , uniqueMaintainOrder :: !Bool
    }
    deriving stock (Eq, Show)

defaultUniqueOptions :: UniqueOptions
defaultUniqueOptions =
    UniqueOptions
        { uniqueSubset = Nothing
        , uniqueKeepStrategy = KeepAny
        , uniqueMaintainOrder = False
        }

data LazyFrameTopKOptions = LazyFrameTopKOptions
    { lazyFrameTopKBy :: ![Expr]
    , lazyFrameTopKReverse :: ![Bool]
    , lazyFrameTopKMaintainOrder :: !Bool
    }
    deriving stock (Eq, Show)

defaultLazyFrameTopKOptions :: LazyFrameTopKOptions
defaultLazyFrameTopKOptions =
    LazyFrameTopKOptions
        { lazyFrameTopKBy = []
        , lazyFrameTopKReverse = [False]
        , lazyFrameTopKMaintainOrder = False
        }

-- | Options for exploding lazy List columns into long format.
data LazyFrameExplodeOptions = LazyFrameExplodeOptions
    { lazyFrameExplodeColumns :: ![Text]
    , lazyFrameExplodeEmptyAsNull :: !Bool
    , lazyFrameExplodeKeepNulls :: !Bool
    }
    deriving stock (Eq, Show)

defaultLazyFrameExplodeOptions :: LazyFrameExplodeOptions
defaultLazyFrameExplodeOptions =
    LazyFrameExplodeOptions
        { lazyFrameExplodeColumns = []
        , lazyFrameExplodeEmptyAsNull = True
        , lazyFrameExplodeKeepNulls = True
        }

-- | Options for unpivoting a lazy frame from wide to long format.
data LazyFrameUnpivotOptions = LazyFrameUnpivotOptions
    { lazyFrameUnpivotOn :: !(Maybe [Text])
    , lazyFrameUnpivotIndex :: ![Text]
    , lazyFrameUnpivotVariableName :: !(Maybe Text)
    , lazyFrameUnpivotValueName :: !(Maybe Text)
    }
    deriving stock (Eq, Show)

defaultLazyFrameUnpivotOptions :: LazyFrameUnpivotOptions
defaultLazyFrameUnpivotOptions =
    LazyFrameUnpivotOptions
        { lazyFrameUnpivotOn = Nothing
        , lazyFrameUnpivotIndex = []
        , lazyFrameUnpivotVariableName = Nothing
        , lazyFrameUnpivotValueName = Nothing
        }

-- | Execution engine used to materialize a lazy query.
data LazyExecutionEngine
    = LazyAuto
    | LazyStreaming
    | LazyInMemory
    | LazyGpu
    deriving stock (Eq, Show)

scanCsv :: FilePath -> IO (Either PolarsError LazyFrame)
scanCsv = scanCsvWith defaultCsvReadOptions

scanCsvWith :: CsvReadOptions -> FilePath -> IO (Either PolarsError LazyFrame)
scanCsvWith options path =
    case csvReadWordOptions options of
        Left err -> pure (Left err)
        Right (hasNRows, nRows, skipRows, skipRowsAfterHeader, hasInferSchemaLength, inferSchemaLength) ->
            withFilePathCString path $ \cPath ->
                withMaybeTextCString (csvReadNullValue options) $ \cNullValue hasNullValue ->
                    lazyFrameOut
                        ( phs_scan_csv_options
                            cPath
                            (toCBool (csvReadHasHeader options))
                            (CUChar (csvReadSeparator options))
                            (toCBool hasNullValue)
                            cNullValue
                            (toCBool hasNRows)
                            nRows
                            skipRows
                            skipRowsAfterHeader
                            (toCBool hasInferSchemaLength)
                            inferSchemaLength
                            (toCBool (csvReadIgnoreErrors options))
                            (toCBool (csvReadTruncateRaggedLines options))
                            (toCBool (csvReadMissingIsNull options))
                            (toCBool (csvReadLowMemory options))
                            (toCBool (csvReadRechunk options))
                        )

scanParquet :: FilePath -> IO (Either PolarsError LazyFrame)
scanParquet = scanParquetWith defaultParquetScanOptions

scanParquetWith :: ParquetScanOptions -> FilePath -> IO (Either PolarsError LazyFrame)
scanParquetWith options path =
    case optionalNonNegativeWord64 "parquetScanNRows" (parquetScanNRows options) of
        Left err -> pure (Left err)
        Right (hasNRows, nRows) ->
            withFilePathCString path $ \cPath ->
                lazyFrameOut
                    ( phs_scan_parquet_options
                        cPath
                        (toCBool hasNRows)
                        nRows
                        (parquetParallelCode (parquetScanParallel options))
                        (toCBool (parquetScanUseStatistics options))
                        (toCBool (parquetScanLowMemory options))
                        (toCBool (parquetScanRechunk options))
                        (toCBool (parquetScanCache options))
                    )

collect :: LazyFrame -> IO (Either PolarsError DataFrame)
collect lf = withLazyFrame lf $ \ptr -> dataframeOut (phs_lazyframe_collect ptr)

-- | Collect a lazy query with a specific Polars execution engine.
collectWithEngine :: LazyExecutionEngine -> LazyFrame -> IO (Either PolarsError DataFrame)
collectWithEngine engine lf = withLazyFrame lf $ \ptr ->
    dataframeOut (phs_lazyframe_collect_with_engine ptr (lazyExecutionEngineCode engine))

-- | Collect multiple lazy queries with the in-memory Polars execution engine.
collectAll :: [LazyFrame] -> IO (Either PolarsError [DataFrame])
collectAll = collectAllWithEngine LazyInMemory

-- | Collect multiple lazy queries together with a specific Polars execution engine.
collectAllWithEngine :: LazyExecutionEngine -> [LazyFrame] -> IO (Either PolarsError [DataFrame])
collectAllWithEngine engine lfs = withLazyFrameList lfs $ \ptr len ->
    dataframeArrayOut (phs_lazyframe_collect_all_with_engine ptr len (lazyExecutionEngineCode engine))

-- | Collect a lazy query with the Polars streaming engine.
collectStreaming :: LazyFrame -> IO (Either PolarsError DataFrame)
collectStreaming = collectWithEngine LazyStreaming

-- | Cast named lazy frame columns to datatypes.
castColumns :: [(Text, DataType)] -> Bool -> LazyFrame -> IO (Either PolarsError LazyFrame)
castColumns columns strict lf =
    case traverse (dtypeCode . snd) columns of
        Left err -> pure (Left err)
        Right dtypeCodes ->
            withLazyFrame lf $ \ptr ->
                withCastPairs columns dtypeCodes $ \names dtypes len ->
                    lazyFrameOut (phs_lazyframe_cast ptr names dtypes len (toCBool strict))

-- | Cast every lazy frame column to a datatype.
castAllColumns :: DataType -> Bool -> LazyFrame -> IO (Either PolarsError LazyFrame)
castAllColumns dtype strict lf =
    case dtypeCode dtype of
        Left err -> pure (Left err)
        Right dtypeC ->
            withLazyFrame lf $ \ptr ->
                lazyFrameOut (phs_lazyframe_cast_all ptr dtypeC (toCBool strict))

-- | Resolve the schema of the current lazy logical plan.
collectSchema :: LazyFrame -> IO (Either PolarsError [Field])
collectSchema lf = withLazyFrame lf $ \ptr -> schemaOut (phs_lazyframe_collect_schema ptr)

explain :: Bool -> LazyFrame -> IO (Either PolarsError Text)
explain optimized lf = withLazyFrame lf $ \ptr ->
    bytesOut (phs_lazyframe_explain ptr (toCBool optimized))

-- | Explain the optimized plan for multiple lazy queries collected together.
explainAll :: [LazyFrame] -> IO (Either PolarsError Text)
explainAll lfs = withLazyFrameList lfs $ \ptr len ->
    bytesOut (phs_lazyframe_explain_all ptr len)

-- | Describe the naive logical plan as flat text.
describePlan :: LazyFrame -> IO (Either PolarsError Text)
describePlan = describePlanWith False False

-- | Describe the naive logical plan as a tree.
describePlanTree :: LazyFrame -> IO (Either PolarsError Text)
describePlanTree = describePlanWith False True

-- | Describe the optimized logical plan as flat text.
describeOptimizedPlan :: LazyFrame -> IO (Either PolarsError Text)
describeOptimizedPlan = describePlanWith True False

-- | Describe the optimized logical plan as a tree.
describeOptimizedPlanTree :: LazyFrame -> IO (Either PolarsError Text)
describeOptimizedPlanTree = describePlanWith True True

-- | Render the lazy logical plan as DOT graph text.
toDot :: Bool -> LazyFrame -> IO (Either PolarsError Text)
toDot optimized lf = withLazyFrame lf $ \ptr ->
    bytesOut (phs_lazyframe_to_dot ptr (toCBool optimized))

describePlanWith :: Bool -> Bool -> LazyFrame -> IO (Either PolarsError Text)
describePlanWith optimized tree lf = withLazyFrame lf $ \ptr ->
    bytesOut (phs_lazyframe_describe_plan ptr (toCBool optimized) (toCBool tree))

profile :: LazyFrame -> IO (Either PolarsError (DataFrame, DataFrame))
profile lf = withLazyFrame lf $ \ptr -> profileOut (phs_lazyframe_profile ptr)

-- | Disable lazy optimizer flags for subsequent plan execution.
withoutOptimizations :: LazyFrame -> IO (Either PolarsError LazyFrame)
withoutOptimizations lf = withLazyFrame lf $ \ptr ->
    lazyFrameOut (phs_lazyframe_without_optimizations ptr)

-- | Toggle projection pushdown for a lazy query.
withProjectionPushdown :: Bool -> LazyFrame -> IO (Either PolarsError LazyFrame)
withProjectionPushdown = optimizerToggle 0

-- | Toggle predicate pushdown for a lazy query.
withPredicatePushdown :: Bool -> LazyFrame -> IO (Either PolarsError LazyFrame)
withPredicatePushdown = optimizerToggle 1

-- | Toggle type coercion during lazy IR conversion.
withTypeCoercion :: Bool -> LazyFrame -> IO (Either PolarsError LazyFrame)
withTypeCoercion = optimizerToggle 2

-- | Toggle type checking during lazy IR conversion.
withTypeCheck :: Bool -> LazyFrame -> IO (Either PolarsError LazyFrame)
withTypeCheck = optimizerToggle 3

-- | Toggle expression simplification for a lazy query.
withSimplifyExpr :: Bool -> LazyFrame -> IO (Either PolarsError LazyFrame)
withSimplifyExpr = optimizerToggle 4

-- | Toggle slice and limit pushdown for a lazy query.
withSlicePushdown :: Bool -> LazyFrame -> IO (Either PolarsError LazyFrame)
withSlicePushdown = optimizerToggle 5

-- | Toggle clustering of independent consecutive with-columns nodes.
withClusterWithColumns :: Bool -> LazyFrame -> IO (Either PolarsError LazyFrame)
withClusterWithColumns = optimizerToggle 6

-- | Toggle order-dependency checks in lazy optimization.
withCheckOrder :: Bool -> LazyFrame -> IO (Either PolarsError LazyFrame)
withCheckOrder = optimizerToggle 7

-- | Toggle row-count estimation used by lazy joins.
withRowEstimate :: Bool -> LazyFrame -> IO (Either PolarsError LazyFrame)
withRowEstimate = optimizerToggle 8

optimizerToggle :: CInt -> Bool -> LazyFrame -> IO (Either PolarsError LazyFrame)
optimizerToggle code enabled lf = withLazyFrame lf $ \ptr ->
    lazyFrameOut (phs_lazyframe_with_optimization ptr code (toCBool enabled))

filter :: Expr -> LazyFrame -> IO (Either PolarsError LazyFrame)
filter predicate lf = lazyFrameExprOut predicate lf phs_lazyframe_filter

-- | Remove rows where the predicate evaluates to true.
removeRows :: Expr -> LazyFrame -> IO (Either PolarsError LazyFrame)
removeRows predicate lf = lazyFrameExprOut predicate lf phs_lazyframe_remove

select :: [Expr] -> LazyFrame -> IO (Either PolarsError LazyFrame)
select exprs lf = withLazyFrame lf $ \lfPtr ->
    withCompiledExprs exprs $ \exprArray len -> lazyFrameOut (phs_lazyframe_select lfPtr exprArray len)

withColumns :: [Expr] -> LazyFrame -> IO (Either PolarsError LazyFrame)
withColumns exprs lf = withLazyFrame lf $ \lfPtr ->
    withCompiledExprs exprs $ \exprArray len -> lazyFrameOut (phs_lazyframe_with_columns lfPtr exprArray len)

withRowIndex :: Text -> Maybe Int -> LazyFrame -> IO (Either PolarsError LazyFrame)
withRowIndex name offset lf = case optionalNonNegativeWord64 "withRowIndex offset" offset of
    Left err -> pure (Left err)
    Right (hasOffset, offsetValue) ->
        withLazyFrame lf $ \lfPtr ->
            withTextCString name $ \namePtr ->
                lazyFrameOut (phs_lazyframe_with_row_index lfPtr namePtr (toCBool hasOffset) offsetValue)

-- | Gather every nth lazy row, starting at the offset.
gatherEvery :: Int -> Int -> LazyFrame -> IO (Either PolarsError LazyFrame)
gatherEvery step offset lf
    | step == 0 = pure (Left (invalidArgument "gatherEvery step must be positive"))
    | otherwise =
        case (nonNegativeWord64 "gatherEvery step" step, nonNegativeWord64 "gatherEvery offset" offset) of
            (Left err, _) -> pure (Left err)
            (_, Left err) -> pure (Left err)
            (Right stepValue, Right offsetValue) ->
                withLazyFrame lf $ \lfPtr ->
                    lazyFrameOut (phs_lazyframe_gather_every lfPtr stepValue offsetValue)

-- | Unpivot a lazy frame from wide to long format.
unpivot :: LazyFrameUnpivotOptions -> LazyFrame -> IO (Either PolarsError LazyFrame)
unpivot options lf =
    withLazyFrame lf $ \lfPtr ->
        withMaybeCStringList (lazyFrameUnpivotOn options) $ \onArray onLen hasOn ->
            withCStringList (lazyFrameUnpivotIndex options) $ \indexArray indexLen ->
                withMaybeTextCString (lazyFrameUnpivotVariableName options) $ \variablePtr _ ->
                    withMaybeTextCString (lazyFrameUnpivotValueName options) $ \valuePtr _ ->
                        lazyFrameOut
                            ( phs_lazyframe_unpivot
                                lfPtr
                                (toCBool hasOn)
                                onArray
                                onLen
                                indexArray
                                indexLen
                                variablePtr
                                valuePtr
                            )

-- | Return an empty lazy frame with the same schema.
lazyClear :: LazyFrame -> IO (Either PolarsError LazyFrame)
lazyClear lf = withLazyFrame lf $ \lfPtr -> lazyFrameOut (phs_lazyframe_clear lfPtr)

-- | Cache this lazy plan node for repeated use during query execution.
cache :: LazyFrame -> IO (Either PolarsError LazyFrame)
cache lf = withLazyFrame lf $ \lfPtr -> lazyFrameOut (phs_lazyframe_cache lfPtr)

-- | Return the first row of a lazy frame.
lazyFirst :: LazyFrame -> IO (Either PolarsError LazyFrame)
lazyFirst lf = withLazyFrame lf $ \lfPtr -> lazyFrameOut (phs_lazyframe_first lfPtr)

-- | Return the last row of a lazy frame.
lazyLast :: LazyFrame -> IO (Either PolarsError LazyFrame)
lazyLast lf = withLazyFrame lf $ \lfPtr -> lazyFrameOut (phs_lazyframe_last lfPtr)

dropColumns :: [Text] -> LazyFrame -> IO (Either PolarsError LazyFrame)
dropColumns [] _ = pure (Left (invalidArgument "dropColumns requires at least one column name"))
dropColumns names lf = withLazyFrame lf $ \lfPtr -> withCStringList names $ \nameArray len ->
    lazyFrameOut (phs_lazyframe_drop lfPtr nameArray len)

rename :: RenameOptions -> [(Text, Text)] -> LazyFrame -> IO (Either PolarsError LazyFrame)
rename _ [] _ = pure (Left (invalidArgument "rename requires at least one column pair"))
rename options pairs lf = withLazyFrame lf $ \lfPtr -> withRenamePairs pairs $ \existingArray newArray len ->
    lazyFrameOut (phs_lazyframe_rename lfPtr existingArray newArray len (toCBool (renameStrict options)))

sort :: [Text] -> LazyFrame -> IO (Either PolarsError LazyFrame)
sort names lf = withLazyFrame lf $ \lfPtr -> withCStringList names $ \nameArray len ->
    lazyFrameOut (phs_lazyframe_sort lfPtr nameArray len)

limit :: Word -> LazyFrame -> IO (Either PolarsError LazyFrame)
limit n lf = withLazyFrame lf $ \lfPtr -> lazyFrameOut (phs_lazyframe_limit lfPtr (fromIntegral n))

topK :: LazyFrameTopKOptions -> Int -> LazyFrame -> IO (Either PolarsError LazyFrame)
topK = topBottomK "topK" phs_lazyframe_top_k

bottomK :: LazyFrameTopKOptions -> Int -> LazyFrame -> IO (Either PolarsError LazyFrame)
bottomK = topBottomK "bottomK" phs_lazyframe_bottom_k

explode :: LazyFrameExplodeOptions -> LazyFrame -> IO (Either PolarsError LazyFrame)
explode options lf
    | null (lazyFrameExplodeColumns options) =
        pure (Left (invalidArgument "explode requires at least one column name"))
    | otherwise =
        withLazyFrame lf $ \lfPtr ->
            withCStringList (lazyFrameExplodeColumns options) $ \nameArray nameLen ->
                lazyFrameOut
                    ( phs_lazyframe_explode
                        lfPtr
                        nameArray
                        nameLen
                        (toCBool (lazyFrameExplodeEmptyAsNull options))
                        (toCBool (lazyFrameExplodeKeepNulls options))
                    )

reverse :: LazyFrame -> IO (Either PolarsError LazyFrame)
reverse lf = withLazyFrame lf $ \lfPtr -> lazyFrameOut (phs_lazyframe_reverse lfPtr)

slice :: Int -> Int -> LazyFrame -> IO (Either PolarsError LazyFrame)
slice offset len lf = case nonNegativeWord64 "slice length" len of
    Left err -> pure (Left err)
    Right lenWord -> withLazyFrame lf $ \lfPtr ->
        lazyFrameOut (phs_lazyframe_slice lfPtr (fromIntegral offset) lenWord)

lazyHead :: Int -> LazyFrame -> IO (Either PolarsError LazyFrame)
lazyHead n lf = case nonNegativeWord64 "lazyHead count" n of
    Left err -> pure (Left err)
    Right nWord -> withLazyFrame lf $ \lfPtr -> lazyFrameOut (phs_lazyframe_head lfPtr nWord)

lazyTail :: Int -> LazyFrame -> IO (Either PolarsError LazyFrame)
lazyTail n lf = case nonNegativeWord64 "lazyTail count" n of
    Left err -> pure (Left err)
    Right nWord -> withLazyFrame lf $ \lfPtr -> lazyFrameOut (phs_lazyframe_tail lfPtr nWord)

-- | Shift all LazyFrame columns by an expression period.
lazyShift :: Expr -> LazyFrame -> IO (Either PolarsError LazyFrame)
lazyShift n lf = lazyFrameExprOut n lf phs_lazyframe_shift

-- | Shift all LazyFrame columns and fill the holes created by the shift.
lazyShiftAndFill :: Expr -> Expr -> LazyFrame -> IO (Either PolarsError LazyFrame)
lazyShiftAndFill n fillValue lf = do
    compiledN <- compileExpr n
    compiledFill <- compileExpr fillValue
    case (compiledN, compiledFill) of
        (Right nManaged, Right fillManaged) ->
            withLazyFrame lf $ \lfPtr ->
                withManagedExpr nManaged $ \nPtr ->
                    withManagedExpr fillManaged $ \fillPtr ->
                        lazyFrameOut (phs_lazyframe_shift_and_fill lfPtr nPtr fillPtr)
        (Left err, _) -> pure (Left err)
        (_, Left err) -> pure (Left err)

dropNulls :: Maybe [Text] -> LazyFrame -> IO (Either PolarsError LazyFrame)
dropNulls (Just []) _ = pure (Left (invalidArgument "dropNulls subset requires at least one column name"))
dropNulls subset lf = withLazyFrame lf $ \lfPtr -> withMaybeCStringList subset $ \nameArray len hasSubset ->
    lazyFrameOut (phs_lazyframe_drop_nulls lfPtr nameArray len (toCBool hasSubset))

-- | Drop rows containing NaN values, optionally restricted to named columns.
dropNans :: Maybe [Text] -> LazyFrame -> IO (Either PolarsError LazyFrame)
dropNans (Just []) _ = pure (Left (invalidArgument "dropNans subset requires at least one column name"))
dropNans subset lf = withLazyFrame lf $ \lfPtr -> withMaybeCStringList subset $ \nameArray len hasSubset ->
    lazyFrameOut (phs_lazyframe_drop_nans lfPtr nameArray len (toCBool hasSubset))

fillNulls :: Expr -> LazyFrame -> IO (Either PolarsError LazyFrame)
fillNulls value lf = lazyFrameExprOut value lf phs_lazyframe_fill_null

fillNans :: Expr -> LazyFrame -> IO (Either PolarsError LazyFrame)
fillNans value lf = lazyFrameExprOut value lf phs_lazyframe_fill_nan

-- | Count non-null values for each column.
count :: LazyFrame -> IO (Either PolarsError LazyFrame)
count lf = withLazyFrame lf $ \lfPtr -> lazyFrameOut (phs_lazyframe_count lfPtr)

nullCount :: LazyFrame -> IO (Either PolarsError LazyFrame)
nullCount lf = withLazyFrame lf $ \lfPtr -> lazyFrameOut (phs_lazyframe_null_count lfPtr)

unique :: UniqueOptions -> LazyFrame -> IO (Either PolarsError LazyFrame)
unique options lf = case uniqueSubset options of
    Just [] -> pure (Left (invalidArgument "unique subset requires at least one column name"))
    subset -> withLazyFrame lf $ \lfPtr -> withMaybeCStringList subset $ \nameArray len hasSubset ->
        lazyFrameOut
            ( phs_lazyframe_unique
                lfPtr
                nameArray
                len
                (toCBool hasSubset)
                (keepStrategyCode (uniqueKeepStrategy options))
                (toCBool (uniqueMaintainOrder options))
            )

lazyFrameExprOut ::
    Expr ->
    LazyFrame ->
    (Ptr RawLazyFrame -> Ptr RawExpr -> Ptr (Ptr RawLazyFrame) -> Ptr (Ptr RawError) -> IO CInt) ->
    IO (Either PolarsError LazyFrame)
lazyFrameExprOut expr lf action = do
    compiled <- compileExpr expr
    case compiled of
        Left err -> pure (Left err)
        Right managed -> withLazyFrame lf $ \lfPtr ->
            withManagedExpr managed $ \exprPtr -> lazyFrameOut (action lfPtr exprPtr)

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

bytesOut :: (Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt) -> IO (Either PolarsError Text)
bytesOut action =
    alloca $ \outPtr ->
        alloca $ \errPtr -> do
            poke outPtr nullPtr
            poke errPtr nullPtr
            status <- action outPtr errPtr
            if fromIntegralStatus status == 0
                then do
                    ptr <- peek outPtr
                    if ptr == nullPtr
                        then pure (Left (nullPointerError "lazyframe bytes output"))
                        else Right . TE.decodeUtf8 <$> copyAndFreeBytes ptr
                else Left <$> (consumeError (fromIntegralStatus status) =<< peek errPtr)

schemaOut :: (Ptr (Ptr RawBytes) -> Ptr (Ptr RawError) -> IO CInt) -> IO (Either PolarsError [Field])
schemaOut action =
    alloca $ \outPtr ->
        alloca $ \errPtr -> do
            poke outPtr nullPtr
            poke errPtr nullPtr
            status <- action outPtr errPtr
            if fromIntegralStatus status == 0
                then do
                    ptr <- peek outPtr
                    if ptr == nullPtr
                        then pure (Left (nullPointerError "lazyframe schema output"))
                        else parseSchemaBytes <$> copyAndFreeBytes ptr
                else Left <$> (consumeError (fromIntegralStatus status) =<< peek errPtr)

dataframeOut :: (Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt) -> IO (Either PolarsError DataFrame)
dataframeOut action =
    alloca $ \outPtr ->
        alloca $ \errPtr -> do
            poke outPtr nullPtr
            poke errPtr nullPtr
            status <- action outPtr errPtr
            if fromIntegralStatus status == 0
                then do
                    ptr <- peek outPtr
                    if ptr == nullPtr
                        then pure (Left (nullPointerError "dataframe output"))
                        else Right <$> mkDataFrame ptr
                else Left <$> (consumeError (fromIntegralStatus status) =<< peek errPtr)

dataframeArrayOut :: (Ptr (Ptr RawDataFrameArray) -> Ptr (Ptr RawError) -> IO CInt) -> IO (Either PolarsError [DataFrame])
dataframeArrayOut action =
    alloca $ \outPtr ->
        alloca $ \errPtr -> do
            poke outPtr nullPtr
            poke errPtr nullPtr
            status <- action outPtr errPtr
            if fromIntegralStatus status == 0
                then do
                    array <- peek outPtr
                    if array == nullPtr
                        then pure (Left (nullPointerError "dataframe array output"))
                        else bracket (pure array) phs_dataframe_array_free dataframeArrayToList
                else Left <$> (consumeError (fromIntegralStatus status) =<< peek errPtr)

dataframeArrayToList :: Ptr RawDataFrameArray -> IO (Either PolarsError [DataFrame])
dataframeArrayToList array = do
    len <- phs_dataframe_array_len array
    let go index acc
            | index >= len = pure (Right (P.reverse acc))
            | otherwise =
                alloca $ \outPtr ->
                    alloca $ \errPtr -> do
                        poke outPtr nullPtr
                        poke errPtr nullPtr
                        status <- phs_dataframe_array_get array index outPtr errPtr
                        if fromIntegralStatus status == 0
                            then do
                                ptr <- peek outPtr
                                if ptr == nullPtr
                                    then pure (Left (nullPointerError "dataframe array item output"))
                                    else do
                                        df <- mkDataFrame ptr
                                        go (index + 1) (df : acc)
                            else Left <$> (consumeError (fromIntegralStatus status) =<< peek errPtr)
    go 0 []

profileOut :: (Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawDataFrame) -> Ptr (Ptr RawError) -> IO CInt) -> IO (Either PolarsError (DataFrame, DataFrame))
profileOut action =
    alloca $ \resultPtr ->
        alloca $ \profilePtr ->
            alloca $ \errPtr -> do
                poke resultPtr nullPtr
                poke profilePtr nullPtr
                poke errPtr nullPtr
                status <- action resultPtr profilePtr errPtr
                if fromIntegralStatus status == 0
                    then do
                        resultRaw <- peek resultPtr
                        profileRaw <- peek profilePtr
                        if resultRaw == nullPtr
                            then pure (Left (nullPointerError "profile result output"))
                            else
                                if profileRaw == nullPtr
                                    then pure (Left (nullPointerError "profile timing output"))
                                    else do
                                        result <- mkDataFrame resultRaw
                                        profileFrame <- mkDataFrame profileRaw
                                        pure (Right (result, profileFrame))
                    else Left <$> (consumeError (fromIntegralStatus status) =<< peek errPtr)

withCStringList :: [Text] -> (Ptr CString -> CSize -> IO a) -> IO a
withCStringList values action = go values []
  where
    go [] acc = withArray (P.reverse acc) $ \ptr -> action ptr (fromIntegral (length acc))
    go (value : rest) acc = withTextCString value $ \ptr -> go rest (ptr : acc)

withWord8List :: [Word8] -> (Ptr Word8 -> CSize -> IO a) -> IO a
withWord8List values action = withArray values $ \ptr -> action ptr (fromIntegral (length values))

withMaybeCStringList :: Maybe [Text] -> (Ptr CString -> CSize -> Bool -> IO a) -> IO a
withMaybeCStringList Nothing action = action nullPtr 0 False
withMaybeCStringList (Just values) action = withCStringList values $ \ptr len -> action ptr len True

withLazyFrameList :: [LazyFrame] -> (Ptr (Ptr RawLazyFrame) -> CSize -> IO a) -> IO a
withLazyFrameList lfs action = go lfs []
  where
    go [] acc = withArray (P.reverse acc) $ \ptr -> action ptr (fromIntegral (length acc))
    go (lf : rest) acc = withLazyFrame lf $ \ptr -> go rest (ptr : acc)

withCastPairs :: [(Text, DataType)] -> [CInt] -> (Ptr CString -> Ptr CInt -> CSize -> IO a) -> IO a
withCastPairs columns dtypeCodes action =
    withCStringList (map fst columns) $ \names len ->
        withArray dtypeCodes $ \dtypes -> action names dtypes len

withRenamePairs :: [(Text, Text)] -> (Ptr CString -> Ptr CString -> CSize -> IO a) -> IO a
withRenamePairs values action = go values [] []
  where
    go [] existing new =
        withArray (P.reverse existing) $ \existingPtr ->
            withArray (P.reverse new) $ \newPtr ->
                action existingPtr newPtr (fromIntegral (length existing))
    go ((existingName, newName) : rest) existing new =
        withTextCString existingName $ \existingPtr ->
            withTextCString newName $ \newPtr ->
                go rest (existingPtr : existing) (newPtr : new)

nonNegativeWord64 :: Text -> Int -> Either PolarsError Word64
nonNegativeWord64 label value
    | value < 0 = Left (invalidArgument (label <> " must be non-negative"))
    | otherwise = Right (fromIntegral value)

topBottomK ::
    Text ->
    (Ptr RawLazyFrame -> Word64 -> Ptr (Ptr RawExpr) -> CSize -> Ptr Word8 -> CSize -> CBool -> Ptr (Ptr RawLazyFrame) -> Ptr (Ptr RawError) -> IO CInt) ->
    LazyFrameTopKOptions ->
    Int ->
    LazyFrame ->
    IO (Either PolarsError LazyFrame)
topBottomK label raw options rowCount lf =
    case validateLazyFrameTopKOptions label options rowCount of
        Left err -> pure (Left err)
        Right (countWord, reverseBytes) ->
            withLazyFrame lf $ \lfPtr ->
                withCompiledExprs (lazyFrameTopKBy options) $ \exprArray exprLen ->
                    withWord8List reverseBytes $ \reversePtr reverseLen ->
                        lazyFrameOut
                            ( raw
                                lfPtr
                                countWord
                                exprArray
                                exprLen
                                reversePtr
                                reverseLen
                                (toCBool (lazyFrameTopKMaintainOrder options))
                            )

validateLazyFrameTopKOptions :: Text -> LazyFrameTopKOptions -> Int -> Either PolarsError (Word64, [Word8])
validateLazyFrameTopKOptions label options rowCount = do
    countWord <- nonNegativeWord64 (label <> " count") rowCount
    let byCount = length (lazyFrameTopKBy options)
        reverseValues = lazyFrameTopKReverse options
        reverseCount = length reverseValues
    if reverseCount == 1 || reverseCount == byCount
        then Right ()
        else Left (invalidArgument (label <> " reverse must contain one value or one value per sort expression"))
    Right (countWord, map boolToWord8 reverseValues)

boolToWord8 :: Bool -> Word8
boolToWord8 False = 0
boolToWord8 True = 1

lazyExecutionEngineCode :: LazyExecutionEngine -> CInt
lazyExecutionEngineCode LazyAuto = 0
lazyExecutionEngineCode LazyStreaming = 1
lazyExecutionEngineCode LazyInMemory = 2
lazyExecutionEngineCode LazyGpu = 3

optionalNonNegativeWord64 :: Text -> Maybe Int -> Either PolarsError (Bool, Word64)
optionalNonNegativeWord64 _ Nothing = Right (False, 0)
optionalNonNegativeWord64 label (Just value) = do
    word <- nonNegativeWord64 label value
    Right (True, word)

csvReadWordOptions :: CsvReadOptions -> Either PolarsError (Bool, Word64, Word64, Word64, Bool, Word64)
csvReadWordOptions options = do
    (hasNRows, nRows) <- optionalNonNegativeWord64 "csvReadNRows" (csvReadNRows options)
    skipRows <- nonNegativeWord64 "csvReadSkipRows" (csvReadSkipRows options)
    skipRowsAfterHeader <- nonNegativeWord64 "csvReadSkipRowsAfterHeader" (csvReadSkipRowsAfterHeader options)
    (hasInferSchemaLength, inferSchemaLength) <- optionalNonNegativeWord64 "csvReadInferSchemaLength" (csvReadInferSchemaLength options)
    Right (hasNRows, nRows, skipRows, skipRowsAfterHeader, hasInferSchemaLength, inferSchemaLength)

parquetParallelCode :: ParquetParallelStrategy -> CInt
parquetParallelCode ParquetParallelAuto = 0
parquetParallelCode ParquetParallelNone = 1
parquetParallelCode ParquetParallelColumns = 2
parquetParallelCode ParquetParallelRowGroups = 3
parquetParallelCode ParquetParallelPrefiltered = 4

keepStrategyCode :: UniqueKeepStrategy -> CInt
keepStrategyCode KeepFirst = 0
keepStrategyCode KeepLast = 1
keepStrategyCode KeepNone = 2
keepStrategyCode KeepAny = 3

toCBool :: Bool -> CBool
toCBool False = CBool 0
toCBool True = CBool 1

invalidArgument :: Text -> PolarsError
invalidArgument = PolarsError InvalidArgument

fromIntegralStatus :: (Integral a) => a -> CInt
fromIntegralStatus = fromIntegral
