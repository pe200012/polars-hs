{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DerivingStrategies #-}

{- |
Module      : Polars.LazyFrame
Description : Safe lazy query operations backed by Rust Polars LazyFrame handles.

Lazy operations clone Rust logical plans and return new managed LazyFrame values.
Expression inputs are compiled from pure Haskell AST nodes at each FFI boundary.
-}
module Polars.LazyFrame
    ( CsvReadOptions (..)
    , LazyFrame
    , LazyFrameExplodeOptions (..)
    , LazyFrameTopKOptions (..)
    , ParquetParallelStrategy (..)
    , ParquetScanOptions (..)
    , RenameOptions (..)
    , UniqueKeepStrategy (..)
    , UniqueOptions (..)
    , collect
    , defaultCsvReadOptions
    , defaultLazyFrameExplodeOptions
    , defaultLazyFrameTopKOptions
    , defaultParquetScanOptions
    , defaultRenameOptions
    , defaultUniqueOptions
    , dropColumns
    , dropNulls
    , explode
    , explain
    , fillNans
    , fillNulls
    , filter
    , lazyHead
    , lazyTail
    , limit
    , nullCount
    , profile
    , rename
    , reverse
    , scanCsv
    , scanCsvWith
    , scanParquet
    , scanParquetWith
    , select
    , slice
    , sort
    , topK
    , bottomK
    , unique
    , withColumns
    , withRowIndex
    ) where

import Prelude hiding (filter, reverse)

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
import Polars.Internal.Expr (compileExpr, withCompiledExprs)
import Polars.Internal.Managed (DataFrame, LazyFrame, mkDataFrame, mkLazyFrame, withLazyFrame, withManagedExpr)
import Polars.Internal.Raw
    ( RawBytes
    , RawDataFrame
    , RawError
    , RawExpr
    , RawLazyFrame
    , phs_lazyframe_collect
    , phs_lazyframe_drop
    , phs_lazyframe_drop_nulls
    , phs_lazyframe_explode
    , phs_lazyframe_explain
    , phs_lazyframe_fill_nan
    , phs_lazyframe_fill_null
    , phs_lazyframe_filter
    , phs_lazyframe_head
    , phs_lazyframe_limit
    , phs_lazyframe_bottom_k
    , phs_lazyframe_null_count
    , phs_lazyframe_profile
    , phs_lazyframe_rename
    , phs_lazyframe_reverse
    , phs_lazyframe_select
    , phs_lazyframe_slice
    , phs_lazyframe_sort
    , phs_lazyframe_tail
    , phs_lazyframe_top_k
    , phs_lazyframe_unique
    , phs_lazyframe_with_columns
    , phs_lazyframe_with_row_index
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

explain :: Bool -> LazyFrame -> IO (Either PolarsError Text)
explain optimized lf = withLazyFrame lf $ \ptr ->
    bytesOut (phs_lazyframe_explain ptr (toCBool optimized))

profile :: LazyFrame -> IO (Either PolarsError (DataFrame, DataFrame))
profile lf = withLazyFrame lf $ \ptr -> profileOut (phs_lazyframe_profile ptr)

filter :: Expr -> LazyFrame -> IO (Either PolarsError LazyFrame)
filter predicate lf = do
    compiled <- compileExpr predicate
    case compiled of
        Left err -> pure (Left err)
        Right managed -> withLazyFrame lf $ \lfPtr ->
            withManagedExpr managed $ \exprPtr -> lazyFrameOut (phs_lazyframe_filter lfPtr exprPtr)

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

dropNulls :: Maybe [Text] -> LazyFrame -> IO (Either PolarsError LazyFrame)
dropNulls (Just []) _ = pure (Left (invalidArgument "dropNulls subset requires at least one column name"))
dropNulls subset lf = withLazyFrame lf $ \lfPtr -> withMaybeCStringList subset $ \nameArray len hasSubset ->
    lazyFrameOut (phs_lazyframe_drop_nulls lfPtr nameArray len (toCBool hasSubset))

fillNulls :: Expr -> LazyFrame -> IO (Either PolarsError LazyFrame)
fillNulls value lf = lazyFrameExprOut value lf phs_lazyframe_fill_null

fillNans :: Expr -> LazyFrame -> IO (Either PolarsError LazyFrame)
fillNans value lf = lazyFrameExprOut value lf phs_lazyframe_fill_nan

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
topBottomK label raw options count lf =
    case validateLazyFrameTopKOptions label options count of
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
validateLazyFrameTopKOptions label options count = do
    countWord <- nonNegativeWord64 (label <> " count") count
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
