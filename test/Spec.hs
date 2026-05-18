{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Main (main) where

import Prelude hiding (filter, head)

import Control.Exception (bracket)
import Control.Monad (when)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BSC
import Data.Foldable (forM_)
import Data.Int (Int16, Int32, Int64, Int8)
import Data.Maybe (isJust)
import qualified Data.Text as T
import qualified Data.Vector as V
import Data.Word (Word16, Word32, Word64, Word8)
import System.Exit (ExitCode (..))
import Foreign.Ptr (nullPtr)
import System.Directory (doesFileExist, removeFile)
import System.Environment (lookupEnv)
import System.FilePath ((</>))
import System.IO (hClose, hSetBinaryMode, openTempFile)
import System.Mem (performGC)
import System.Process (StdStream (CreatePipe), createProcess, proc, std_err, std_out, waitForProcess)
import Test.Hspec

import ArrowRecordBatch (withAgeArray, withPeopleRecordBatch)
import qualified Polars as Pl

-- | Compare two vectors of Maybe Double with approximate tolerance for
-- finite values, exact comparison for Nothing, and sign-sensitivity for
-- infinities. NaN is matched via isNaN.
shouldApproximate :: Double -> V.Vector (Maybe Double) -> V.Vector (Maybe Double) -> IO ()
shouldApproximate tolerance expected actual = do
    V.length actual `shouldBe` V.length expected
    V.zip actual expected `forM_` \(actualVal, expectedVal) ->
        case (actualVal, expectedVal) of
            (Just a, Just e)
                | aIsNaN a && aIsNaN e -> pure ()
                | aIsNaN a || aIsNaN e ->
                    expectationFailure ("NaN mismatch: " <> show actualVal <> " vs " <> show expectedVal)
                | aIsInfinite a || aIsInfinite e ->
                    if a == e then pure ()
                    else expectationFailure ("Inf mismatch: " <> show actualVal <> " vs " <> show expectedVal)
                | otherwise ->
                    abs (a - e) `shouldSatisfy` (<= tolerance)
            (Nothing, Nothing) -> pure ()
            _ -> expectationFailure ("null mismatch: " <> show actualVal <> " vs " <> show expectedVal)
  where
    aIsNaN d = d /= d
    aIsInfinite = isInfinite

shouldApproximateMaybe :: Double -> Maybe Double -> Maybe Double -> IO ()
shouldApproximateMaybe tolerance expected actual =
    shouldApproximate tolerance (V.singleton expected) (V.singleton actual)

withTempFilePath :: String -> (FilePath -> IO a) -> IO a
withTempFilePath suffix =
    bracket
        ( do
            (path, handle) <- openTempFile "/tmp" suffix
            hClose handle
            removeFileIfExists path
            pure path
        )
        removeFileIfExists

withTempFileContent :: String -> BS.ByteString -> (FilePath -> IO a) -> IO a
withTempFileContent suffix content action =
    withTempFilePath suffix $ \path -> do
        BS.writeFile path content
        action path

removeFileIfExists :: FilePath -> IO ()
removeFileIfExists path = do
    exists <- doesFileExist path
    when exists (removeFile path)

expectPolarsFailure :: Either Pl.PolarsError a -> IO ()
expectPolarsFailure result =
    case result of
        Left err -> Pl.polarsErrorCode err `shouldBe` Pl.PolarsFailure
        Right _ -> expectationFailure "expected PolarsFailure"

expectLazyCollectPolarsFailure :: Either Pl.PolarsError Pl.LazyFrame -> IO ()
expectLazyCollectPolarsFailure result =
    case result of
        Left err -> Pl.polarsErrorCode err `shouldBe` Pl.PolarsFailure
        Right lf -> do
            collected <- Pl.collect lf
            expectPolarsFailure collected

expectInvalidArgumentMessage :: T.Text -> Either Pl.PolarsError a -> IO ()
expectInvalidArgumentMessage expected result =
    case result of
        Left err -> do
            Pl.polarsErrorCode err `shouldBe` Pl.InvalidArgument
            Pl.polarsErrorMessage err `shouldBe` expected
        Right _ -> expectationFailure ("expected InvalidArgument: " <> T.unpack expected)

expectValuesFrame :: Pl.DataFrame -> IO ()
expectValuesFrame df = do
    Pl.shape df `shouldReturn` Right (3, 4)
    schemaResult <- Pl.schema df
    fmap (map Pl.fieldName) schemaResult `shouldBe` Right ["name", "age", "score", "active"]
    fmap (map Pl.fieldType) schemaResult `shouldBe` Right [Pl.Utf8, Pl.Int64, Pl.Float64, Pl.Boolean]
    Pl.column @T.Text df "name" `shouldReturn` Right (V.fromList [Just "Alice", Just "Bob", Just "Carol"])
    Pl.column @Int64 df "age" `shouldReturn` Right (V.fromList [Just 34, Nothing, Just 29])
    scoreResult <- Pl.column @Double df "score"
    case scoreResult of
        Left err -> expectationFailure (show err)
        Right scores -> shouldApproximate 1.0e-12 (V.fromList [Just 9.5, Just 8.25, Nothing]) scores
    Pl.column @Bool df "active" `shouldReturn` Right (V.fromList [Just True, Just False, Nothing])

canonicalDataFrameCsv :: Pl.DataFrame -> IO BS.ByteString
canonicalDataFrameCsv df =
    withTempFilePath "polars-hs-canonical.csv" $ \path -> do
        writeResult <-
            Pl.writeCsvWith
                Pl.defaultCsvWriteOptions {Pl.csvWriteNullValue = "NULL"}
                path
                df
        writeResult `shouldBe` Right ()
        BS.readFile path

runRustOracle :: [String] -> IO BS.ByteString
runRustOracle args = do
    configuredOracle <- lookupEnv "POLARS_HS_ORACLE"
    let releaseOracle = "rust" </> "polars-hs-ffi" </> "target" </> "release" </> "polars_hs_oracle"
    releaseOracleExists <- doesFileExist releaseOracle
    case configuredOracle of
        Just oracle -> runProcessBytes oracle args
        Nothing
            | releaseOracleExists -> runProcessBytes releaseOracle args
            | otherwise ->
                runProcessBytes
                    "cargo"
                    ( [ "run"
                      , "--quiet"
                      , "--release"
                      , "--manifest-path"
                      , "rust/polars-hs-ffi/Cargo.toml"
                      , "--bin"
                      , "polars_hs_oracle"
                      , "--"
                      ]
                        <> args
                    )

runProcessBytes :: FilePath -> [String] -> IO BS.ByteString
runProcessBytes command args = do
    (_, Just stdoutHandle, Just stderrHandle, processHandle) <-
        createProcess
            (proc command args)
                { std_out = CreatePipe
                , std_err = CreatePipe
                }
    hSetBinaryMode stdoutHandle True
    hSetBinaryMode stderrHandle True
    out <- BS.hGetContents stdoutHandle
    err <- BS.hGetContents stderrHandle
    code <- waitForProcess processHandle
    BS.length out `seq` BS.length err `seq`
        case code of
            ExitSuccess -> pure out
            ExitFailure exitCode -> do
                expectationFailure ("process exited " <> show exitCode <> ": " <> BSC.unpack err)
                pure ""

fixtureCsv :: FilePath
fixtureCsv = "test/data/people.csv"

salesCsv :: FilePath
salesCsv = "test/data/sales.csv"

employeesCsv :: FilePath
employeesCsv = "test/data/employees.csv"

departmentsCsv :: FilePath
departmentsCsv = "test/data/departments.csv"

valuesCsv :: FilePath
valuesCsv = "test/data/values.csv"

polarsIrisCsv :: FilePath
polarsIrisCsv = "test/data/generated/polars_iris.csv"

floatSpecialsCsv :: FilePath
floatSpecialsCsv = "test/data/float_specials.csv"

stringsCsv :: FilePath
stringsCsv = "test/data/strings.csv"

temporalCsv :: FilePath
temporalCsv = "test/data/temporal.csv"

phrasesCsv :: FilePath
phrasesCsv = "test/data/phrases.csv"

predicatesCsv :: FilePath
predicatesCsv = "test/data/predicates.csv"

horizontalCsv :: FilePath
horizontalCsv = "test/data/horizontal.csv"

nameOpsCsv :: FilePath
nameOpsCsv = "test/data/name_ops.csv"

concatCsv :: FilePath
concatCsv = "test/data/concat.csv"

stringMoreCsv :: FilePath
stringMoreCsv = "test/data/string_more.csv"

metasynPeopleCsv :: FilePath
metasynPeopleCsv = "test/data/generated/metasyn_people.csv"

main :: IO ()
main = hspec $ do
    describe "Polars.DataFrame" $ do
        it "reads a CSV file and reports shape" $ do
            result <- Pl.readCsv fixtureCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> Pl.shape df `shouldReturn` Right (3, 2)

        it "reports schema field names" $ do
            result <- Pl.readCsv fixtureCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    schemaResult <- Pl.schema df
                    fmap (map Pl.fieldName) schemaResult `shouldBe` Right ["name", "age"]

        it "returns a typed error for a missing CSV file" $ do
            result <- Pl.readCsv "test/data/missing.csv"
            case result of
                Right _ -> expectationFailure "expected a Polars error"
                Left err -> Pl.polarsErrorCode err `shouldBe` Pl.PolarsFailure

        it "renders a dataframe to text" $ do
            result <- Pl.readCsv fixtureCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    textResult <- Pl.toText df
                    fmap (T.isInfixOf "Alice") textResult `shouldBe` Right True

        it "writes CSV files and reads them back" $
            withTempFilePath "polars-hs-values.csv" $ \path -> do
                result <- Pl.readCsv valuesCsv
                case result of
                    Left err -> expectationFailure (show err)
                    Right sourceDf -> do
                        writeResult <- Pl.writeCsv path sourceDf
                        writeResult `shouldBe` Right ()
                        roundTrip <- Pl.readCsv path
                        case roundTrip of
                            Left err -> expectationFailure (show err)
                            Right df -> expectValuesFrame df

        it "reads CSV files with parser options" $
            withTempFileContent "polars-hs-custom-read.csv" "Alice;34\nBob;NA\n" $ \path -> do
                let options =
                        Pl.defaultCsvReadOptions
                            { Pl.csvReadHasHeader = False
                            , Pl.csvReadSeparator = 59
                            , Pl.csvReadNullValue = Just "NA"
                            }
                result <- Pl.readCsvWith options path
                case result of
                    Left err -> expectationFailure (show err)
                    Right df -> do
                        Pl.shape df `shouldReturn` Right (2, 2)
                        schemaResult <- Pl.schema df
                        fmap (map Pl.fieldName) schemaResult `shouldBe` Right ["column_1", "column_2"]
                        Pl.column @T.Text df "column_1" `shouldReturn` Right (V.fromList [Just "Alice", Just "Bob"])
                        Pl.column @Int64 df "column_2" `shouldReturn` Right (V.fromList [Just 34, Nothing])

        it "reads CSV files with row controls" $
            withTempFileContent "polars-hs-row-controls.csv" "metadata,skip\nname,age\nSkip,0\nAlice,34\nBob,29\nCarol,31\n" $ \path -> do
                let options =
                        Pl.defaultCsvReadOptions
                            { Pl.csvReadSkipRows = 1
                            , Pl.csvReadSkipRowsAfterHeader = 1
                            , Pl.csvReadNRows = Just 2
                            , Pl.csvReadLowMemory = True
                            , Pl.csvReadRechunk = True
                            }
                result <- Pl.readCsvWith options path
                case result of
                    Left err -> expectationFailure (show err)
                    Right df -> do
                        Pl.shape df `shouldReturn` Right (2, 2)
                        Pl.column @T.Text df "name" `shouldReturn` Right (V.fromList [Just "Alice", Just "Bob"])
                        Pl.column @Int64 df "age" `shouldReturn` Right (V.fromList [Just 34, Just 29])

        it "reads CSV files with inference and ragged-line controls" $
            withTempFileContent "polars-hs-ragged.csv" "value,label\n1,one,extra\nbad,two\n3,three\n" $ \path -> do
                let options =
                        Pl.defaultCsvReadOptions
                            { Pl.csvReadInferSchemaLength = Just 1
                            , Pl.csvReadIgnoreErrors = True
                            , Pl.csvReadTruncateRaggedLines = True
                            , Pl.csvReadMissingIsNull = True
                            }
                result <- Pl.readCsvWith options path
                case result of
                    Left err -> expectationFailure (show err)
                    Right df -> do
                        Pl.shape df `shouldReturn` Right (3, 2)
                        Pl.column @Int64 df "value" `shouldReturn` Right (V.fromList [Just 1, Nothing, Just 3])
                        Pl.column @T.Text df "label" `shouldReturn` Right (V.fromList [Just "one", Just "two", Just "three"])

        it "reads CSV files with missing-field null controls" $
            withTempFileContent "polars-hs-missing-fields.csv" "name,score\nAlice,10\nBob\n" $ \path -> do
                let nullOptions =
                        Pl.defaultCsvReadOptions
                            { Pl.csvReadInferSchemaLength = Just 0
                            , Pl.csvReadMissingIsNull = True
                            }
                    emptyOptions =
                        Pl.defaultCsvReadOptions
                            { Pl.csvReadInferSchemaLength = Just 0
                            , Pl.csvReadMissingIsNull = False
                            }
                nullResult <- Pl.readCsvWith nullOptions path
                emptyResult <- Pl.readCsvWith emptyOptions path
                case (nullResult, emptyResult) of
                    (Right nullDf, Right emptyDf) -> do
                        Pl.column @T.Text nullDf "score" `shouldReturn` Right (V.fromList [Just "10", Nothing])
                        Pl.column @T.Text emptyDf "score" `shouldReturn` Right (V.fromList [Just "10", Just ""])
                    (Left err, _) -> expectationFailure (show err)
                    (_, Left err) -> expectationFailure (show err)

        it "writes CSV files with writer options" $
            withTempFilePath "polars-hs-custom-write.csv" $ \path -> do
                result <- Pl.readCsv valuesCsv
                case result of
                    Left err -> expectationFailure (show err)
                    Right sourceDf -> do
                        let writeOptions =
                                Pl.defaultCsvWriteOptions
                                    { Pl.csvWriteIncludeHeader = False
                                    , Pl.csvWriteSeparator = 59
                                    , Pl.csvWriteNullValue = "NA"
                                    }
                            readOptions =
                                Pl.defaultCsvReadOptions
                                    { Pl.csvReadHasHeader = False
                                    , Pl.csvReadSeparator = 59
                                    , Pl.csvReadNullValue = Just "NA"
                                    }
                        writeResult <- Pl.writeCsvWith writeOptions path sourceDf
                        writeResult `shouldBe` Right ()
                        bytes <- BS.readFile path
                        bytes `shouldSatisfy` BS.isPrefixOf "Alice;34;9.5;true\nBob;NA;8.25;false\n"
                        roundTrip <- Pl.readCsvWith readOptions path
                        case roundTrip of
                            Left err -> expectationFailure (show err)
                            Right df -> do
                                Pl.shape df `shouldReturn` Right (3, 4)
                                Pl.column @Int64 df "column_2" `shouldReturn` Right (V.fromList [Just 34, Nothing, Just 29])
                                Pl.column @Double df "column_3" `shouldReturn` Right (V.fromList [Just 9.5, Just 8.25, Nothing])

        it "writes Parquet files and reads them back" $
            withTempFilePath "polars-hs-values.parquet" $ \path -> do
                result <- Pl.readCsv valuesCsv
                case result of
                    Left err -> expectationFailure (show err)
                    Right sourceDf -> do
                        writeResult <- Pl.writeParquet path sourceDf
                        writeResult `shouldBe` Right ()
                        roundTrip <- Pl.readParquet path
                        case roundTrip of
                            Left err -> expectationFailure (show err)
                            Right df -> expectValuesFrame df

        it "reads and writes Parquet files with options" $
            withTempFilePath "polars-hs-values-options.parquet" $ \path -> do
                result <- Pl.readCsv valuesCsv
                case result of
                    Left err -> expectationFailure (show err)
                    Right sourceDf -> do
                        let writeOptions =
                                Pl.defaultParquetWriteOptions
                                    { Pl.parquetWriteCompression = Pl.ParquetSnappy
                                    , Pl.parquetWriteRowGroupSize = Just 1
                                    , Pl.parquetWriteDataPageSize = Just 1024
                                    , Pl.parquetWriteStatistics =
                                        Pl.defaultParquetStatisticsOptions
                                            { Pl.parquetStatisticsDistinctCount = True
                                            }
                                    , Pl.parquetWriteParallel = False
                                    }
                        writeResult <- Pl.writeParquetWith writeOptions path sourceDf
                        writeResult `shouldBe` Right ()
                        fullRoundTrip <- Pl.readParquet path
                        case fullRoundTrip of
                            Left err -> expectationFailure (show err)
                            Right df -> expectValuesFrame df
                        limited <-
                            Pl.readParquetWith
                                Pl.defaultParquetReadOptions
                                    { Pl.parquetReadNRows = Just 2
                                    , Pl.parquetReadParallel = Pl.ParquetParallelRowGroups
                                    , Pl.parquetReadLowMemory = True
                                    , Pl.parquetReadRechunk = True
                                    }
                                path
                        case limited of
                            Left err -> expectationFailure (show err)
                            Right df -> do
                                Pl.shape df `shouldReturn` Right (2, 4)
                                Pl.column @T.Text df "name" `shouldReturn` Right (V.fromList [Just "Alice", Just "Bob"])

        it "reports InvalidArgument for negative Parquet row limits" $ do
            result <- Pl.readParquetWith Pl.defaultParquetReadOptions {Pl.parquetReadNRows = Just (-1)} valuesCsv
            expectInvalidArgumentMessage "parquetReadNRows must be non-negative" result

        it "reports InvalidArgument for negative Parquet writer controls" $ do
            sourceResult <- Pl.readCsv valuesCsv
            case sourceResult of
                Left err -> expectationFailure (show err)
                Right sourceDf -> do
                    result <-
                        Pl.writeParquetWith
                            Pl.defaultParquetWriteOptions {Pl.parquetWriteDataPageSize = Just (-1)}
                            "test/data/unused-negative.parquet"
                            sourceDf
                    expectInvalidArgumentMessage "parquetWriteDataPageSize must be non-negative" result

        it "reports InvalidArgument for negative CSV read controls" $ do
            nRowsResult <- Pl.readCsvWith Pl.defaultCsvReadOptions {Pl.csvReadNRows = Just (-1)} valuesCsv
            skipRowsResult <- Pl.readCsvWith Pl.defaultCsvReadOptions {Pl.csvReadSkipRows = -1} valuesCsv
            skipRowsAfterHeaderResult <- Pl.readCsvWith Pl.defaultCsvReadOptions {Pl.csvReadSkipRowsAfterHeader = -1} valuesCsv
            inferResult <- Pl.readCsvWith Pl.defaultCsvReadOptions {Pl.csvReadInferSchemaLength = Just (-1)} valuesCsv
            expectInvalidArgumentMessage "csvReadNRows must be non-negative" nRowsResult
            expectInvalidArgumentMessage "csvReadSkipRows must be non-negative" skipRowsResult
            expectInvalidArgumentMessage "csvReadSkipRowsAfterHeader must be non-negative" skipRowsAfterHeaderResult
            expectInvalidArgumentMessage "csvReadInferSchemaLength must be non-negative" inferResult

        it "returns writer errors for paths below missing directories" $
            withTempFilePath "polars-hs-writer-anchor" $ \anchor -> do
                result <- Pl.readCsv valuesCsv
                case result of
                    Left err -> expectationFailure (show err)
                    Right df -> do
                        csvResult <- Pl.writeCsv (anchor <> ".missing" </> "out.csv") df
                        parquetResult <- Pl.writeParquet (anchor <> ".missing" </> "out.parquet") df
                        expectPolarsFailure csvResult
                        expectPolarsFailure parquetResult

        it "selects, drops, and renames eager DataFrame columns" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    selected <- Pl.dataFrameSelect ["name", "age"] df
                    dropped <- Pl.dataFrameDropColumns ["score"] df
                    renamed <- Pl.dataFrameRename [("age", "years")] df
                    setNames <- Pl.dataFrameSetColumnNames ["person", "years", "points", "enabled"] df
                    unicodeNames <- Pl.dataFrameSetColumnNames ["名前", "年齢", "café", "有効"] df
                    shortNames <- Pl.dataFrameSetColumnNames ["only", "two"] df
                    duplicateNames <- Pl.dataFrameSetColumnNames ["person", "person", "points", "enabled"] df
                    case (selected, dropped, renamed, setNames, unicodeNames) of
                        (Right selectedDf, Right droppedDf, Right renamedDf, Right setNamesDf, Right unicodeDf) -> do
                            Pl.shape selectedDf `shouldReturn` Right (3, 2)
                            selectedSchema <- Pl.schema selectedDf
                            fmap (map Pl.fieldName) selectedSchema `shouldBe` Right ["name", "age"]
                            Pl.shape droppedDf `shouldReturn` Right (3, 3)
                            droppedSchema <- Pl.schema droppedDf
                            fmap (map Pl.fieldName) droppedSchema `shouldBe` Right ["name", "age", "active"]
                            renamedSchema <- Pl.schema renamedDf
                            fmap (map Pl.fieldName) renamedSchema `shouldBe` Right ["name", "years", "score", "active"]
                            Pl.column @Int64 renamedDf "years" `shouldReturn` Right (V.fromList [Just 34, Nothing, Just 29])
                            setSchema <- Pl.schema setNamesDf
                            fmap (map Pl.fieldName) setSchema `shouldBe` Right ["person", "years", "points", "enabled"]
                            Pl.column @T.Text setNamesDf "person" `shouldReturn` Right (V.fromList [Just "Alice", Just "Bob", Just "Carol"])
                            unicodeSchema <- Pl.schema unicodeDf
                            fmap (map Pl.fieldName) unicodeSchema `shouldBe` Right ["名前", "年齢", "café", "有効"]
                            Pl.column @T.Text unicodeDf "名前" `shouldReturn` Right (V.fromList [Just "Alice", Just "Bob", Just "Carol"])
                            originalSchema <- Pl.schema df
                            fmap (map Pl.fieldName) originalSchema `shouldBe` Right ["name", "age", "score", "active"]
                        (Left err, _, _, _, _) -> expectationFailure (show err)
                        (_, Left err, _, _, _) -> expectationFailure (show err)
                        (_, _, Left err, _, _) -> expectationFailure (show err)
                        (_, _, _, Left err, _) -> expectationFailure (show err)
                        (_, _, _, _, Left err) -> expectationFailure (show err)
                    expectPolarsFailure shortNames
                    expectPolarsFailure duplicateNames

        it "slices, reverses, drops nulls, and counts nulls in eager DataFrames" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    sliced <- Pl.dataFrameSlice 1 2 df
                    reversed <- Pl.dataFrameReverse df
                    dense <- Pl.dataFrameDropNulls Nothing df
                    ageDense <- Pl.dataFrameDropNulls (Just ["age"]) df
                    counts <- Pl.dataFrameNullCount df
                    case (sliced, reversed, dense, ageDense, counts) of
                        (Right slicedDf, Right reversedDf, Right denseDf, Right ageDenseDf, Right countsDf) -> do
                            Pl.column @T.Text slicedDf "name" `shouldReturn` Right (V.fromList [Just "Bob", Just "Carol"])
                            Pl.column @T.Text reversedDf "name" `shouldReturn` Right (V.fromList [Just "Carol", Just "Bob", Just "Alice"])
                            Pl.column @T.Text denseDf "name" `shouldReturn` Right (V.fromList [Just "Alice"])
                            Pl.column @T.Text ageDenseDf "name" `shouldReturn` Right (V.fromList [Just "Alice", Just "Carol"])
                            Pl.column @Word32 countsDf "name" `shouldReturn` Right (V.fromList [Just 0])
                            Pl.column @Word32 countsDf "age" `shouldReturn` Right (V.fromList [Just 1])
                            Pl.column @Word32 countsDf "score" `shouldReturn` Right (V.fromList [Just 1])
                            Pl.column @Word32 countsDf "active" `shouldReturn` Right (V.fromList [Just 1])
                        (Left err, _, _, _, _) -> expectationFailure (show err)
                        (_, Left err, _, _, _) -> expectationFailure (show err)
                        (_, _, Left err, _, _) -> expectationFailure (show err)
                        (_, _, _, Left err, _) -> expectationFailure (show err)
                        (_, _, _, _, Left err) -> expectationFailure (show err)

        it "compares eager DataFrames with Polars null semantics" $ do
            leftResult <- Pl.readCsv valuesCsv
            rightResult <- Pl.readCsv valuesCsv
            employeesLeftResult <- Pl.readCsv employeesCsv
            employeesRightResult <- Pl.readCsv employeesCsv
            case (leftResult, rightResult, employeesLeftResult, employeesRightResult) of
                (Right left, Right right, Right employeesLeft, Right employeesRight) -> do
                    leftHead <- Pl.head 1 left
                    rightHead <- Pl.head 1 right
                    leftTail <- Pl.tail 1 left
                    reordered <- Pl.dataFrameSelect ["active", "score", "age", "name"] right
                    case (leftHead, rightHead, leftTail, reordered) of
                        (Right leftHeadDf, Right rightHeadDf, Right leftTailDf, Right reorderedDf) -> do
                            Pl.dataFrameEqualsMissing left right `shouldReturn` Right True
                            Pl.dataFrameEquals left right `shouldReturn` Right False
                            Pl.dataFrameEqualsMissing employeesLeft employeesRight `shouldReturn` Right True
                            Pl.dataFrameEquals employeesLeft employeesRight `shouldReturn` Right True
                            Pl.dataFrameEquals leftHeadDf rightHeadDf `shouldReturn` Right True
                            Pl.dataFrameEqualsMissing leftHeadDf leftTailDf `shouldReturn` Right False
                            Pl.dataFrameEqualsMissing left leftTailDf `shouldReturn` Right False
                            Pl.dataFrameEqualsMissing left reorderedDf `shouldReturn` Right False
                        (Left err, _, _, _) -> expectationFailure (show err)
                        (_, Left err, _, _) -> expectationFailure (show err)
                        (_, _, Left err, _) -> expectationFailure (show err)
                        (_, _, _, Left err) -> expectationFailure (show err)
                (Left err, _, _, _) -> expectationFailure (show err)
                (_, Left err, _, _) -> expectationFailure (show err)
                (_, _, Left err, _) -> expectationFailure (show err)
                (_, _, _, Left err) -> expectationFailure (show err)

        it "inspects clears and splits eager DataFrame views" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    originalSchema <- Pl.schema df
                    vstacked <- Pl.dataFrameVStack df df
                    estimatedSize <- Pl.dataFrameEstimatedSize df
                    firstChunks <- Pl.dataFrameFirstColNChunks df
                    maxChunks <- Pl.dataFrameMaxNChunks df
                    isEmpty <- Pl.dataFrameIsEmpty df
                    cleared <- Pl.dataFrameClear df
                    split <- Pl.dataFrameSplitAt 2 df
                    negativeSplit <- Pl.dataFrameSplitAt (-1) df
                    oversizedSplit <- Pl.dataFrameSplitAt 99 df
                    undersizedSplit <- Pl.dataFrameSplitAt (-99) df
                    case (originalSchema, vstacked, estimatedSize, firstChunks, maxChunks, isEmpty, cleared, split, negativeSplit, oversizedSplit, undersizedSplit) of
                        (Right originalFields, Right vstackedDf, Right sizeBytes, Right firstChunkCount, Right maxChunkCount, Right emptyFlag, Right clearedDf, Right (left, right), Right (negativeLeft, negativeRight), Right (oversizedLeft, oversizedRight), Right (undersizedLeft, undersizedRight)) -> do
                            sizeBytes `shouldSatisfy` (> 0)
                            firstChunkCount `shouldBe` 1
                            maxChunkCount `shouldBe` 1
                            Pl.dataFrameFirstColNChunks vstackedDf `shouldReturn` Right 2
                            Pl.dataFrameMaxNChunks vstackedDf `shouldReturn` Right 2
                            emptyFlag `shouldBe` False
                            Pl.dataFrameIsEmpty clearedDf `shouldReturn` Right True
                            Pl.shape clearedDf `shouldReturn` Right (0, 4)
                            clearedSchema <- Pl.schema clearedDf
                            fmap (map Pl.fieldName) clearedSchema `shouldBe` Right ["name", "age", "score", "active"]
                            clearedSchema `shouldBe` Right originalFields
                            Pl.shape left `shouldReturn` Right (2, 4)
                            Pl.shape right `shouldReturn` Right (1, 4)
                            Pl.column @T.Text left "name" `shouldReturn` Right (V.fromList [Just "Alice", Just "Bob"])
                            Pl.column @Int64 left "age" `shouldReturn` Right (V.fromList [Just 34, Nothing])
                            Pl.column @Double right "score" `shouldReturn` Right (V.singleton Nothing)
                            Pl.column @Bool right "active" `shouldReturn` Right (V.singleton Nothing)
                            Pl.column @T.Text right "name" `shouldReturn` Right (V.singleton (Just "Carol"))
                            Pl.column @T.Text negativeLeft "name" `shouldReturn` Right (V.fromList [Just "Alice", Just "Bob"])
                            Pl.column @T.Text negativeRight "name" `shouldReturn` Right (V.singleton (Just "Carol"))
                            Pl.shape oversizedLeft `shouldReturn` Right (3, 4)
                            Pl.shape oversizedRight `shouldReturn` Right (0, 4)
                            Pl.shape undersizedLeft `shouldReturn` Right (0, 4)
                            Pl.shape undersizedRight `shouldReturn` Right (3, 4)
                        (Left err, _, _, _, _, _, _, _, _, _, _) -> expectationFailure (show err)
                        (_, Left err, _, _, _, _, _, _, _, _, _) -> expectationFailure (show err)
                        (_, _, Left err, _, _, _, _, _, _, _, _) -> expectationFailure (show err)
                        (_, _, _, Left err, _, _, _, _, _, _, _) -> expectationFailure (show err)
                        (_, _, _, _, Left err, _, _, _, _, _, _) -> expectationFailure (show err)
                        (_, _, _, _, _, Left err, _, _, _, _, _) -> expectationFailure (show err)
                        (_, _, _, _, _, _, Left err, _, _, _, _) -> expectationFailure (show err)
                        (_, _, _, _, _, _, _, Left err, _, _, _) -> expectationFailure (show err)
                        (_, _, _, _, _, _, _, _, Left err, _, _) -> expectationFailure (show err)
                        (_, _, _, _, _, _, _, _, _, Left err, _) -> expectationFailure (show err)
                        (_, _, _, _, _, _, _, _, _, _, Left err) -> expectationFailure (show err)

        it "rechunks aligns and expands eager DataFrame rows" $ do
            leftResult <- Pl.series @Int64 "value" (V.fromList [Just 1, Just 2, Just 3])
            rightResult <- Pl.series @Int64 "value" (V.fromList [Just 4, Just 5, Just 6])
            otherResult <- Pl.series @Int64 "other" (V.fromList [Just 10, Just 11, Just 12, Just 13, Just 14, Just 15])
            valuesResult <- Pl.readCsv valuesCsv
            case (leftResult, rightResult, otherResult, valuesResult) of
                (Right left, Right right, Right other, Right valuesDf) -> do
                    appended <- Pl.seriesAppend left right
                    case appended of
                        Left err -> expectationFailure (show err)
                        Right chunked -> do
                            dfResult <- Pl.dataFrame [chunked, other]
                            repeated <- Pl.dataFrameNewFromIndex 1 4 valuesDf
                            nullRepeated <- Pl.dataFrameNewFromIndex 99 2 valuesDf
                            emptyRepeated <- Pl.dataFrameNewFromIndex 1 0 valuesDf
                            negativeIndex <- Pl.dataFrameNewFromIndex (-1) 1 valuesDf
                            negativeLen <- Pl.dataFrameNewFromIndex 1 (-1) valuesDf
                            case (dfResult, repeated, nullRepeated, emptyRepeated, negativeIndex, negativeLen) of
                                (Right misalignedDf, Right repeatedDf, Right nullRepeatedDf, Right emptyRepeatedDf, Left indexErr, Left lenErr) -> do
                                    Pl.dataFrameShouldRechunk misalignedDf `shouldReturn` Right True
                                    Pl.dataFrameMaxNChunks misalignedDf `shouldReturn` Right 2
                                    aligned <- Pl.dataFrameAlignChunks misalignedDf
                                    rechunked <- Pl.dataFrameRechunk misalignedDf
                                    case (aligned, rechunked) of
                                        (Right alignedDf, Right rechunkedDf) -> do
                                            Pl.dataFrameShouldRechunk alignedDf `shouldReturn` Right False
                                            Pl.dataFrameShouldRechunk rechunkedDf `shouldReturn` Right False
                                            Pl.dataFrameMaxNChunks alignedDf `shouldReturn` Right 1
                                            Pl.dataFrameMaxNChunks rechunkedDf `shouldReturn` Right 1
                                            Pl.column @Int64 rechunkedDf "value" `shouldReturn` Right (V.fromList [Just 1, Just 2, Just 3, Just 4, Just 5, Just 6])
                                            Pl.column @Int64 alignedDf "other" `shouldReturn` Right (V.fromList [Just 10, Just 11, Just 12, Just 13, Just 14, Just 15])
                                        (Left err, _) -> expectationFailure (show err)
                                        (_, Left err) -> expectationFailure (show err)
                                    Pl.shape repeatedDf `shouldReturn` Right (4, 4)
                                    Pl.column @T.Text repeatedDf "name" `shouldReturn` Right (V.replicate 4 (Just "Bob"))
                                    Pl.column @Int64 repeatedDf "age" `shouldReturn` Right (V.replicate 4 Nothing)
                                    Pl.column @Double repeatedDf "score" `shouldReturn` Right (V.replicate 4 (Just 8.25))
                                    Pl.column @Bool repeatedDf "active" `shouldReturn` Right (V.replicate 4 (Just False))
                                    Pl.shape nullRepeatedDf `shouldReturn` Right (2, 4)
                                    Pl.column @T.Text nullRepeatedDf "name" `shouldReturn` Right (V.replicate 2 Nothing)
                                    Pl.column @Bool nullRepeatedDf "active" `shouldReturn` Right (V.replicate 2 Nothing)
                                    Pl.shape emptyRepeatedDf `shouldReturn` Right (0, 4)
                                    Pl.polarsErrorCode indexErr `shouldBe` Pl.InvalidArgument
                                    Pl.polarsErrorCode lenErr `shouldBe` Pl.InvalidArgument
                                (Left err, _, _, _, _, _) -> expectationFailure (show err)
                                (_, Left err, _, _, _, _) -> expectationFailure (show err)
                                (_, _, Left err, _, _, _) -> expectationFailure (show err)
                                (_, _, _, Left err, _, _) -> expectationFailure (show err)
                                (_, _, _, _, Right _, _) -> expectationFailure "expected negative index to fail"
                                (_, _, _, _, _, Right _) -> expectationFailure "expected negative length to fail"
                (Left err, _, _, _) -> expectationFailure (show err)
                (_, Left err, _, _) -> expectationFailure (show err)
                (_, _, Left err, _) -> expectationFailure (show err)
                (_, _, _, Left err) -> expectationFailure (show err)

        it "computes eager DataFrame row distinct masks" $ do
            nameResult <- Pl.series @T.Text "name" (V.fromList [Just "a", Just "b", Just "a", Just "c", Just "b"])
            ageResult <- Pl.series @Int64 "age" (V.fromList [Just 1, Just 2, Just 1, Just 3, Just 2])
            case (nameResult, ageResult) of
                (Right name, Right age) -> do
                    dfResult <- Pl.dataFrame [name, age]
                    case dfResult of
                        Left err -> expectationFailure (show err)
                        Right df -> do
                            duplicated <- Pl.dataFrameIsDuplicated df
                            unique <- Pl.dataFrameIsUnique df
                            case (duplicated, unique) of
                                (Right duplicatedMask, Right uniqueMask) -> do
                                    Pl.seriesBool duplicatedMask `shouldReturn` Right (V.fromList [Just True, Just True, Just True, Just False, Just True])
                                    Pl.seriesBool uniqueMask `shouldReturn` Right (V.fromList [Just False, Just False, Just False, Just True, Just False])
                                    filtered <- Pl.dataFrameFilter duplicatedMask df
                                    case filtered of
                                        Left err -> expectationFailure (show err)
                                        Right dupes -> do
                                            Pl.shape dupes `shouldReturn` Right (4, 2)
                                            Pl.column @T.Text dupes "name" `shouldReturn` Right (V.fromList [Just "a", Just "b", Just "a", Just "b"])
                                (Left err, _) -> expectationFailure (show err)
                                (_, Left err) -> expectationFailure (show err)
                (Left err, _) -> expectationFailure (show err)
                (_, Left err) -> expectationFailure (show err)

        it "samples eager DataFrame rows with seeded options" $ do
            valueResult <- Pl.series @Int64 "value" (V.fromList (Just <$> [10, 20, 30, 40, 50]))
            labelResult <- Pl.series @T.Text "label" (V.fromList [Just "a", Nothing, Just "c", Just "d", Just "e"])
            case (valueResult, labelResult) of
                (Right value, Right label) -> do
                    dfResult <- Pl.dataFrame [value, label]
                    case dfResult of
                        Left err -> expectationFailure (show err)
                        Right df -> do
                            let seeded = Pl.defaultDataFrameSampleOptions {Pl.dataFrameSampleSeed = Just 0}
                                replacement = seeded {Pl.dataFrameSampleWithReplacement = True}
                                shuffled = seeded {Pl.dataFrameSampleShuffle = True}
                            sampled <- Pl.dataFrameSampleN seeded 2 df
                            fracSampled <- Pl.dataFrameSampleFrac seeded 0.4 df
                            sampledWithReplacement <- Pl.dataFrameSampleN replacement 7 df
                            fracSampledWithReplacement <- Pl.dataFrameSampleFrac replacement 1.4 df
                            shuffledSample <- Pl.dataFrameSampleN shuffled 5 df
                            sampledEmpty <- Pl.dataFrameSampleN seeded 0 df
                            tooLarge <- Pl.dataFrameSampleN seeded 6 df
                            negative <- Pl.dataFrameSampleN seeded (-1) df
                            negativeFrac <- Pl.dataFrameSampleFrac seeded (-0.1) df
                            nanFrac <- Pl.dataFrameSampleFrac seeded (0 / 0) df
                            infiniteFrac <- Pl.dataFrameSampleFrac seeded (1 / 0) df
                            tooLargeFrac <- Pl.dataFrameSampleFrac seeded 1.1 df
                            case sampled of
                                Left err -> expectationFailure (show err)
                                Right sampledDf -> do
                                    Pl.column @Int64 sampledDf "value" `shouldReturn` Right (V.fromList [Just 50, Just 20])
                                    Pl.column @T.Text sampledDf "label" `shouldReturn` Right (V.fromList [Just "e", Nothing])
                            case fracSampled of
                                Left err -> expectationFailure (show err)
                                Right fracDf ->
                                    Pl.column @Int64 fracDf "value" `shouldReturn` Right (V.fromList [Just 50, Just 20])
                            case sampledWithReplacement of
                                Left err -> expectationFailure (show err)
                                Right replacementDf ->
                                    Pl.column @Int64 replacementDf "value" `shouldReturn` Right (V.fromList [Just 20, Just 20, Just 20, Just 10, Just 30, Just 10, Just 50])
                            case fracSampledWithReplacement of
                                Left err -> expectationFailure (show err)
                                Right replacementFracDf ->
                                    Pl.column @Int64 replacementFracDf "value" `shouldReturn` Right (V.fromList [Just 20, Just 20, Just 20, Just 10, Just 30, Just 10, Just 50])
                            case shuffledSample of
                                Left err -> expectationFailure (show err)
                                Right shuffledDf -> do
                                    Pl.column @Int64 shuffledDf "value" `shouldReturn` Right (V.fromList [Just 40, Just 10, Just 20, Just 50, Just 30])
                                    Pl.column @T.Text shuffledDf "label" `shouldReturn` Right (V.fromList [Just "d", Just "a", Nothing, Just "e", Just "c"])
                            case sampledEmpty of
                                Left err -> expectationFailure (show err)
                                Right emptyDf -> Pl.shape emptyDf `shouldReturn` Right (0, 2)
                            expectPolarsFailure tooLarge
                            expectInvalidArgumentMessage "dataFrameSampleN size must be non-negative" negative
                            expectInvalidArgumentMessage "dataFrameSampleFrac fraction must be non-negative" negativeFrac
                            expectInvalidArgumentMessage "dataFrameSampleFrac fraction must be finite" nanFrac
                            expectInvalidArgumentMessage "dataFrameSampleFrac fraction must be finite" infiniteFrac
                            expectInvalidArgumentMessage "dataFrameSampleFrac fraction must be at most 1.0 without replacement" tooLargeFrac
                (Left err, _) -> expectationFailure (show err)
                (_, Left err) -> expectationFailure (show err)

        it "adds row indexes and shifts eager DataFrames" $ do
            valueResult <- Pl.series @Int64 "value" (V.fromList (Just <$> [10, 20, 30]))
            labelResult <- Pl.series @T.Text "label" (V.fromList [Just "a", Nothing, Just "c"])
            case (valueResult, labelResult) of
                (Right value, Right label) -> do
                    dfResult <- Pl.dataFrame [value, label]
                    case dfResult of
                        Left err -> expectationFailure (show err)
                        Right df -> do
                            indexed <- Pl.dataFrameWithRowIndex "row_nr" (Just 5) df
                            defaultIndexed <- Pl.dataFrameWithRowIndex "row_nr" Nothing df
                            duplicate <- Pl.dataFrameWithRowIndex "value" Nothing df
                            negative <- Pl.dataFrameWithRowIndex "row_nr" (Just (-1)) df
                            shiftedDown <- Pl.dataFrameShift 1 df
                            shiftedUp <- Pl.dataFrameShift (-1) df
                            shiftedZero <- Pl.dataFrameShift 0 df
                            case indexed of
                                Left err -> expectationFailure (show err)
                                Right indexedDf -> do
                                    Pl.shape indexedDf `shouldReturn` Right (3, 3)
                                    schemaResult <- Pl.schema indexedDf
                                    fmap (map Pl.fieldName) schemaResult `shouldBe` Right ["row_nr", "value", "label"]
                                    Pl.column @Word32 indexedDf "row_nr" `shouldReturn` Right (V.fromList [Just 5, Just 6, Just 7])
                                    Pl.column @Int64 indexedDf "value" `shouldReturn` Right (V.fromList [Just 10, Just 20, Just 30])
                            case defaultIndexed of
                                Left err -> expectationFailure (show err)
                                Right defaultIndexedDf ->
                                    Pl.column @Word32 defaultIndexedDf "row_nr" `shouldReturn` Right (V.fromList [Just 0, Just 1, Just 2])
                            expectPolarsFailure duplicate
                            expectInvalidArgumentMessage "dataFrameWithRowIndex offset must be non-negative" negative
                            case shiftedDown of
                                Left err -> expectationFailure (show err)
                                Right shiftedDf -> do
                                    Pl.column @Int64 shiftedDf "value" `shouldReturn` Right (V.fromList [Nothing, Just 10, Just 20])
                                    Pl.column @T.Text shiftedDf "label" `shouldReturn` Right (V.fromList [Nothing, Just "a", Nothing])
                            case shiftedUp of
                                Left err -> expectationFailure (show err)
                                Right shiftedDf -> do
                                    Pl.column @Int64 shiftedDf "value" `shouldReturn` Right (V.fromList [Just 20, Just 30, Nothing])
                                    Pl.column @T.Text shiftedDf "label" `shouldReturn` Right (V.fromList [Nothing, Just "c", Nothing])
                            case shiftedZero of
                                Left err -> expectationFailure (show err)
                                Right shiftedDf ->
                                    Pl.column @Int64 shiftedDf "value" `shouldReturn` Right (V.fromList [Just 10, Just 20, Just 30])
                (Left err, _) -> expectationFailure (show err)
                (_, Left err) -> expectationFailure (show err)

        it "takes eager DataFrame rows by explicit indices" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    taken <- Pl.dataFrameTake (V.fromList [2, 0, 2]) df
                    empty <- Pl.dataFrameTake V.empty df
                    case (taken, empty) of
                        (Right takenDf, Right emptyDf) -> do
                            Pl.column @T.Text takenDf "name" `shouldReturn` Right (V.fromList [Just "Carol", Just "Alice", Just "Carol"])
                            Pl.column @Int64 takenDf "age" `shouldReturn` Right (V.fromList [Just 29, Just 34, Just 29])
                            Pl.shape emptyDf `shouldReturn` Right (0, 4)
                        (Left err, _) -> expectationFailure (show err)
                        (_, Left err) -> expectationFailure (show err)

        it "preserves nulls when taking eager DataFrame rows by index" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    taken <- Pl.dataFrameTake (V.fromList [1, 2, 1, 0]) df
                    case taken of
                        Left err -> expectationFailure (show err)
                        Right takenDf -> do
                            Pl.column @Int64 takenDf "age" `shouldReturn` Right (V.fromList [Nothing, Just 29, Nothing, Just 34])
                            Pl.column @Double takenDf "score" `shouldReturn` Right (V.fromList [Just 8.25, Nothing, Just 8.25, Just 9.5])
                            Pl.column @Bool takenDf "active" `shouldReturn` Right (V.fromList [Just False, Nothing, Just False, Just True])

        it "reports errors for invalid eager DataFrame take indices" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    outOfBounds <- Pl.dataFrameTake (V.fromList [0, 3]) df
                    let overflowingIndex = fromIntegral (maxBound :: Word32) + 1
                    overflow <- Pl.dataFrameTake (V.fromList [overflowingIndex]) df
                    expectPolarsFailure outOfBounds
                    expectInvalidArgumentMessage "dataframe take index exceeds Polars index size" overflow

        it "sorts eager DataFrames with explicit options" $ do
            result <- Pl.readCsv employeesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    sorted <-
                        Pl.dataFrameSort
                            Pl.defaultDataFrameSortOptions
                                { Pl.dataFrameSortDescending = [False, True]
                                , Pl.dataFrameSortNullsLast = [False]
                                }
                            ["department", "salary"]
                            df
                    case sorted of
                        Left err -> expectationFailure (show err)
                        Right sortedDf -> do
                            Pl.column @T.Text sortedDf "name" `shouldReturn` Right (V.fromList [Just "Bob", Just "Alice", Just "Carol", Just "Eve"])
                            Pl.column @Int64 sortedDf "salary" `shouldReturn` Right (V.fromList [Just 150, Just 100, Just 90, Just 80])

        it "sorts eager DataFrames with null placement controls" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    sorted <-
                        Pl.dataFrameSort
                            Pl.defaultDataFrameSortOptions {Pl.dataFrameSortNullsLast = [True]}
                            ["age"]
                            df
                    case sorted of
                        Left err -> expectationFailure (show err)
                        Right sortedDf ->
                            Pl.column @T.Text sortedDf "name" `shouldReturn` Right (V.fromList [Just "Carol", Just "Alice", Just "Bob"])

        it "reports InvalidArgument for invalid eager DataFrame sort options" $ do
            result <- Pl.readCsv employeesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    emptyColumns <- Pl.dataFrameSort Pl.defaultDataFrameSortOptions [] df
                    emptyDescending <-
                        Pl.dataFrameSort
                            Pl.defaultDataFrameSortOptions {Pl.dataFrameSortDescending = []}
                            ["department"]
                            df
                    mismatchedDescending <-
                        Pl.dataFrameSort
                            Pl.defaultDataFrameSortOptions {Pl.dataFrameSortDescending = [False, True, False]}
                            ["department", "salary"]
                            df
                    mismatchedNullsLast <-
                        Pl.dataFrameSort
                            Pl.defaultDataFrameSortOptions {Pl.dataFrameSortNullsLast = [False, True, False]}
                            ["department", "salary"]
                            df
                    negativeLimit <-
                        Pl.dataFrameSort
                            Pl.defaultDataFrameSortOptions {Pl.dataFrameSortLimit = Just (-1)}
                            ["department"]
                            df
                    let overflowingLimit = fromIntegral (maxBound :: Word32) + 1
                    overflowingLimitResult <-
                        Pl.dataFrameSort
                            Pl.defaultDataFrameSortOptions {Pl.dataFrameSortLimit = Just overflowingLimit}
                            ["department"]
                            df
                    expectInvalidArgumentMessage "dataFrameSort requires at least one column name" emptyColumns
                    expectInvalidArgumentMessage "dataFrameSortDescending must contain one value or one value per sort column" emptyDescending
                    expectInvalidArgumentMessage "dataFrameSortDescending must contain one value or one value per sort column" mismatchedDescending
                    expectInvalidArgumentMessage "dataFrameSortNullsLast must contain one value or one value per sort column" mismatchedNullsLast
                    expectInvalidArgumentMessage "dataFrameSort limit must be non-negative" negativeLimit
                    expectInvalidArgumentMessage "dataframe sort limit exceeds Polars index size" overflowingLimitResult

        it "keeps unique eager DataFrame rows by subset" $ do
            result <- Pl.readCsv employeesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    uniqueFirst <-
                        Pl.dataFrameUnique
                            Pl.defaultDataFrameUniqueOptions
                                { Pl.dataFrameUniqueSubset = Just ["department"]
                                , Pl.dataFrameUniqueKeepStrategy = Pl.DataFrameKeepFirst
                                , Pl.dataFrameUniqueMaintainOrder = True
                                }
                            df
                    uniqueLast <-
                        Pl.dataFrameUnique
                            Pl.defaultDataFrameUniqueOptions
                                { Pl.dataFrameUniqueSubset = Just ["department"]
                                , Pl.dataFrameUniqueKeepStrategy = Pl.DataFrameKeepLast
                                , Pl.dataFrameUniqueMaintainOrder = True
                                }
                            df
                    uniqueNone <-
                        Pl.dataFrameUnique
                            Pl.defaultDataFrameUniqueOptions
                                { Pl.dataFrameUniqueSubset = Just ["department"]
                                , Pl.dataFrameUniqueKeepStrategy = Pl.DataFrameKeepNone
                                , Pl.dataFrameUniqueMaintainOrder = True
                                }
                            df
                    case (uniqueFirst, uniqueLast, uniqueNone) of
                        (Right firstDf, Right lastDf, Right noneDf) -> do
                            Pl.column @T.Text firstDf "name" `shouldReturn` Right (V.fromList [Just "Alice", Just "Carol", Just "Eve"])
                            Pl.column @T.Text lastDf "name" `shouldReturn` Right (V.fromList [Just "Bob", Just "Carol", Just "Eve"])
                            Pl.column @T.Text noneDf "name" `shouldReturn` Right (V.fromList [Just "Carol", Just "Eve"])
                        (Left err, _, _) -> expectationFailure (show err)
                        (_, Left err, _) -> expectationFailure (show err)
                        (_, _, Left err) -> expectationFailure (show err)

        it "reports InvalidArgument for invalid eager DataFrame unique options" $ do
            result <- Pl.readCsv employeesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    uniqueResult <-
                        Pl.dataFrameUnique
                            Pl.defaultDataFrameUniqueOptions {Pl.dataFrameUniqueSubset = Just []}
                            df
                    expectInvalidArgumentMessage "dataFrameUnique subset requires at least one column name" uniqueResult

        it "fills eager DataFrame nulls with forward strategy" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    filled <- Pl.dataFrameFillNull (Pl.FillForward Nothing) df
                    case filled of
                        Left err -> expectationFailure (show err)
                        Right filledDf -> do
                            Pl.column @Int64 filledDf "age" `shouldReturn` Right (V.fromList [Just 34, Just 34, Just 29])
                            scoreResult <- Pl.column @Double filledDf "score"
                            case scoreResult of
                                Left err -> expectationFailure (show err)
                                Right scores -> shouldApproximate 1.0e-12 (V.fromList [Just 9.5, Just 8.25, Just 8.25]) scores
                            Pl.column @Bool filledDf "active" `shouldReturn` Right (V.fromList [Just True, Just False, Just False])

        it "reports InvalidArgument for invalid eager DataFrame fill-null limits" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    negative <- Pl.dataFrameFillNull (Pl.FillForward (Just (-1))) df
                    let overflowingLimit = fromIntegral (maxBound :: Word32) + 1
                    overflow <- Pl.dataFrameFillNull (Pl.FillBackward (Just overflowingLimit)) df
                    expectInvalidArgumentMessage "fill null limit must be non-negative" negative
                    expectInvalidArgumentMessage "fill null limit exceeds Polars index size" overflow

        it "filters eager DataFrames with boolean Series masks" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    ageResult <- Pl.column @Pl.Series df "age"
                    case ageResult of
                        Left err -> expectationFailure (show err)
                        Right age -> do
                            presentMask <- Pl.seriesIsNotNull age
                            missingMask <- Pl.seriesIsNull age
                            case (presentMask, missingMask) of
                                (Right present, Right missing) -> do
                                    presentRows <- Pl.dataFrameFilter present df
                                    missingRows <- Pl.dataFrameFilter missing df
                                    case (presentRows, missingRows) of
                                        (Right presentDf, Right missingDf) -> do
                                            Pl.column @T.Text presentDf "name" `shouldReturn` Right (V.fromList [Just "Alice", Just "Carol"])
                                            Pl.column @T.Text missingDf "name" `shouldReturn` Right (V.fromList [Just "Bob"])
                                        (Left err, _) -> expectationFailure (show err)
                                        (_, Left err) -> expectationFailure (show err)
                                (Left err, _) -> expectationFailure (show err)
                                (_, Left err) -> expectationFailure (show err)

        it "reports Polars errors for invalid eager DataFrame filter masks" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    ageResult <- Pl.column @Pl.Series df "age"
                    wrongLengthMask <- Pl.series @Bool "mask" (V.fromList [Just True, Just False])
                    case (ageResult, wrongLengthMask) of
                        (Right age, Right mask) -> do
                            wrongDtype <- Pl.dataFrameFilter age df
                            wrongLength <- Pl.dataFrameFilter mask df
                            expectPolarsFailure wrongDtype
                            expectPolarsFailure wrongLength
                        (Left err, _) -> expectationFailure (show err)
                        (_, Left err) -> expectationFailure (show err)

        it "treats null eager DataFrame filter mask values as false" $ do
            result <- Pl.readCsv valuesCsv
            maskResult <- Pl.series @Bool "mask" (V.fromList [Just True, Nothing, Just False])
            case (result, maskResult) of
                (Right df, Right mask) -> do
                    filtered <- Pl.dataFrameFilter mask df
                    case filtered of
                        Right filteredDf ->
                            Pl.column @T.Text filteredDf "name" `shouldReturn` Right (V.fromList [Just "Alice"])
                        Left err -> expectationFailure (show err)
                (Left err, _) -> expectationFailure (show err)
                (_, Left err) -> expectationFailure (show err)

        it "inner joins eager DataFrames by column names" $ do
            employeesResult <- Pl.readCsv employeesCsv
            departmentsResult <- Pl.readCsv departmentsCsv
            case (employeesResult, departmentsResult) of
                (Right employees, Right departments) -> do
                    joined <-
                        Pl.dataFrameJoin
                            Pl.defaultDataFrameJoinOptions
                                { Pl.dataFrameJoinLeftOn = ["department"]
                                , Pl.dataFrameJoinRightOn = ["department"]
                                }
                            employees
                            departments
                    case joined of
                        Left err -> expectationFailure (show err)
                        Right df -> do
                            Pl.shape df `shouldReturn` Right (3, 6)
                            schemaResult <- Pl.schema df
                            fmap (map Pl.fieldName) schemaResult
                                `shouldBe` Right ["id", "name", "department", "salary", "name_right", "budget"]
                            textResult <- Pl.toText df
                            fmap (T.isInfixOf "Grace") textResult `shouldBe` Right True
                            fmap (T.isInfixOf "Heidi") textResult `shouldBe` Right True
                (Left err, _) -> expectationFailure (show err)
                (_, Left err) -> expectationFailure (show err)

        it "supports eager DataFrame outer join modes" $ do
            employeesResult <- Pl.readCsv employeesCsv
            departmentsResult <- Pl.readCsv departmentsCsv
            case (employeesResult, departmentsResult) of
                (Right employees, Right departments) -> do
                    leftJoined <-
                        Pl.dataFrameJoin
                            Pl.defaultDataFrameJoinOptions
                                { Pl.dataFrameJoinType = Pl.DataFrameJoinLeft
                                , Pl.dataFrameJoinLeftOn = ["department"]
                                , Pl.dataFrameJoinRightOn = ["department"]
                                }
                            employees
                            departments
                    rightJoined <-
                        Pl.dataFrameJoin
                            Pl.defaultDataFrameJoinOptions
                                { Pl.dataFrameJoinType = Pl.DataFrameJoinRight
                                , Pl.dataFrameJoinLeftOn = ["department"]
                                , Pl.dataFrameJoinRightOn = ["department"]
                                }
                            employees
                            departments
                    fullJoined <-
                        Pl.dataFrameJoin
                            Pl.defaultDataFrameJoinOptions
                                { Pl.dataFrameJoinType = Pl.DataFrameJoinFull
                                , Pl.dataFrameJoinLeftOn = ["department"]
                                , Pl.dataFrameJoinRightOn = ["department"]
                                }
                            employees
                            departments
                    case (leftJoined, rightJoined, fullJoined) of
                        (Right leftDf, Right rightDf, Right fullDf) -> do
                            Pl.shape leftDf `shouldReturn` Right (4, 6)
                            Pl.shape rightDf `shouldReturn` Right (4, 6)
                            Pl.shape fullDf `shouldReturn` Right (5, 7)
                            leftText <- Pl.toText leftDf
                            rightText <- Pl.toText rightDf
                            fullText <- Pl.toText fullDf
                            fmap (T.isInfixOf "Support") leftText `shouldBe` Right True
                            fmap (T.isInfixOf "Finance") rightText `shouldBe` Right True
                            fmap (T.isInfixOf "Support") fullText `shouldBe` Right True
                            fmap (T.isInfixOf "Finance") fullText `shouldBe` Right True
                        (Left err, _, _) -> expectationFailure (show err)
                        (_, Left err, _) -> expectationFailure (show err)
                        (_, _, Left err) -> expectationFailure (show err)
                (Left err, _) -> expectationFailure (show err)
                (_, Left err) -> expectationFailure (show err)

        it "supports eager DataFrame semi anti and cross joins" $ do
            employeesResult <- Pl.readCsv employeesCsv
            departmentsResult <- Pl.readCsv departmentsCsv
            case (employeesResult, departmentsResult) of
                (Right employees, Right departments) -> do
                    semiJoined <-
                        Pl.dataFrameJoin
                            Pl.defaultDataFrameJoinOptions
                                { Pl.dataFrameJoinType = Pl.DataFrameJoinSemi
                                , Pl.dataFrameJoinLeftOn = ["department"]
                                , Pl.dataFrameJoinRightOn = ["department"]
                                }
                            employees
                            departments
                    antiJoined <-
                        Pl.dataFrameJoin
                            Pl.defaultDataFrameJoinOptions
                                { Pl.dataFrameJoinType = Pl.DataFrameJoinAnti
                                , Pl.dataFrameJoinLeftOn = ["department"]
                                , Pl.dataFrameJoinRightOn = ["department"]
                                }
                            employees
                            departments
                    crossJoined <-
                        Pl.dataFrameJoin
                            Pl.defaultDataFrameJoinOptions {Pl.dataFrameJoinType = Pl.DataFrameJoinCross}
                            employees
                            departments
                    case (semiJoined, antiJoined, crossJoined) of
                        (Right semiDf, Right antiDf, Right crossDf) -> do
                            Pl.shape semiDf `shouldReturn` Right (3, 4)
                            Pl.column @T.Text semiDf "name"
                                `shouldReturn` Right (V.fromList [Just "Alice", Just "Bob", Just "Carol"])
                            Pl.shape antiDf `shouldReturn` Right (1, 4)
                            Pl.column @T.Text antiDf "name" `shouldReturn` Right (V.fromList [Just "Eve"])
                            Pl.shape crossDf `shouldReturn` Right (12, 7)
                            schemaResult <- Pl.schema crossDf
                            fmap (map Pl.fieldName) schemaResult
                                `shouldBe` Right ["id", "name", "department", "salary", "department_right", "name_right", "budget"]
                        (Left err, _, _) -> expectationFailure (show err)
                        (_, Left err, _) -> expectationFailure (show err)
                        (_, _, Left err) -> expectationFailure (show err)
                (Left err, _) -> expectationFailure (show err)
                (_, Left err) -> expectationFailure (show err)

        it "uses custom suffixes for eager DataFrame joins" $ do
            employeesResult <- Pl.readCsv employeesCsv
            departmentsResult <- Pl.readCsv departmentsCsv
            case (employeesResult, departmentsResult) of
                (Right employees, Right departments) -> do
                    joined <-
                        Pl.dataFrameJoin
                            Pl.defaultDataFrameJoinOptions
                                { Pl.dataFrameJoinType = Pl.DataFrameJoinLeft
                                , Pl.dataFrameJoinLeftOn = ["department"]
                                , Pl.dataFrameJoinRightOn = ["department"]
                                , Pl.dataFrameJoinSuffix = Just "_dept"
                                }
                            employees
                            departments
                    case joined of
                        Left err -> expectationFailure (show err)
                        Right df -> do
                            schemaResult <- Pl.schema df
                            fmap (map Pl.fieldName) schemaResult
                                `shouldBe` Right ["id", "name", "department", "salary", "name_dept", "budget"]
                (Left err, _) -> expectationFailure (show err)
                (_, Left err) -> expectationFailure (show err)

        it "validates eager DataFrame join options" $ do
            employeesResult <- Pl.readCsv employeesCsv
            departmentsResult <- Pl.readCsv departmentsCsv
            case (employeesResult, departmentsResult) of
                (Right employees, Right departments) -> do
                    emptyLeft <- Pl.dataFrameJoin Pl.defaultDataFrameJoinOptions employees departments
                    mismatched <-
                        Pl.dataFrameJoin
                            Pl.defaultDataFrameJoinOptions
                                { Pl.dataFrameJoinLeftOn = ["department", "name"]
                                , Pl.dataFrameJoinRightOn = ["department"]
                                }
                            employees
                            departments
                    keyedCross <-
                        Pl.dataFrameJoin
                            Pl.defaultDataFrameJoinOptions
                                { Pl.dataFrameJoinType = Pl.DataFrameJoinCross
                                , Pl.dataFrameJoinLeftOn = ["department"]
                                , Pl.dataFrameJoinRightOn = ["department"]
                                }
                            employees
                            departments
                    expectInvalidArgumentMessage "dataFrameJoin left keys must contain at least one column name" emptyLeft
                    expectInvalidArgumentMessage "dataFrameJoin left and right key counts must match" mismatched
                    expectInvalidArgumentMessage "dataFrameJoin cross join requires empty join key lists" keyedCross
                (Left err, _) -> expectationFailure (show err)
                (_, Left err) -> expectationFailure (show err)

        it "vertically stacks eager DataFrames" $ do
            leftResult <- Pl.readCsv valuesCsv
            rightResult <- Pl.readCsv valuesCsv
            case (leftResult, rightResult) of
                (Right leftDf, Right rightDf) -> do
                    stacked <- Pl.dataFrameVStack leftDf rightDf
                    case stacked of
                        Left err -> expectationFailure (show err)
                        Right df -> do
                            Pl.shape df `shouldReturn` Right (6, 4)
                            Pl.column @T.Text df "name"
                                `shouldReturn` Right
                                    ( V.fromList
                                        [ Just "Alice"
                                        , Just "Bob"
                                        , Just "Carol"
                                        , Just "Alice"
                                        , Just "Bob"
                                        , Just "Carol"
                                        ]
                                    )
                (Left err, _) -> expectationFailure (show err)
                (_, Left err) -> expectationFailure (show err)

        it "horizontally stacks Series onto eager DataFrames" $ do
            dfResult <- Pl.readCsv valuesCsv
            cityResult <- Pl.series @T.Text "city" (V.fromList [Just "Tokyo", Just "Paris", Just "Oslo"])
            rankResult <- Pl.series @Int64 "rank" (V.fromList [Just 1, Just 2, Just 3])
            case (dfResult, cityResult, rankResult) of
                (Right df, Right city, Right rank) -> do
                    stacked <- Pl.dataFrameHStack [city, rank] df
                    case stacked of
                        Left err -> expectationFailure (show err)
                        Right wide -> do
                            Pl.shape wide `shouldReturn` Right (3, 6)
                            Pl.column @T.Text wide "city"
                                `shouldReturn` Right (V.fromList [Just "Tokyo", Just "Paris", Just "Oslo"])
                            Pl.column @Int64 wide "rank"
                                `shouldReturn` Right (V.fromList [Just 1, Just 2, Just 3])
                (Left err, _, _) -> expectationFailure (show err)
                (_, Left err, _) -> expectationFailure (show err)
                (_, _, Left err) -> expectationFailure (show err)

        it "reports errors for invalid eager DataFrame stacking" $ do
            valuesResult <- Pl.readCsv valuesCsv
            employeesResult <- Pl.readCsv employeesCsv
            duplicateNameResult <- Pl.series @T.Text "name" (V.fromList [Just "A", Just "B", Just "C"])
            shortSeriesResult <- Pl.series @Int64 "short" (V.fromList [Just 1, Just 2])
            case (valuesResult, employeesResult, duplicateNameResult, shortSeriesResult) of
                (Right valuesDf, Right employeesDf, Right duplicateName, Right shortSeries) -> do
                    schemaMismatch <- Pl.dataFrameVStack valuesDf employeesDf
                    duplicateColumn <- Pl.dataFrameHStack [duplicateName] valuesDf
                    lengthMismatch <- Pl.dataFrameHStack [shortSeries] valuesDf
                    emptyColumns <- Pl.dataFrameHStack [] valuesDf
                    expectPolarsFailure schemaMismatch
                    expectPolarsFailure duplicateColumn
                    expectPolarsFailure lengthMismatch
                    expectInvalidArgumentMessage "dataFrameHStack requires at least one Series" emptyColumns
                (Left err, _, _, _) -> expectationFailure (show err)
                (_, Left err, _, _) -> expectationFailure (show err)
                (_, _, Left err, _) -> expectationFailure (show err)
                (_, _, _, Left err) -> expectationFailure (show err)

        it "adds and replaces eager DataFrame columns" $ do
            dfResult <- Pl.readCsv valuesCsv
            replacementAgeResult <- Pl.series @Int64 "age" (V.fromList [Just 40, Just 41, Just 42])
            cityResult <- Pl.series @T.Text "city" (V.fromList [Just "Tokyo", Just "Paris", Just "Oslo"])
            case (dfResult, replacementAgeResult, cityResult) of
                (Right df, Right replacementAge, Right city) -> do
                    updated <- Pl.dataFrameWithColumns [replacementAge, city] df
                    case updated of
                        Left err -> expectationFailure (show err)
                        Right out -> do
                            Pl.shape out `shouldReturn` Right (3, 5)
                            Pl.column @Int64 out "age"
                                `shouldReturn` Right (V.fromList [Just 40, Just 41, Just 42])
                            Pl.column @T.Text out "city"
                                `shouldReturn` Right (V.fromList [Just "Tokyo", Just "Paris", Just "Oslo"])
                (Left err, _, _) -> expectationFailure (show err)
                (_, Left err, _) -> expectationFailure (show err)
                (_, _, Left err) -> expectationFailure (show err)

        it "inserts and replaces eager DataFrame columns by index" $ do
            dfResult <- Pl.readCsv valuesCsv
            activeResult <- Pl.series @Bool "active_inserted" (V.fromList [Just True, Just False, Nothing])
            cityResult <- Pl.series @T.Text "city" (V.fromList [Just "Tokyo", Just "Paris", Just "Oslo"])
            scoreResult <- Pl.series @Double "score_replaced" (V.fromList [Just 9.5, Just 8.0, Just 7.25])
            duplicateResult <- Pl.series @Int64 "age" (V.fromList [Just 40, Just 41, Just 42])
            shortResult <- Pl.series @Int64 "short" (V.fromList [Just 1, Just 2])
            unitResult <- Pl.series @Double "unit" (V.singleton (Just 1.0))
            case (dfResult, activeResult, cityResult, scoreResult, duplicateResult, shortResult, unitResult) of
                (Right df, Right active, Right city, Right score, Right duplicateAge, Right short, Right unit) -> do
                    inserted <- Pl.dataFrameInsertColumn 1 active df
                    appended <- Pl.dataFrameInsertColumn 4 city df
                    insertTooFar <- Pl.dataFrameInsertColumn 5 city df
                    duplicate <- Pl.dataFrameInsertColumn 1 duplicateAge df
                    negativeInsert <- Pl.dataFrameInsertColumn (-1) active df
                    shortInsert <- Pl.dataFrameInsertColumn 1 short df
                    unitInsert <- Pl.dataFrameInsertColumn 1 unit df
                    replaced <- Pl.dataFrameReplaceColumn 1 score df
                    replaceDuplicate <- Pl.dataFrameReplaceColumn 0 duplicateAge df
                    replaceMissing <- Pl.dataFrameReplaceColumn 4 score df
                    negativeReplace <- Pl.dataFrameReplaceColumn (-1) score df
                    shortReplace <- Pl.dataFrameReplaceColumn 0 short df
                    unitReplace <- Pl.dataFrameReplaceColumn 0 unit df
                    case inserted of
                        Left err -> expectationFailure (show err)
                        Right insertedDf -> do
                            schemaResult <- Pl.schema insertedDf
                            fmap (map Pl.fieldName) schemaResult `shouldBe` Right ["name", "active_inserted", "age", "score", "active"]
                            Pl.column @Bool insertedDf "active_inserted" `shouldReturn` Right (V.fromList [Just True, Just False, Nothing])
                    case appended of
                        Left err -> expectationFailure (show err)
                        Right appendedDf -> do
                            schemaResult <- Pl.schema appendedDf
                            fmap (map Pl.fieldName) schemaResult `shouldBe` Right ["name", "age", "score", "active", "city"]
                            Pl.column @T.Text appendedDf "city" `shouldReturn` Right (V.fromList [Just "Tokyo", Just "Paris", Just "Oslo"])
                    expectInvalidArgumentMessage "dataframe insert-column index 5 exceeds width 4" insertTooFar
                    expectPolarsFailure duplicate
                    expectInvalidArgumentMessage "dataFrameInsertColumn index must be non-negative" negativeInsert
                    expectPolarsFailure shortInsert
                    expectPolarsFailure unitInsert
                    case replaced of
                        Left err -> expectationFailure (show err)
                        Right replacedDf -> do
                            schemaResult <- Pl.schema replacedDf
                            fmap (map Pl.fieldName) schemaResult `shouldBe` Right ["name", "score_replaced", "score", "active"]
                            Pl.column @Double replacedDf "score_replaced" `shouldReturn` Right (V.fromList [Just 9.5, Just 8.0, Just 7.25])
                    expectInvalidArgumentMessage "dataframe replace-column name \"age\" already exists at index 1" replaceDuplicate
                    expectPolarsFailure replaceMissing
                    expectInvalidArgumentMessage "dataFrameReplaceColumn index must be non-negative" negativeReplace
                    expectPolarsFailure shortReplace
                    expectPolarsFailure unitReplace
                (Left err, _, _, _, _, _, _) -> expectationFailure (show err)
                (_, Left err, _, _, _, _, _) -> expectationFailure (show err)
                (_, _, Left err, _, _, _, _) -> expectationFailure (show err)
                (_, _, _, Left err, _, _, _) -> expectationFailure (show err)
                (_, _, _, _, Left err, _, _) -> expectationFailure (show err)
                (_, _, _, _, _, Left err, _) -> expectationFailure (show err)
                (_, _, _, _, _, _, Left err) -> expectationFailure (show err)

        it "partitions eager DataFrames by groups" $ do
            result <- Pl.readCsv employeesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right employees -> do
                    stableParts <-
                        Pl.dataFramePartitionBy
                            Pl.defaultDataFramePartitionOptions
                                { Pl.dataFramePartitionColumns = ["department"]
                                , Pl.dataFramePartitionMaintainOrder = True
                                }
                            employees
                    noKeyParts <-
                        Pl.dataFramePartitionBy
                            Pl.defaultDataFramePartitionOptions
                                { Pl.dataFramePartitionColumns = ["department"]
                                , Pl.dataFramePartitionIncludeKey = False
                                , Pl.dataFramePartitionMaintainOrder = True
                                }
                            employees
                    emptyColumns <- Pl.dataFramePartitionBy Pl.defaultDataFramePartitionOptions employees
                    missingColumn <-
                        Pl.dataFramePartitionBy
                            Pl.defaultDataFramePartitionOptions {Pl.dataFramePartitionColumns = ["missing"]}
                            employees
                    case stableParts of
                        Left err -> expectationFailure (show err)
                        Right [engineering, sales, support] -> do
                            Pl.column @T.Text engineering "name" `shouldReturn` Right (V.fromList [Just "Alice", Just "Bob"])
                            Pl.column @T.Text sales "name" `shouldReturn` Right (V.singleton (Just "Carol"))
                            Pl.column @T.Text support "name" `shouldReturn` Right (V.singleton (Just "Eve"))
                            Pl.column @T.Text engineering "department" `shouldReturn` Right (V.fromList [Just "Engineering", Just "Engineering"])
                        Right parts -> expectationFailure ("unexpected partition count: " <> show (length parts))
                    case noKeyParts of
                        Left err -> expectationFailure (show err)
                        Right (engineeringNoKey : _) -> do
                            schemaResult <- Pl.schema engineeringNoKey
                            fmap (map Pl.fieldName) schemaResult `shouldBe` Right ["id", "name", "salary"]
                        Right [] -> expectationFailure "expected partition output"
                    expectInvalidArgumentMessage "dataFramePartitionBy requires at least one column name" emptyColumns
                    expectPolarsFailure missingColumn

        it "explodes eager DataFrame list columns" $ do
            scanResult <- Pl.scanCsv phrasesCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    withParts <- Pl.withColumns [Pl.alias "parts" (Pl.strSplit (Pl.col "phrase") (Pl.litText " "))] lf0
                    case withParts of
                        Left err -> expectationFailure (show err)
                        Right lf1 -> do
                            collected <- Pl.collect lf1
                            case collected of
                                Left err -> expectationFailure (show err)
                                Right df -> do
                                    exploded <-
                                        Pl.dataFrameExplode
                                            Pl.defaultDataFrameExplodeOptions {Pl.dataFrameExplodeColumns = ["parts"]}
                                            df
                                    emptyColumns <- Pl.dataFrameExplode Pl.defaultDataFrameExplodeOptions df
                                    missingColumn <-
                                        Pl.dataFrameExplode
                                            Pl.defaultDataFrameExplodeOptions {Pl.dataFrameExplodeColumns = ["missing"]}
                                            df
                                    scalarColumn <-
                                        Pl.dataFrameExplode
                                            Pl.defaultDataFrameExplodeOptions {Pl.dataFrameExplodeColumns = ["phrase"]}
                                            df
                                    case exploded of
                                        Left err -> expectationFailure (show err)
                                        Right out -> do
                                            Pl.shape out `shouldReturn` Right (8, 2)
                                            Pl.column @T.Text out "phrase"
                                                `shouldReturn` Right
                                                    ( V.fromList
                                                        [ Just "red green blue"
                                                        , Just "red green blue"
                                                        , Just "red green blue"
                                                        , Just "red red"
                                                        , Just "red red"
                                                        , Just "日本 語"
                                                        , Just "日本 語"
                                                        , Just "solo"
                                                        ]
                                                    )
                                            Pl.column @T.Text out "parts"
                                                `shouldReturn` Right
                                                    ( V.fromList
                                                        [ Just "red"
                                                        , Just "green"
                                                        , Just "blue"
                                                        , Just "red"
                                                        , Just "red"
                                                        , Just "日本"
                                                        , Just "語"
                                                        , Just "solo"
                                                        ]
                                                    )
                                    expectInvalidArgumentMessage "dataFrameExplode requires at least one column name" emptyColumns
                                    expectPolarsFailure missingColumn
                                    expectPolarsFailure scalarColumn

        it "gathers every nth eager DataFrame row" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    everyTwo <- Pl.dataFrameGatherEvery 2 0 df
                    offsetRows <- Pl.dataFrameGatherEvery 2 1 df
                    oversizedStep <- Pl.dataFrameGatherEvery 10 0 df
                    oversizedOffset <- Pl.dataFrameGatherEvery 2 10 df
                    zeroStep <- Pl.dataFrameGatherEvery 0 0 df
                    negativeStep <- Pl.dataFrameGatherEvery (-1) 0 df
                    negativeOffset <- Pl.dataFrameGatherEvery 2 (-1) df
                    case everyTwo of
                        Left err -> expectationFailure (show err)
                        Right out -> do
                            Pl.shape out `shouldReturn` Right (2, 4)
                            Pl.column @T.Text out "name" `shouldReturn` Right (V.fromList [Just "Alice", Just "Carol"])
                    case offsetRows of
                        Left err -> expectationFailure (show err)
                        Right out -> do
                            Pl.shape out `shouldReturn` Right (1, 4)
                            Pl.column @T.Text out "name" `shouldReturn` Right (V.singleton (Just "Bob"))
                            Pl.column @Int64 out "age" `shouldReturn` Right (V.singleton Nothing)
                            Pl.column @Bool out "active" `shouldReturn` Right (V.singleton (Just False))
                    case oversizedStep of
                        Left err -> expectationFailure (show err)
                        Right out -> do
                            Pl.shape out `shouldReturn` Right (1, 4)
                            Pl.column @T.Text out "name" `shouldReturn` Right (V.singleton (Just "Alice"))
                    case oversizedOffset of
                        Left err -> expectationFailure (show err)
                        Right out -> do
                            Pl.shape out `shouldReturn` Right (0, 4)
                            schemaResult <- Pl.schema out
                            fmap (map Pl.fieldName) schemaResult `shouldBe` Right ["name", "age", "score", "active"]
                    expectInvalidArgumentMessage "dataFrameGatherEvery step must be positive" zeroStep
                    expectInvalidArgumentMessage "dataFrameGatherEvery step must be non-negative" negativeStep
                    expectInvalidArgumentMessage "dataFrameGatherEvery offset must be non-negative" negativeOffset

        it "transposes eager DataFrames with configurable output names" $ do
            rowNameResult <- Pl.series @T.Text "row_name" (V.fromList [Just "r1", Just "r2", Just "r3"])
            xResult <- Pl.series @Int64 "x" (V.fromList [Just 1, Just 2, Just 3])
            yResult <- Pl.series @Int64 "y" (V.fromList [Just 4, Just 5, Just 6])
            case (rowNameResult, xResult, yResult) of
                (Right rowName, Right x, Right y) -> do
                    numericResult <- Pl.dataFrame [x, y]
                    namedResult <- Pl.dataFrame [rowName, x, y]
                    case (numericResult, namedResult) of
                        (Right numericDf, Right namedDf) -> do
                            defaultOut <- Pl.dataFrameTranspose Pl.defaultDataFrameTransposeOptions numericDf
                            explicitOut <-
                                Pl.dataFrameTranspose
                                    Pl.defaultDataFrameTransposeOptions
                                        { Pl.dataFrameTransposeKeepNamesAs = Just "metric"
                                        , Pl.dataFrameTransposeColumnNames = Pl.TransposeColumnNames ["r1", "r2", "r3"]
                                        }
                                    numericDf
                            sourceOut <-
                                Pl.dataFrameTranspose
                                    Pl.defaultDataFrameTransposeOptions
                                        { Pl.dataFrameTransposeKeepNamesAs = Just "metric"
                                        , Pl.dataFrameTransposeColumnNames = Pl.TransposeColumnNamesFrom "row_name"
                                        }
                                    namedDf
                            mismatchedNames <-
                                Pl.dataFrameTranspose
                                    Pl.defaultDataFrameTransposeOptions
                                        { Pl.dataFrameTransposeColumnNames = Pl.TransposeColumnNames ["only_one"]
                                        }
                                    numericDf
                            case (defaultOut, explicitOut, sourceOut) of
                                (Right defaultDf, Right explicitDf, Right sourceDf) -> do
                                    Pl.shape defaultDf `shouldReturn` Right (2, 3)
                                    defaultSchema <- Pl.schema defaultDf
                                    fmap (map Pl.fieldName) defaultSchema `shouldBe` Right ["column_0", "column_1", "column_2"]
                                    Pl.column @Int64 defaultDf "column_0" `shouldReturn` Right (V.fromList [Just 1, Just 4])
                                    Pl.column @Int64 defaultDf "column_1" `shouldReturn` Right (V.fromList [Just 2, Just 5])
                                    Pl.column @Int64 defaultDf "column_2" `shouldReturn` Right (V.fromList [Just 3, Just 6])
                                    Pl.shape explicitDf `shouldReturn` Right (2, 4)
                                    explicitSchema <- Pl.schema explicitDf
                                    fmap (map Pl.fieldName) explicitSchema `shouldBe` Right ["metric", "r1", "r2", "r3"]
                                    Pl.column @T.Text explicitDf "metric" `shouldReturn` Right (V.fromList [Just "x", Just "y"])
                                    Pl.column @Int64 explicitDf "r3" `shouldReturn` Right (V.fromList [Just 3, Just 6])
                                    sourceSchema <- Pl.schema sourceDf
                                    fmap (map Pl.fieldName) sourceSchema `shouldBe` Right ["metric", "r1", "r2", "r3"]
                                    Pl.column @T.Text sourceDf "metric" `shouldReturn` Right (V.fromList [Just "x", Just "y"])
                                    Pl.column @Int64 sourceDf "r2" `shouldReturn` Right (V.fromList [Just 2, Just 5])
                                    Pl.column @Int64 numericDf "x" `shouldReturn` Right (V.fromList [Just 1, Just 2, Just 3])
                                (Left err, _, _) -> expectationFailure (show err)
                                (_, Left err, _) -> expectationFailure (show err)
                                (_, _, Left err) -> expectationFailure (show err)
                            expectPolarsFailure mismatchedNames
                        (Left err, _) -> expectationFailure (show err)
                        (_, Left err) -> expectationFailure (show err)
                (Left err, _, _) -> expectationFailure (show err)
                (_, Left err, _) -> expectationFailure (show err)
                (_, _, Left err) -> expectationFailure (show err)

        it "unpivots eager DataFrames from wide to long format" $ do
            result <- Pl.readCsv employeesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    explicitOut <-
                        Pl.dataFrameUnpivot
                            Pl.defaultDataFrameUnpivotOptions
                                { Pl.dataFrameUnpivotIndex = ["department"]
                                , Pl.dataFrameUnpivotOn = Just ["salary"]
                                , Pl.dataFrameUnpivotVariableName = Just "metric"
                                , Pl.dataFrameUnpivotValueName = Just "amount"
                                }
                            df
                    defaultOnOut <-
                        Pl.dataFrameUnpivot
                            Pl.defaultDataFrameUnpivotOptions
                                { Pl.dataFrameUnpivotIndex = ["department"]
                                , Pl.dataFrameUnpivotOn = Nothing
                                }
                            df
                    emptyOnOut <-
                        Pl.dataFrameUnpivot
                            Pl.defaultDataFrameUnpivotOptions
                                { Pl.dataFrameUnpivotIndex = ["department"]
                                , Pl.dataFrameUnpivotOn = Just []
                                }
                            df
                    missingColumn <-
                        Pl.dataFrameUnpivot
                            Pl.defaultDataFrameUnpivotOptions
                                { Pl.dataFrameUnpivotIndex = ["missing"]
                                , Pl.dataFrameUnpivotOn = Just ["salary"]
                                }
                            df
                    case (explicitOut, defaultOnOut, emptyOnOut) of
                        (Right explicitDf, Right defaultDf, Right emptyDf) -> do
                            Pl.shape explicitDf `shouldReturn` Right (4, 3)
                            explicitSchema <- Pl.schema explicitDf
                            fmap (map Pl.fieldName) explicitSchema `shouldBe` Right ["department", "metric", "amount"]
                            Pl.column @T.Text explicitDf "department"
                                `shouldReturn` Right (V.fromList [Just "Engineering", Just "Engineering", Just "Sales", Just "Support"])
                            Pl.column @T.Text explicitDf "metric"
                                `shouldReturn` Right (V.fromList [Just "salary", Just "salary", Just "salary", Just "salary"])
                            Pl.column @Int64 explicitDf "amount"
                                `shouldReturn` Right (V.fromList [Just 100, Just 150, Just 90, Just 80])
                            Pl.shape defaultDf `shouldReturn` Right (12, 3)
                            Pl.column @T.Text defaultDf "variable"
                                `shouldReturn` Right
                                    ( V.fromList
                                        [ Just "id"
                                        , Just "id"
                                        , Just "id"
                                        , Just "id"
                                        , Just "name"
                                        , Just "name"
                                        , Just "name"
                                        , Just "name"
                                        , Just "salary"
                                        , Just "salary"
                                        , Just "salary"
                                        , Just "salary"
                                        ]
                                    )
                            Pl.shape emptyDf `shouldReturn` Right (0, 3)
                            emptySchema <- Pl.schema emptyDf
                            fmap (map Pl.fieldName) emptySchema `shouldBe` Right ["department", "variable", "value"]
                        (Left err, _, _) -> expectationFailure (show err)
                        (_, Left err, _) -> expectationFailure (show err)
                        (_, _, Left err) -> expectationFailure (show err)
                    expectPolarsFailure missingColumn

        it "encodes eager DataFrames as dummy indicator columns" $ do
            employeesResult <- Pl.readCsv employeesCsv
            valuesResult <- Pl.readCsv valuesCsv
            case (employeesResult, valuesResult) of
                (Right employeesDf, Right valuesDf) -> do
                    allColumnsOut <- Pl.dataFrameToDummies Pl.defaultDataFrameToDummiesOptions employeesDf
                    selectedOut <-
                        Pl.dataFrameToDummies
                            Pl.defaultDataFrameToDummiesOptions
                                { Pl.dataFrameToDummiesColumns = Just ["department"]
                                , Pl.dataFrameToDummiesSeparator = Just ":"
                                }
                            employeesDf
                    dropFirstOut <-
                        Pl.dataFrameToDummies
                            Pl.defaultDataFrameToDummiesOptions
                                { Pl.dataFrameToDummiesColumns = Just ["department"]
                                , Pl.dataFrameToDummiesDropFirst = True
                                }
                            employeesDf
                    dropNullsOut <-
                        Pl.dataFrameToDummies
                            Pl.defaultDataFrameToDummiesOptions
                                { Pl.dataFrameToDummiesColumns = Just ["age"]
                                , Pl.dataFrameToDummiesDropFirst = True
                                , Pl.dataFrameToDummiesDropNulls = True
                                }
                            valuesDf
                    emptyColumnsOut <-
                        Pl.dataFrameToDummies
                            Pl.defaultDataFrameToDummiesOptions {Pl.dataFrameToDummiesColumns = Just []}
                            employeesDf
                    missingColumn <-
                        Pl.dataFrameToDummies
                            Pl.defaultDataFrameToDummiesOptions {Pl.dataFrameToDummiesColumns = Just ["missing"]}
                            employeesDf
                    case (allColumnsOut, selectedOut, dropFirstOut, dropNullsOut, emptyColumnsOut, missingColumn) of
                        (Right allColumnsDf, Right selectedDf, Right dropFirstDf, Right dropNullsDf, Right emptyColumnsDf, Right missingDf) -> do
                            Pl.shape allColumnsDf `shouldReturn` Right (4, 15)
                            Pl.column @Word8 allColumnsDf "name_Alice"
                                `shouldReturn` Right (V.fromList [Just 1, Just 0, Just 0, Just 0])

                            selectedSchema <- Pl.schema selectedDf
                            fmap (map Pl.fieldName) selectedSchema
                                `shouldBe` Right
                                    [ "id"
                                    , "name"
                                    , "department:Engineering"
                                    , "department:Sales"
                                    , "department:Support"
                                    , "salary"
                                    ]
                            Pl.column @Word8 selectedDf "department:Engineering"
                                `shouldReturn` Right (V.fromList [Just 1, Just 1, Just 0, Just 0])

                            dropFirstSchema <- Pl.schema dropFirstDf
                            fmap (map Pl.fieldName) dropFirstSchema
                                `shouldBe` Right ["id", "name", "department_Sales", "department_Support", "salary"]

                            dropNullsSchema <- Pl.schema dropNullsDf
                            fmap (map Pl.fieldName) dropNullsSchema
                                `shouldBe` Right ["name", "age_29", "score", "active"]
                            Pl.column @Word8 dropNullsDf "age_29"
                                `shouldReturn` Right (V.fromList [Just 0, Just 0, Just 1])

                            emptySchema <- Pl.schema emptyColumnsDf
                            fmap (map Pl.fieldName) emptySchema
                                `shouldBe` Right ["id", "name", "department", "salary"]
                            Pl.column @T.Text emptyColumnsDf "department"
                                `shouldReturn` Right
                                    ( V.fromList
                                        [ Just "Engineering"
                                        , Just "Engineering"
                                        , Just "Sales"
                                        , Just "Support"
                                        ]
                                    )
                            missingSchema <- Pl.schema missingDf
                            fmap (map Pl.fieldName) missingSchema
                                `shouldBe` Right ["id", "name", "department", "salary"]
                        (Left err, _, _, _, _, _) -> expectationFailure (show err)
                        (_, Left err, _, _, _, _) -> expectationFailure (show err)
                        (_, _, Left err, _, _, _) -> expectationFailure (show err)
                        (_, _, _, Left err, _, _) -> expectationFailure (show err)
                        (_, _, _, _, Left err, _) -> expectationFailure (show err)
                        (_, _, _, _, _, Left err) -> expectationFailure (show err)
                (Left err, _) -> expectationFailure (show err)
                (_, Left err) -> expectationFailure (show err)

        it "broadcasts unit-length eager DataFrame columns" $ do
            dfResult <- Pl.readCsv valuesCsv
            scoreResult <- Pl.series @Double "score" (V.fromList [Just 10.0])
            case (dfResult, scoreResult) of
                (Right df, Right score) -> do
                    updated <- Pl.dataFrameWithColumns [score] df
                    case updated of
                        Left err -> expectationFailure (show err)
                        Right out ->
                            Pl.column @Double out "score"
                                `shouldReturn` Right (V.fromList [Just 10.0, Just 10.0, Just 10.0])
                (Left err, _) -> expectationFailure (show err)
                (_, Left err) -> expectationFailure (show err)

        it "reports errors for invalid eager DataFrame with-columns inputs" $ do
            dfResult <- Pl.readCsv valuesCsv
            shortSeriesResult <- Pl.series @Int64 "short" (V.fromList [Just 1, Just 2])
            case (dfResult, shortSeriesResult) of
                (Right df, Right shortSeries) -> do
                    emptyColumns <- Pl.dataFrameWithColumns [] df
                    lengthMismatch <- Pl.dataFrameWithColumns [shortSeries] df
                    expectInvalidArgumentMessage "dataFrameWithColumns requires at least one Series" emptyColumns
                    expectPolarsFailure lengthMismatch
                (Left err, _) -> expectationFailure (show err)
                (_, Left err) -> expectationFailure (show err)

        it "validates eager DataFrame transform arguments" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    selectResult <- Pl.dataFrameSelect [] df
                    dropResult <- Pl.dataFrameDropColumns [] df
                    renameResult <- Pl.dataFrameRename [] df
                    sliceResult <- Pl.dataFrameSlice 0 (-1) df
                    headResult <- Pl.head (-1) df
                    tailResult <- Pl.tail (-1) df
                    dropNullsResult <- Pl.dataFrameDropNulls (Just []) df
                    case selectResult of
                        Right _ -> expectationFailure "expected InvalidArgument for empty dataFrameSelect"
                        Left err -> Pl.polarsErrorCode err `shouldBe` Pl.InvalidArgument
                    case dropResult of
                        Right _ -> expectationFailure "expected InvalidArgument for empty dataFrameDropColumns"
                        Left err -> Pl.polarsErrorCode err `shouldBe` Pl.InvalidArgument
                    case renameResult of
                        Right _ -> expectationFailure "expected InvalidArgument for empty dataFrameRename"
                        Left err -> Pl.polarsErrorCode err `shouldBe` Pl.InvalidArgument
                    case sliceResult of
                        Right _ -> expectationFailure "expected InvalidArgument for negative dataFrameSlice length"
                        Left err -> Pl.polarsErrorCode err `shouldBe` Pl.InvalidArgument
                    expectInvalidArgumentMessage "head count must be non-negative" headResult
                    expectInvalidArgumentMessage "tail count must be non-negative" tailResult
                    case dropNullsResult of
                        Right _ -> expectationFailure "expected InvalidArgument for empty dataFrameDropNulls subset"
                        Left err -> Pl.polarsErrorCode err `shouldBe` Pl.InvalidArgument

        it "constructs Series from Haskell vectors and builds a DataFrame" $ do
            nameResult <- Pl.series @T.Text "name" (V.fromList [Just "Alice", Just "Bob", Just "Carol"])
            ageResult <- Pl.series @Int64 "age" (V.fromList [Just 34, Nothing, Just 29])
            scoreResult <- Pl.series @Double "score" (V.fromList [Just 9.5, Just 8.25, Nothing])
            activeResult <- Pl.series @Bool "active" (V.fromList [Just True, Just False, Nothing])
            case (nameResult, ageResult, scoreResult, activeResult) of
                (Right name, Right age, Right score, Right active) -> do
                    dfResult <- Pl.dataFrame [name, age, score, active]
                    case dfResult of
                        Left err -> expectationFailure (show err)
                        Right df -> do
                            Pl.shape df `shouldReturn` Right (3, 4)
                            Pl.column @T.Text df "name" `shouldReturn` Right (V.fromList [Just "Alice", Just "Bob", Just "Carol"])
                            Pl.column @Int64 df "age" `shouldReturn` Right (V.fromList [Just 34, Nothing, Just 29])
                            Pl.column @Double df "score" `shouldReturn` Right (V.fromList [Just 9.5, Just 8.25, Nothing])
                            Pl.column @Bool df "active" `shouldReturn` Right (V.fromList [Just True, Just False, Nothing])
                (Left err, _, _, _) -> expectationFailure (show err)
                (_, Left err, _, _) -> expectationFailure (show err)
                (_, _, Left err, _) -> expectationFailure (show err)
                (_, _, _, Left err) -> expectationFailure (show err)

        it "constructs and extracts scalar dtype matrix values" $ do
            i8Result <- Pl.series @Int8 "i8" (V.fromList [Just (-128), Nothing, Just 127])
            i16Result <- Pl.series @Int16 "i16" (V.fromList [Just (-32768), Nothing, Just 32767])
            i32Result <- Pl.series @Int32 "i32" (V.fromList [Just (-2147483648), Nothing, Just 2147483647])
            u8Result <- Pl.series @Word8 "u8" (V.fromList [Just 0, Nothing, Just 255])
            u16Result <- Pl.series @Word16 "u16" (V.fromList [Just 0, Nothing, Just 65535])
            u32Result <- Pl.series @Word32 "u32" (V.fromList [Just 0, Nothing, Just 4294967295])
            u64Result <- Pl.series @Word64 "u64" (V.fromList [Just 0, Nothing, Just 9223372036854775808])
            f32Result <- Pl.series @Float "f32" (V.fromList [Just 1.5, Nothing, Just (-2.25)])
            case (i8Result, i16Result, i32Result, u8Result, u16Result, u32Result, u64Result, f32Result) of
                (Right i8, Right i16, Right i32, Right u8, Right u16, Right u32, Right u64, Right f32) -> do
                    dfResult <- Pl.dataFrame [i8, i16, i32, u8, u16, u32, u64, f32]
                    case dfResult of
                        Left err -> expectationFailure (show err)
                        Right df -> do
                            Pl.shape df `shouldReturn` Right (3, 8)
                            Pl.column @Int8 df "i8" `shouldReturn` Right (V.fromList [Just (-128), Nothing, Just 127])
                            Pl.column @Int16 df "i16" `shouldReturn` Right (V.fromList [Just (-32768), Nothing, Just 32767])
                            Pl.column @Int32 df "i32" `shouldReturn` Right (V.fromList [Just (-2147483648), Nothing, Just 2147483647])
                            Pl.column @Word8 df "u8" `shouldReturn` Right (V.fromList [Just 0, Nothing, Just 255])
                            Pl.column @Word16 df "u16" `shouldReturn` Right (V.fromList [Just 0, Nothing, Just 65535])
                            Pl.column @Word32 df "u32" `shouldReturn` Right (V.fromList [Just 0, Nothing, Just 4294967295])
                            Pl.column @Word64 df "u64" `shouldReturn` Right (V.fromList [Just 0, Nothing, Just 9223372036854775808])
                            Pl.column @Float df "f32" `shouldReturn` Right (V.fromList [Just 1.5, Nothing, Just (-2.25)])
                            schemaResult <- Pl.schema df
                            fmap (map Pl.fieldType) schemaResult
                                `shouldBe` Right [Pl.Int8, Pl.Int16, Pl.Int32, Pl.UInt8, Pl.UInt16, Pl.UInt32, Pl.UInt64, Pl.Float32]
                (Left err, _, _, _, _, _, _, _) -> expectationFailure (show err)
                (_, Left err, _, _, _, _, _, _) -> expectationFailure (show err)
                (_, _, Left err, _, _, _, _, _) -> expectationFailure (show err)
                (_, _, _, Left err, _, _, _, _) -> expectationFailure (show err)
                (_, _, _, _, Left err, _, _, _) -> expectationFailure (show err)
                (_, _, _, _, _, Left err, _, _) -> expectationFailure (show err)
                (_, _, _, _, _, _, Left err, _) -> expectationFailure (show err)
                (_, _, _, _, _, _, _, Left err) -> expectationFailure (show err)

        it "reports Polars errors for invalid DataFrame construction" $ do
            first <- Pl.series @Int64 "value" (V.fromList [Just 1, Just 2])
            second <- Pl.series @Int64 "value" (V.fromList [Just 3, Just 4])
            short <- Pl.series @Int64 "short" (V.fromList [Just 5])
            case (first, second, short) of
                (Right left, Right duplicate, Right shortSeries) -> do
                    duplicateResult <- Pl.dataFrame [left, duplicate]
                    case duplicateResult of
                        Right _ -> expectationFailure "expected a Polars error for duplicate names"
                        Left err -> Pl.polarsErrorCode err `shouldBe` Pl.PolarsFailure
                    lengthResult <- Pl.dataFrame [left, shortSeries]
                    case lengthResult of
                        Right _ -> expectationFailure "expected a Polars error for mismatched lengths"
                        Left err -> Pl.polarsErrorCode err `shouldBe` Pl.PolarsFailure
                (Left err, _, _) -> expectationFailure (show err)
                (_, Left err, _) -> expectationFailure (show err)
                (_, _, Left err) -> expectationFailure (show err)

    describe "Polars.LazyFrame" $ do
        it "filters, selects, and collects a lazy CSV scan" $ do
            scanResult <- Pl.scanCsv fixtureCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    filtered <- Pl.filter (Pl.col "age" Pl..> Pl.litInt 35) lf0
                    case filtered of
                        Left err -> expectationFailure (show err)
                        Right lf1 -> do
                            selected <- Pl.select [Pl.col "name"] lf1
                            case selected of
                                Left err -> expectationFailure (show err)
                                Right lf2 -> do
                                    collected <- Pl.collect lf2
                                    case collected of
                                        Left err -> expectationFailure (show err)
                                        Right df -> Pl.shape df `shouldReturn` Right (1, 1)

        it "scans CSV files with parser options" $
            withTempFileContent "polars-hs-custom-scan.csv" "Alice;34\nBob;NA\n" $ \path -> do
                let options =
                        Pl.defaultCsvReadOptions
                            { Pl.csvReadHasHeader = False
                            , Pl.csvReadSeparator = 59
                            , Pl.csvReadNullValue = Just "NA"
                            }
                scanResult <- Pl.scanCsvWith options path
                case scanResult of
                    Left err -> expectationFailure (show err)
                    Right lf -> do
                        collected <- Pl.collect lf
                        case collected of
                            Left err -> expectationFailure (show err)
                            Right df -> do
                                Pl.shape df `shouldReturn` Right (2, 2)
                                Pl.column @Int64 df "column_2" `shouldReturn` Right (V.fromList [Just 34, Nothing])

        it "scans CSV files with row controls" $
            withTempFileContent "polars-hs-scan-row-controls.csv" "metadata,skip\nname,age\nSkip,0\nAlice,34\nBob,29\nCarol,31\n" $ \path -> do
                let options =
                        Pl.defaultCsvReadOptions
                            { Pl.csvReadSkipRows = 1
                            , Pl.csvReadSkipRowsAfterHeader = 1
                            , Pl.csvReadNRows = Just 2
                            , Pl.csvReadLowMemory = True
                            , Pl.csvReadRechunk = True
                            }
                scanResult <- Pl.scanCsvWith options path
                case scanResult of
                    Left err -> expectationFailure (show err)
                    Right lf -> do
                        collected <- Pl.collect lf
                        case collected of
                            Left err -> expectationFailure (show err)
                            Right df -> do
                                Pl.shape df `shouldReturn` Right (2, 2)
                                Pl.column @T.Text df "name" `shouldReturn` Right (V.fromList [Just "Alice", Just "Bob"])

        it "scans Parquet files with scan options" $
            withTempFilePath "polars-hs-scan-options.parquet" $ \path -> do
                result <- Pl.readCsv valuesCsv
                case result of
                    Left err -> expectationFailure (show err)
                    Right sourceDf -> do
                        writeResult <- Pl.writeParquet path sourceDf
                        writeResult `shouldBe` Right ()
                        scanResult <-
                            Pl.scanParquetWith
                                Pl.defaultParquetScanOptions
                                    { Pl.parquetScanNRows = Just 2
                                    , Pl.parquetScanParallel = Pl.ParquetParallelColumns
                                    , Pl.parquetScanUseStatistics = True
                                    , Pl.parquetScanLowMemory = True
                                    , Pl.parquetScanRechunk = True
                                    , Pl.parquetScanCache = False
                                    }
                                path
                        case scanResult of
                            Left err -> expectationFailure (show err)
                            Right lf -> do
                                collected <- Pl.collect lf
                                case collected of
                                    Left err -> expectationFailure (show err)
                                    Right df -> Pl.shape df `shouldReturn` Right (2, 4)

        it "explains optimized and unoptimized lazy plans" $ do
            scanResult <- Pl.scanCsv fixtureCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    filtered <- Pl.filter (Pl.col "age" Pl..> Pl.litInt 35) lf0
                    case filtered of
                        Left err -> expectationFailure (show err)
                        Right lf1 -> do
                            optimized <- Pl.explain True lf1
                            unoptimized <- Pl.explain False lf1
                            fmap (T.isInfixOf "SCAN") optimized `shouldBe` Right True
                            fmap (T.isInfixOf "FILTER") unoptimized `shouldBe` Right True

        it "profiles lazy execution and returns result and timing frames" $ do
            scanResult <- Pl.scanCsv salesCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf -> do
                    grouped <-
                        Pl.agg
                            [ Pl.alias "salary_sum" (Pl.sum_ (Pl.col "salary"))
                            , Pl.alias "age_mean" (Pl.mean_ (Pl.col "age"))
                            ]
                            (Pl.groupByStable [Pl.col "department"] lf)
                    profiled <- case grouped of
                        Left err -> pure (Left err)
                        Right groupedLf -> Pl.profile groupedLf
                    case profiled of
                        Left err -> expectationFailure (show err)
                        Right (df, profileDf) -> do
                            Pl.shape df `shouldReturn` Right (2, 3)
                            profileFields <- Pl.schema profileDf
                            case profileFields of
                                Left err -> expectationFailure (show err)
                                Right fields -> map Pl.fieldName fields `shouldBe` ["node", "start", "end"]

        it "drops and renames lazy columns" $ do
            scanResult <- Pl.scanCsv valuesCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    dropped <- Pl.dropColumns ["score"] lf0
                    case dropped of
                        Left err -> expectationFailure (show err)
                        Right lf1 -> do
                            renamed <- Pl.rename Pl.defaultRenameOptions [("age", "years")] lf1
                            case renamed of
                                Left err -> expectationFailure (show err)
                                Right lf2 -> do
                                    collected <- Pl.collect lf2
                                    case collected of
                                        Left err -> expectationFailure (show err)
                                        Right df -> do
                                            fields <- Pl.schema df
                                            fmap (map Pl.fieldName) fields `shouldBe` Right ["name", "years", "active"]
                                            Pl.column @Int64 df "years" `shouldReturn` Right (V.fromList [Just 34, Nothing, Just 29])

        it "slices lazy rows and takes lazy head and tail" $ do
            scanResult <- Pl.scanCsv valuesCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    sliced <- Pl.slice 1 2 lf0
                    headed <- Pl.lazyHead 2 lf0
                    tailed <- Pl.lazyTail 1 lf0
                    case (sliced, headed, tailed) of
                        (Right sliceLf, Right headLf, Right tailLf) -> do
                            sliceDf <- Pl.collect sliceLf
                            headDf <- Pl.collect headLf
                            tailDf <- Pl.collect tailLf
                            case (sliceDf, headDf, tailDf) of
                                (Right sDf, Right hDf, Right tDf) -> do
                                    Pl.column @T.Text sDf "name" `shouldReturn` Right (V.fromList [Just "Bob", Just "Carol"])
                                    Pl.column @T.Text hDf "name" `shouldReturn` Right (V.fromList [Just "Alice", Just "Bob"])
                                    Pl.column @T.Text tDf "name" `shouldReturn` Right (V.fromList [Just "Carol"])
                                (Left err, _, _) -> expectationFailure (show err)
                                (_, Left err, _) -> expectationFailure (show err)
                                (_, _, Left err) -> expectationFailure (show err)
                        (Left err, _, _) -> expectationFailure (show err)
                        (_, Left err, _) -> expectationFailure (show err)
                        (_, _, Left err) -> expectationFailure (show err)

        it "selects lazy top and bottom rows by expressions" $ do
            scanResult <- Pl.scanCsv employeesCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf -> do
                    let bySalary = Pl.defaultLazyFrameTopKOptions {Pl.lazyFrameTopKBy = [Pl.col "salary"]}
                    topResult <- Pl.topK bySalary 2 lf
                    bottomResult <- Pl.bottomK bySalary 2 lf
                    reversedTopResult <- Pl.topK bySalary {Pl.lazyFrameTopKReverse = [True]} 2 lf
                    maintainedResult <- Pl.topK bySalary {Pl.lazyFrameTopKMaintainOrder = True} 2 lf
                    headLikeResult <- Pl.topK Pl.defaultLazyFrameTopKOptions 2 lf
                    case (topResult, bottomResult, reversedTopResult, maintainedResult, headLikeResult) of
                        (Right topLf, Right bottomLf, Right reversedTopLf, Right maintainedLf, Right headLikeLf) -> do
                            topDf <- Pl.collect topLf
                            bottomDf <- Pl.collect bottomLf
                            reversedTopDf <- Pl.collect reversedTopLf
                            maintainedDf <- Pl.collect maintainedLf
                            headLikeDf <- Pl.collect headLikeLf
                            case (topDf, bottomDf, reversedTopDf, maintainedDf, headLikeDf) of
                                (Right topDf', Right bottomDf', Right reversedTopDf', Right maintainedDf', Right headLikeDf') -> do
                                    Pl.column @Int64 topDf' "salary" `shouldReturn` Right (V.fromList [Just 150, Just 100])
                                    Pl.column @Int64 bottomDf' "salary" `shouldReturn` Right (V.fromList [Just 80, Just 90])
                                    Pl.column @Int64 reversedTopDf' "salary" `shouldReturn` Right (V.fromList [Just 80, Just 90])
                                    Pl.column @Int64 maintainedDf' "salary" `shouldReturn` Right (V.fromList [Just 150, Just 100])
                                    Pl.column @T.Text headLikeDf' "name" `shouldReturn` Right (V.fromList [Just "Alice", Just "Bob"])
                                (Left err, _, _, _, _) -> expectationFailure (show err)
                                (_, Left err, _, _, _) -> expectationFailure (show err)
                                (_, _, Left err, _, _) -> expectationFailure (show err)
                                (_, _, _, Left err, _) -> expectationFailure (show err)
                                (_, _, _, _, Left err) -> expectationFailure (show err)
                        (Left err, _, _, _, _) -> expectationFailure (show err)
                        (_, Left err, _, _, _) -> expectationFailure (show err)
                        (_, _, Left err, _, _) -> expectationFailure (show err)
                        (_, _, _, Left err, _) -> expectationFailure (show err)
                        (_, _, _, _, Left err) -> expectationFailure (show err)

        it "explodes lazy list columns" $ do
            scanResult <- Pl.scanCsv phrasesCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    withParts <- Pl.withColumns [Pl.alias "parts" (Pl.strSplit (Pl.col "phrase") (Pl.litText " "))] lf0
                    case withParts of
                        Left err -> expectationFailure (show err)
                        Right lf1 -> do
                            exploded <-
                                Pl.explode
                                    Pl.defaultLazyFrameExplodeOptions {Pl.lazyFrameExplodeColumns = ["parts"]}
                                    lf1
                            emptyColumns <- Pl.explode Pl.defaultLazyFrameExplodeOptions lf1
                            missingColumn <-
                                Pl.explode
                                    Pl.defaultLazyFrameExplodeOptions {Pl.lazyFrameExplodeColumns = ["missing"]}
                                    lf1
                            scalarColumn <-
                                Pl.explode
                                    Pl.defaultLazyFrameExplodeOptions {Pl.lazyFrameExplodeColumns = ["phrase"]}
                                    lf1
                            case exploded of
                                Left err -> expectationFailure (show err)
                                Right explodedLf -> do
                                    collected <- Pl.collect explodedLf
                                    case collected of
                                        Left err -> expectationFailure (show err)
                                        Right df -> do
                                            Pl.shape df `shouldReturn` Right (8, 2)
                                            Pl.column @T.Text df "phrase"
                                                `shouldReturn` Right
                                                    ( V.fromList
                                                        [ Just "red green blue"
                                                        , Just "red green blue"
                                                        , Just "red green blue"
                                                        , Just "red red"
                                                        , Just "red red"
                                                        , Just "日本 語"
                                                        , Just "日本 語"
                                                        , Just "solo"
                                                        ]
                                                    )
                                            Pl.column @T.Text df "parts"
                                                `shouldReturn` Right
                                                    ( V.fromList
                                                        [ Just "red"
                                                        , Just "green"
                                                        , Just "blue"
                                                        , Just "red"
                                                        , Just "red"
                                                        , Just "日本"
                                                        , Just "語"
                                                        , Just "solo"
                                                        ]
                                                    )
                            expectInvalidArgumentMessage "explode requires at least one column name" emptyColumns
                            expectLazyCollectPolarsFailure missingColumn
                            expectLazyCollectPolarsFailure scalarColumn

        it "reverses lazy row order" $ do
            scanResult <- Pl.scanCsv valuesCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    reversed <- Pl.reverse lf0
                    case reversed of
                        Left err -> expectationFailure (show err)
                        Right reversedLf -> do
                            collected <- Pl.collect reversedLf
                            case collected of
                                Left err -> expectationFailure (show err)
                                Right df -> do
                                    Pl.shape df `shouldReturn` Right (3, 4)
                                    Pl.column @T.Text df "name" `shouldReturn` Right (V.fromList [Just "Carol", Just "Bob", Just "Alice"])
                                    Pl.column @Int64 df "age" `shouldReturn` Right (V.fromList [Just 29, Nothing, Just 34])

        it "adds lazy row index columns" $ do
            scanResult <- Pl.scanCsv valuesCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    indexed <- Pl.withRowIndex "row_nr" (Just 10) lf0
                    defaultIndexed <- Pl.withRowIndex "row_nr" Nothing lf0
                    duplicate <- Pl.withRowIndex "name" Nothing lf0
                    negative <- Pl.withRowIndex "row_nr" (Just (-1)) lf0
                    case (indexed, defaultIndexed) of
                        (Right indexedLf, Right defaultLf) -> do
                            indexedDf <- Pl.collect indexedLf
                            defaultDf <- Pl.collect defaultLf
                            case (indexedDf, defaultDf) of
                                (Right indexedDf', Right defaultDf') -> do
                                    fields <- Pl.schema indexedDf'
                                    fmap (map Pl.fieldName) fields `shouldBe` Right ["row_nr", "name", "age", "score", "active"]
                                    Pl.column @Word32 indexedDf' "row_nr" `shouldReturn` Right (V.fromList [Just 10, Just 11, Just 12])
                                    Pl.column @Word32 defaultDf' "row_nr" `shouldReturn` Right (V.fromList [Just 0, Just 1, Just 2])
                                (Left err, _) -> expectationFailure (show err)
                                (_, Left err) -> expectationFailure (show err)
                        (Left err, _) -> expectationFailure (show err)
                        (_, Left err) -> expectationFailure (show err)
                    expectLazyCollectPolarsFailure duplicate
                    expectInvalidArgumentMessage "withRowIndex offset must be non-negative" negative

        it "gathers every nth lazy row" $ do
            scanResult <- Pl.scanCsv valuesCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    everyTwo <- Pl.gatherEvery 2 0 lf0
                    offsetRows <- Pl.gatherEvery 2 1 lf0
                    emptyRows <- Pl.gatherEvery 2 10 lf0
                    zeroStep <- Pl.gatherEvery 0 0 lf0
                    negativeStep <- Pl.gatherEvery (-1) 0 lf0
                    negativeOffset <- Pl.gatherEvery 2 (-1) lf0
                    case (everyTwo, offsetRows, emptyRows) of
                        (Right everyTwoLf, Right offsetLf, Right emptyLf) -> do
                            everyTwoDf <- Pl.collect everyTwoLf
                            offsetDf <- Pl.collect offsetLf
                            emptyDf <- Pl.collect emptyLf
                            case (everyTwoDf, offsetDf, emptyDf) of
                                (Right everyTwoDf', Right offsetDf', Right emptyDf') -> do
                                    Pl.shape everyTwoDf' `shouldReturn` Right (2, 4)
                                    Pl.column @T.Text everyTwoDf' "name" `shouldReturn` Right (V.fromList [Just "Alice", Just "Carol"])
                                    Pl.column @Int64 everyTwoDf' "age" `shouldReturn` Right (V.fromList [Just 34, Just 29])
                                    Pl.shape offsetDf' `shouldReturn` Right (1, 4)
                                    Pl.column @T.Text offsetDf' "name" `shouldReturn` Right (V.fromList [Just "Bob"])
                                    Pl.shape emptyDf' `shouldReturn` Right (0, 4)
                                (Left err, _, _) -> expectationFailure (show err)
                                (_, Left err, _) -> expectationFailure (show err)
                                (_, _, Left err) -> expectationFailure (show err)
                        (Left err, _, _) -> expectationFailure (show err)
                        (_, Left err, _) -> expectationFailure (show err)
                        (_, _, Left err) -> expectationFailure (show err)
                    expectInvalidArgumentMessage "gatherEvery step must be positive" zeroStep
                    expectInvalidArgumentMessage "gatherEvery step must be non-negative" negativeStep
                    expectInvalidArgumentMessage "gatherEvery offset must be non-negative" negativeOffset

        it "unpivots lazy frames from wide to long format" $ do
            scanResult <- Pl.scanCsv employeesCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    explicitOut <-
                        Pl.unpivot
                            Pl.defaultLazyFrameUnpivotOptions
                                { Pl.lazyFrameUnpivotIndex = ["department"]
                                , Pl.lazyFrameUnpivotOn = Just ["salary"]
                                , Pl.lazyFrameUnpivotVariableName = Just "metric"
                                , Pl.lazyFrameUnpivotValueName = Just "amount"
                                }
                            lf0
                    defaultOnOut <-
                        Pl.unpivot
                            Pl.defaultLazyFrameUnpivotOptions
                                { Pl.lazyFrameUnpivotIndex = ["department"]
                                , Pl.lazyFrameUnpivotOn = Nothing
                                }
                            lf0
                    emptyOnOut <-
                        Pl.unpivot
                            Pl.defaultLazyFrameUnpivotOptions
                                { Pl.lazyFrameUnpivotIndex = ["department"]
                                , Pl.lazyFrameUnpivotOn = Just []
                                }
                            lf0
                    missingColumn <-
                        Pl.unpivot
                            Pl.defaultLazyFrameUnpivotOptions
                                { Pl.lazyFrameUnpivotIndex = ["missing"]
                                , Pl.lazyFrameUnpivotOn = Just ["salary"]
                                }
                            lf0
                    case (explicitOut, defaultOnOut, emptyOnOut) of
                        (Right explicitLf, Right defaultLf, Right emptyLf) -> do
                            explicitDf <- Pl.collect explicitLf
                            defaultDf <- Pl.collect defaultLf
                            emptyDf <- Pl.collect emptyLf
                            case (explicitDf, defaultDf, emptyDf) of
                                (Right explicitDf', Right defaultDf', Right emptyDf') -> do
                                    Pl.shape explicitDf' `shouldReturn` Right (4, 3)
                                    explicitSchema <- Pl.schema explicitDf'
                                    fmap (map Pl.fieldName) explicitSchema `shouldBe` Right ["department", "metric", "amount"]
                                    Pl.column @T.Text explicitDf' "department"
                                        `shouldReturn` Right (V.fromList [Just "Engineering", Just "Engineering", Just "Sales", Just "Support"])
                                    Pl.column @T.Text explicitDf' "metric"
                                        `shouldReturn` Right (V.fromList [Just "salary", Just "salary", Just "salary", Just "salary"])
                                    Pl.column @Int64 explicitDf' "amount"
                                        `shouldReturn` Right (V.fromList [Just 100, Just 150, Just 90, Just 80])
                                    Pl.shape defaultDf' `shouldReturn` Right (12, 3)
                                    Pl.column @T.Text defaultDf' "variable"
                                        `shouldReturn` Right
                                            ( V.fromList
                                                [ Just "id"
                                                , Just "id"
                                                , Just "id"
                                                , Just "id"
                                                , Just "name"
                                                , Just "name"
                                                , Just "name"
                                                , Just "name"
                                                , Just "salary"
                                                , Just "salary"
                                                , Just "salary"
                                                , Just "salary"
                                                ]
                                            )
                                    Pl.shape emptyDf' `shouldReturn` Right (0, 3)
                                    emptySchema <- Pl.schema emptyDf'
                                    fmap (map Pl.fieldName) emptySchema `shouldBe` Right ["department", "variable", "value"]
                                (Left err, _, _) -> expectationFailure (show err)
                                (_, Left err, _) -> expectationFailure (show err)
                                (_, _, Left err) -> expectationFailure (show err)
                        (Left err, _, _) -> expectationFailure (show err)
                        (_, Left err, _) -> expectationFailure (show err)
                        (_, _, Left err) -> expectationFailure (show err)
                    expectLazyCollectPolarsFailure missingColumn

        it "validates lazy top and bottom row arguments" $ do
            scanResult <- Pl.scanCsv employeesCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf -> do
                    let bySalary = Pl.defaultLazyFrameTopKOptions {Pl.lazyFrameTopKBy = [Pl.col "salary"]}
                    negative <- Pl.topK bySalary (-1) lf
                    emptyReverse <- Pl.topK bySalary {Pl.lazyFrameTopKReverse = []} 1 lf
                    mismatchedReverse <- Pl.bottomK bySalary {Pl.lazyFrameTopKReverse = [False, True]} 1 lf
                    expectInvalidArgumentMessage "topK count must be non-negative" negative
                    expectInvalidArgumentMessage "topK reverse must contain one value or one value per sort expression" emptyReverse
                    expectInvalidArgumentMessage "bottomK reverse must contain one value or one value per sort expression" mismatchedReverse

        it "drops nulls and fills nulls and NaNs in lazy frames" $ do
            valuesScan <- Pl.scanCsv valuesCsv
            specialsScan <- Pl.scanCsv floatSpecialsCsv
            case (valuesScan, specialsScan) of
                (Right valuesLf, Right specialsLf) -> do
                    dense <- Pl.dropNulls Nothing valuesLf
                    ageOnly <- Pl.select [Pl.col "age"] valuesLf
                    filledNulls <- case ageOnly of
                        Left err -> pure (Left err)
                        Right lf -> Pl.fillNulls (Pl.litInt 0) lf
                    filledNans <- Pl.fillNans (Pl.litDouble 0.0) specialsLf
                    case (dense, filledNulls, filledNans) of
                        (Right denseLf, Right nullLf, Right nanLf) -> do
                            denseDf <- Pl.collect denseLf
                            nullDf <- Pl.collect nullLf
                            nanDf <- Pl.collect nanLf
                            case (denseDf, nullDf, nanDf) of
                                (Right dDf, Right nDf, Right fDf) -> do
                                    Pl.column @T.Text dDf "name" `shouldReturn` Right (V.fromList [Just "Alice"])
                                    Pl.column @Int64 nDf "age" `shouldReturn` Right (V.fromList [Just 34, Just 0, Just 29])
                                    values <- Pl.column @Double fDf "value"
                                    case values of
                                        Left err -> expectationFailure (show err)
                                        Right actual -> shouldApproximate 1.0e-12 (V.fromList [Just 1.0, Just 0.0, Just (1 / 0), Just (-(1 / 0))]) actual
                                (Left err, _, _) -> expectationFailure (show err)
                                (_, Left err, _) -> expectationFailure (show err)
                                (_, _, Left err) -> expectationFailure (show err)
                        (Left err, _, _) -> expectationFailure (show err)
                        (_, Left err, _) -> expectationFailure (show err)
                        (_, _, Left err) -> expectationFailure (show err)
                (Left err, _) -> expectationFailure (show err)
                (_, Left err) -> expectationFailure (show err)

        it "counts nulls and keeps unique rows by subset" $ do
            valuesScan <- Pl.scanCsv valuesCsv
            salesScan <- Pl.scanCsv salesCsv
            case (valuesScan, salesScan) of
                (Right valuesLf, Right salesLf) -> do
                    counts <- Pl.nullCount valuesLf
                    uniqueDepartments <-
                        Pl.unique
                            Pl.defaultUniqueOptions
                                { Pl.uniqueSubset = Just ["department"]
                                , Pl.uniqueKeepStrategy = Pl.KeepFirst
                                , Pl.uniqueMaintainOrder = True
                                }
                            salesLf
                    case (counts, uniqueDepartments) of
                        (Right countsLf, Right uniqueLf) -> do
                            countsDf <- Pl.collect countsLf
                            uniqueDf <- Pl.collect uniqueLf
                            case (countsDf, uniqueDf) of
                                (Right cDf, Right uDf) -> do
                                    Pl.column @Word32 cDf "age" `shouldReturn` Right (V.fromList [Just 1])
                                    Pl.column @Word32 cDf "score" `shouldReturn` Right (V.fromList [Just 1])
                                    Pl.column @Word32 cDf "active" `shouldReturn` Right (V.fromList [Just 1])
                                    Pl.column @T.Text uDf "name" `shouldReturn` Right (V.fromList [Just "Alice", Just "Carol"])
                                (Left err, _) -> expectationFailure (show err)
                                (_, Left err) -> expectationFailure (show err)
                        (Left err, _) -> expectationFailure (show err)
                        (_, Left err) -> expectationFailure (show err)
                (Left err, _) -> expectationFailure (show err)
                (_, Left err) -> expectationFailure (show err)

        it "validates lazy transform arguments" $ do
            scanResult <- Pl.scanCsv valuesCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf -> do
                    dropResult <- Pl.dropColumns [] lf
                    renameResult <- Pl.rename Pl.defaultRenameOptions [] lf
                    sliceResult <- Pl.slice 0 (-1) lf
                    headResult <- Pl.lazyHead (-1) lf
                    dropNullsResult <- Pl.dropNulls (Just []) lf
                    uniqueResult <- Pl.unique Pl.defaultUniqueOptions {Pl.uniqueSubset = Just []} lf
                    case dropResult of
                        Right _ -> expectationFailure "expected InvalidArgument for empty dropColumns"
                        Left err -> Pl.polarsErrorCode err `shouldBe` Pl.InvalidArgument
                    case renameResult of
                        Right _ -> expectationFailure "expected InvalidArgument for empty rename"
                        Left err -> Pl.polarsErrorCode err `shouldBe` Pl.InvalidArgument
                    case sliceResult of
                        Right _ -> expectationFailure "expected InvalidArgument for negative slice length"
                        Left err -> Pl.polarsErrorCode err `shouldBe` Pl.InvalidArgument
                    case headResult of
                        Right _ -> expectationFailure "expected InvalidArgument for negative lazyHead"
                        Left err -> Pl.polarsErrorCode err `shouldBe` Pl.InvalidArgument
                    case dropNullsResult of
                        Right _ -> expectationFailure "expected InvalidArgument for empty dropNulls subset"
                        Left err -> Pl.polarsErrorCode err `shouldBe` Pl.InvalidArgument
                    case uniqueResult of
                        Right _ -> expectationFailure "expected InvalidArgument for empty unique subset"
                        Left err -> Pl.polarsErrorCode err `shouldBe` Pl.InvalidArgument

    describe "Rust Polars parity harness" $ do
        it "matches Rust Polars for CSV parser options" $
            withTempFileContent "polars-hs-oracle-csv.csv" "Alice;34\nBob;NA\n" $ \path -> do
                let options =
                        Pl.defaultCsvReadOptions
                            { Pl.csvReadHasHeader = False
                            , Pl.csvReadSeparator = 59
                            , Pl.csvReadNullValue = Just "NA"
                            }
                oracle <- runRustOracle ["csv-read-options", path]
                result <- Pl.readCsvWith options path
                case result of
                    Left err -> expectationFailure (show err)
                    Right df -> do
                        actual <- canonicalDataFrameCsv df
                        actual `shouldBe` oracle

        it "matches Rust Polars for CSV row controls" $
            withTempFileContent "polars-hs-oracle-csv-rows.csv" "metadata,skip\nname,age\nSkip,0\nAlice,34\nBob,29\nCarol,31\n" $ \path -> do
                let options =
                        Pl.defaultCsvReadOptions
                            { Pl.csvReadSkipRows = 1
                            , Pl.csvReadSkipRowsAfterHeader = 1
                            , Pl.csvReadNRows = Just 2
                            , Pl.csvReadLowMemory = True
                            , Pl.csvReadRechunk = True
                            }
                oracle <- runRustOracle ["csv-read-row-options", path]
                result <- Pl.readCsvWith options path
                case result of
                    Left err -> expectationFailure (show err)
                    Right df -> do
                        actual <- canonicalDataFrameCsv df
                        actual `shouldBe` oracle

        it "matches Rust Polars for Parquet read options" $
            withTempFilePath "polars-hs-oracle.parquet" $ \path -> do
                sourceResult <- Pl.readCsv valuesCsv
                case sourceResult of
                    Left err -> expectationFailure (show err)
                    Right sourceDf -> do
                        writeResult <- Pl.writeParquet path sourceDf
                        writeResult `shouldBe` Right ()
                        oracle <- runRustOracle ["parquet-read-options-phase2", path]
                        result <-
                            Pl.readParquetWith
                                Pl.defaultParquetReadOptions
                                    { Pl.parquetReadNRows = Just 2
                                    , Pl.parquetReadParallel = Pl.ParquetParallelRowGroups
                                    , Pl.parquetReadLowMemory = True
                                    , Pl.parquetReadRechunk = True
                                    }
                                path
                        case result of
                            Left err -> expectationFailure (show err)
                            Right df -> do
                                actual <- canonicalDataFrameCsv df
                                actual `shouldBe` oracle

    describe "Polars.GroupBy" $ do
        it "groups a lazy CSV scan and aggregates columns" $ do
            scanResult <- Pl.scanCsv salesCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    groupedResult <-
                        Pl.agg
                            [ Pl.alias "salary_sum" (Pl.sum_ (Pl.col "salary"))
                            , Pl.alias "age_mean" (Pl.mean_ (Pl.col "age"))
                            , Pl.alias "people" (Pl.count_ (Pl.col "name"))
                            ]
                            (Pl.groupByStable [Pl.col "department"] lf0)
                    case groupedResult of
                        Left err -> expectationFailure (show err)
                        Right lf1 -> do
                            collected <- Pl.collect lf1
                            case collected of
                                Left err -> expectationFailure (show err)
                                Right df -> do
                                    Pl.shape df `shouldReturn` Right (2, 4)
                                    schemaResult <- Pl.schema df
                                    fmap (map Pl.fieldName) schemaResult
                                        `shouldBe` Right ["department", "salary_sum", "age_mean", "people"]
                                    textResult <- Pl.toText df
                                    fmap (T.isInfixOf "Engineering") textResult `shouldBe` Right True
                                    fmap (T.isInfixOf "250") textResult `shouldBe` Right True
                                    fmap (T.isInfixOf "Sales") textResult `shouldBe` Right True
                                    fmap (T.isInfixOf "200") textResult `shouldBe` Right True

        it "rejects an empty aggregation list" $ do
            scanResult <- Pl.scanCsv salesCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf -> do
                    result <- Pl.agg [] (Pl.groupBy [Pl.col "department"] lf)
                    case result of
                        Right _ -> expectationFailure "expected InvalidArgument for empty aggregation list"
                        Left err -> Pl.polarsErrorCode err `shouldBe` Pl.InvalidArgument

        it "reports missing aggregation columns during collect" $ do
            scanResult <- Pl.scanCsv salesCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    groupedResult <-
                        Pl.agg
                            [Pl.alias "missing_sum" (Pl.sum_ (Pl.col "missing"))]
                            (Pl.groupBy [Pl.col "department"] lf0)
                    case groupedResult of
                        Left err -> expectationFailure (show err)
                        Right lf1 -> do
                            collected <- Pl.collect lf1
                            case collected of
                                Right _ -> expectationFailure "expected a Polars failure for missing column"
                                Left err -> Pl.polarsErrorCode err `shouldBe` Pl.PolarsFailure

    describe "Polars.Column" $ do
        it "extracts text columns with null preservation" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> Pl.columnText df "name" `shouldReturn` Right (V.fromList [Just "Alice", Just "Bob", Just "Carol"])

        it "extracts int64 columns with null preservation" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> Pl.columnInt64 df "age" `shouldReturn` Right (V.fromList [Just 34, Nothing, Just 29])

        it "extracts double columns with null preservation" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> Pl.columnDouble df "score" `shouldReturn` Right (V.fromList [Just 9.5, Just 8.25, Nothing])

        it "extracts bool columns with null preservation" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> Pl.columnBool df "active" `shouldReturn` Right (V.fromList [Just True, Just False, Nothing])

        it "reports a Polars error for missing columns" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    columnResult <- Pl.columnText df "missing"
                    case columnResult of
                        Right _ -> expectationFailure "expected a Polars error for a missing column"
                        Left err -> Pl.polarsErrorCode err `shouldBe` Pl.PolarsFailure

        it "reports a Polars error for column dtype mismatches" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    columnResult <- Pl.columnInt64 df "name"
                    case columnResult of
                        Right _ -> expectationFailure "expected a Polars error for a dtype mismatch"
                        Left err -> Pl.polarsErrorCode err `shouldBe` Pl.PolarsFailure

        it "extracts grouped aggregation result columns" $ do
            scanResult <- Pl.scanCsv salesCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    groupedResult <-
                        Pl.agg
                            [Pl.alias "salary_sum" (Pl.sum_ (Pl.col "salary"))]
                            (Pl.groupByStable [Pl.col "department"] lf0)
                    case groupedResult of
                        Left err -> expectationFailure (show err)
                        Right lf1 -> do
                            collected <- Pl.collect lf1
                            case collected of
                                Left err -> expectationFailure (show err)
                                Right df -> Pl.columnInt64 df "salary_sum" `shouldReturn` Right (V.fromList [Just 250, Just 200])

        it "extracts join result columns with null preservation" $ do
            employeesResult <- Pl.scanCsv employeesCsv
            departmentsResult <- Pl.scanCsv departmentsCsv
            case (employeesResult, departmentsResult) of
                (Right employees, Right departments) -> do
                    let options =
                            Pl.defaultJoinOptions
                                { Pl.joinType = Pl.JoinLeft
                                , Pl.leftOn = [Pl.col "department"]
                                , Pl.rightOn = [Pl.col "department"]
                                , Pl.suffix = Just "_dept"
                                }
                    joined <- Pl.joinWith options employees departments
                    case joined of
                        Left err -> expectationFailure (show err)
                        Right lf -> do
                            collected <- Pl.collect lf
                            case collected of
                                Left err -> expectationFailure (show err)
                                Right df -> Pl.columnText df "name_dept" `shouldReturn` Right (V.fromList [Just "Grace", Just "Grace", Just "Heidi", Nothing])
                (Left err, _) -> expectationFailure (show err)
                (_, Left err) -> expectationFailure (show err)

        it "selects a column as a Series handle and reports metadata" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    seriesResult <- Pl.column @Pl.Series df "age"
                    case seriesResult of
                        Left err -> expectationFailure (show err)
                        Right age -> do
                            Pl.seriesName age `shouldReturn` Right "age"
                            Pl.seriesLength age `shouldReturn` Right 3
                            Pl.seriesNullCount age `shouldReturn` Right 1
                            Pl.seriesDataType age `shouldReturn` Right Pl.Int64
                            Pl.seriesInt64 age `shouldReturn` Right (V.fromList [Just 34, Nothing, Just 29])

        it "uses visible type applications for typed column values" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    Pl.column @Int64 df "age" `shouldReturn` Right (V.fromList [Just 34, Nothing, Just 29])
                    Pl.column @Double df "score" `shouldReturn` Right (V.fromList [Just 9.5, Just 8.25, Nothing])
                    Pl.column @T.Text df "name" `shouldReturn` Right (V.fromList [Just "Alice", Just "Bob", Just "Carol"])
                    Pl.column @Bool df "active" `shouldReturn` Right (V.fromList [Just True, Just False, Nothing])

        it "slices Series handles and converts them to DataFrames" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    seriesResult <- Pl.column @Pl.Series df "age"
                    case seriesResult of
                        Left err -> expectationFailure (show err)
                        Right age -> do
                            headResult <- Pl.seriesHead 2 age
                            case headResult of
                                Left err -> expectationFailure (show err)
                                Right firstTwo -> Pl.seriesInt64 firstTwo `shouldReturn` Right (V.fromList [Just 34, Nothing])
                            tailResult <- Pl.seriesTail 1 age
                            case tailResult of
                                Left err -> expectationFailure (show err)
                                Right lastOne -> Pl.seriesInt64 lastOne `shouldReturn` Right (V.fromList [Just 29])
                            sliceResult <- Pl.seriesSlice 1 2 age
                            case sliceResult of
                                Left err -> expectationFailure (show err)
                                Right middleTwo -> Pl.seriesInt64 middleTwo `shouldReturn` Right (V.fromList [Nothing, Just 29])
                            frameResult <- Pl.seriesToFrame age
                            case frameResult of
                                Left err -> expectationFailure (show err)
                                Right oneColumn -> Pl.shape oneColumn `shouldReturn` Right (3, 1)

        it "filters Series handles with boolean masks" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    ageResult <- Pl.column @Pl.Series df "age"
                    case ageResult of
                        Left err -> expectationFailure (show err)
                        Right age -> do
                            maskResult <- Pl.seriesIsNotNull age
                            case maskResult of
                                Left err -> expectationFailure (show err)
                                Right mask -> do
                                    Pl.seriesBool mask `shouldReturn` Right (V.fromList [Just True, Just False, Just True])
                                    filtered <- Pl.seriesFilter mask age
                                    case filtered of
                                        Left err -> expectationFailure (show err)
                                        Right denseAge -> Pl.seriesInt64 denseAge `shouldReturn` Right (V.fromList [Just 34, Just 29])

        it "reports Polars errors for invalid Series filter masks" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    ageResult <- Pl.column @Pl.Series df "age"
                    wrongLengthMask <- Pl.series @Bool "mask" (V.fromList [Just True, Just False])
                    case (ageResult, wrongLengthMask) of
                        (Right age, Right mask) -> do
                            wrongDtype <- Pl.seriesFilter age age
                            wrongLength <- Pl.seriesFilter mask age
                            expectPolarsFailure wrongDtype
                            expectPolarsFailure wrongLength
                        (Left err, _) -> expectationFailure (show err)
                        (_, Left err) -> expectationFailure (show err)

        it "treats null Series filter mask values as false" $ do
            result <- Pl.readCsv valuesCsv
            maskResult <- Pl.series @Bool "mask" (V.fromList [Just True, Nothing, Just False])
            case (result, maskResult) of
                (Right df, Right mask) -> do
                    ageResult <- Pl.column @Pl.Series df "age"
                    case ageResult of
                        Right age -> do
                            filtered <- Pl.seriesFilter mask age
                            case filtered of
                                Right filteredAge -> Pl.seriesInt64 filteredAge `shouldReturn` Right (V.fromList [Just 34])
                                Left err -> expectationFailure (show err)
                        Left err -> expectationFailure (show err)
                (Left err, _) -> expectationFailure (show err)
                (_, Left err) -> expectationFailure (show err)

        it "takes Series values by explicit indices" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    ageResult <- Pl.column @Pl.Series df "age"
                    case ageResult of
                        Left err -> expectationFailure (show err)
                        Right age -> do
                            taken <- Pl.seriesTake (V.fromList [2, 0, 2]) age
                            empty <- Pl.seriesTake V.empty age
                            case (taken, empty) of
                                (Right takenAge, Right emptyAge) -> do
                                    Pl.seriesInt64 takenAge `shouldReturn` Right (V.fromList [Just 29, Just 34, Just 29])
                                    Pl.seriesLength emptyAge `shouldReturn` Right 0
                                (Left err, _) -> expectationFailure (show err)
                                (_, Left err) -> expectationFailure (show err)

        it "preserves nulls when taking Series values by index" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    ageResult <- Pl.column @Pl.Series df "age"
                    case ageResult of
                        Left err -> expectationFailure (show err)
                        Right age -> do
                            taken <- Pl.seriesTake (V.fromList [1, 2, 1, 0]) age
                            case taken of
                                Left err -> expectationFailure (show err)
                                Right takenAge ->
                                    Pl.seriesInt64 takenAge `shouldReturn` Right (V.fromList [Nothing, Just 29, Nothing, Just 34])

        it "reports errors for invalid Series take indices" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    ageResult <- Pl.column @Pl.Series df "age"
                    case ageResult of
                        Left err -> expectationFailure (show err)
                        Right age -> do
                            outOfBounds <- Pl.seriesTake (V.fromList [0, 3]) age
                            let overflowingIndex = fromIntegral (maxBound :: Word32) + 1
                            overflow <- Pl.seriesTake (V.fromList [overflowingIndex]) age
                            expectPolarsFailure outOfBounds
                            expectInvalidArgumentMessage "series take index exceeds Polars index size" overflow

        it "computes Series NaN and infinity predicates" $ do
            result <- Pl.readCsv floatSpecialsCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    valueResult <- Pl.column @Pl.Series df "value"
                    case valueResult of
                        Left err -> expectationFailure (show err)
                        Right value -> do
                            isNanResult <- Pl.seriesIsNan value
                            isNotNanResult <- Pl.seriesIsNotNan value
                            isFiniteResult <- Pl.seriesIsFinite value
                            isInfiniteResult <- Pl.seriesIsInfinite value
                            case (isNanResult, isNotNanResult, isFiniteResult, isInfiniteResult) of
                                (Right isNanSeries, Right isNotNanSeries, Right isFiniteSeries, Right isInfiniteSeries) -> do
                                    Pl.seriesBool isNanSeries `shouldReturn` Right (V.fromList [Just False, Just True, Just False, Just False])
                                    Pl.seriesBool isNotNanSeries `shouldReturn` Right (V.fromList [Just True, Just False, Just True, Just True])
                                    Pl.seriesBool isFiniteSeries `shouldReturn` Right (V.fromList [Just True, Just False, Just False, Just False])
                                    Pl.seriesBool isInfiniteSeries `shouldReturn` Right (V.fromList [Just False, Just False, Just True, Just True])
                                (Left err, _, _, _) -> expectationFailure (show err)
                                (_, Left err, _, _) -> expectationFailure (show err)
                                (_, _, Left err, _) -> expectationFailure (show err)
                                (_, _, _, Left err) -> expectationFailure (show err)

        it "preserves null validity for integer Series float predicates" $ do
            seriesResult <- Pl.series @Int64 "numbers" (V.fromList [Just 1, Nothing, Just 3])
            case seriesResult of
                Left err -> expectationFailure (show err)
                Right numbers -> do
                    isNanResult <- Pl.seriesIsNan numbers
                    isNotNanResult <- Pl.seriesIsNotNan numbers
                    isFiniteResult <- Pl.seriesIsFinite numbers
                    isInfiniteResult <- Pl.seriesIsInfinite numbers
                    case (isNanResult, isNotNanResult, isFiniteResult, isInfiniteResult) of
                        (Right isNanSeries, Right isNotNanSeries, Right isFiniteSeries, Right isInfiniteSeries) -> do
                            Pl.seriesBool isNanSeries `shouldReturn` Right (V.fromList [Just False, Nothing, Just False])
                            Pl.seriesBool isNotNanSeries `shouldReturn` Right (V.fromList [Just True, Nothing, Just True])
                            Pl.seriesBool isFiniteSeries `shouldReturn` Right (V.fromList [Just True, Nothing, Just True])
                            Pl.seriesBool isInfiniteSeries `shouldReturn` Right (V.fromList [Just False, Nothing, Just False])
                        (Left err, _, _, _) -> expectationFailure (show err)
                        (_, Left err, _, _) -> expectationFailure (show err)
                        (_, _, Left err, _) -> expectationFailure (show err)
                        (_, _, _, Left err) -> expectationFailure (show err)

        it "reports Polars errors for invalid Series float predicate dtypes" $ do
            textResult <- Pl.series @T.Text "text" (V.fromList [Just "a", Just "b"])
            case textResult of
                Left err -> expectationFailure (show err)
                Right textSeries -> do
                    isNanResult <- Pl.seriesIsNan textSeries
                    isNotNanResult <- Pl.seriesIsNotNan textSeries
                    isFiniteResult <- Pl.seriesIsFinite textSeries
                    isInfiniteResult <- Pl.seriesIsInfinite textSeries
                    expectPolarsFailure isNanResult
                    expectPolarsFailure isNotNanResult
                    expectPolarsFailure isFiniteResult
                    expectPolarsFailure isInfiniteResult

        it "computes Series distinct predicates" $ do
            seriesResult <- Pl.series @T.Text "value" (V.fromList [Just "a", Just "b", Just "a", Nothing, Nothing, Just "c"])
            case seriesResult of
                Left err -> expectationFailure (show err)
                Right values -> do
                    duplicatedResult <- Pl.seriesIsDuplicated values
                    uniqueResult <- Pl.seriesIsUnique values
                    firstResult <- Pl.seriesIsFirstDistinct values
                    lastResult <- Pl.seriesIsLastDistinct values
                    case (duplicatedResult, uniqueResult, firstResult, lastResult) of
                        (Right duplicated, Right unique, Right firstDistinct, Right lastDistinct) -> do
                            Pl.seriesBool duplicated
                                `shouldReturn` Right (V.fromList [Just True, Just False, Just True, Just True, Just True, Just False])
                            Pl.seriesBool unique
                                `shouldReturn` Right (V.fromList [Just False, Just True, Just False, Just False, Just False, Just True])
                            Pl.seriesBool firstDistinct
                                `shouldReturn` Right (V.fromList [Just True, Just True, Just False, Just True, Just False, Just True])
                            Pl.seriesBool lastDistinct
                                `shouldReturn` Right (V.fromList [Just False, Just True, Just True, Just False, Just True, Just True])
                        (Left err, _, _, _) -> expectationFailure (show err)
                        (_, Left err, _, _) -> expectationFailure (show err)
                        (_, _, Left err, _) -> expectationFailure (show err)
                        (_, _, _, Left err) -> expectationFailure (show err)

        it "computes Series distinct predicates for numeric and edge inputs" $ do
            numericResult <- Pl.series @Int64 "number" (V.fromList [Just 1, Just 1, Just 2, Nothing])
            boolResult <- Pl.series @Bool "flag" (V.fromList [Just True, Just False, Just True])
            emptyResult <- Pl.series @Int64 "empty" V.empty
            singletonResult <- Pl.series @Bool "single" (V.singleton (Just True))
            case (numericResult, boolResult, emptyResult, singletonResult) of
                (Right numbers, Right flags, Right empty, Right singleton) -> do
                    duplicatedNumbers <- Pl.seriesIsDuplicated numbers
                    uniqueNumbers <- Pl.seriesIsUnique numbers
                    firstNumbers <- Pl.seriesIsFirstDistinct numbers
                    lastNumbers <- Pl.seriesIsLastDistinct numbers
                    case (duplicatedNumbers, uniqueNumbers, firstNumbers, lastNumbers) of
                        (Right duplicated, Right unique, Right firstDistinct, Right lastDistinct) -> do
                            Pl.seriesBool duplicated `shouldReturn` Right (V.fromList [Just True, Just True, Just False, Just False])
                            Pl.seriesBool unique `shouldReturn` Right (V.fromList [Just False, Just False, Just True, Just True])
                            Pl.seriesBool firstDistinct `shouldReturn` Right (V.fromList [Just True, Just False, Just True, Just True])
                            Pl.seriesBool lastDistinct `shouldReturn` Right (V.fromList [Just False, Just True, Just True, Just True])
                        (Left err, _, _, _) -> expectationFailure (show err)
                        (_, Left err, _, _) -> expectationFailure (show err)
                        (_, _, Left err, _) -> expectationFailure (show err)
                        (_, _, _, Left err) -> expectationFailure (show err)

                    duplicatedFlags <- Pl.seriesIsDuplicated flags
                    firstEmpty <- Pl.seriesIsFirstDistinct empty
                    lastSingleton <- Pl.seriesIsLastDistinct singleton
                    case (duplicatedFlags, firstEmpty, lastSingleton) of
                        (Right duplicated, Right emptyMask, Right singletonMask) -> do
                            Pl.seriesBool duplicated `shouldReturn` Right (V.fromList [Just True, Just False, Just True])
                            Pl.seriesBool emptyMask `shouldReturn` Right V.empty
                            Pl.seriesBool singletonMask `shouldReturn` Right (V.singleton (Just True))
                        (Left err, _, _) -> expectationFailure (show err)
                        (_, Left err, _) -> expectationFailure (show err)
                        (_, _, Left err) -> expectationFailure (show err)
                (Left err, _, _, _) -> expectationFailure (show err)
                (_, Left err, _, _) -> expectationFailure (show err)
                (_, _, Left err, _) -> expectationFailure (show err)
                (_, _, _, Left err) -> expectationFailure (show err)

        it "applies Series absolute value transforms" $ do
            intResult <- Pl.series @Int64 "number" (V.fromList [Just (-3), Just 0, Nothing, Just 4])
            doubleResult <- Pl.series @Double "float" (V.fromList [Just (-1.5), Nothing, Just 2.25])
            textResult <- Pl.series @T.Text "text" (V.fromList [Just "a"])
            case (intResult, doubleResult, textResult) of
                (Right numbers, Right floats, Right textSeries) -> do
                    absNumbers <- Pl.seriesAbs numbers
                    absFloats <- Pl.seriesAbs floats
                    absText <- Pl.seriesAbs textSeries
                    case (absNumbers, absFloats) of
                        (Right numberSeries, Right floatSeries) -> do
                            Pl.seriesInt64 numberSeries `shouldReturn` Right (V.fromList [Just 3, Just 0, Nothing, Just 4])
                            Pl.seriesDouble floatSeries `shouldReturn` Right (V.fromList [Just 1.5, Nothing, Just 2.25])
                        (Left err, _) -> expectationFailure (show err)
                        (_, Left err) -> expectationFailure (show err)
                    expectPolarsFailure absText
                (Left err, _, _) -> expectationFailure (show err)
                (_, Left err, _) -> expectationFailure (show err)
                (_, _, Left err) -> expectationFailure (show err)

        it "rounds floors and ceils Series values" $ do
            valuesResult <- Pl.series @Double "value" (V.fromList [Just 2.5, Just 3.5, Just (-2.5), Just 1.25, Nothing])
            preciseResult <- Pl.series @Double "precise" (V.fromList [Just 1.234, Just (-1.235), Nothing])
            intResult <- Pl.series @Int64 "number" (V.fromList [Just 2, Just (-3), Nothing])
            textResult <- Pl.series @T.Text "text" (V.fromList [Just "a"])
            case (valuesResult, preciseResult, intResult, textResult) of
                (Right values, Right precise, Right numbers, Right textSeries) -> do
                    roundEven <- Pl.seriesRound 0 Pl.RoundHalfToEven values
                    roundAway <- Pl.seriesRound 0 Pl.RoundHalfAwayFromZero values
                    roundTwo <- Pl.seriesRound 2 Pl.RoundHalfAwayFromZero precise
                    floorResult <- Pl.seriesFloor values
                    ceilResult <- Pl.seriesCeil values
                    roundInts <- Pl.seriesRound 0 Pl.RoundHalfAwayFromZero numbers
                    floorInts <- Pl.seriesFloor numbers
                    ceilInts <- Pl.seriesCeil numbers
                    case (roundEven, roundAway, roundTwo, floorResult, ceilResult, roundInts, floorInts, ceilInts) of
                        (Right evenSeries, Right awaySeries, Right roundTwoSeries, Right floorSeries, Right ceilSeries, Right roundedNumbers, Right flooredNumbers, Right ceiledNumbers) -> do
                            Pl.seriesDouble evenSeries `shouldReturn` Right (V.fromList [Just 2.0, Just 4.0, Just (-2.0), Just 1.0, Nothing])
                            Pl.seriesDouble awaySeries `shouldReturn` Right (V.fromList [Just 3.0, Just 4.0, Just (-3.0), Just 1.0, Nothing])
                            Pl.seriesDouble roundTwoSeries `shouldReturn` Right (V.fromList [Just 1.23, Just (-1.24), Nothing])
                            Pl.seriesDouble floorSeries `shouldReturn` Right (V.fromList [Just 2.0, Just 3.0, Just (-3.0), Just 1.0, Nothing])
                            Pl.seriesDouble ceilSeries `shouldReturn` Right (V.fromList [Just 3.0, Just 4.0, Just (-2.0), Just 2.0, Nothing])
                            Pl.seriesInt64 roundedNumbers `shouldReturn` Right (V.fromList [Just 2, Just (-3), Nothing])
                            Pl.seriesInt64 flooredNumbers `shouldReturn` Right (V.fromList [Just 2, Just (-3), Nothing])
                            Pl.seriesInt64 ceiledNumbers `shouldReturn` Right (V.fromList [Just 2, Just (-3), Nothing])
                        (Left err, _, _, _, _, _, _, _) -> expectationFailure (show err)
                        (_, Left err, _, _, _, _, _, _) -> expectationFailure (show err)
                        (_, _, Left err, _, _, _, _, _) -> expectationFailure (show err)
                        (_, _, _, Left err, _, _, _, _) -> expectationFailure (show err)
                        (_, _, _, _, Left err, _, _, _) -> expectationFailure (show err)
                        (_, _, _, _, _, Left err, _, _) -> expectationFailure (show err)
                        (_, _, _, _, _, _, Left err, _) -> expectationFailure (show err)
                        (_, _, _, _, _, _, _, Left err) -> expectationFailure (show err)

                    negativeDecimals <- Pl.seriesRound (-1) Pl.RoundHalfToEven values
                    tooLargeDecimals <- Pl.seriesRound (fromIntegral (maxBound :: Word32) + 1) Pl.RoundHalfToEven values
                    roundText <- Pl.seriesRound 0 Pl.RoundHalfToEven textSeries
                    floorText <- Pl.seriesFloor textSeries
                    ceilText <- Pl.seriesCeil textSeries
                    expectInvalidArgumentMessage "seriesRound decimals must be non-negative" negativeDecimals
                    expectInvalidArgumentMessage "seriesRound decimals exceeds Word32 range" tooLargeDecimals
                    expectPolarsFailure roundText
                    expectPolarsFailure floorText
                    expectPolarsFailure ceilText
                (Left err, _, _, _) -> expectationFailure (show err)
                (_, Left err, _, _) -> expectationFailure (show err)
                (_, _, Left err, _) -> expectationFailure (show err)
                (_, _, _, Left err) -> expectationFailure (show err)

        it "computes Series differences" $ do
            let int64MaxAsWord64 = fromIntegral (maxBound :: Int64) :: Word64
            valuesResult <- Pl.series @Int64 "value" (V.fromList [Just 10, Just 13, Nothing, Just 20])
            unsignedResult <- Pl.series @Word8 "small" (V.fromList [Just 1, Just 4, Just 9])
            unsigned16Result <- Pl.series @Word16 "medium" (V.fromList [Just 1, Just 4, Just 9])
            unsigned32Result <- Pl.series @Word32 "large" (V.fromList [Just 1, Just 4, Just 9])
            unsigned64Result <- Pl.series @Word64 "wide" (V.fromList [Just 1, Just 4, Just 9])
            overflow64Result <- Pl.series @Word64 "overflow" (V.fromList [Just (int64MaxAsWord64 + 1), Just (int64MaxAsWord64 + 3)])
            textResult <- Pl.series @T.Text "text" (V.fromList [Just "a", Just "b"])
            case (valuesResult, unsignedResult, unsigned16Result, unsigned32Result, unsigned64Result, overflow64Result, textResult) of
                (Right values, Right unsigned, Right unsigned16, Right unsigned32, Right unsigned64, Right overflow64, Right textSeries) -> do
                    ignoreResult <- Pl.seriesDiff 1 Pl.SeriesDiffIgnore values
                    ignoreNegativeResult <- Pl.seriesDiff (-1) Pl.SeriesDiffIgnore values
                    ignoreTooLargeResult <- Pl.seriesDiff 10 Pl.SeriesDiffIgnore values
                    zeroResult <- Pl.seriesDiff 0 Pl.SeriesDiffIgnore values
                    dropResult <- Pl.seriesDiff 1 Pl.SeriesDiffDrop values
                    negativeDropResult <- Pl.seriesDiff (-1) Pl.SeriesDiffDrop values
                    dropTooLargeResult <- Pl.seriesDiff 10 Pl.SeriesDiffDrop values
                    dropNegativeTooLargeResult <- Pl.seriesDiff (-10) Pl.SeriesDiffDrop values
                    unsignedResult' <- Pl.seriesDiff 1 Pl.SeriesDiffIgnore unsigned
                    unsigned16Result' <- Pl.seriesDiff 1 Pl.SeriesDiffIgnore unsigned16
                    unsigned32Result' <- Pl.seriesDiff 1 Pl.SeriesDiffIgnore unsigned32
                    unsigned64Result' <- Pl.seriesDiff 1 Pl.SeriesDiffIgnore unsigned64
                    overflow64Result' <- Pl.seriesDiff 1 Pl.SeriesDiffIgnore overflow64
                    textDiff <- Pl.seriesDiff 1 Pl.SeriesDiffIgnore textSeries
                    case (ignoreResult, ignoreNegativeResult, ignoreTooLargeResult, zeroResult, dropResult, negativeDropResult, unsignedResult', unsigned16Result', unsigned32Result', unsigned64Result', overflow64Result') of
                        (Right ignoreSeries, Right ignoreNegativeSeries, Right ignoreTooLargeSeries, Right zeroSeries, Right dropSeries, Right negativeDropSeries, Right unsignedSeries, Right unsigned16Series, Right unsigned32Series, Right unsigned64Series, Right overflow64Series) -> do
                            Pl.seriesInt64 ignoreSeries `shouldReturn` Right (V.fromList [Nothing, Just 3, Nothing, Nothing])
                            Pl.seriesInt64 ignoreNegativeSeries `shouldReturn` Right (V.fromList [Just (-3), Nothing, Nothing, Nothing])
                            Pl.seriesInt64 ignoreTooLargeSeries `shouldReturn` Right (V.fromList [Nothing, Nothing, Nothing, Nothing])
                            Pl.seriesInt64 zeroSeries `shouldReturn` Right (V.fromList [Just 0, Just 0, Nothing, Just 0])
                            Pl.seriesInt64 dropSeries `shouldReturn` Right (V.fromList [Just 3, Nothing, Nothing])
                            Pl.seriesInt64 negativeDropSeries `shouldReturn` Right (V.fromList [Just (-3), Nothing, Nothing])
                            Pl.seriesInt16 unsignedSeries `shouldReturn` Right (V.fromList [Nothing, Just 3, Just 5])
                            Pl.seriesInt32 unsigned16Series `shouldReturn` Right (V.fromList [Nothing, Just 3, Just 5])
                            Pl.seriesInt64 unsigned32Series `shouldReturn` Right (V.fromList [Nothing, Just 3, Just 5])
                            Pl.seriesInt64 unsigned64Series `shouldReturn` Right (V.fromList [Nothing, Just 3, Just 5])
                            Pl.seriesInt64 overflow64Series `shouldReturn` Right (V.fromList [Nothing, Nothing])
                        (Left err, _, _, _, _, _, _, _, _, _, _) -> expectationFailure (show err)
                        (_, Left err, _, _, _, _, _, _, _, _, _) -> expectationFailure (show err)
                        (_, _, Left err, _, _, _, _, _, _, _, _) -> expectationFailure (show err)
                        (_, _, _, Left err, _, _, _, _, _, _, _) -> expectationFailure (show err)
                        (_, _, _, _, Left err, _, _, _, _, _, _) -> expectationFailure (show err)
                        (_, _, _, _, _, Left err, _, _, _, _, _) -> expectationFailure (show err)
                        (_, _, _, _, _, _, Left err, _, _, _, _) -> expectationFailure (show err)
                        (_, _, _, _, _, _, _, Left err, _, _, _) -> expectationFailure (show err)
                        (_, _, _, _, _, _, _, _, Left err, _, _) -> expectationFailure (show err)
                        (_, _, _, _, _, _, _, _, _, Left err, _) -> expectationFailure (show err)
                        (_, _, _, _, _, _, _, _, _, _, Left err) -> expectationFailure (show err)
                    expectInvalidArgumentMessage "series diff period 10 exceeds series length 4" dropTooLargeResult
                    expectInvalidArgumentMessage "series diff period 10 exceeds series length 4" dropNegativeTooLargeResult
                    expectPolarsFailure textDiff
                (Left err, _, _, _, _, _, _) -> expectationFailure (show err)
                (_, Left err, _, _, _, _, _) -> expectationFailure (show err)
                (_, _, Left err, _, _, _, _) -> expectationFailure (show err)
                (_, _, _, Left err, _, _, _) -> expectationFailure (show err)
                (_, _, _, _, Left err, _, _) -> expectationFailure (show err)
                (_, _, _, _, _, Left err, _) -> expectationFailure (show err)
                (_, _, _, _, _, _, Left err) -> expectationFailure (show err)

        it "interpolates Series nulls" $ do
            valuesResult <- Pl.series @Word32 "value" (V.fromList [Nothing, Just 1, Nothing, Nothing, Just 4, Just 5, Nothing])
            descendingResult <- Pl.series @Word32 "descending" (V.fromList [Just 4, Nothing, Nothing, Just 1])
            doubleResult <- Pl.series @Double "double" (V.fromList [Just 1.5, Nothing, Just 4.5])
            allNullResult <- Pl.series @Word32 "all_null" (V.fromList [Nothing, Nothing, Nothing])
            textResult <- Pl.series @T.Text "text" (V.fromList [Just "a", Nothing, Just "b"])
            boolResult <- Pl.series @Bool "flag" (V.fromList [Just True, Nothing, Just False])
            case (valuesResult, descendingResult, doubleResult, allNullResult, textResult, boolResult) of
                (Right values, Right descending, Right doubles, Right allNull, Right textSeries, Right boolSeries) -> do
                    linearResult <- Pl.seriesInterpolate Pl.SeriesInterpolateLinear values
                    nearestResult <- Pl.seriesInterpolate Pl.SeriesInterpolateNearest values
                    descendingResult' <- Pl.seriesInterpolate Pl.SeriesInterpolateLinear descending
                    doubleResult' <- Pl.seriesInterpolate Pl.SeriesInterpolateLinear doubles
                    allNullResult' <- Pl.seriesInterpolate Pl.SeriesInterpolateLinear allNull
                    linearTextResult <- Pl.seriesInterpolate Pl.SeriesInterpolateLinear textSeries
                    nearestTextResult <- Pl.seriesInterpolate Pl.SeriesInterpolateNearest textSeries
                    nearestBoolResult <- Pl.seriesInterpolate Pl.SeriesInterpolateNearest boolSeries
                    case (linearResult, nearestResult, descendingResult', doubleResult', allNullResult', linearTextResult) of
                        (Right linear, Right nearest, Right descendingLinear, Right doubleLinear, Right allNullLinear, Right linearText) -> do
                            Pl.seriesDouble linear `shouldReturn` Right (V.fromList [Nothing, Just 1.0, Just 2.0, Just 3.0, Just 4.0, Just 5.0, Nothing])
                            Pl.seriesWord32 nearest `shouldReturn` Right (V.fromList [Nothing, Just 1, Just 1, Just 4, Just 4, Just 5, Nothing])
                            Pl.seriesDouble descendingLinear `shouldReturn` Right (V.fromList [Just 4.0, Just 3.0, Just 2.0, Just 1.0])
                            Pl.seriesDouble doubleLinear `shouldReturn` Right (V.fromList [Just 1.5, Just 3.0, Just 4.5])
                            Pl.seriesDouble allNullLinear `shouldReturn` Right (V.fromList [Nothing, Nothing, Nothing])
                            Pl.seriesText linearText `shouldReturn` Right (V.fromList [Just "a", Nothing, Just "b"])
                        (Left err, _, _, _, _, _) -> expectationFailure (show err)
                        (_, Left err, _, _, _, _) -> expectationFailure (show err)
                        (_, _, Left err, _, _, _) -> expectationFailure (show err)
                        (_, _, _, Left err, _, _) -> expectationFailure (show err)
                        (_, _, _, _, Left err, _) -> expectationFailure (show err)
                        (_, _, _, _, _, Left err) -> expectationFailure (show err)
                    expectInvalidArgumentMessage "series interpolate nearest is unsupported for dtype String" nearestTextResult
                    expectInvalidArgumentMessage "series interpolate nearest is unsupported for dtype Boolean" nearestBoolResult
                (Left err, _, _, _, _, _) -> expectationFailure (show err)
                (_, Left err, _, _, _, _) -> expectationFailure (show err)
                (_, _, Left err, _, _, _) -> expectationFailure (show err)
                (_, _, _, Left err, _, _) -> expectationFailure (show err)
                (_, _, _, _, Left err, _) -> expectationFailure (show err)
                (_, _, _, _, _, Left err) -> expectationFailure (show err)

        it "fills Series nulls with strategies" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    ageResult <- Pl.column @Pl.Series df "age"
                    case ageResult of
                        Left err -> expectationFailure (show err)
                        Right age -> do
                            forward <- Pl.seriesFillNull (Pl.FillForward Nothing) age
                            backward <- Pl.seriesFillNull (Pl.FillBackward Nothing) age
                            zero <- Pl.seriesFillNull Pl.FillZero age
                            case (forward, backward, zero) of
                                (Right forwardAge, Right backwardAge, Right zeroAge) -> do
                                    Pl.seriesInt64 forwardAge `shouldReturn` Right (V.fromList [Just 34, Just 34, Just 29])
                                    Pl.seriesInt64 backwardAge `shouldReturn` Right (V.fromList [Just 34, Just 29, Just 29])
                                    Pl.seriesInt64 zeroAge `shouldReturn` Right (V.fromList [Just 34, Just 0, Just 29])
                                (Left err, _, _) -> expectationFailure (show err)
                                (_, Left err, _) -> expectationFailure (show err)
                                (_, _, Left err) -> expectationFailure (show err)

        it "reports InvalidArgument for invalid Series fill-null limits" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    ageResult <- Pl.column @Pl.Series df "age"
                    case ageResult of
                        Left err -> expectationFailure (show err)
                        Right age -> do
                            negative <- Pl.seriesFillNull (Pl.FillForward (Just (-1))) age
                            let overflowingLimit = fromIntegral (maxBound :: Word32) + 1
                            overflow <- Pl.seriesFillNull (Pl.FillBackward (Just overflowingLimit)) age
                            expectInvalidArgumentMessage "fill null limit must be non-negative" negative
                            expectInvalidArgumentMessage "fill null limit exceeds Polars index size" overflow

        it "applies Series arithmetic with null propagation" $ do
            leftResult <- Pl.series @Double "left" (V.fromList [Just 10.0, Nothing, Just 7.5])
            rightResult <- Pl.series @Double "right" (V.fromList [Just 2.0, Just 4.0, Just 2.5])
            case (leftResult, rightResult) of
                (Right left, Right right) -> do
                    added <- Pl.seriesAdd left right
                    subtracted <- Pl.seriesSub left right
                    multiplied <- Pl.seriesMul left right
                    divided <- Pl.seriesDiv left right
                    remaindered <- Pl.seriesRem left right
                    case (added, subtracted, multiplied, divided, remaindered) of
                        (Right addSeries, Right subSeries, Right mulSeries, Right divSeries, Right remSeries) -> do
                            addValues <- Pl.seriesDouble addSeries
                            subValues <- Pl.seriesDouble subSeries
                            mulValues <- Pl.seriesDouble mulSeries
                            divValues <- Pl.seriesDouble divSeries
                            remValues <- Pl.seriesDouble remSeries
                            case (addValues, subValues, mulValues, divValues, remValues) of
                                (Right addVec, Right subVec, Right mulVec, Right divVec, Right remVec) -> do
                                    shouldApproximate 1.0e-12 (V.fromList [Just 12.0, Nothing, Just 10.0]) addVec
                                    shouldApproximate 1.0e-12 (V.fromList [Just 8.0, Nothing, Just 5.0]) subVec
                                    shouldApproximate 1.0e-12 (V.fromList [Just 20.0, Nothing, Just 18.75]) mulVec
                                    shouldApproximate 1.0e-12 (V.fromList [Just 5.0, Nothing, Just 3.0]) divVec
                                    shouldApproximate 1.0e-12 (V.fromList [Just 0.0, Nothing, Just 0.0]) remVec
                                (Left err, _, _, _, _) -> expectationFailure (show err)
                                (_, Left err, _, _, _) -> expectationFailure (show err)
                                (_, _, Left err, _, _) -> expectationFailure (show err)
                                (_, _, _, Left err, _) -> expectationFailure (show err)
                                (_, _, _, _, Left err) -> expectationFailure (show err)
                        (Left err, _, _, _, _) -> expectationFailure (show err)
                        (_, Left err, _, _, _) -> expectationFailure (show err)
                        (_, _, Left err, _, _) -> expectationFailure (show err)
                        (_, _, _, Left err, _) -> expectationFailure (show err)
                        (_, _, _, _, Left err) -> expectationFailure (show err)
                (Left err, _) -> expectationFailure (show err)
                (_, Left err) -> expectationFailure (show err)

        it "reports Polars errors for invalid Series arithmetic" $ do
            numbersResult <- Pl.series @Double "numbers" (V.fromList [Just 1.0, Just 2.0, Just 3.0])
            shortResult <- Pl.series @Double "short" (V.fromList [Just 1.0, Just 2.0])
            textResult <- Pl.series @T.Text "text" (V.fromList [Just "a", Just "b", Just "c"])
            case (numbersResult, shortResult, textResult) of
                (Right numbers, Right short, Right textSeries) -> do
                    lengthMismatch <- Pl.seriesAdd numbers short
                    invalidDtype <- Pl.seriesSub textSeries numbers
                    expectPolarsFailure lengthMismatch
                    expectPolarsFailure invalidDtype
                (Left err, _, _) -> expectationFailure (show err)
                (_, Left err, _) -> expectationFailure (show err)
                (_, _, Left err) -> expectationFailure (show err)

        it "compares Series values into boolean masks" $ do
            leftResult <- Pl.series @Int64 "left" (V.fromList [Just 1, Just 2, Nothing, Just 4])
            rightResult <- Pl.series @Int64 "right" (V.fromList [Just 1, Just 3, Nothing, Just 2])
            scalarResult <- Pl.series @Int64 "scalar" (V.singleton (Just 2))
            textResult <- Pl.series @T.Text "text" (V.fromList [Just "a", Just "b", Nothing, Just "d"])
            textScalarResult <- Pl.series @T.Text "text_scalar" (V.singleton (Just "b"))
            shortResult <- Pl.series @Int64 "short" (V.fromList [Just 1, Just 2])
            case (leftResult, rightResult, scalarResult, textResult, textScalarResult, shortResult) of
                (Right left, Right right, Right scalar, Right textValues, Right textScalar, Right short) -> do
                    equal <- Pl.seriesEqual left right
                    notEqual <- Pl.seriesNotEqual left right
                    equalMissing <- Pl.seriesEqualMissing left right
                    notEqualMissing <- Pl.seriesNotEqualMissing left right
                    greater <- Pl.seriesGreater left right
                    greaterEqual <- Pl.seriesGreaterEqual left right
                    less <- Pl.seriesLess left right
                    lessEqual <- Pl.seriesLessEqual left right
                    scalarGreater <- Pl.seriesGreater left scalar
                    textLess <- Pl.seriesLess textValues textScalar
                    textGreaterEqual <- Pl.seriesGreaterEqual textValues textScalar
                    lengthMismatch <- Pl.seriesEqual left short
                    dtypeMismatch <- Pl.seriesGreater left textScalar
                    case
                        ( equal
                        , notEqual
                        , equalMissing
                        , notEqualMissing
                        , greater
                        , greaterEqual
                        , less
                        , lessEqual
                        , scalarGreater
                        , textLess
                        , textGreaterEqual
                        ) of
                            ( Right equalOut
                                , Right notEqualOut
                                , Right equalMissingOut
                                , Right notEqualMissingOut
                                , Right greaterOut
                                , Right greaterEqualOut
                                , Right lessOut
                                , Right lessEqualOut
                                , Right scalarGreaterOut
                                , Right textLessOut
                                , Right textGreaterEqualOut
                                ) -> do
                                    Pl.seriesBool equalOut `shouldReturn` Right (V.fromList [Just True, Just False, Nothing, Just False])
                                    Pl.seriesBool notEqualOut `shouldReturn` Right (V.fromList [Just False, Just True, Nothing, Just True])
                                    Pl.seriesBool equalMissingOut `shouldReturn` Right (V.fromList [Just True, Just False, Just True, Just False])
                                    Pl.seriesBool notEqualMissingOut `shouldReturn` Right (V.fromList [Just False, Just True, Just False, Just True])
                                    Pl.seriesBool greaterOut `shouldReturn` Right (V.fromList [Just False, Just False, Nothing, Just True])
                                    Pl.seriesBool greaterEqualOut `shouldReturn` Right (V.fromList [Just True, Just False, Nothing, Just True])
                                    Pl.seriesBool lessOut `shouldReturn` Right (V.fromList [Just False, Just True, Nothing, Just False])
                                    Pl.seriesBool lessEqualOut `shouldReturn` Right (V.fromList [Just True, Just True, Nothing, Just False])
                                    Pl.seriesBool scalarGreaterOut `shouldReturn` Right (V.fromList [Just False, Just False, Nothing, Just True])
                                    Pl.seriesBool textLessOut `shouldReturn` Right (V.fromList [Just True, Just False, Nothing, Just False])
                                    Pl.seriesBool textGreaterEqualOut `shouldReturn` Right (V.fromList [Just False, Just True, Nothing, Just True])
                                    expectPolarsFailure lengthMismatch
                                    expectPolarsFailure dtypeMismatch
                            (Left err, _, _, _, _, _, _, _, _, _, _) -> expectationFailure (show err)
                            (_, Left err, _, _, _, _, _, _, _, _, _) -> expectationFailure (show err)
                            (_, _, Left err, _, _, _, _, _, _, _, _) -> expectationFailure (show err)
                            (_, _, _, Left err, _, _, _, _, _, _, _) -> expectationFailure (show err)
                            (_, _, _, _, Left err, _, _, _, _, _, _) -> expectationFailure (show err)
                            (_, _, _, _, _, Left err, _, _, _, _, _) -> expectationFailure (show err)
                            (_, _, _, _, _, _, Left err, _, _, _, _) -> expectationFailure (show err)
                            (_, _, _, _, _, _, _, Left err, _, _, _) -> expectationFailure (show err)
                            (_, _, _, _, _, _, _, _, Left err, _, _) -> expectationFailure (show err)
                            (_, _, _, _, _, _, _, _, _, Left err, _) -> expectationFailure (show err)
                            (_, _, _, _, _, _, _, _, _, _, Left err) -> expectationFailure (show err)
                (Left err, _, _, _, _, _) -> expectationFailure (show err)
                (_, Left err, _, _, _, _) -> expectationFailure (show err)
                (_, _, Left err, _, _, _) -> expectationFailure (show err)
                (_, _, _, Left err, _, _) -> expectationFailure (show err)
                (_, _, _, _, Left err, _) -> expectationFailure (show err)
                (_, _, _, _, _, Left err) -> expectationFailure (show err)

        it "samples and shuffles Series values" $ do
            valuesResult <- Pl.series @Int64 "value" (V.fromList (Just <$> [10, 20, 30, 40, 50]))
            singleResult <- Pl.series @Int64 "single" (V.singleton (Just 42))
            textResult <- Pl.series @T.Text "text" (V.fromList [Just "a", Nothing, Just "c", Just "d", Just "e"])
            let seeded =
                    Pl.defaultSeriesSampleOptions
                        { Pl.seriesSampleSeed = Just 0
                        }
                replacement =
                    Pl.defaultSeriesSampleOptions
                        { Pl.seriesSampleWithReplacement = True
                        , Pl.seriesSampleSeed = Just 0
                        }
                replacementShuffle =
                    replacement
                        { Pl.seriesSampleShuffle = True
                        , Pl.seriesSampleSeed = Just 13
                        }
            case (valuesResult, singleResult, textResult) of
                (Right values, Right single, Right textValues) -> do
                    sampled <- Pl.seriesSampleN seeded 2 values
                    fracSampled <- Pl.seriesSampleFrac seeded 0.4 values
                    shuffled <- Pl.seriesShuffle (Just 0) values
                    sampledWithReplacement <- Pl.seriesSampleN replacement 7 values
                    sampledEmpty <- Pl.seriesSampleN seeded 0 values
                    sampledRepeated <- Pl.seriesSampleN replacementShuffle 4 single
                    fracRepeated <- Pl.seriesSampleFrac replacementShuffle 3.0 single
                    textAll <- Pl.seriesSampleN seeded 5 textValues
                    tooLarge <- Pl.seriesSampleN seeded 6 values
                    negative <- Pl.seriesSampleN seeded (-1) values
                    negativeFrac <- Pl.seriesSampleFrac seeded (-0.1) values
                    nanFrac <- Pl.seriesSampleFrac seeded (0 / 0) values
                    tooLargeFrac <- Pl.seriesSampleFrac seeded 1.1 values
                    case
                        ( sampled
                        , fracSampled
                        , shuffled
                        , sampledWithReplacement
                        , sampledEmpty
                        , sampledRepeated
                        , fracRepeated
                        , textAll
                        ) of
                            ( Right sampledOut
                                , Right fracSampledOut
                                , Right shuffledOut
                                , Right sampledWithReplacementOut
                                , Right sampledEmptyOut
                                , Right sampledRepeatedOut
                                , Right fracRepeatedOut
                                , Right textAllOut
                                ) -> do
                                    Pl.seriesInt64 sampledOut `shouldReturn` Right (V.fromList [Just 50, Just 20])
                                    Pl.seriesInt64 fracSampledOut `shouldReturn` Right (V.fromList [Just 50, Just 20])
                                    Pl.seriesInt64 shuffledOut `shouldReturn` Right (V.fromList [Just 40, Just 10, Just 20, Just 50, Just 30])
                                    Pl.seriesInt64 sampledWithReplacementOut `shouldReturn` Right (V.fromList [Just 20, Just 20, Just 20, Just 10, Just 30, Just 10, Just 50])
                                    Pl.seriesInt64 sampledEmptyOut `shouldReturn` Right V.empty
                                    Pl.seriesInt64 sampledRepeatedOut `shouldReturn` Right (V.fromList [Just 42, Just 42, Just 42, Just 42])
                                    Pl.seriesInt64 fracRepeatedOut `shouldReturn` Right (V.fromList [Just 42, Just 42, Just 42])
                                    Pl.seriesText textAllOut `shouldReturn` Right (V.fromList [Just "a", Nothing, Just "c", Just "d", Just "e"])
                                    expectPolarsFailure tooLarge
                                    expectInvalidArgumentMessage "series sample size must be non-negative" negative
                                    expectInvalidArgumentMessage "series sample fraction must be non-negative" negativeFrac
                                    expectInvalidArgumentMessage "series sample fraction must be finite" nanFrac
                                    expectInvalidArgumentMessage "series sample fraction must be at most 1.0 without replacement" tooLargeFrac
                            (Left err, _, _, _, _, _, _, _) -> expectationFailure (show err)
                            (_, Left err, _, _, _, _, _, _) -> expectationFailure (show err)
                            (_, _, Left err, _, _, _, _, _) -> expectationFailure (show err)
                            (_, _, _, Left err, _, _, _, _) -> expectationFailure (show err)
                            (_, _, _, _, Left err, _, _, _) -> expectationFailure (show err)
                            (_, _, _, _, _, Left err, _, _) -> expectationFailure (show err)
                            (_, _, _, _, _, _, Left err, _) -> expectationFailure (show err)
                            (_, _, _, _, _, _, _, Left err) -> expectationFailure (show err)
                (Left err, _, _) -> expectationFailure (show err)
                (_, Left err, _) -> expectationFailure (show err)
                (_, _, Left err) -> expectationFailure (show err)

        it "computes Series scalar statistics" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    scoreResult <- Pl.column @Pl.Series df "score"
                    case scoreResult of
                        Left err -> expectationFailure (show err)
                        Right score -> do
                            meanResult <- Pl.seriesMean score
                            stdResult <- Pl.seriesStd 1 score
                            varResult <- Pl.seriesVar 1 score
                            case (meanResult, stdResult, varResult) of
                                (Right meanValue, Right stdValue, Right varValue) -> do
                                    shouldApproximateMaybe 1.0e-12 (Just 8.875) meanValue
                                    shouldApproximateMaybe 1.0e-12 (Just 0.8838834764831844) stdValue
                                    shouldApproximateMaybe 1.0e-12 (Just 0.78125) varValue
                                (Left err, _, _) -> expectationFailure (show err)
                                (_, Left err, _) -> expectationFailure (show err)
                                (_, _, Left err) -> expectationFailure (show err)

        it "computes Series median and unique counts" $ do
            result <- Pl.readCsv valuesCsv
            textResult <- Pl.series @T.Text "text" (V.fromList [Just "a", Just "b", Just "a", Nothing])
            boolResult <- Pl.series @Bool "flag" (V.fromList [Just True, Just False, Nothing, Just True])
            allNullResult <- Pl.series @Double "all_null" (V.fromList [Nothing, Nothing])
            emptyResult <- Pl.series @Double "empty" V.empty
            case (result, textResult, boolResult, allNullResult, emptyResult) of
                (Right df, Right textSeries, Right boolSeries, Right allNull, Right empty) -> do
                    scoreResult <- Pl.column @Pl.Series df "score"
                    case scoreResult of
                        Left err -> expectationFailure (show err)
                        Right score -> do
                            medianResult <- Pl.seriesMedian score
                            textMedianResult <- Pl.seriesMedian textSeries
                            allNullMedianResult <- Pl.seriesMedian allNull
                            emptyMedianResult <- Pl.seriesMedian empty
                            scoreNUnique <- Pl.seriesNUnique score
                            textNUnique <- Pl.seriesNUnique textSeries
                            boolNUnique <- Pl.seriesNUnique boolSeries
                            allNullNUnique <- Pl.seriesNUnique allNull
                            emptyNUnique <- Pl.seriesNUnique empty
                            case medianResult of
                                Left err -> expectationFailure (show err)
                                Right medianValue -> shouldApproximateMaybe 1.0e-12 (Just 8.875) medianValue
                            textMedianResult `shouldBe` Right Nothing
                            allNullMedianResult `shouldBe` Right Nothing
                            emptyMedianResult `shouldBe` Right Nothing
                            scoreNUnique `shouldBe` Right 3
                            textNUnique `shouldBe` Right 3
                            boolNUnique `shouldBe` Right 3
                            allNullNUnique `shouldBe` Right 1
                            emptyNUnique `shouldBe` Right 0
                (Left err, _, _, _, _) -> expectationFailure (show err)
                (_, Left err, _, _, _) -> expectationFailure (show err)
                (_, _, Left err, _, _) -> expectationFailure (show err)
                (_, _, _, Left err, _) -> expectationFailure (show err)
                (_, _, _, _, Left err) -> expectationFailure (show err)

        it "returns first indexes of unique Series values" $ do
            numericResult <- Pl.series @Int64 "value" (V.fromList [Just 1, Just 2, Just 1, Nothing, Just 3, Nothing])
            textResult <- Pl.series @T.Text "text" (V.fromList [Just "a", Just "b", Just "a", Nothing])
            boolResult <- Pl.series @Bool "flag" (V.fromList [Just True, Just False, Nothing, Just True])
            allNullResult <- Pl.series @Double "all_null" (V.fromList [Nothing, Nothing])
            emptyResult <- Pl.series @Double "empty" V.empty
            case (numericResult, textResult, boolResult, allNullResult, emptyResult) of
                (Right numeric, Right textSeries, Right boolSeries, Right allNull, Right empty) -> do
                    numericArg <- Pl.seriesArgUnique numeric
                    textArg <- Pl.seriesArgUnique textSeries
                    boolArg <- Pl.seriesArgUnique boolSeries
                    allNullArg <- Pl.seriesArgUnique allNull
                    emptyArg <- Pl.seriesArgUnique empty
                    case (numericArg, textArg, boolArg, allNullArg, emptyArg) of
                        (Right numericIdx, Right textIdx, Right boolIdx, Right allNullIdx, Right emptyIdx) -> do
                            Pl.seriesDataType numericIdx `shouldReturn` Right Pl.UInt32
                            Pl.seriesWord32 numericIdx `shouldReturn` Right (V.fromList [Just 0, Just 1, Just 3, Just 4])
                            Pl.seriesWord32 textIdx `shouldReturn` Right (V.fromList [Just 0, Just 1, Just 3])
                            Pl.seriesWord32 boolIdx `shouldReturn` Right (V.fromList [Just 0, Just 1, Just 2])
                            Pl.seriesWord32 allNullIdx `shouldReturn` Right (V.fromList [Just 0])
                            Pl.seriesWord32 emptyIdx `shouldReturn` Right V.empty
                        (Left err, _, _, _, _) -> expectationFailure (show err)
                        (_, Left err, _, _, _) -> expectationFailure (show err)
                        (_, _, Left err, _, _) -> expectationFailure (show err)
                        (_, _, _, Left err, _) -> expectationFailure (show err)
                        (_, _, _, _, Left err) -> expectationFailure (show err)
                (Left err, _, _, _, _) -> expectationFailure (show err)
                (_, Left err, _, _, _) -> expectationFailure (show err)
                (_, _, Left err, _, _) -> expectationFailure (show err)
                (_, _, _, Left err, _) -> expectationFailure (show err)
                (_, _, _, _, Left err) -> expectationFailure (show err)

        it "computes Series arg-sort indexes with sort options" $ do
            numericResult <- Pl.series @Int64 "value" (V.fromList [Just 3, Nothing, Just 1, Just 2])
            tieResult <- Pl.series @Int64 "tie" (V.fromList [Just 2, Just 1, Just 2, Just 1])
            textResult <- Pl.series @T.Text "text" (V.fromList [Just "b", Just "a", Nothing, Just "a"])
            emptyResult <- Pl.series @Int64 "empty" V.empty
            case (numericResult, tieResult, textResult, emptyResult) of
                (Right numeric, Right tieSeries, Right textSeries, Right empty) -> do
                    defaultIndexes <- Pl.seriesArgSort Pl.defaultSeriesSortOptions numeric
                    nullsLastIndexes <-
                        Pl.seriesArgSort
                            Pl.defaultSeriesSortOptions { Pl.seriesSortNullsLast = True }
                            numeric
                    descendingIndexes <-
                        Pl.seriesArgSort
                            Pl.defaultSeriesSortOptions
                                { Pl.seriesSortDescending = True
                                , Pl.seriesSortNullsLast = True
                                }
                            numeric
                    stableTieIndexes <-
                        Pl.seriesArgSort
                            Pl.defaultSeriesSortOptions { Pl.seriesSortMaintainOrder = True }
                            tieSeries
                    textIndexes <-
                        Pl.seriesArgSort
                            Pl.defaultSeriesSortOptions { Pl.seriesSortNullsLast = True }
                            textSeries
                    emptyIndexes <- Pl.seriesArgSort Pl.defaultSeriesSortOptions empty
                    negativeLimit <-
                        Pl.seriesArgSort
                            Pl.defaultSeriesSortOptions { Pl.seriesSortLimit = Just (-1) }
                            numeric
                    case (defaultIndexes, nullsLastIndexes, descendingIndexes, stableTieIndexes, textIndexes, emptyIndexes) of
                        (Right defaultIdx, Right nullsLastIdx, Right descendingIdx, Right stableTieIdx, Right textIdx, Right emptyIdx) -> do
                            Pl.seriesDataType defaultIdx `shouldReturn` Right Pl.UInt32
                            Pl.seriesWord32 defaultIdx `shouldReturn` Right (V.fromList [Just 1, Just 2, Just 3, Just 0])
                            Pl.seriesWord32 nullsLastIdx `shouldReturn` Right (V.fromList [Just 2, Just 3, Just 0, Just 1])
                            Pl.seriesWord32 descendingIdx `shouldReturn` Right (V.fromList [Just 0, Just 3, Just 2, Just 1])
                            Pl.seriesWord32 stableTieIdx `shouldReturn` Right (V.fromList [Just 1, Just 3, Just 0, Just 2])
                            Pl.seriesWord32 textIdx `shouldReturn` Right (V.fromList [Just 1, Just 3, Just 0, Just 2])
                            Pl.seriesWord32 emptyIdx `shouldReturn` Right V.empty
                            expectInvalidArgumentMessage "series arg sort limit must be non-negative" negativeLimit
                        (Left err, _, _, _, _, _) -> expectationFailure (show err)
                        (_, Left err, _, _, _, _) -> expectationFailure (show err)
                        (_, _, Left err, _, _, _) -> expectationFailure (show err)
                        (_, _, _, Left err, _, _) -> expectationFailure (show err)
                        (_, _, _, _, Left err, _) -> expectationFailure (show err)
                        (_, _, _, _, _, Left err) -> expectationFailure (show err)
                (Left err, _, _, _) -> expectationFailure (show err)
                (_, Left err, _, _) -> expectationFailure (show err)
                (_, _, Left err, _) -> expectationFailure (show err)
                (_, _, _, Left err) -> expectationFailure (show err)

        it "ranks Series handles with deterministic rank methods" $ do
            valuesResult <- Pl.series @Int64 "rank" (V.fromList [Just 1, Just 2, Just 3, Just 2, Just 2, Just 3, Just 0])
            nullValuesResult <- Pl.series @Int64 "rank_nulls" (V.fromList [Just 1, Just 2, Just 3, Just 2, Nothing, Nothing, Just 0])
            descendingResult <- Pl.series @Int64 "rank_desc" (V.fromList [Nothing, Just 1, Just 1, Just 5, Nothing])
            textResult <- Pl.series @T.Text "rank_text" (V.fromList [Just "b", Just "a", Nothing, Just "b"])
            allNullResult <- Pl.series @Word32 "rank_all_null" (V.fromList [Nothing, Nothing, Nothing])
            emptyResult <- Pl.series @Word32 "rank_empty" V.empty
            case (valuesResult, nullValuesResult, descendingResult, textResult, allNullResult, emptyResult) of
                (Right values, Right nullValues, Right descendingValues, Right textValues, Right allNull, Right empty) -> do
                    dense <- Pl.seriesRank Pl.defaultRankOptions values
                    minRank <- Pl.seriesRank Pl.defaultRankOptions { Pl.rankMethod = Pl.RankMin } values
                    maxRank <- Pl.seriesRank Pl.defaultRankOptions { Pl.rankMethod = Pl.RankMax } values
                    ordinal <- Pl.seriesRank Pl.defaultRankOptions { Pl.rankMethod = Pl.RankOrdinal } values
                    average <- Pl.seriesRank Pl.defaultRankOptions { Pl.rankMethod = Pl.RankAverage } values
                    averageWithNulls <- Pl.seriesRank Pl.defaultRankOptions { Pl.rankMethod = Pl.RankAverage } nullValues
                    descendingDense <- Pl.seriesRank Pl.defaultRankOptions { Pl.rankDescending = True } descendingValues
                    textDense <- Pl.seriesRank Pl.defaultRankOptions textValues
                    allNullDense <- Pl.seriesRank Pl.defaultRankOptions allNull
                    emptyAverage <- Pl.seriesRank Pl.defaultRankOptions { Pl.rankMethod = Pl.RankAverage } empty
                    emptyMax <- Pl.seriesRank Pl.defaultRankOptions { Pl.rankMethod = Pl.RankMax } empty
                    case (dense, minRank, maxRank, ordinal, average, averageWithNulls, descendingDense, textDense, allNullDense, emptyAverage, emptyMax) of
                        (Right denseRank, Right minRanked, Right maxRanked, Right ordinalRank, Right averageRank, Right averageNullRank, Right descendingRank, Right textRank, Right allNullRank, Right emptyAverageRank, Right emptyMaxRank) -> do
                            Pl.seriesDataType denseRank `shouldReturn` Right Pl.UInt32
                            Pl.seriesDataType averageRank `shouldReturn` Right Pl.Float64
                            Pl.seriesWord32 denseRank `shouldReturn` Right (V.fromList [Just 2, Just 3, Just 4, Just 3, Just 3, Just 4, Just 1])
                            Pl.seriesWord32 minRanked `shouldReturn` Right (V.fromList [Just 2, Just 3, Just 6, Just 3, Just 3, Just 6, Just 1])
                            Pl.seriesWord32 maxRanked `shouldReturn` Right (V.fromList [Just 2, Just 5, Just 7, Just 5, Just 5, Just 7, Just 1])
                            Pl.seriesWord32 ordinalRank `shouldReturn` Right (V.fromList [Just 2, Just 3, Just 6, Just 4, Just 5, Just 7, Just 1])
                            Pl.seriesDouble averageRank `shouldReturn` Right (V.fromList [Just 2.0, Just 4.0, Just 6.5, Just 4.0, Just 4.0, Just 6.5, Just 1.0])
                            Pl.seriesDouble averageNullRank `shouldReturn` Right (V.fromList [Just 2.0, Just 3.5, Just 5.0, Just 3.5, Nothing, Nothing, Just 1.0])
                            Pl.seriesWord32 descendingRank `shouldReturn` Right (V.fromList [Nothing, Just 2, Just 2, Just 1, Nothing])
                            Pl.seriesWord32 textRank `shouldReturn` Right (V.fromList [Just 2, Just 1, Nothing, Just 2])
                            Pl.seriesWord32 allNullRank `shouldReturn` Right (V.fromList [Nothing, Nothing, Nothing])
                            Pl.seriesDataType emptyAverageRank `shouldReturn` Right Pl.Float64
                            Pl.seriesDouble emptyAverageRank `shouldReturn` Right V.empty
                            Pl.seriesDataType emptyMaxRank `shouldReturn` Right Pl.UInt32
                            Pl.seriesWord32 emptyMaxRank `shouldReturn` Right V.empty
                        (Left err, _, _, _, _, _, _, _, _, _, _) -> expectationFailure (show err)
                        (_, Left err, _, _, _, _, _, _, _, _, _) -> expectationFailure (show err)
                        (_, _, Left err, _, _, _, _, _, _, _, _) -> expectationFailure (show err)
                        (_, _, _, Left err, _, _, _, _, _, _, _) -> expectationFailure (show err)
                        (_, _, _, _, Left err, _, _, _, _, _, _) -> expectationFailure (show err)
                        (_, _, _, _, _, Left err, _, _, _, _, _) -> expectationFailure (show err)
                        (_, _, _, _, _, _, Left err, _, _, _, _) -> expectationFailure (show err)
                        (_, _, _, _, _, _, _, Left err, _, _, _) -> expectationFailure (show err)
                        (_, _, _, _, _, _, _, _, Left err, _, _) -> expectationFailure (show err)
                        (_, _, _, _, _, _, _, _, _, Left err, _) -> expectationFailure (show err)
                        (_, _, _, _, _, _, _, _, _, _, Left err) -> expectationFailure (show err)
                (Left err, _, _, _, _, _) -> expectationFailure (show err)
                (_, Left err, _, _, _, _) -> expectationFailure (show err)
                (_, _, Left err, _, _, _) -> expectationFailure (show err)
                (_, _, _, Left err, _, _) -> expectationFailure (show err)
                (_, _, _, _, Left err, _) -> expectationFailure (show err)
                (_, _, _, _, _, Left err) -> expectationFailure (show err)

        it "counts unique Series values in first-seen order" $ do
            numericResult <- Pl.series @Int64 "value" (V.fromList [Just 1, Just 2, Just 1, Nothing, Just 3, Nothing])
            textResult <- Pl.series @T.Text "text" (V.fromList [Just "b", Just "a", Nothing, Just "b", Just "a"])
            boolResult <- Pl.series @Bool "flag" (V.fromList [Just True, Just False, Nothing, Just True, Just False])
            allNullResult <- Pl.series @Double "all_null" (V.fromList [Nothing, Nothing, Nothing])
            emptyResult <- Pl.series @Double "empty" V.empty
            case (numericResult, textResult, boolResult, allNullResult, emptyResult) of
                (Right numeric, Right textSeries, Right boolSeries, Right allNull, Right empty) -> do
                    numericCounts <- Pl.seriesUniqueCounts numeric
                    textCounts <- Pl.seriesUniqueCounts textSeries
                    boolCounts <- Pl.seriesUniqueCounts boolSeries
                    allNullCounts <- Pl.seriesUniqueCounts allNull
                    emptyCounts <- Pl.seriesUniqueCounts empty
                    case (numericCounts, textCounts, boolCounts, allNullCounts, emptyCounts) of
                        (Right numericOut, Right textOut, Right boolOut, Right allNullOut, Right emptyOut) -> do
                            Pl.seriesDataType numericOut `shouldReturn` Right Pl.UInt32
                            Pl.seriesWord32 numericOut `shouldReturn` Right (V.fromList [Just 2, Just 1, Just 2, Just 1])
                            Pl.seriesWord32 textOut `shouldReturn` Right (V.fromList [Just 2, Just 2, Just 1])
                            Pl.seriesWord32 boolOut `shouldReturn` Right (V.fromList [Just 2, Just 2, Just 1])
                            Pl.seriesWord32 allNullOut `shouldReturn` Right (V.fromList [Just 3])
                            Pl.seriesWord32 emptyOut `shouldReturn` Right V.empty
                        (Left err, _, _, _, _) -> expectationFailure (show err)
                        (_, Left err, _, _, _) -> expectationFailure (show err)
                        (_, _, Left err, _, _) -> expectationFailure (show err)
                        (_, _, _, Left err, _) -> expectationFailure (show err)
                        (_, _, _, _, Left err) -> expectationFailure (show err)
                (Left err, _, _, _, _) -> expectationFailure (show err)
                (_, Left err, _, _, _) -> expectationFailure (show err)
                (_, _, Left err, _, _) -> expectationFailure (show err)
                (_, _, _, Left err, _) -> expectationFailure (show err)
                (_, _, _, _, Left err) -> expectationFailure (show err)

        it "returns Series mode values with maintained tie order" $ do
            numericResult <- Pl.series @Int64 "value" (V.fromList [Just 1, Just 2, Just 2, Just 3, Just 3])
            textResult <- Pl.series @T.Text "text" (V.fromList [Just "a", Just "b", Just "a", Just "c"])
            nullResult <- Pl.series @Int64 "nullable" (V.fromList [Nothing, Just 1, Nothing, Just 2])
            emptyResult <- Pl.series @Int64 "empty" V.empty
            case (numericResult, textResult, nullResult, emptyResult) of
                (Right numeric, Right textSeries, Right nullSeries, Right empty) -> do
                    let orderedOptions =
                            Pl.defaultSeriesModeOptions
                                { Pl.seriesModeMaintainOrder = True
                                }
                    numericMode <- Pl.seriesMode orderedOptions numeric
                    textMode <- Pl.seriesMode orderedOptions textSeries
                    nullMode <- Pl.seriesMode orderedOptions nullSeries
                    emptyMode <- Pl.seriesMode orderedOptions empty
                    case (numericMode, textMode, nullMode, emptyMode) of
                        (Right numericOut, Right textOut, Right nullOut, Right emptyOut) -> do
                            Pl.seriesInt64 numericOut `shouldReturn` Right (V.fromList [Just 2, Just 3])
                            Pl.seriesText textOut `shouldReturn` Right (V.fromList [Just "a"])
                            Pl.seriesInt64 nullOut `shouldReturn` Right (V.fromList [Nothing])
                            Pl.seriesDataType emptyOut `shouldReturn` Right Pl.Int64
                            Pl.seriesInt64 emptyOut `shouldReturn` Right V.empty
                        (Left err, _, _, _) -> expectationFailure (show err)
                        (_, Left err, _, _) -> expectationFailure (show err)
                        (_, _, Left err, _) -> expectationFailure (show err)
                        (_, _, _, Left err) -> expectationFailure (show err)
                (Left err, _, _, _) -> expectationFailure (show err)
                (_, Left err, _, _) -> expectationFailure (show err)
                (_, _, Left err, _) -> expectationFailure (show err)
                (_, _, _, Left err) -> expectationFailure (show err)

        it "selects eager Series values with a boolean mask" $ do
            maskResult <- Pl.series @Bool "mask" (V.fromList [Just True, Just False, Nothing, Just True])
            trueResult <- Pl.series @Int64 "true" (V.fromList [Just 10, Just 10, Just 10, Just 10])
            falseResult <- Pl.series @Int64 "false" (V.fromList [Just 1, Just 2, Just 3, Just 4])
            broadcastMaskResult <- Pl.series @Bool "mask" (V.fromList [Just True, Just False, Just True, Just False])
            broadcastTrueResult <- Pl.series @Int64 "true" (V.fromList [Just 99])
            numericTrueResult <- Pl.series @Int64 "true" (V.fromList [Just 1, Just 2])
            numericFalseResult <- Pl.series @Double "false" (V.fromList [Just 0.5, Just 0.25])
            textMaskResult <- Pl.series @Bool "mask" (V.fromList [Just False, Just True, Nothing])
            textTrueResult <- Pl.series @T.Text "true" (V.fromList [Just "left", Just "yes", Just "skip"])
            textFalseResult <- Pl.series @T.Text "false" (V.fromList [Just "right", Just "no", Just "fallback"])
            shortMaskResult <- Pl.series @Bool "short_mask" (V.fromList [Just True, Just False])
            nonBoolMaskResult <- Pl.series @Int64 "not_mask" (V.fromList [Just 1, Just 0, Just 1, Just 0])
            case
                ( maskResult
                , trueResult
                , falseResult
                , broadcastMaskResult
                , broadcastTrueResult
                , numericTrueResult
                , numericFalseResult
                , textMaskResult
                , textTrueResult
                , textFalseResult
                , shortMaskResult
                , nonBoolMaskResult
                )
                of
                    ( Right mask
                        , Right trueValues
                        , Right falseValues
                        , Right broadcastMask
                        , Right broadcastTrue
                        , Right numericTrue
                        , Right numericFalse
                        , Right textMask
                        , Right textTrue
                        , Right textFalse
                        , Right shortMask
                        , Right nonBoolMask
                        ) -> do
                            selected <- Pl.seriesZipWith mask trueValues falseValues
                            broadcasted <- Pl.seriesZipWith broadcastMask broadcastTrue falseValues
                            coerced <- Pl.seriesZipWith shortMask numericTrue numericFalse
                            textSelected <- Pl.seriesZipWith textMask textTrue textFalse
                            shapeMismatch <- Pl.seriesZipWith shortMask trueValues falseValues
                            wrongMask <- Pl.seriesZipWith nonBoolMask trueValues falseValues
                            case (selected, broadcasted, coerced, textSelected) of
                                (Right selectedOut, Right broadcastedOut, Right coercedOut, Right textOut) -> do
                                    Pl.seriesInt64 selectedOut `shouldReturn` Right (V.fromList [Just 10, Just 2, Just 3, Just 10])
                                    Pl.seriesInt64 broadcastedOut `shouldReturn` Right (V.fromList [Just 99, Just 2, Just 99, Just 4])
                                    Pl.seriesDataType coercedOut `shouldReturn` Right Pl.Float64
                                    Pl.seriesDouble coercedOut `shouldReturn` Right (V.fromList [Just 1.0, Just 0.25])
                                    Pl.seriesText textOut `shouldReturn` Right (V.fromList [Just "right", Just "yes", Just "fallback"])
                                    expectPolarsFailure shapeMismatch
                                    expectPolarsFailure wrongMask
                                (Left err, _, _, _) -> expectationFailure (show err)
                                (_, Left err, _, _) -> expectationFailure (show err)
                                (_, _, Left err, _) -> expectationFailure (show err)
                                (_, _, _, Left err) -> expectationFailure (show err)
                    (Left err, _, _, _, _, _, _, _, _, _, _, _) -> expectationFailure (show err)
                    (_, Left err, _, _, _, _, _, _, _, _, _, _) -> expectationFailure (show err)
                    (_, _, Left err, _, _, _, _, _, _, _, _, _) -> expectationFailure (show err)
                    (_, _, _, Left err, _, _, _, _, _, _, _, _) -> expectationFailure (show err)
                    (_, _, _, _, Left err, _, _, _, _, _, _, _) -> expectationFailure (show err)
                    (_, _, _, _, _, Left err, _, _, _, _, _, _) -> expectationFailure (show err)
                    (_, _, _, _, _, _, Left err, _, _, _, _, _) -> expectationFailure (show err)
                    (_, _, _, _, _, _, _, Left err, _, _, _, _) -> expectationFailure (show err)
                    (_, _, _, _, _, _, _, _, Left err, _, _, _) -> expectationFailure (show err)
                    (_, _, _, _, _, _, _, _, _, Left err, _, _) -> expectationFailure (show err)
                    (_, _, _, _, _, _, _, _, _, _, Left err, _) -> expectationFailure (show err)
                    (_, _, _, _, _, _, _, _, _, _, _, Left err) -> expectationFailure (show err)

        it "gathers every nth Series value with an offset" $ do
            numericResult <- Pl.series @Int64 "value" (V.fromList [Just 0, Just 1, Just 2, Just 3, Just 4])
            textResult <- Pl.series @T.Text "text" (V.fromList [Just "a", Nothing, Just "c", Just "d", Just "e"])
            case (numericResult, textResult) of
                (Right numeric, Right textSeries) -> do
                    evens <- Pl.seriesGatherEvery 2 0 numeric
                    offsetValues <- Pl.seriesGatherEvery 2 1 textSeries
                    largeStep <- Pl.seriesGatherEvery 10 0 numeric
                    emptyOffset <- Pl.seriesGatherEvery 2 5 textSeries
                    zeroStep <- Pl.seriesGatherEvery 0 0 numeric
                    negativeStep <- Pl.seriesGatherEvery (-1) 0 numeric
                    negativeOffset <- Pl.seriesGatherEvery 2 (-1) numeric
                    case (evens, offsetValues, largeStep, emptyOffset) of
                        (Right evensOut, Right offsetOut, Right largeStepOut, Right emptyOffsetOut) -> do
                            Pl.seriesInt64 evensOut `shouldReturn` Right (V.fromList [Just 0, Just 2, Just 4])
                            Pl.seriesText offsetOut `shouldReturn` Right (V.fromList [Nothing, Just "d"])
                            Pl.seriesInt64 largeStepOut `shouldReturn` Right (V.fromList [Just 0])
                            Pl.seriesDataType emptyOffsetOut `shouldReturn` Right Pl.Utf8
                            Pl.seriesText emptyOffsetOut `shouldReturn` Right V.empty
                            expectInvalidArgumentMessage "seriesGatherEvery step must be positive" zeroStep
                            expectInvalidArgumentMessage "seriesGatherEvery step must be positive" negativeStep
                            expectInvalidArgumentMessage "seriesGatherEvery offset must be non-negative" negativeOffset
                        (Left err, _, _, _) -> expectationFailure (show err)
                        (_, Left err, _, _) -> expectationFailure (show err)
                        (_, _, Left err, _) -> expectationFailure (show err)
                        (_, _, _, Left err) -> expectationFailure (show err)
                (Left err, _) -> expectationFailure (show err)
                (_, Left err) -> expectationFailure (show err)

        it "computes Series percentage changes with Polars semantics" $ do
            baseResult <- Pl.series @Int64 "value" (V.fromList [Just 10, Just 15, Just 30])
            zeroResult <- Pl.series @Int64 "zero" (V.fromList [Just 0, Just 0, Just 1, Just (-1)])
            nullableResult <- Pl.series @Int64 "nullable" (V.fromList [Just 10, Nothing, Just 15, Just 30])
            floatResult <- Pl.series @Float "float" (V.fromList [Just 1.0, Just 2.0, Just 4.0])
            textResult <- Pl.series @T.Text "text" (V.fromList [Just "10", Just "15", Just "30"])
            invalidTextResult <- Pl.series @T.Text "bad_text" (V.fromList [Just "a", Just "b"])
            case (baseResult, zeroResult, nullableResult, floatResult, textResult, invalidTextResult) of
                (Right base, Right zeroSeries, Right nullable, Right floatSeries, Right textSeries, Right invalidText) -> do
                    pct1 <- Pl.seriesPctChange 1 base
                    pct2 <- Pl.seriesPctChange 2 base
                    pctFuture <- Pl.seriesPctChange (-1) base
                    pctZeroDenom <- Pl.seriesPctChange 1 zeroSeries
                    pctZeroPeriod <- Pl.seriesPctChange 0 zeroSeries
                    pctNullable <- Pl.seriesPctChange 1 nullable
                    pctFloat <- Pl.seriesPctChange 1 floatSeries
                    pctText <- Pl.seriesPctChange 1 textSeries
                    pctInvalidText <- Pl.seriesPctChange 1 invalidText
                    let nan = 0 / 0 :: Double
                        posInf = 1 / 0 :: Double
                        expectDouble tolerance expected series =
                            Pl.seriesDouble series
                                >>= either (expectationFailure . show) (shouldApproximate tolerance expected)
                    case (pct1, pct2, pctFuture, pctZeroDenom, pctZeroPeriod, pctNullable, pctFloat, pctText, pctInvalidText) of
                        ( Right pct1Out
                            , Right pct2Out
                            , Right pctFutureOut
                            , Right pctZeroDenomOut
                            , Right pctZeroPeriodOut
                            , Right pctNullableOut
                            , Right pctFloatOut
                            , Right pctTextOut
                            , Right pctInvalidTextOut
                            ) -> do
                                expectDouble 1.0e-12 (V.fromList [Nothing, Just 0.5, Just 1.0]) pct1Out
                                expectDouble 1.0e-12 (V.fromList [Nothing, Nothing, Just 2.0]) pct2Out
                                expectDouble 1.0e-12 (V.fromList [Just (-(1.0 / 3.0)), Just (-0.5), Nothing]) pctFutureOut
                                expectDouble 0.0 (V.fromList [Nothing, Just nan, Just posInf, Just (-2.0)]) pctZeroDenomOut
                                expectDouble 0.0 (V.fromList [Just nan, Just nan, Just 0.0, Just (-0.0)]) pctZeroPeriodOut
                                expectDouble 1.0e-12 (V.fromList [Nothing, Nothing, Nothing, Just 1.0]) pctNullableOut
                                Pl.seriesDataType pctFloatOut `shouldReturn` Right Pl.Float32
                                Pl.seriesFloat pctFloatOut `shouldReturn` Right (V.fromList [Nothing, Just 1.0, Just 1.0])
                                expectDouble 1.0e-12 (V.fromList [Nothing, Just 0.5, Just 1.0]) pctTextOut
                                Pl.seriesDouble pctInvalidTextOut `shouldReturn` Right (V.fromList [Nothing, Nothing])
                        (Left err, _, _, _, _, _, _, _, _) -> expectationFailure (show err)
                        (_, Left err, _, _, _, _, _, _, _) -> expectationFailure (show err)
                        (_, _, Left err, _, _, _, _, _, _) -> expectationFailure (show err)
                        (_, _, _, Left err, _, _, _, _, _) -> expectationFailure (show err)
                        (_, _, _, _, Left err, _, _, _, _) -> expectationFailure (show err)
                        (_, _, _, _, _, Left err, _, _, _) -> expectationFailure (show err)
                        (_, _, _, _, _, _, Left err, _, _) -> expectationFailure (show err)
                        (_, _, _, _, _, _, _, Left err, _) -> expectationFailure (show err)
                        (_, _, _, _, _, _, _, _, Left err) -> expectationFailure (show err)
                (Left err, _, _, _, _, _) -> expectationFailure (show err)
                (_, Left err, _, _, _, _) -> expectationFailure (show err)
                (_, _, Left err, _, _, _) -> expectationFailure (show err)
                (_, _, _, Left err, _, _) -> expectationFailure (show err)
                (_, _, _, _, Left err, _) -> expectationFailure (show err)
                (_, _, _, _, _, Left err) -> expectationFailure (show err)

        it "computes Series is-between masks" $ do
            valuesResult <- Pl.series @Int64 "value" (V.fromList [Just 1, Just 2, Just 3, Just 4, Just 5, Nothing])
            lowerResult <- Pl.series @Int64 "lower" (V.singleton (Just 2))
            upperResult <- Pl.series @Int64 "upper" (V.singleton (Just 4))
            reversedLowerResult <- Pl.series @Int64 "lower" (V.singleton (Just 4))
            reversedUpperResult <- Pl.series @Int64 "upper" (V.singleton (Just 2))
            textResult <- Pl.series @T.Text "text" (V.fromList [Just "a", Just "b", Just "c", Just "d", Nothing])
            textLowerResult <- Pl.series @T.Text "lower" (V.singleton (Just "b"))
            textUpperResult <- Pl.series @T.Text "upper" (V.singleton (Just "d"))
            case (valuesResult, lowerResult, upperResult, reversedLowerResult, reversedUpperResult, textResult, textLowerResult, textUpperResult) of
                (Right values, Right lower, Right upper, Right reversedLower, Right reversedUpper, Right textValues, Right textLower, Right textUpper) -> do
                    both <- Pl.seriesIsBetween Pl.ClosedBoth values lower upper
                    left <- Pl.seriesIsBetween Pl.ClosedLeft values lower upper
                    right <- Pl.seriesIsBetween Pl.ClosedRight values lower upper
                    open <- Pl.seriesIsBetween Pl.ClosedNone values lower upper
                    reversed <- Pl.seriesIsBetween Pl.ClosedBoth values reversedLower reversedUpper
                    textLeft <- Pl.seriesIsBetween Pl.ClosedLeft textValues textLower textUpper
                    mismatch <- Pl.seriesIsBetween Pl.ClosedBoth values textLower textUpper
                    case (both, left, right, open, reversed, textLeft) of
                        (Right bothOut, Right leftOut, Right rightOut, Right openOut, Right reversedOut, Right textLeftOut) -> do
                            Pl.seriesBool bothOut `shouldReturn` Right (V.fromList [Just False, Just True, Just True, Just True, Just False, Nothing])
                            Pl.seriesBool leftOut `shouldReturn` Right (V.fromList [Just False, Just True, Just True, Just False, Just False, Nothing])
                            Pl.seriesBool rightOut `shouldReturn` Right (V.fromList [Just False, Just False, Just True, Just True, Just False, Nothing])
                            Pl.seriesBool openOut `shouldReturn` Right (V.fromList [Just False, Just False, Just True, Just False, Just False, Nothing])
                            Pl.seriesBool reversedOut `shouldReturn` Right (V.fromList [Just False, Just False, Just False, Just False, Just False, Nothing])
                            Pl.seriesBool textLeftOut `shouldReturn` Right (V.fromList [Just False, Just True, Just True, Just False, Nothing])
                            expectPolarsFailure mismatch
                        (Left err, _, _, _, _, _) -> expectationFailure (show err)
                        (_, Left err, _, _, _, _) -> expectationFailure (show err)
                        (_, _, Left err, _, _, _) -> expectationFailure (show err)
                        (_, _, _, Left err, _, _) -> expectationFailure (show err)
                        (_, _, _, _, Left err, _) -> expectationFailure (show err)
                        (_, _, _, _, _, Left err) -> expectationFailure (show err)
                (Left err, _, _, _, _, _, _, _) -> expectationFailure (show err)
                (_, Left err, _, _, _, _, _, _) -> expectationFailure (show err)
                (_, _, Left err, _, _, _, _, _) -> expectationFailure (show err)
                (_, _, _, Left err, _, _, _, _) -> expectationFailure (show err)
                (_, _, _, _, Left err, _, _, _) -> expectationFailure (show err)
                (_, _, _, _, _, Left err, _, _) -> expectationFailure (show err)
                (_, _, _, _, _, _, Left err, _) -> expectationFailure (show err)
                (_, _, _, _, _, _, _, Left err) -> expectationFailure (show err)

        it "searches sorted Series insertion indexes" $ do
            sortedResult <- Pl.series @Int64 "value" (V.fromList [Just 1, Just 2, Just 2, Just 4])
            needlesResult <- Pl.series @Int64 "value" (V.fromList [Just 0, Just 2, Just 3, Just 5])
            descendingSortedResult <- Pl.series @Int64 "value" (V.fromList [Just 4, Just 2, Just 2, Just 1])
            descendingNeedlesResult <- Pl.series @Int64 "value" (V.fromList [Just 5, Just 2, Just 0])
            textSortedResult <- Pl.series @T.Text "text" (V.fromList [Just "a", Just "b", Just "b", Just "z"])
            textNeedlesResult <- Pl.series @T.Text "text" (V.fromList [Just "b", Just "c"])
            nullsFirstResult <- Pl.series @Int64 "value" (V.fromList [Nothing, Nothing, Just 1, Just 3])
            nullsLastResult <- Pl.series @Int64 "value" (V.fromList [Just 1, Just 3, Nothing, Nothing])
            nullNeedlesResult <- Pl.series @Int64 "value" (V.fromList [Nothing, Just 0, Just 3, Just 4])
            emptyResult <- Pl.series @Int64 "value" V.empty
            mismatchNeedlesResult <- Pl.series @Double "value" (V.fromList [Just 1.0, Just 2.0])
            case
                ( sortedResult
                , needlesResult
                , descendingSortedResult
                , descendingNeedlesResult
                , textSortedResult
                , textNeedlesResult
                , nullsFirstResult
                , nullsLastResult
                , nullNeedlesResult
                , emptyResult
                , mismatchNeedlesResult
                ) of
                ( Right sorted
                    , Right needles
                    , Right descendingSorted
                    , Right descendingNeedles
                    , Right textSorted
                    , Right textNeedles
                    , Right nullsFirst
                    , Right nullsLast
                    , Right nullNeedles
                    , Right empty
                    , Right mismatchNeedles
                    ) -> do
                        left <- Pl.seriesSearchSorted Pl.SearchSortedLeft False sorted needles
                        right <- Pl.seriesSearchSorted Pl.SearchSortedRight False sorted needles
                        anySide <- Pl.seriesSearchSorted Pl.SearchSortedAny False sorted needles
                        descendingLeft <- Pl.seriesSearchSorted Pl.SearchSortedLeft True descendingSorted descendingNeedles
                        descendingRight <- Pl.seriesSearchSorted Pl.SearchSortedRight True descendingSorted descendingNeedles
                        textLeft <- Pl.seriesSearchSorted Pl.SearchSortedLeft False textSorted textNeedles
                        textRight <- Pl.seriesSearchSorted Pl.SearchSortedRight False textSorted textNeedles
                        nullsFirstLeft <- Pl.seriesSearchSorted Pl.SearchSortedLeft False nullsFirst nullNeedles
                        nullsFirstRight <- Pl.seriesSearchSorted Pl.SearchSortedRight False nullsFirst nullNeedles
                        nullsLastLeft <- Pl.seriesSearchSorted Pl.SearchSortedLeft False nullsLast nullNeedles
                        nullsLastRight <- Pl.seriesSearchSorted Pl.SearchSortedRight False nullsLast nullNeedles
                        emptyIndexes <- Pl.seriesSearchSorted Pl.SearchSortedLeft False empty needles
                        mismatch <- Pl.seriesSearchSorted Pl.SearchSortedLeft False sorted mismatchNeedles
                        case
                            ( left
                            , right
                            , anySide
                            , descendingLeft
                            , descendingRight
                            , textLeft
                            , textRight
                            , nullsFirstLeft
                            , nullsFirstRight
                            , nullsLastLeft
                            , nullsLastRight
                            , emptyIndexes
                            ) of
                            ( Right leftOut
                                , Right rightOut
                                , Right anyOut
                                , Right descendingLeftOut
                                , Right descendingRightOut
                                , Right textLeftOut
                                , Right textRightOut
                                , Right nullsFirstLeftOut
                                , Right nullsFirstRightOut
                                , Right nullsLastLeftOut
                                , Right nullsLastRightOut
                                , Right emptyOut
                                ) -> do
                                    Pl.seriesDataType leftOut `shouldReturn` Right Pl.UInt32
                                    Pl.seriesWord32 leftOut `shouldReturn` Right (V.fromList [Just 0, Just 1, Just 3, Just 4])
                                    Pl.seriesWord32 rightOut `shouldReturn` Right (V.fromList [Just 0, Just 3, Just 3, Just 4])
                                    Pl.seriesWord32 anyOut `shouldReturn` Right (V.fromList [Just 0, Just 1, Just 3, Just 4])
                                    Pl.seriesWord32 descendingLeftOut `shouldReturn` Right (V.fromList [Just 0, Just 1, Just 4])
                                    Pl.seriesWord32 descendingRightOut `shouldReturn` Right (V.fromList [Just 0, Just 3, Just 4])
                                    Pl.seriesWord32 textLeftOut `shouldReturn` Right (V.fromList [Just 1, Just 3])
                                    Pl.seriesWord32 textRightOut `shouldReturn` Right (V.fromList [Just 3, Just 3])
                                    Pl.seriesWord32 nullsFirstLeftOut `shouldReturn` Right (V.fromList [Just 0, Just 2, Just 3, Just 4])
                                    Pl.seriesWord32 nullsFirstRightOut `shouldReturn` Right (V.fromList [Just 2, Just 2, Just 4, Just 4])
                                    Pl.seriesWord32 nullsLastLeftOut `shouldReturn` Right (V.fromList [Just 2, Just 0, Just 1, Just 2])
                                    Pl.seriesWord32 nullsLastRightOut `shouldReturn` Right (V.fromList [Just 4, Just 0, Just 2, Just 2])
                                    Pl.seriesWord32 emptyOut `shouldReturn` Right (V.fromList [Just 0, Just 0, Just 0, Just 0])
                                    case mismatch of
                                        Left err -> do
                                            Pl.polarsErrorCode err `shouldBe` Pl.PolarsFailure
                                            Pl.polarsErrorMessage err `shouldSatisfy` T.isInfixOf "search_sorted"
                                        Right _ -> expectationFailure "expected PolarsFailure for dtype mismatch"
                            (Left err, _, _, _, _, _, _, _, _, _, _, _) -> expectationFailure (show err)
                            (_, Left err, _, _, _, _, _, _, _, _, _, _) -> expectationFailure (show err)
                            (_, _, Left err, _, _, _, _, _, _, _, _, _) -> expectationFailure (show err)
                            (_, _, _, Left err, _, _, _, _, _, _, _, _) -> expectationFailure (show err)
                            (_, _, _, _, Left err, _, _, _, _, _, _, _) -> expectationFailure (show err)
                            (_, _, _, _, _, Left err, _, _, _, _, _, _) -> expectationFailure (show err)
                            (_, _, _, _, _, _, Left err, _, _, _, _, _) -> expectationFailure (show err)
                            (_, _, _, _, _, _, _, Left err, _, _, _, _) -> expectationFailure (show err)
                            (_, _, _, _, _, _, _, _, Left err, _, _, _) -> expectationFailure (show err)
                            (_, _, _, _, _, _, _, _, _, Left err, _, _) -> expectationFailure (show err)
                            (_, _, _, _, _, _, _, _, _, _, Left err, _) -> expectationFailure (show err)
                            (_, _, _, _, _, _, _, _, _, _, _, Left err) -> expectationFailure (show err)
                (Left err, _, _, _, _, _, _, _, _, _, _) -> expectationFailure (show err)
                (_, Left err, _, _, _, _, _, _, _, _, _) -> expectationFailure (show err)
                (_, _, Left err, _, _, _, _, _, _, _, _) -> expectationFailure (show err)
                (_, _, _, Left err, _, _, _, _, _, _, _) -> expectationFailure (show err)
                (_, _, _, _, Left err, _, _, _, _, _, _) -> expectationFailure (show err)
                (_, _, _, _, _, Left err, _, _, _, _, _) -> expectationFailure (show err)
                (_, _, _, _, _, _, Left err, _, _, _, _) -> expectationFailure (show err)
                (_, _, _, _, _, _, _, Left err, _, _, _) -> expectationFailure (show err)
                (_, _, _, _, _, _, _, _, Left err, _, _) -> expectationFailure (show err)
                (_, _, _, _, _, _, _, _, _, Left err, _) -> expectationFailure (show err)
                (_, _, _, _, _, _, _, _, _, _, Left err) -> expectationFailure (show err)

        it "computes Series value counts as a DataFrame" $ do
            colorResult <- Pl.series @T.Text "color" (V.fromList [Just "blue", Just "red", Just "blue", Just "green", Just "blue", Just "red"])
            emptyResult <- Pl.series @T.Text "color" V.empty
            case (colorResult, emptyResult) of
                (Right colors, Right emptyColors) -> do
                    let sortedOptions =
                            Pl.defaultSeriesValueCountsOptions
                                { Pl.seriesValueCountsSort = True
                                , Pl.seriesValueCountsName = "n"
                                }
                        normalizedOptions =
                            sortedOptions
                                { Pl.seriesValueCountsName = "fraction"
                                , Pl.seriesValueCountsNormalize = True
                                }
                        duplicateNameOptions =
                            sortedOptions { Pl.seriesValueCountsName = "color" }
                    countsResult <- Pl.seriesValueCounts sortedOptions colors
                    normalizedResult <- Pl.seriesValueCounts normalizedOptions colors
                    duplicateResult <- Pl.seriesValueCounts duplicateNameOptions colors
                    emptyCountsResult <- Pl.seriesValueCounts sortedOptions emptyColors
                    case (countsResult, normalizedResult, emptyCountsResult) of
                        (Right countsDf, Right normalizedDf, Right emptyCountsDf) -> do
                            Pl.shape countsDf `shouldReturn` Right (3, 2)
                            Pl.column @T.Text countsDf "color" `shouldReturn` Right (V.fromList [Just "blue", Just "red", Just "green"])
                            Pl.column @Word32 countsDf "n" `shouldReturn` Right (V.fromList [Just 3, Just 2, Just 1])

                            Pl.shape normalizedDf `shouldReturn` Right (3, 2)
                            Pl.column @T.Text normalizedDf "color" `shouldReturn` Right (V.fromList [Just "blue", Just "red", Just "green"])
                            fractionResult <- Pl.column @Double normalizedDf "fraction"
                            case fractionResult of
                                Left err -> expectationFailure (show err)
                                Right fractions ->
                                    shouldApproximate 1.0e-12 (V.fromList [Just 0.5, Just (1.0 / 3.0), Just (1.0 / 6.0)]) fractions

                            Pl.shape emptyCountsDf `shouldReturn` Right (0, 2)
                            Pl.column @T.Text emptyCountsDf "color" `shouldReturn` Right V.empty
                            Pl.column @Word32 emptyCountsDf "n" `shouldReturn` Right V.empty
                        (Left err, _, _) -> expectationFailure (show err)
                        (_, Left err, _) -> expectationFailure (show err)
                        (_, _, Left err) -> expectationFailure (show err)
                    case duplicateResult of
                        Left err -> do
                            Pl.polarsErrorCode err `shouldBe` Pl.PolarsFailure
                            Pl.polarsErrorMessage err `shouldSatisfy` T.isInfixOf "duplicate column names"
                        Right _ -> expectationFailure "expected PolarsFailure for duplicate value-count column name"
                (Left err, _) -> expectationFailure (show err)
                (_, Left err) -> expectationFailure (show err)

        it "computes Series sum min and max" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    scoreResult <- Pl.column @Pl.Series df "score"
                    case scoreResult of
                        Left err -> expectationFailure (show err)
                        Right score -> do
                            sumResult <- Pl.seriesSum score
                            minResult <- Pl.seriesMin score
                            maxResult <- Pl.seriesMax score
                            case (sumResult, minResult, maxResult) of
                                (Right sumValue, Right minValue, Right maxValue) -> do
                                    shouldApproximateMaybe 1.0e-12 (Just 17.75) sumValue
                                    shouldApproximateMaybe 1.0e-12 (Just 8.25) minValue
                                    shouldApproximateMaybe 1.0e-12 (Just 9.5) maxValue
                                (Left err, _, _) -> expectationFailure (show err)
                                (_, Left err, _) -> expectationFailure (show err)
                                (_, _, Left err) -> expectationFailure (show err)

        it "handles all-null and empty Series sum min max" $ do
            allNullResult <- Pl.series @Double "all_null" (V.fromList [Nothing, Nothing])
            emptyResult <- Pl.series @Double "empty" V.empty
            case (allNullResult, emptyResult) of
                (Right allNull, Right empty) -> do
                    Pl.seriesSum allNull `shouldReturn` Right (Just 0.0)
                    Pl.seriesMin allNull `shouldReturn` Right Nothing
                    Pl.seriesMax allNull `shouldReturn` Right Nothing
                    Pl.seriesSum empty `shouldReturn` Right (Just 0.0)
                    Pl.seriesMin empty `shouldReturn` Right Nothing
                    Pl.seriesMax empty `shouldReturn` Right Nothing
                (Left err, _) -> expectationFailure (show err)
                (_, Left err) -> expectationFailure (show err)

        it "reports Polars errors for invalid Series sum min max dtypes" $ do
            textResult <- Pl.series @T.Text "text" (V.fromList [Just "a", Just "b"])
            case textResult of
                Left err -> expectationFailure (show err)
                Right textSeries -> do
                    sumResult <- Pl.seriesSum textSeries
                    minResult <- Pl.seriesMin textSeries
                    maxResult <- Pl.seriesMax textSeries
                    expectInvalidArgumentMessage "series sum requires numeric dtype" sumResult
                    expectInvalidArgumentMessage "series min requires numeric dtype" minResult
                    expectInvalidArgumentMessage "series max requires numeric dtype" maxResult

        it "handles all-null Series stats and invalid ddof" $ do
            seriesResult <- Pl.series @Double "all_null" (V.fromList [Nothing, Nothing])
            case seriesResult of
                Left err -> expectationFailure (show err)
                Right allNull -> do
                    Pl.seriesMean allNull `shouldReturn` Right Nothing
                    Pl.seriesStd 1 allNull `shouldReturn` Right Nothing
                    Pl.seriesVar 1 allNull `shouldReturn` Right Nothing
                    negativeStd <- Pl.seriesStd (-1) allNull
                    overflowingVar <- Pl.seriesVar 256 allNull
                    expectInvalidArgumentMessage "series std ddof must be between 0 and 255" negativeStd
                    expectInvalidArgumentMessage "series var ddof must be between 0 and 255" overflowingVar

        it "reports InvalidArgument for negative Series slices" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    seriesResult <- Pl.column @Pl.Series df "age"
                    case seriesResult of
                        Left err -> expectationFailure (show err)
                        Right age -> do
                            headResult <- Pl.seriesHead (-1) age
                            tailResult <- Pl.seriesTail (-1) age
                            sliceResult <- Pl.seriesSlice 0 (-1) age
                            expectInvalidArgumentMessage "series head count must be non-negative" headResult
                            expectInvalidArgumentMessage "series tail count must be non-negative" tailResult
                            expectInvalidArgumentMessage "series slice length must be non-negative" sliceResult

        it "keeps Series handles usable after DataFrame ownership leaves scope" $ do
            seriesResult <- do
                result <- Pl.readCsv valuesCsv
                case result of
                    Left err -> pure (Left err)
                    Right df -> Pl.column @Pl.Series df "age"
            performGC
            case seriesResult of
                Left err -> expectationFailure (show err)
                Right age -> Pl.seriesInt64 age `shouldReturn` Right (V.fromList [Just 34, Nothing, Just 29])

        it "renames Series handles and preserves values" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    seriesResult <- Pl.column @Pl.Series df "age"
                    case seriesResult of
                        Left err -> expectationFailure (show err)
                        Right age -> do
                            renamedResult <- Pl.seriesRename "age_years" age
                            case renamedResult of
                                Left err -> expectationFailure (show err)
                                Right renamed -> do
                                    Pl.seriesName renamed `shouldReturn` Right "age_years"
                                    Pl.seriesInt64 renamed `shouldReturn` Right (V.fromList [Just 34, Nothing, Just 29])

        it "casts Series handles with visible type applications" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    seriesResult <- Pl.column @Pl.Series df "age"
                    case seriesResult of
                        Left err -> expectationFailure (show err)
                        Right age -> do
                            doubleResult <- Pl.seriesCast @Double age
                            case doubleResult of
                                Left err -> expectationFailure (show err)
                                Right doubleAge -> do
                                    Pl.seriesDataType doubleAge `shouldReturn` Right Pl.Float64
                                    Pl.seriesDouble doubleAge `shouldReturn` Right (V.fromList [Just 34.0, Nothing, Just 29.0])
                            textResult <- Pl.seriesCast @T.Text age
                            case textResult of
                                Left err -> expectationFailure (show err)
                                Right textAge -> Pl.seriesText textAge `shouldReturn` Right (V.fromList [Just "34", Nothing, Just "29"])

        it "casts Series handles to scalar dtype matrix values" $ do
            integerResult <- Pl.series @Int64 "value" (V.fromList [Just 1, Nothing, Just 127])
            unsignedResult <- Pl.series @Int64 "unsigned" (V.fromList [Just 0, Nothing, Just 255])
            doubleResult <- Pl.series @Double "float_value" (V.fromList [Just 1.5, Nothing, Just (-2.25)])
            case (integerResult, unsignedResult, doubleResult) of
                (Right integerSeries, Right unsignedSeries, Right doubleSeries) -> do
                    i8 <- Pl.seriesCast @Int8 integerSeries
                    case i8 of
                        Left err -> expectationFailure (show err)
                        Right casted -> do
                            Pl.seriesDataType casted `shouldReturn` Right Pl.Int8
                            Pl.seriesInt8 casted `shouldReturn` Right (V.fromList [Just 1, Nothing, Just 127])
                    i16 <- Pl.seriesCast @Int16 integerSeries
                    case i16 of
                        Left err -> expectationFailure (show err)
                        Right casted -> do
                            Pl.seriesDataType casted `shouldReturn` Right Pl.Int16
                            Pl.seriesInt16 casted `shouldReturn` Right (V.fromList [Just 1, Nothing, Just 127])
                    i32 <- Pl.seriesCast @Int32 integerSeries
                    case i32 of
                        Left err -> expectationFailure (show err)
                        Right casted -> do
                            Pl.seriesDataType casted `shouldReturn` Right Pl.Int32
                            Pl.seriesInt32 casted `shouldReturn` Right (V.fromList [Just 1, Nothing, Just 127])
                    u8 <- Pl.seriesCast @Word8 unsignedSeries
                    case u8 of
                        Left err -> expectationFailure (show err)
                        Right casted -> do
                            Pl.seriesDataType casted `shouldReturn` Right Pl.UInt8
                            Pl.seriesWord8 casted `shouldReturn` Right (V.fromList [Just 0, Nothing, Just 255])
                    u16 <- Pl.seriesCast @Word16 unsignedSeries
                    case u16 of
                        Left err -> expectationFailure (show err)
                        Right casted -> do
                            Pl.seriesDataType casted `shouldReturn` Right Pl.UInt16
                            Pl.seriesWord16 casted `shouldReturn` Right (V.fromList [Just 0, Nothing, Just 255])
                    u32 <- Pl.seriesCast @Word32 unsignedSeries
                    case u32 of
                        Left err -> expectationFailure (show err)
                        Right casted -> do
                            Pl.seriesDataType casted `shouldReturn` Right Pl.UInt32
                            Pl.seriesWord32 casted `shouldReturn` Right (V.fromList [Just 0, Nothing, Just 255])
                    u64 <- Pl.seriesCast @Word64 unsignedSeries
                    case u64 of
                        Left err -> expectationFailure (show err)
                        Right casted -> do
                            Pl.seriesDataType casted `shouldReturn` Right Pl.UInt64
                            Pl.seriesWord64 casted `shouldReturn` Right (V.fromList [Just 0, Nothing, Just 255])
                    f32 <- Pl.seriesCast @Float doubleSeries
                    case f32 of
                        Left err -> expectationFailure (show err)
                        Right casted -> do
                            Pl.seriesDataType casted `shouldReturn` Right Pl.Float32
                            Pl.seriesFloat casted `shouldReturn` Right (V.fromList [Just 1.5, Nothing, Just (-2.25)])
                (Left err, _, _) -> expectationFailure (show err)
                (_, Left err, _) -> expectationFailure (show err)
                (_, _, Left err) -> expectationFailure (show err)

        it "sorts Series handles with explicit options" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    seriesResult <- Pl.column @Pl.Series df "age"
                    case seriesResult of
                        Left err -> expectationFailure (show err)
                        Right age -> do
                            let options =
                                    Pl.defaultSeriesSortOptions
                                        { Pl.seriesSortDescending = True
                                        , Pl.seriesSortNullsLast = True
                                        }
                            sortedResult <- Pl.seriesSort options age
                            case sortedResult of
                                Left err -> expectationFailure (show err)
                                Right sorted -> Pl.seriesInt64 sorted `shouldReturn` Right (V.fromList [Just 34, Just 29, Nothing])

        it "reports InvalidArgument for negative Series sort limits" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    seriesResult <- Pl.column @Pl.Series df "age"
                    case seriesResult of
                        Left err -> expectationFailure (show err)
                        Right age -> do
                            let options = Pl.defaultSeriesSortOptions { Pl.seriesSortLimit = Just (-1) }
                            sortedResult <- Pl.seriesSort options age
                            expectInvalidArgumentMessage "series sort limit must be non-negative" sortedResult

        it "reports InvalidArgument when Series sort limit exceeds Polars index size" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    seriesResult <- Pl.column @Pl.Series df "age"
                    case seriesResult of
                        Left err -> expectationFailure (show err)
                        Right age -> do
                            let overflowingLimit = fromIntegral (maxBound :: Word32) + 1
                                options = Pl.defaultSeriesSortOptions { Pl.seriesSortLimit = Just overflowingLimit }
                            sortedResult <- Pl.seriesSort options age
                            expectInvalidArgumentMessage "series sort limit exceeds Polars index size" sortedResult

        it "uniques, reverses, and drops nulls from Series handles" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    activeResult <- Pl.column @Pl.Series df "active"
                    ageResult <- Pl.column @Pl.Series df "age"
                    case (activeResult, ageResult) of
                        (Right active, Right age) -> do
                            uniqueActive <- Pl.seriesUnique active
                            case uniqueActive of
                                Left err -> expectationFailure (show err)
                                Right uniqueSeries -> Pl.seriesLength uniqueSeries `shouldReturn` Right 3
                            reverseAge <- Pl.seriesReverse age
                            case reverseAge of
                                Left err -> expectationFailure (show err)
                                Right reversed -> Pl.seriesInt64 reversed `shouldReturn` Right (V.fromList [Just 29, Nothing, Just 34])
                            denseAge <- Pl.seriesDropNulls age
                            case denseAge of
                                Left err -> expectationFailure (show err)
                                Right dense -> Pl.seriesInt64 dense `shouldReturn` Right (V.fromList [Just 34, Just 29])
                        (Left err, _) -> expectationFailure (show err)
                        (_, Left err) -> expectationFailure (show err)

        it "keeps stable unique order for text Series" $ do
            result <- Pl.readCsv employeesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    departmentResult <- Pl.column @Pl.Series df "department"
                    case departmentResult of
                        Left err -> expectationFailure (show err)
                        Right department -> do
                            uniqueResult <- Pl.seriesUniqueStable department
                            case uniqueResult of
                                Left err -> expectationFailure (show err)
                                Right stable -> Pl.seriesText stable `shouldReturn` Right (V.fromList [Just "Engineering", Just "Sales", Just "Support"])

        it "shifts Series handles in both directions" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    seriesResult <- Pl.column @Pl.Series df "age"
                    case seriesResult of
                        Left err -> expectationFailure (show err)
                        Right age -> do
                            shiftedForward <- Pl.seriesShift 1 age
                            case shiftedForward of
                                Left err -> expectationFailure (show err)
                                Right shifted -> Pl.seriesInt64 shifted `shouldReturn` Right (V.fromList [Nothing, Just 34, Nothing])
                            shiftedBackward <- Pl.seriesShift (-1) age
                            case shiftedBackward of
                                Left err -> expectationFailure (show err)
                                Right shifted -> Pl.seriesInt64 shifted `shouldReturn` Right (V.fromList [Nothing, Just 29, Nothing])
                            Pl.seriesInt64 age `shouldReturn` Right (V.fromList [Just 34, Nothing, Just 29])

        it "appends Series handles left-to-right and keeps the left name" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    seriesResult <- Pl.column @Pl.Series df "age"
                    case seriesResult of
                        Left err -> expectationFailure (show err)
                        Right age -> do
                            headResult <- Pl.seriesHead 1 age
                            case headResult of
                                Left err -> expectationFailure (show err)
                                Right firstAge -> do
                                    appendedResult <- Pl.seriesAppend age firstAge
                                    case appendedResult of
                                        Left err -> expectationFailure (show err)
                                        Right appended -> do
                                            Pl.seriesName appended `shouldReturn` Right "age"
                                            Pl.seriesInt64 appended `shouldReturn` Right (V.fromList [Just 34, Nothing, Just 29, Just 34])
                                            Pl.seriesInt64 age `shouldReturn` Right (V.fromList [Just 34, Nothing, Just 29])

        it "appends Series handles after explicit compatible casts" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    ageResult <- Pl.column @Pl.Series df "age"
                    scoreResult <- Pl.column @Pl.Series df "score"
                    case (ageResult, scoreResult) of
                        (Right age, Right score) -> do
                            castAge <- Pl.seriesCast @Double age
                            case castAge of
                                Left err -> expectationFailure (show err)
                                Right ageDouble -> do
                                    appendedResult <- Pl.seriesAppend ageDouble score
                                    case appendedResult of
                                        Left err -> expectationFailure (show err)
                                        Right appended -> Pl.seriesDouble appended `shouldReturn` Right (V.fromList [Just 34.0, Nothing, Just 29.0, Just 9.5, Just 8.25, Nothing])
                        (Left err, _) -> expectationFailure (show err)
                        (_, Left err) -> expectationFailure (show err)

        it "reports a Polars error for incompatible Series append dtypes" $ do
            result <- Pl.readCsv valuesCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    ageResult <- Pl.column @Pl.Series df "age"
                    nameResult <- Pl.column @Pl.Series df "name"
                    case (ageResult, nameResult) of
                        (Right age, Right nameSeries) -> do
                            appendedResult <- Pl.seriesAppend age nameSeries
                            case appendedResult of
                                Right _ -> expectationFailure "expected a Polars error for incompatible append dtypes"
                                Left err -> Pl.polarsErrorCode err `shouldBe` Pl.PolarsFailure
                        (Left err, _) -> expectationFailure (show err)
                        (_, Left err) -> expectationFailure (show err)

        it "inspects chunks and rechunks Series handles" $ do
            leftResult <- Pl.series @Int64 "value" (V.fromList [Just 1, Just 2, Just 3])
            rightResult <- Pl.series @Int64 "value" (V.fromList [Just 4, Just 5])
            textLeftResult <- Pl.series @T.Text "text" (V.fromList [Just "a", Nothing])
            textRightResult <- Pl.series @T.Text "text" (V.singleton (Just "c"))
            case (leftResult, rightResult, textLeftResult, textRightResult) of
                (Right left, Right right, Right textLeft, Right textRight) -> do
                    appendedResult <- Pl.seriesAppend left right
                    textAppendedResult <- Pl.seriesAppend textLeft textRight
                    case (appendedResult, textAppendedResult) of
                        (Right appended, Right textAppended) -> do
                            Pl.seriesNChunks appended `shouldReturn` Right 2
                            Pl.seriesChunkLengths appended `shouldReturn` Right (V.fromList [3, 2])
                            rechunkedResult <- Pl.seriesRechunk appended
                            textRechunkedResult <- Pl.seriesRechunk textAppended
                            case (rechunkedResult, textRechunkedResult) of
                                (Right rechunked, Right textRechunked) -> do
                                    Pl.seriesInt64 rechunked `shouldReturn` Right (V.fromList [Just 1, Just 2, Just 3, Just 4, Just 5])
                                    Pl.seriesNChunks rechunked `shouldReturn` Right 1
                                    Pl.seriesChunkLengths rechunked `shouldReturn` Right (V.singleton 5)
                                    Pl.seriesNChunks appended `shouldReturn` Right 2
                                    Pl.seriesText textRechunked `shouldReturn` Right (V.fromList [Just "a", Nothing, Just "c"])
                                    Pl.seriesNChunks textRechunked `shouldReturn` Right 1
                                (Left err, _) -> expectationFailure (show err)
                                (_, Left err) -> expectationFailure (show err)
                        (Left err, _) -> expectationFailure (show err)
                        (_, Left err) -> expectationFailure (show err)
                (Left err, _, _, _) -> expectationFailure (show err)
                (_, Left err, _, _) -> expectationFailure (show err)
                (_, _, Left err, _) -> expectationFailure (show err)
                (_, _, _, Left err) -> expectationFailure (show err)

        it "estimates size clears and expands Series values" $ do
            valuesResult <- Pl.series @Int64 "value" (V.fromList [Just 10, Nothing, Just 30])
            textResult <- Pl.series @T.Text "text" (V.fromList [Just "a", Just "bb", Nothing])
            emptyResult <- Pl.series @Int64 "empty" V.empty
            case (valuesResult, textResult, emptyResult) of
                (Right values, Right textSeries, Right empty) -> do
                    valuesSize <- Pl.seriesEstimatedSize values
                    textSize <- Pl.seriesEstimatedSize textSeries
                    emptySize <- Pl.seriesEstimatedSize empty
                    case (valuesSize, textSize, emptySize) of
                        (Right valueBytes, Right textBytes, Right emptyBytes) -> do
                            valueBytes `shouldSatisfy` (> 0)
                            textBytes `shouldSatisfy` (> 0)
                            emptyBytes `shouldSatisfy` (>= 0)
                        (Left err, _, _) -> expectationFailure (show err)
                        (_, Left err, _) -> expectationFailure (show err)
                        (_, _, Left err) -> expectationFailure (show err)

                    clearedResult <- Pl.seriesClear values
                    textClearedResult <- Pl.seriesClear textSeries
                    expandedResult <- Pl.seriesNewFromIndex 2 4 values
                    nullExpandedResult <- Pl.seriesNewFromIndex 1 3 values
                    textExpandedResult <- Pl.seriesNewFromIndex 0 2 textSeries
                    negativeIndex <- Pl.seriesNewFromIndex (-1) 2 values
                    negativeLength <- Pl.seriesNewFromIndex 1 (-2) values
                    outOfBounds <- Pl.seriesNewFromIndex 3 1 values
                    emptySourceResult <- Pl.seriesNewFromIndex 0 3 empty
                    case (clearedResult, textClearedResult, expandedResult, nullExpandedResult, textExpandedResult) of
                        (Right cleared, Right textCleared, Right expanded, Right nullExpanded, Right textExpanded) -> do
                            Pl.seriesName cleared `shouldReturn` Right "value"
                            Pl.seriesDataType cleared `shouldReturn` Right Pl.Int64
                            Pl.seriesLength cleared `shouldReturn` Right 0
                            Pl.seriesInt64 cleared `shouldReturn` Right V.empty
                            Pl.seriesName textCleared `shouldReturn` Right "text"
                            Pl.seriesDataType textCleared `shouldReturn` Right Pl.Utf8
                            Pl.seriesText textCleared `shouldReturn` Right V.empty
                            Pl.seriesInt64 values `shouldReturn` Right (V.fromList [Just 10, Nothing, Just 30])
                            Pl.seriesInt64 expanded `shouldReturn` Right (V.fromList [Just 30, Just 30, Just 30, Just 30])
                            Pl.seriesInt64 nullExpanded `shouldReturn` Right (V.fromList [Nothing, Nothing, Nothing])
                            Pl.seriesText textExpanded `shouldReturn` Right (V.fromList [Just "a", Just "a"])
                            case emptySourceResult of
                                Left err -> expectationFailure (show err)
                                Right emptySource -> do
                                    Pl.seriesName emptySource `shouldReturn` Right "empty"
                                    Pl.seriesDataType emptySource `shouldReturn` Right Pl.Int64
                                    Pl.seriesInt64 emptySource `shouldReturn` Right V.empty
                            expectInvalidArgumentMessage "series new-from-index index must be non-negative" negativeIndex
                            expectInvalidArgumentMessage "series new-from-index length must be non-negative" negativeLength
                            expectInvalidArgumentMessage "series new-from-index index out of bounds" outOfBounds
                        (Left err, _, _, _, _) -> expectationFailure (show err)
                        (_, Left err, _, _, _) -> expectationFailure (show err)
                        (_, _, Left err, _, _) -> expectationFailure (show err)
                        (_, _, _, Left err, _) -> expectationFailure (show err)
                        (_, _, _, _, Left err) -> expectationFailure (show err)
                (Left err, _, _) -> expectationFailure (show err)
                (_, Left err, _) -> expectationFailure (show err)
                (_, _, Left err) -> expectationFailure (show err)

        it "checks Series view metadata limits and split views" $ do
            valuesResult <- Pl.series @Int64 "value" (V.fromList [Just 1, Nothing, Just 3, Just 4])
            noNullResult <- Pl.series @Int64 "plain" (V.fromList [Just 1, Just 2])
            emptyResult <- Pl.series @Int64 "empty" V.empty
            textResult <- Pl.series @T.Text "text" (V.fromList [Just "a", Nothing, Just "c"])
            case (valuesResult, noNullResult, emptyResult, textResult) of
                (Right values, Right noNull, Right empty, Right textSeries) -> do
                    Pl.seriesHasNulls values `shouldReturn` Right True
                    Pl.seriesHasNulls noNull `shouldReturn` Right False
                    Pl.seriesHasNulls empty `shouldReturn` Right False
                    Pl.seriesIsEmpty values `shouldReturn` Right False
                    Pl.seriesIsEmpty empty `shouldReturn` Right True

                    limitedResult <- Pl.seriesLimit 2 values
                    oversizedLimitResult <- Pl.seriesLimit 99 values
                    emptyLimitResult <- Pl.seriesLimit 0 values
                    negativeLimit <- Pl.seriesLimit (-1) values
                    splitResult <- Pl.seriesSplitAt 2 values
                    negativeSplitResult <- Pl.seriesSplitAt (-1) values
                    oversizedSplitResult <- Pl.seriesSplitAt 99 values
                    undersizedSplitResult <- Pl.seriesSplitAt (-99) values
                    textSplitResult <- Pl.seriesSplitAt 2 textSeries
                    case (limitedResult, oversizedLimitResult, emptyLimitResult) of
                        (Right limited, Right oversizedLimit, Right emptyLimit) -> do
                            Pl.seriesInt64 limited `shouldReturn` Right (V.fromList [Just 1, Nothing])
                            Pl.seriesInt64 oversizedLimit `shouldReturn` Right (V.fromList [Just 1, Nothing, Just 3, Just 4])
                            Pl.seriesInt64 emptyLimit `shouldReturn` Right V.empty
                        (Left err, _, _) -> expectationFailure (show err)
                        (_, Left err, _) -> expectationFailure (show err)
                        (_, _, Left err) -> expectationFailure (show err)
                    case splitResult of
                        Left err -> expectationFailure (show err)
                        Right (left, right) -> do
                            Pl.seriesInt64 left `shouldReturn` Right (V.fromList [Just 1, Nothing])
                            Pl.seriesInt64 right `shouldReturn` Right (V.fromList [Just 3, Just 4])
                    case negativeSplitResult of
                        Left err -> expectationFailure (show err)
                        Right (left, right) -> do
                            Pl.seriesInt64 left `shouldReturn` Right (V.fromList [Just 1, Nothing, Just 3])
                            Pl.seriesInt64 right `shouldReturn` Right (V.singleton (Just 4))
                    case oversizedSplitResult of
                        Left err -> expectationFailure (show err)
                        Right (left, right) -> do
                            Pl.seriesInt64 left `shouldReturn` Right (V.fromList [Just 1, Nothing, Just 3, Just 4])
                            Pl.seriesInt64 right `shouldReturn` Right V.empty
                    case undersizedSplitResult of
                        Left err -> expectationFailure (show err)
                        Right (left, right) -> do
                            Pl.seriesInt64 left `shouldReturn` Right V.empty
                            Pl.seriesInt64 right `shouldReturn` Right (V.fromList [Just 1, Nothing, Just 3, Just 4])
                    case textSplitResult of
                        Left err -> expectationFailure (show err)
                        Right (left, right) -> do
                            Pl.seriesText left `shouldReturn` Right (V.fromList [Just "a", Nothing])
                            Pl.seriesText right `shouldReturn` Right (V.singleton (Just "c"))
                    expectInvalidArgumentMessage "series limit count must be non-negative" negativeLimit
                (Left err, _, _, _) -> expectationFailure (show err)
                (_, Left err, _, _) -> expectationFailure (show err)
                (_, _, Left err, _) -> expectationFailure (show err)
                (_, _, _, Left err) -> expectationFailure (show err)

    describe "Polars.Join" $ do
        it "inner joins two lazy CSV scans and applies the default suffix" $ do
            employeesResult <- Pl.scanCsv employeesCsv
            departmentsResult <- Pl.scanCsv departmentsCsv
            case (employeesResult, departmentsResult) of
                (Right employees, Right departments) -> do
                    joined <- Pl.innerJoin [Pl.col "department"] [Pl.col "department"] employees departments
                    case joined of
                        Left err -> expectationFailure (show err)
                        Right lf -> do
                            collected <- Pl.collect lf
                            case collected of
                                Left err -> expectationFailure (show err)
                                Right df -> do
                                    Pl.shape df `shouldReturn` Right (3, 6)
                                    schemaResult <- Pl.schema df
                                    fmap (map Pl.fieldName) schemaResult
                                        `shouldBe` Right ["id", "name", "department", "salary", "name_right", "budget"]
                                    textResult <- Pl.toText df
                                    fmap (T.isInfixOf "Grace") textResult `shouldBe` Right True
                                    fmap (T.isInfixOf "Heidi") textResult `shouldBe` Right True
                (Left err, _) -> expectationFailure (show err)
                (_, Left err) -> expectationFailure (show err)

        it "left joins and keeps unmatched left rows" $ do
            employeesResult <- Pl.scanCsv employeesCsv
            departmentsResult <- Pl.scanCsv departmentsCsv
            case (employeesResult, departmentsResult) of
                (Right employees, Right departments) -> do
                    joined <- Pl.leftJoin [Pl.col "department"] [Pl.col "department"] employees departments
                    case joined of
                        Left err -> expectationFailure (show err)
                        Right lf -> do
                            collected <- Pl.collect lf
                            case collected of
                                Left err -> expectationFailure (show err)
                                Right df -> do
                                    Pl.shape df `shouldReturn` Right (4, 6)
                                    textResult <- Pl.toText df
                                    fmap (T.isInfixOf "Support") textResult `shouldBe` Right True
                (Left err, _) -> expectationFailure (show err)
                (_, Left err) -> expectationFailure (show err)

        it "right joins and keeps unmatched right rows" $ do
            employeesResult <- Pl.scanCsv employeesCsv
            departmentsResult <- Pl.scanCsv departmentsCsv
            case (employeesResult, departmentsResult) of
                (Right employees, Right departments) -> do
                    joined <- Pl.rightJoin [Pl.col "department"] [Pl.col "department"] employees departments
                    case joined of
                        Left err -> expectationFailure (show err)
                        Right lf -> do
                            collected <- Pl.collect lf
                            case collected of
                                Left err -> expectationFailure (show err)
                                Right df -> do
                                    Pl.shape df `shouldReturn` Right (4, 6)
                                    textResult <- Pl.toText df
                                    fmap (T.isInfixOf "Finance") textResult `shouldBe` Right True
                (Left err, _) -> expectationFailure (show err)
                (_, Left err) -> expectationFailure (show err)

        it "full joins and keeps unmatched rows from both sides" $ do
            employeesResult <- Pl.scanCsv employeesCsv
            departmentsResult <- Pl.scanCsv departmentsCsv
            case (employeesResult, departmentsResult) of
                (Right employees, Right departments) -> do
                    joined <- Pl.fullJoin [Pl.col "department"] [Pl.col "department"] employees departments
                    case joined of
                        Left err -> expectationFailure (show err)
                        Right lf -> do
                            collected <- Pl.collect lf
                            case collected of
                                Left err -> expectationFailure (show err)
                                Right df -> do
                                    Pl.shape df `shouldReturn` Right (5, 7)
                                    schemaResult <- Pl.schema df
                                    fmap (map Pl.fieldName) schemaResult
                                        `shouldBe` Right ["id", "name", "department", "salary", "department_right", "name_right", "budget"]
                                    textResult <- Pl.toText df
                                    fmap (T.isInfixOf "Support") textResult `shouldBe` Right True
                                    fmap (T.isInfixOf "Finance") textResult `shouldBe` Right True
                (Left err, _) -> expectationFailure (show err)
                (_, Left err) -> expectationFailure (show err)

        it "semi joins and keeps matching left rows" $ do
            employeesResult <- Pl.scanCsv employeesCsv
            departmentsResult <- Pl.scanCsv departmentsCsv
            case (employeesResult, departmentsResult) of
                (Right employees, Right departments) -> do
                    joined <- Pl.semiJoin [Pl.col "department"] [Pl.col "department"] employees departments
                    case joined of
                        Left err -> expectationFailure (show err)
                        Right lf -> do
                            collected <- Pl.collect lf
                            case collected of
                                Left err -> expectationFailure (show err)
                                Right df -> do
                                    Pl.shape df `shouldReturn` Right (3, 4)
                                    schemaResult <- Pl.schema df
                                    fmap (map Pl.fieldName) schemaResult
                                        `shouldBe` Right ["id", "name", "department", "salary"]
                                    Pl.column @T.Text df "name"
                                        `shouldReturn` Right (V.fromList [Just "Alice", Just "Bob", Just "Carol"])
                (Left err, _) -> expectationFailure (show err)
                (_, Left err) -> expectationFailure (show err)

        it "anti joins and keeps unmatched left rows" $ do
            employeesResult <- Pl.scanCsv employeesCsv
            departmentsResult <- Pl.scanCsv departmentsCsv
            case (employeesResult, departmentsResult) of
                (Right employees, Right departments) -> do
                    joined <- Pl.antiJoin [Pl.col "department"] [Pl.col "department"] employees departments
                    case joined of
                        Left err -> expectationFailure (show err)
                        Right lf -> do
                            collected <- Pl.collect lf
                            case collected of
                                Left err -> expectationFailure (show err)
                                Right df -> do
                                    Pl.shape df `shouldReturn` Right (1, 4)
                                    schemaResult <- Pl.schema df
                                    fmap (map Pl.fieldName) schemaResult
                                        `shouldBe` Right ["id", "name", "department", "salary"]
                                    Pl.column @T.Text df "name" `shouldReturn` Right (V.fromList [Just "Eve"])
                                    Pl.column @T.Text df "department" `shouldReturn` Right (V.fromList [Just "Support"])
                (Left err, _) -> expectationFailure (show err)
                (_, Left err) -> expectationFailure (show err)

        it "cross joins and returns the Cartesian product" $ do
            employeesResult <- Pl.scanCsv employeesCsv
            departmentsResult <- Pl.scanCsv departmentsCsv
            case (employeesResult, departmentsResult) of
                (Right employees, Right departments) -> do
                    joined <- Pl.crossJoin employees departments
                    case joined of
                        Left err -> expectationFailure (show err)
                        Right lf -> do
                            collected <- Pl.collect lf
                            case collected of
                                Left err -> expectationFailure (show err)
                                Right df -> do
                                    Pl.shape df `shouldReturn` Right (12, 7)
                                    schemaResult <- Pl.schema df
                                    fmap (map Pl.fieldName) schemaResult
                                        `shouldBe` Right ["id", "name", "department", "salary", "department_right", "name_right", "budget"]
                                    textResult <- Pl.toText df
                                    fmap (T.isInfixOf "Grace") textResult `shouldBe` Right True
                                    fmap (T.isInfixOf "Ivan") textResult `shouldBe` Right True
                                    fmap (T.isInfixOf "Eve") textResult `shouldBe` Right True
                (Left err, _) -> expectationFailure (show err)
                (_, Left err) -> expectationFailure (show err)

        it "uses a custom suffix for duplicate right-side column names" $ do
            employeesResult <- Pl.scanCsv employeesCsv
            departmentsResult <- Pl.scanCsv departmentsCsv
            case (employeesResult, departmentsResult) of
                (Right employees, Right departments) -> do
                    let options =
                            Pl.defaultJoinOptions
                                { Pl.joinType = Pl.JoinLeft
                                , Pl.leftOn = [Pl.col "department"]
                                , Pl.rightOn = [Pl.col "department"]
                                , Pl.suffix = Just "_dept"
                                }
                    joined <- Pl.joinWith options employees departments
                    case joined of
                        Left err -> expectationFailure (show err)
                        Right lf -> do
                            collected <- Pl.collect lf
                            case collected of
                                Left err -> expectationFailure (show err)
                                Right df -> do
                                    schemaResult <- Pl.schema df
                                    fmap (map Pl.fieldName) schemaResult
                                        `shouldBe` Right ["id", "name", "department", "salary", "name_dept", "budget"]
                (Left err, _) -> expectationFailure (show err)
                (_, Left err) -> expectationFailure (show err)

        it "rejects empty left join keys" $ do
            employeesResult <- Pl.scanCsv employeesCsv
            departmentsResult <- Pl.scanCsv departmentsCsv
            case (employeesResult, departmentsResult) of
                (Right employees, Right departments) -> do
                    result <- Pl.innerJoin [] [Pl.col "department"] employees departments
                    case result of
                        Right _ -> expectationFailure "expected InvalidArgument for empty left join keys"
                        Left err -> Pl.polarsErrorCode err `shouldBe` Pl.InvalidArgument
                (Left err, _) -> expectationFailure (show err)
                (_, Left err) -> expectationFailure (show err)

        it "rejects mismatched join key counts" $ do
            employeesResult <- Pl.scanCsv employeesCsv
            departmentsResult <- Pl.scanCsv departmentsCsv
            case (employeesResult, departmentsResult) of
                (Right employees, Right departments) -> do
                    result <- Pl.innerJoin [Pl.col "department", Pl.col "name"] [Pl.col "department"] employees departments
                    case result of
                        Right _ -> expectationFailure "expected InvalidArgument for mismatched join key counts"
                        Left err -> Pl.polarsErrorCode err `shouldBe` Pl.InvalidArgument
                (Left err, _) -> expectationFailure (show err)
                (_, Left err) -> expectationFailure (show err)

        it "rejects keyed cross joins" $ do
            employeesResult <- Pl.scanCsv employeesCsv
            departmentsResult <- Pl.scanCsv departmentsCsv
            case (employeesResult, departmentsResult) of
                (Right employees, Right departments) -> do
                    let options =
                            Pl.defaultJoinOptions
                                { Pl.joinType = Pl.JoinCross
                                , Pl.leftOn = [Pl.col "department"]
                                , Pl.rightOn = [Pl.col "department"]
                                }
                    result <- Pl.joinWith options employees departments
                    case result of
                        Right _ -> expectationFailure "expected InvalidArgument for keyed cross join"
                        Left err -> Pl.polarsErrorCode err `shouldBe` Pl.InvalidArgument
                (Left err, _) -> expectationFailure (show err)
                (_, Left err) -> expectationFailure (show err)

    describe "Polars.Arrow" $ do
        it "imports a standard Arrow RecordBatch into a DataFrame" $
            withPeopleRecordBatch $ \schemaPtr arrayPtr -> do
                result <- Pl.fromArrowRecordBatch (Pl.unsafeArrowRecordBatch schemaPtr arrayPtr)
                case result of
                    Left err -> expectationFailure (show err)
                    Right df -> do
                        Pl.shape df `shouldReturn` Right (3, 2)
                        Pl.column @T.Text df "name" `shouldReturn` Right (V.fromList [Just "Alice", Just "Bob", Nothing])
                        Pl.column @Int64 df "age" `shouldReturn` Right (V.fromList [Just 34, Nothing, Just 29])

        it "imports a standard Arrow array into a Series" $
            withAgeArray $ \schemaPtr arrayPtr -> do
                result <- Pl.fromArrowSeries (Pl.unsafeArrowSeries schemaPtr arrayPtr)
                case result of
                    Left err -> expectationFailure (show err)
                    Right series -> do
                        Pl.seriesName series `shouldReturn` Right "age"
                        Pl.seriesLength series `shouldReturn` Right 3
                        Pl.seriesNullCount series `shouldReturn` Right 1
                        Pl.seriesInt64 series `shouldReturn` Right (V.fromList [Just 34, Nothing, Just 29])

        it "reports InvalidArgument for null Arrow RecordBatch pointers" $ do
            result <- Pl.fromArrowRecordBatch (Pl.unsafeArrowRecordBatch nullPtr nullPtr)
            case result of
                Right _ -> expectationFailure "expected InvalidArgument for null Arrow pointers"
                Left err -> Pl.polarsErrorCode err `shouldBe` Pl.InvalidArgument

        it "reports InvalidArgument for null Arrow Series pointers" $ do
            result <- Pl.fromArrowSeries (Pl.unsafeArrowSeries nullPtr nullPtr)
            case result of
                Right _ -> expectationFailure "expected InvalidArgument for null Arrow pointers"
                Left err -> Pl.polarsErrorCode err `shouldBe` Pl.InvalidArgument

        it "exports a DataFrame to an Arrow RecordBatch and imports it back" $ do
            nameResult <- Pl.series @T.Text "name" (V.fromList [Just "Alice", Just "Bob", Nothing])
            ageResult <- Pl.series @Int64 "age" (V.fromList [Just 34, Nothing, Just 29])
            case (nameResult, ageResult) of
                (Right name, Right age) -> do
                    dfResult <- Pl.dataFrame [name, age]
                    case dfResult of
                        Left err -> expectationFailure (show err)
                        Right df -> do
                            roundTrip <- Pl.withArrowRecordBatch df $ \schemaPtr arrayPtr ->
                                Pl.fromArrowRecordBatch (Pl.unsafeArrowRecordBatch schemaPtr arrayPtr)
                            case roundTrip of
                                Left err -> expectationFailure (show err)
                                Right (Left err) -> expectationFailure (show err)
                                Right (Right imported) -> do
                                    Pl.shape imported `shouldReturn` Right (3, 2)
                                    Pl.column @T.Text imported "name" `shouldReturn` Right (V.fromList [Just "Alice", Just "Bob", Nothing])
                                    Pl.column @Int64 imported "age" `shouldReturn` Right (V.fromList [Just 34, Nothing, Just 29])
                (Left err, _) -> expectationFailure (show err)
                (_, Left err) -> expectationFailure (show err)

        it "round-trips scalar dtype matrix columns through Arrow RecordBatch" $ do
            i8Result <- Pl.series @Int8 "i8" (V.fromList [Just (-128), Nothing, Just 127])
            i16Result <- Pl.series @Int16 "i16" (V.fromList [Just (-32768), Nothing, Just 32767])
            i32Result <- Pl.series @Int32 "i32" (V.fromList [Just (-2147483648), Nothing, Just 2147483647])
            u8Result <- Pl.series @Word8 "u8" (V.fromList [Just 0, Nothing, Just 255])
            u16Result <- Pl.series @Word16 "u16" (V.fromList [Just 0, Nothing, Just 65535])
            u32Result <- Pl.series @Word32 "u32" (V.fromList [Just 0, Nothing, Just 4294967295])
            u64Result <- Pl.series @Word64 "u64" (V.fromList [Just 0, Nothing, Just 9223372036854775808])
            f32Result <- Pl.series @Float "f32" (V.fromList [Just 1.5, Nothing, Just (-2.25)])
            case (i8Result, i16Result, i32Result, u8Result, u16Result, u32Result, u64Result, f32Result) of
                (Right i8, Right i16, Right i32, Right u8, Right u16, Right u32, Right u64, Right f32) -> do
                    dfResult <- Pl.dataFrame [i8, i16, i32, u8, u16, u32, u64, f32]
                    case dfResult of
                        Left err -> expectationFailure (show err)
                        Right df -> do
                            roundTrip <- Pl.withArrowRecordBatch df $ \schemaPtr arrayPtr ->
                                Pl.fromArrowRecordBatch (Pl.unsafeArrowRecordBatch schemaPtr arrayPtr)
                            case roundTrip of
                                Left err -> expectationFailure (show err)
                                Right (Left err) -> expectationFailure (show err)
                                Right (Right imported) -> do
                                    Pl.column @Int8 imported "i8" `shouldReturn` Right (V.fromList [Just (-128), Nothing, Just 127])
                                    Pl.column @Int16 imported "i16" `shouldReturn` Right (V.fromList [Just (-32768), Nothing, Just 32767])
                                    Pl.column @Int32 imported "i32" `shouldReturn` Right (V.fromList [Just (-2147483648), Nothing, Just 2147483647])
                                    Pl.column @Word8 imported "u8" `shouldReturn` Right (V.fromList [Just 0, Nothing, Just 255])
                                    Pl.column @Word16 imported "u16" `shouldReturn` Right (V.fromList [Just 0, Nothing, Just 65535])
                                    Pl.column @Word32 imported "u32" `shouldReturn` Right (V.fromList [Just 0, Nothing, Just 4294967295])
                                    Pl.column @Word64 imported "u64" `shouldReturn` Right (V.fromList [Just 0, Nothing, Just 9223372036854775808])
                                    Pl.column @Float imported "f32" `shouldReturn` Right (V.fromList [Just 1.5, Nothing, Just (-2.25)])
                (Left err, _, _, _, _, _, _, _) -> expectationFailure (show err)
                (_, Left err, _, _, _, _, _, _) -> expectationFailure (show err)
                (_, _, Left err, _, _, _, _, _) -> expectationFailure (show err)
                (_, _, _, Left err, _, _, _, _) -> expectationFailure (show err)
                (_, _, _, _, Left err, _, _, _) -> expectationFailure (show err)
                (_, _, _, _, _, Left err, _, _) -> expectationFailure (show err)
                (_, _, _, _, _, _, Left err, _) -> expectationFailure (show err)
                (_, _, _, _, _, _, _, Left err) -> expectationFailure (show err)

        it "exports a Series to an Arrow array and imports it back" $ do
            seriesResult <- Pl.series @Int64 "age" (V.fromList [Just 34, Nothing, Just 29])
            case seriesResult of
                Left err -> expectationFailure (show err)
                Right series -> do
                    roundTrip <- Pl.withArrowSeries series $ \schemaPtr arrayPtr ->
                        Pl.fromArrowSeries (Pl.unsafeArrowSeries schemaPtr arrayPtr)
                    case roundTrip of
                        Left err -> expectationFailure (show err)
                        Right (Left err) -> expectationFailure (show err)
                        Right (Right imported) -> do
                            Pl.seriesName imported `shouldReturn` Right "age"
                            Pl.seriesLength imported `shouldReturn` Right 3
                            Pl.seriesNullCount imported `shouldReturn` Right 1
                            Pl.seriesInt64 imported `shouldReturn` Right (V.fromList [Just 34, Nothing, Just 29])

        it "round-trips scalar dtype matrix Series through Arrow arrays" $ do
            i8Result <- Pl.series @Int8 "i8" (V.fromList [Just (-128), Nothing, Just 127])
            i16Result <- Pl.series @Int16 "i16" (V.fromList [Just (-32768), Nothing, Just 32767])
            i32Result <- Pl.series @Int32 "i32" (V.fromList [Just (-2147483648), Nothing, Just 2147483647])
            u8Result <- Pl.series @Word8 "u8" (V.fromList [Just 0, Nothing, Just 255])
            u16Result <- Pl.series @Word16 "u16" (V.fromList [Just 0, Nothing, Just 65535])
            u32Result <- Pl.series @Word32 "u32" (V.fromList [Just 0, Nothing, Just 4294967295])
            u64Result <- Pl.series @Word64 "u64" (V.fromList [Just 0, Nothing, Just 9223372036854775808])
            f32Result <- Pl.series @Float "f32" (V.fromList [Just 1.5, Nothing, Just (-2.25)])
            case (i8Result, i16Result, i32Result, u8Result, u16Result, u32Result, u64Result, f32Result) of
                (Right i8, Right i16, Right i32, Right u8, Right u16, Right u32, Right u64, Right f32) -> do
                    let roundTripScalar :: String -> Pl.Series -> IO Pl.Series
                        roundTripScalar label input = do
                            roundTrip <- Pl.withArrowSeries input $ \schemaPtr arrayPtr ->
                                Pl.fromArrowSeries (Pl.unsafeArrowSeries schemaPtr arrayPtr)
                            case roundTrip of
                                Left err -> fail (label <> " Arrow export failed: " <> show err)
                                Right (Left err) -> fail (label <> " Arrow import failed: " <> show err)
                                Right (Right imported) -> pure imported
                    i8Imported <- roundTripScalar "i8" i8
                    i16Imported <- roundTripScalar "i16" i16
                    i32Imported <- roundTripScalar "i32" i32
                    u8Imported <- roundTripScalar "u8" u8
                    u16Imported <- roundTripScalar "u16" u16
                    u32Imported <- roundTripScalar "u32" u32
                    u64Imported <- roundTripScalar "u64" u64
                    f32Imported <- roundTripScalar "f32" f32
                    Pl.seriesInt8 i8Imported `shouldReturn` Right (V.fromList [Just (-128), Nothing, Just 127])
                    Pl.seriesInt16 i16Imported `shouldReturn` Right (V.fromList [Just (-32768), Nothing, Just 32767])
                    Pl.seriesInt32 i32Imported `shouldReturn` Right (V.fromList [Just (-2147483648), Nothing, Just 2147483647])
                    Pl.seriesWord8 u8Imported `shouldReturn` Right (V.fromList [Just 0, Nothing, Just 255])
                    Pl.seriesWord16 u16Imported `shouldReturn` Right (V.fromList [Just 0, Nothing, Just 65535])
                    Pl.seriesWord32 u32Imported `shouldReturn` Right (V.fromList [Just 0, Nothing, Just 4294967295])
                    Pl.seriesWord64 u64Imported `shouldReturn` Right (V.fromList [Just 0, Nothing, Just 9223372036854775808])
                    Pl.seriesFloat f32Imported `shouldReturn` Right (V.fromList [Just 1.5, Nothing, Just (-2.25)])
                (Left err, _, _, _, _, _, _, _) -> expectationFailure (show err)
                (_, Left err, _, _, _, _, _, _) -> expectationFailure (show err)
                (_, _, Left err, _, _, _, _, _) -> expectationFailure (show err)
                (_, _, _, Left err, _, _, _, _) -> expectationFailure (show err)
                (_, _, _, _, Left err, _, _, _) -> expectationFailure (show err)
                (_, _, _, _, _, Left err, _, _) -> expectationFailure (show err)
                (_, _, _, _, _, _, Left err, _) -> expectationFailure (show err)
                (_, _, _, _, _, _, _, Left err) -> expectationFailure (show err)

    describe "Dataset-driven fixtures" $ do
        it "reads a Polars public iris fixture" $ do
            result <- Pl.readCsv polarsIrisCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    Pl.shape df `shouldReturn` Right (150, 5)
                    fields <- Pl.schema df
                    fmap (map Pl.fieldName) fields `shouldBe` Right ["sepal_length", "sepal_width", "petal_length", "petal_width", "species"]
                    lengths <- Pl.column @Double df "sepal_length"
                    case lengths of
                        Left err -> expectationFailure (show err)
                        Right values -> V.length values `shouldBe` 150
                    species <- Pl.column @T.Text df "species"
                    case species of
                        Left err -> expectationFailure (show err)
                        Right values -> V.length values `shouldBe` 150

        it "reads a Metasyn synthetic people fixture" $ do
            result <- Pl.readCsv metasynPeopleCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df -> do
                    Pl.shape df `shouldReturn` Right (16, 3)
                    cities <- Pl.column @T.Text df "city"
                    case cities of
                        Left err -> expectationFailure (show err)
                        Right values -> V.length values `shouldBe` 16
                    ages <- Pl.column @Int64 df "age"
                    case ages of
                        Left err -> expectationFailure (show err)
                        Right values -> V.length values `shouldBe` 16
                    scores <- Pl.column @Double df "score"
                    case scores of
                        Left err -> expectationFailure (show err)
                        Right values -> V.length values `shouldBe` 16

    describe "Dataset-driven lazy queries" $ do
        it "filters, groups, sorts, and collects the iris fixture" $ do
            scanResult <- Pl.scanCsv polarsIrisCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    filtered <- Pl.filter (Pl.col "sepal_length" Pl..> Pl.litDouble 5.0) lf0
                    case filtered of
                        Left err -> expectationFailure (show err)
                        Right lf1 -> do
                            grouped <-
                                Pl.agg
                                    [Pl.alias "mean_sepal_width" (Pl.mean_ (Pl.col "sepal_width"))]
                                    (Pl.groupByStable [Pl.col "species"] lf1)
                            case grouped of
                                Left err -> expectationFailure (show err)
                                Right lf2 -> do
                                    sorted <- Pl.sort ["species"] lf2
                                    case sorted of
                                        Left err -> expectationFailure (show err)
                                        Right lf3 -> do
                                            collected <- Pl.collect lf3
                                            case collected of
                                                Left err -> expectationFailure (show err)
                                                Right df -> do
                                                    Pl.shape df `shouldReturn` Right (3, 2)
                                                    Pl.column @T.Text df "species"
                                                        `shouldReturn` Right (V.fromList [Just "setosa", Just "versicolor", Just "virginica"])
                                                    means <- Pl.column @Double df "mean_sepal_width"
                                                    case means of
                                                        Left err -> expectationFailure (show err)
                                                        Right values -> do
                                                            V.length values `shouldBe` 3
                                                            V.toList values `shouldSatisfy` all isJust

        it "adds derived Metasyn columns and filters on them" $ do
            scanResult <- Pl.scanCsv metasynPeopleCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    enriched <- Pl.withColumns [Pl.alias "score_boosted" (Pl.col "score" Pl..+ Pl.litDouble 1.0)] lf0
                    case enriched of
                        Left err -> expectationFailure (show err)
                        Right lf1 -> do
                            filtered <- Pl.filter (Pl.col "age" Pl..>= Pl.litInt 30) lf1
                            case filtered of
                                Left err -> expectationFailure (show err)
                                Right lf2 -> do
                                    sorted <- Pl.sort ["city"] lf2
                                    case sorted of
                                        Left err -> expectationFailure (show err)
                                        Right lf3 -> do
                                            limited <- Pl.limit 8 lf3
                                            case limited of
                                                Left err -> expectationFailure (show err)
                                                Right lf4 -> do
                                                    collected <- Pl.collect lf4
                                                    case collected of
                                                        Left err -> expectationFailure (show err)
                                                        Right df -> do
                                                            shapeResult <- Pl.shape df
                                                            case shapeResult of
                                                                Left err -> expectationFailure (show err)
                                                                Right (rows, columns) -> do
                                                                    rows `shouldSatisfy` (>= 1)
                                                                    rows `shouldSatisfy` (<= 8)
                                                                    columns `shouldBe` 4
                                                            cities <- Pl.column @T.Text df "city"
                                                            case cities of
                                                                Left err -> expectationFailure (show err)
                                                                Right values -> V.length values `shouldSatisfy` (>= 1)
                                                            boosted <- Pl.column @Double df "score_boosted"
                                                            case boosted of
                                                                Left err -> expectationFailure (show err)
                                                                Right values -> V.toList values `shouldSatisfy` all isJust

    describe "Expression DSL core" $ do
        it "casts, fills nulls, and evaluates predicates" $ do
            scanResult <- Pl.scanCsv valuesCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    projected <-
                        Pl.select
                            [ Pl.alias "age_f64" (Pl.cast Pl.Float64 (Pl.fillNull (Pl.litInt 0) (Pl.col "age")))
                            , Pl.alias "age_was_null" (Pl.isNull (Pl.col "age"))
                            ]
                            lf0
                    case projected of
                        Left err -> expectationFailure (show err)
                        Right lf1 -> do
                            collected <- Pl.collect lf1
                            case collected of
                                Left err -> expectationFailure (show err)
                                Right df -> do
                                    Pl.shape df `shouldReturn` Right (3, 2)
                                    Pl.column @Double df "age_f64" `shouldReturn` Right (V.fromList [Just 34.0, Just 0.0, Just 29.0])
                                    Pl.column @Bool df "age_was_null" `shouldReturn` Right (V.fromList [Just False, Just True, Just False])

        it "uses conditionals, expression filters, and statistics" $ do
            scanResult <- Pl.scanCsv valuesCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    projected <-
                        Pl.select
                            [ Pl.alias "status" (Pl.whenThenOtherwise (Pl.isNull (Pl.col "score")) (Pl.litText "missing") (Pl.litText "present"))
                            , Pl.alias "present_score_mean" (Pl.mean_ (Pl.exprFilter (Pl.col "score") (Pl.isNotNull (Pl.col "score"))))
                            , Pl.alias "score_median" (Pl.median_ (Pl.col "score"))
                            , Pl.alias "score_q50" (Pl.quantile_ Pl.QuantileNearest (Pl.litDouble 0.5) (Pl.col "score"))
                            ]
                            lf0
                    case projected of
                        Left err -> expectationFailure (show err)
                        Right lf1 -> do
                            collected <- Pl.collect lf1
                            case collected of
                                Left err -> expectationFailure (show err)
                                Right df -> do
                                    Pl.shape df `shouldReturn` Right (3, 4)
                                    Pl.column @T.Text df "status" `shouldReturn` Right (V.fromList [Just "present", Just "present", Just "missing"])
                                    Pl.column @Double df "present_score_mean" `shouldReturn` Right (V.fromList [Just 8.875, Just 8.875, Just 8.875])
                                    Pl.column @Double df "score_median" `shouldReturn` Right (V.fromList [Just 8.875, Just 8.875, Just 8.875])
                                    Pl.column @Double df "score_q50" `shouldReturn` Right (V.fromList [Just 9.5, Just 9.5, Just 9.5])

        it "uses cumulative expressions, rank, and windows" $ do
            scanResult <- Pl.scanCsv salesCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    projected <-
                        Pl.select
                            [ Pl.col "department"
                            , Pl.alias "salary_rank" (Pl.cast Pl.Int64 (Pl.rank Pl.defaultRankOptions {Pl.rankDescending = True} (Pl.col "salary")))
                            , Pl.alias "department_salary_total" (Pl.over [Pl.col "department"] (Pl.sum_ (Pl.col "salary")))
                            , Pl.alias "salary_cum" (Pl.cumSum False (Pl.col "salary"))
                            ]
                            lf0
                    case projected of
                        Left err -> expectationFailure (show err)
                        Right lf1 -> do
                            collected <- Pl.collect lf1
                            case collected of
                                Left err -> expectationFailure (show err)
                                Right df -> do
                                    Pl.shape df `shouldReturn` Right (4, 4)
                                    Pl.column @Int64 df "salary_rank" `shouldReturn` Right (V.fromList [Just 3, Just 1, Just 4, Just 2])
                                    Pl.column @Int64 df "department_salary_total" `shouldReturn` Right (V.fromList [Just 250, Just 250, Just 200, Just 200])
                                    Pl.column @Int64 df "salary_cum" `shouldReturn` Right (V.fromList [Just 100, Just 250, Just 340, Just 450])

        it "sorts and slices expression values" $ do
            scanResult <- Pl.scanCsv salesCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    projected <-
                        Pl.select
                            [ Pl.alias "top_names" (Pl.exprSlice (Pl.exprSortBy Pl.defaultExprSortOptions {Pl.exprSortDescending = True} [Pl.col "salary"] (Pl.col "name")) (Pl.litInt 0) (Pl.litInt 2))
                            ]
                            lf0
                    case projected of
                        Left err -> expectationFailure (show err)
                        Right lf1 -> do
                            collected <- Pl.collect lf1
                            case collected of
                                Left err -> expectationFailure (show err)
                                Right df -> do
                                    Pl.shape df `shouldReturn` Right (2, 1)
                                    Pl.column @T.Text df "top_names" `shouldReturn` Right (V.fromList [Just "Bob", Just "Dave"])

        it "uses strictCast with typed extraction" $ do
            scanResult <- Pl.scanCsv valuesCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    projected <-
                        Pl.select
                            [ Pl.alias "score_i64" (Pl.strictCast Pl.Int64 (Pl.col "score"))
                            , Pl.alias "score_f64" (Pl.strictCast Pl.Float64 (Pl.col "score"))
                            ]
                            lf0
                    case projected of
                        Left err -> expectationFailure (show err)
                        Right lf1 -> do
                            collected <- Pl.collect lf1
                            case collected of
                                Left err -> expectationFailure (show err)
                                Right df -> do
                                    Pl.shape df `shouldReturn` Right (3, 2)
                                    Pl.column @Int64 df "score_i64" `shouldReturn` Right (V.fromList [Just 9, Just 8, Nothing])
                                    Pl.column @Double df "score_f64" `shouldReturn` Right (V.fromList [Just 9.5, Just 8.25, Nothing])

        it "uses fillNan, isNan, isNotNan, isFinite, and isInfinite" $ do
            scanResult <- Pl.scanCsv floatSpecialsCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    projected <-
                        Pl.select
                            [ Pl.alias "is_nan" (Pl.isNan (Pl.col "value"))
                            , Pl.alias "is_not_nan" (Pl.isNotNan (Pl.col "value"))
                            , Pl.alias "is_finite" (Pl.isFinite (Pl.col "value"))
                            , Pl.alias "is_infinite" (Pl.isInfinite (Pl.col "value"))
                            , Pl.alias "filled_nan" (Pl.fillNan (Pl.litDouble 0.0) (Pl.col "value"))
                            ]
                            lf0
                    case projected of
                        Left err -> expectationFailure (show err)
                        Right lf1 -> do
                            collected <- Pl.collect lf1
                            case collected of
                                Left err -> expectationFailure (show err)
                                Right df -> do
                                    Pl.shape df `shouldReturn` Right (4, 5)
                                    Pl.column @Bool df "is_nan" `shouldReturn` Right (V.fromList [Just False, Just True, Just False, Just False])
                                    Pl.column @Bool df "is_not_nan" `shouldReturn` Right (V.fromList [Just True, Just False, Just True, Just True])
                                    Pl.column @Bool df "is_finite" `shouldReturn` Right (V.fromList [Just True, Just False, Just False, Just False])
                                    Pl.column @Bool df "is_infinite" `shouldReturn` Right (V.fromList [Just False, Just False, Just True, Just True])
                                    actualFilled <- Pl.column @Double df "filled_nan"
                                    case actualFilled of
                                        Left err -> expectationFailure (show err)
                                        Right filled -> do
                                            let expected = V.fromList [Just 1.0, Just 0.0, Just (1.0 / 0.0), Just (negate (1.0 / 0.0))]
                                            shouldApproximate 1e-12 expected filled

        it "computes std, var, and nUnique over scores" $ do
            scanResult <- Pl.scanCsv valuesCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    projected <-
                        Pl.select
                            [ Pl.alias "score_std" (Pl.std_ 1 (Pl.col "score"))
                            , Pl.alias "score_var" (Pl.var_ 1 (Pl.col "score"))
                            , Pl.alias "score_nunique" (Pl.cast Pl.Int64 (Pl.nUnique_ (Pl.col "score")))
                            ]
                            lf0
                    case projected of
                        Left err -> expectationFailure (show err)
                        Right lf1 -> do
                            collected <- Pl.collect lf1
                            case collected of
                                Left err -> expectationFailure (show err)
                                Right df -> do
                                    Pl.shape df `shouldReturn` Right (1, 3)
                                    actualStd <- Pl.column @Double df "score_std"
                                    case actualStd of
                                        Left err -> expectationFailure (show err)
                                        Right vd -> shouldApproximate 1e-12 (V.singleton (Just (sqrt 0.78125))) vd
                                    actualVar <- Pl.column @Double df "score_var"
                                    case actualVar of
                                        Left err -> expectationFailure (show err)
                                        Right vd -> shouldApproximate 1e-12 (V.singleton (Just 0.78125)) vd
                                    Pl.column @Int64 df "score_nunique" `shouldReturn` Right (V.singleton (Just 3))

        it "uses cumCount, cumProd, cumMin, cumMax, and reverse cumSum over salaries" $ do
            scanResult <- Pl.scanCsv salesCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    projected <-
                        Pl.select
                            [ Pl.alias "cum_count" (Pl.cast Pl.Int64 (Pl.cumCount False (Pl.col "salary")))
                            , Pl.alias "cum_prod" (Pl.cumProd False (Pl.col "salary"))
                            , Pl.alias "cum_min" (Pl.cumMin False (Pl.col "salary"))
                            , Pl.alias "cum_max" (Pl.cumMax False (Pl.col "salary"))
                            , Pl.alias "rev_cum_sum" (Pl.cumSum True (Pl.col "salary"))
                            ]
                            lf0
                    case projected of
                        Left err -> expectationFailure (show err)
                        Right lf1 -> do
                            collected <- Pl.collect lf1
                            case collected of
                                Left err -> expectationFailure (show err)
                                Right df -> do
                                    Pl.shape df `shouldReturn` Right (4, 5)
                                    Pl.column @Int64 df "cum_count" `shouldReturn` Right (V.fromList [Just 1, Just 2, Just 3, Just 4])
                                    Pl.column @Int64 df "cum_prod" `shouldReturn` Right (V.fromList [Just 100, Just 15000, Just 1350000, Just 148500000])
                                    Pl.column @Int64 df "cum_min" `shouldReturn` Right (V.fromList [Just 100, Just 100, Just 90, Just 90])
                                    Pl.column @Int64 df "cum_max" `shouldReturn` Right (V.fromList [Just 100, Just 150, Just 150, Just 150])
                                    Pl.column @Int64 df "rev_cum_sum" `shouldReturn` Right (V.fromList [Just 450, Just 350, Just 200, Just 110])

        it "computes additional quantile methods over scores" $ do
            scanResult <- Pl.scanCsv valuesCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    projected <-
                        Pl.select
                            [ Pl.alias "q_lower" (Pl.quantile_ Pl.QuantileLower (Pl.litDouble 0.5) (Pl.col "score"))
                            , Pl.alias "q_higher" (Pl.quantile_ Pl.QuantileHigher (Pl.litDouble 0.5) (Pl.col "score"))
                            , Pl.alias "q_midpoint" (Pl.quantile_ Pl.QuantileMidpoint (Pl.litDouble 0.5) (Pl.col "score"))
                            , Pl.alias "q_linear" (Pl.quantile_ Pl.QuantileLinear (Pl.litDouble 0.5) (Pl.col "score"))
                            ]
                            lf0
                    case projected of
                        Left err -> expectationFailure (show err)
                        Right lf1 -> do
                            collected <- Pl.collect lf1
                            case collected of
                                Left err -> expectationFailure (show err)
                                Right df -> do
                                    Pl.shape df `shouldReturn` Right (1, 4)
                                    Pl.column @Double df "q_lower" `shouldReturn` Right (V.singleton (Just 8.25))
                                    Pl.column @Double df "q_higher" `shouldReturn` Right (V.singleton (Just 9.5))
                                    Pl.column @Double df "q_midpoint" `shouldReturn` Right (V.singleton (Just 8.875))
                                    Pl.column @Double df "q_linear" `shouldReturn` Right (V.singleton (Just 8.875))

    describe "Expression DSL string namespace" $ do
        it "matches and transforms string values" $ do
            scanResult <- Pl.scanCsv stringsCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    projected <-
                        Pl.select
                            [ Pl.alias "contains_li" (Pl.strContainsLiteral (Pl.col "text") (Pl.litText "li"))
                            , Pl.alias "starts_a" (Pl.strStartsWith (Pl.col "text") (Pl.litText " A"))
                            , Pl.alias "ends_space" (Pl.strEndsWith (Pl.col "text") (Pl.litText " "))
                            , Pl.alias "stripped" (Pl.strStrip (Pl.col "text") (Pl.litText " "))
                            , Pl.alias "lowered" (Pl.strToLowercase (Pl.col "text"))
                            , Pl.alias "uppered" (Pl.strToUppercase (Pl.col "text"))
                            , Pl.alias "bytes" (Pl.cast Pl.Int64 (Pl.strLenBytes (Pl.col "text")))
                            , Pl.alias "chars" (Pl.cast Pl.Int64 (Pl.strLenChars (Pl.col "text")))
                            ]
                            lf0
                    case projected of
                        Left err -> expectationFailure (show err)
                        Right lf1 -> do
                            collected <- Pl.collect lf1
                            case collected of
                                Left err -> expectationFailure (show err)
                                Right df -> do
                                    Pl.shape df `shouldReturn` Right (4, 8)
                                    Pl.column @Bool df "contains_li" `shouldReturn` Right (V.fromList [Just True, Just False, Just False, Just False])
                                    Pl.column @Bool df "starts_a" `shouldReturn` Right (V.fromList [Just True, Just False, Just False, Just False])
                                    Pl.column @Bool df "ends_space" `shouldReturn` Right (V.fromList [Just True, Just False, Just False, Just False])
                                    Pl.column @T.Text df "stripped" `shouldReturn` Right (V.fromList [Just "Alice", Just "βeta", Just "CAROL", Just "日本語"])
                                    Pl.column @T.Text df "lowered" `shouldReturn` Right (V.fromList [Just " alice ", Just "βeta", Just "carol", Just "日本語"])
                                    Pl.column @T.Text df "uppered" `shouldReturn` Right (V.fromList [Just " ALICE ", Just "ΒETA", Just "CAROL", Just "日本語"])
                                    Pl.column @Int64 df "bytes" `shouldReturn` Right (V.fromList [Just 7, Just 5, Just 5, Just 9])
                                    Pl.column @Int64 df "chars" `shouldReturn` Right (V.fromList [Just 7, Just 4, Just 5, Just 3])

        it "slices string values by character offsets" $ do
            scanResult <- Pl.scanCsv stringsCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    projected <-
                        Pl.select
                            [ Pl.alias "slice_0_2" (Pl.strSlice (Pl.col "text") (Pl.litInt 0) (Pl.litInt 2))
                            , Pl.alias "head_2" (Pl.strHead (Pl.col "text") (Pl.litInt 2))
                            , Pl.alias "tail_2" (Pl.strTail (Pl.col "text") (Pl.litInt 2))
                            , Pl.alias "strip_start" (Pl.strStripStart (Pl.col "text") (Pl.litText " "))
                            , Pl.alias "strip_end" (Pl.strStripEnd (Pl.col "text") (Pl.litText " "))
                            ]
                            lf0
                    case projected of
                        Left err -> expectationFailure (show err)
                        Right lf1 -> do
                            collected <- Pl.collect lf1
                            case collected of
                                Left err -> expectationFailure (show err)
                                Right df -> do
                                    Pl.shape df `shouldReturn` Right (4, 5)
                                    Pl.column @T.Text df "slice_0_2" `shouldReturn` Right (V.fromList [Just " A", Just "βe", Just "CA", Just "日本"])
                                    Pl.column @T.Text df "head_2" `shouldReturn` Right (V.fromList [Just " A", Just "βe", Just "CA", Just "日本"])
                                    Pl.column @T.Text df "tail_2" `shouldReturn` Right (V.fromList [Just "e ", Just "ta", Just "OL", Just "本語"])
                                    Pl.column @T.Text df "strip_start" `shouldReturn` Right (V.fromList [Just "Alice ", Just "βeta", Just "CAROL", Just "日本語"])
                                    Pl.column @T.Text df "strip_end" `shouldReturn` Right (V.fromList [Just " Alice", Just "βeta", Just "CAROL", Just "日本語"])

        it "uses regex contains, find, extract, count, and replace" $ do
            scanResult <- Pl.scanCsv stringsCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    projected <-
                        Pl.select
                            [ Pl.alias "contains_caps" (Pl.strContainsRegex True (Pl.col "text") (Pl.litText "[A-Z]+"))
                            , Pl.alias "find_li" (Pl.cast Pl.Int64 (Pl.strFindLiteral (Pl.col "text") (Pl.litText "li")))
                            , Pl.alias "find_literal_regex_chars" (Pl.cast Pl.Int64 (Pl.strFindLiteral (Pl.col "text") (Pl.litText "[A-Z]+")))
                            , Pl.alias "find_caps" (Pl.cast Pl.Int64 (Pl.strFindRegex True (Pl.col "text") (Pl.litText "[A-Z]+")))
                            , Pl.alias "extract_caps" (Pl.strExtract 1 (Pl.col "text") (Pl.litText "([A-Z]+)"))
                            , Pl.alias "caps_count" (Pl.cast Pl.Int64 (Pl.strCountMatches False (Pl.col "text") (Pl.litText "[A-Z]")))
                            , Pl.alias "replace_a" (Pl.strReplace True (Pl.col "text") (Pl.litText "A") (Pl.litText "X"))
                            , Pl.alias "replace_all_caps" (Pl.strReplaceAll False (Pl.col "text") (Pl.litText "[A-Z]") (Pl.litText "x"))
                            ]
                            lf0
                    case projected of
                        Left err -> expectationFailure (show err)
                        Right lf1 -> do
                            collected <- Pl.collect lf1
                            case collected of
                                Left err -> expectationFailure (show err)
                                Right df -> do
                                    Pl.shape df `shouldReturn` Right (4, 8)
                                    Pl.column @Bool df "contains_caps" `shouldReturn` Right (V.fromList [Just True, Just False, Just True, Just False])
                                    Pl.column @Int64 df "find_li" `shouldReturn` Right (V.fromList [Just 2, Nothing, Nothing, Nothing])
                                    Pl.column @Int64 df "find_literal_regex_chars" `shouldReturn` Right (V.fromList [Nothing, Nothing, Nothing, Nothing])
                                    Pl.column @Int64 df "find_caps" `shouldReturn` Right (V.fromList [Just 1, Nothing, Just 0, Nothing])
                                    Pl.column @T.Text df "extract_caps" `shouldReturn` Right (V.fromList [Just "A", Nothing, Just "CAROL", Nothing])
                                    Pl.column @Int64 df "caps_count" `shouldReturn` Right (V.fromList [Just 1, Just 0, Just 5, Just 0])
                                    Pl.column @T.Text df "replace_a" `shouldReturn` Right (V.fromList [Just " Xlice ", Just "βeta", Just "CXROL", Just "日本語"])
                                    Pl.column @T.Text df "replace_all_caps" `shouldReturn` Right (V.fromList [Just " xlice ", Just "βeta", Just "xxxxx", Just "日本語"])

        it "reports invalid regex for strict string find" $ do
            scanResult <- Pl.scanCsv stringsCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    projected <- Pl.select [Pl.alias "bad" (Pl.strFindRegex True (Pl.col "text") (Pl.litText "["))] lf0
                    case projected of
                        Left err -> expectationFailure (show err)
                        Right lf1 -> do
                            collected <- Pl.collect lf1
                            case collected of
                                Right _ -> expectationFailure "expected strict regex find to report invalid pattern"
                                Left err -> Pl.polarsErrorCode err `shouldBe` Pl.PolarsFailure

    describe "Expression DSL temporal namespace" $ do
        it "extracts datetime components from a temporal CSV" $ do
            scanResult <- Pl.scanCsv temporalCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    let ts = Pl.cast Pl.Datetime (Pl.col "ts_ms")
                    projected <-
                        Pl.select
                            [ Pl.alias "year" (Pl.cast Pl.Int64 (Pl.dtYear ts))
                            , Pl.alias "iso_year" (Pl.cast Pl.Int64 (Pl.dtIsoYear ts))
                            , Pl.alias "quarter" (Pl.cast Pl.Int64 (Pl.dtQuarter ts))
                            , Pl.alias "month" (Pl.cast Pl.Int64 (Pl.dtMonth ts))
                            , Pl.alias "week" (Pl.cast Pl.Int64 (Pl.dtWeek ts))
                            , Pl.alias "weekday" (Pl.cast Pl.Int64 (Pl.dtWeekday ts))
                            , Pl.alias "day" (Pl.cast Pl.Int64 (Pl.dtDay ts))
                            , Pl.alias "ordinal_day" (Pl.cast Pl.Int64 (Pl.dtOrdinalDay ts))
                            , Pl.alias "hour" (Pl.cast Pl.Int64 (Pl.dtHour ts))
                            , Pl.alias "minute" (Pl.cast Pl.Int64 (Pl.dtMinute ts))
                            , Pl.alias "second" (Pl.cast Pl.Int64 (Pl.dtSecond ts))
                            , Pl.alias "millisecond" (Pl.cast Pl.Int64 (Pl.dtMillisecond ts))
                            , Pl.alias "microsecond" (Pl.cast Pl.Int64 (Pl.dtMicrosecond ts))
                            , Pl.alias "nanosecond" (Pl.cast Pl.Int64 (Pl.dtNanosecond ts))
                            , Pl.alias "timestamp_ms" (Pl.dtTimestamp Pl.Milliseconds ts)
                            , Pl.alias "fmt" (Pl.dtToString "%Y-%m-%d %H:%M:%S%.3f" ts)
                            , Pl.alias "leap" (Pl.dtIsLeapYear ts)
                            , Pl.alias "days_in_month" (Pl.cast Pl.Int64 (Pl.dtDaysInMonth ts))
                            ]
                            lf0
                    case projected of
                        Left err -> expectationFailure (show err)
                        Right lf1 -> do
                            collected <- Pl.collect lf1
                            case collected of
                                Left err -> expectationFailure (show err)
                                Right df -> do
                                    Pl.shape df `shouldReturn` Right (4, 18)
                                    Pl.column @Int64 df "year" `shouldReturn` Right (V.fromList [Just 2024, Just 2024, Nothing, Just 1970])
                                    Pl.column @Int64 df "iso_year" `shouldReturn` Right (V.fromList [Just 2024, Just 2024, Nothing, Just 1970])
                                    Pl.column @Int64 df "quarter" `shouldReturn` Right (V.fromList [Just 1, Just 2, Nothing, Just 1])
                                    Pl.column @Int64 df "month" `shouldReturn` Right (V.fromList [Just 1, Just 6, Nothing, Just 1])
                                    Pl.column @Int64 df "week" `shouldReturn` Right (V.fromList [Just 1, Just 22, Nothing, Just 1])
                                    Pl.column @Int64 df "weekday" `shouldReturn` Right (V.fromList [Just 1, Just 6, Nothing, Just 4])
                                    Pl.column @Int64 df "day" `shouldReturn` Right (V.fromList [Just 1, Just 1, Nothing, Just 1])
                                    Pl.column @Int64 df "ordinal_day" `shouldReturn` Right (V.fromList [Just 1, Just 153, Nothing, Just 1])
                                    Pl.column @Int64 df "hour" `shouldReturn` Right (V.fromList [Just 0, Just 12, Nothing, Just 0])
                                    Pl.column @Int64 df "minute" `shouldReturn` Right (V.fromList [Just 0, Just 34, Nothing, Just 0])
                                    Pl.column @Int64 df "second" `shouldReturn` Right (V.fromList [Just 0, Just 56, Nothing, Just 0])
                                    Pl.column @Int64 df "millisecond" `shouldReturn` Right (V.fromList [Just 123, Just 789, Nothing, Just 0])
                                    Pl.column @Int64 df "microsecond" `shouldReturn` Right (V.fromList [Just 123000, Just 789000, Nothing, Just 0])
                                    Pl.column @Int64 df "nanosecond" `shouldReturn` Right (V.fromList [Just 123000000, Just 789000000, Nothing, Just 0])
                                    Pl.column @Int64 df "timestamp_ms" `shouldReturn` Right (V.fromList [Just 1704067200123, Just 1717245296789, Nothing, Just 0])
                                    Pl.column @T.Text df "fmt" `shouldReturn` Right (V.fromList [Just "2024-01-01 00:00:00.123", Just "2024-06-01 12:34:56.789", Nothing, Just "1970-01-01 00:00:00.000"])
                                    Pl.column @Bool df "leap" `shouldReturn` Right (V.fromList [Just True, Just True, Nothing, Just False])
                                    Pl.column @Int64 df "days_in_month" `shouldReturn` Right (V.fromList [Just 31, Just 30, Nothing, Just 31])

    describe "Expression DSL list namespace" $ do
        it "splits strings into lists and applies list helpers" $ do
            scanResult <- Pl.scanCsv phrasesCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    let split = Pl.strSplit (Pl.col "phrase") (Pl.litText " ")
                    projected <-
                        Pl.select
                            [ Pl.alias "len" (Pl.cast Pl.Int64 (Pl.listLen split))
                            , Pl.alias "first" (Pl.listFirst split)
                            , Pl.alias "last" (Pl.listLast split)
                            , Pl.alias "get1" (Pl.listGet True split (Pl.litInt 1))
                            , Pl.alias "joined" (Pl.listJoin True split (Pl.litText "-"))
                            , Pl.alias "has_red" (Pl.listContains False split (Pl.litText "red"))
                            , Pl.alias "red_count" (Pl.cast Pl.Int64 (Pl.listCountMatches split (Pl.litText "red")))
                            ]
                            lf0
                    case projected of
                        Left err -> expectationFailure (show err)
                        Right lf1 -> do
                            collected <- Pl.collect lf1
                            case collected of
                                Left err -> expectationFailure (show err)
                                Right df -> do
                                    Pl.shape df `shouldReturn` Right (4, 7)
                                    Pl.column @Int64 df "len" `shouldReturn` Right (V.fromList [Just 3, Just 2, Just 2, Just 1])
                                    Pl.column @T.Text df "first" `shouldReturn` Right (V.fromList [Just "red", Just "red", Just "日本", Just "solo"])
                                    Pl.column @T.Text df "last" `shouldReturn` Right (V.fromList [Just "blue", Just "red", Just "語", Just "solo"])
                                    Pl.column @T.Text df "get1" `shouldReturn` Right (V.fromList [Just "green", Just "red", Just "語", Nothing])
                                    Pl.column @T.Text df "joined" `shouldReturn` Right (V.fromList [Just "red-green-blue", Just "red-red", Just "日本-語", Just "solo"])
                                    Pl.column @Bool df "has_red" `shouldReturn` Right (V.fromList [Just True, Just True, Just False, Just False])
                                    Pl.column @Int64 df "red_count" `shouldReturn` Right (V.fromList [Just 1, Just 2, Just 0, Just 0])

    describe "Expression DSL scalar predicates" $ do
        it "evaluates is_between, is_duplicated, is_unique, is_first_distinct, is_last_distinct, is_close, clip, clip_min, and clip_max" $ do
            scanResult <- Pl.scanCsv predicatesCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    let value = Pl.col "value"
                    let near = Pl.col "near"
                    projected <-
                        Pl.select
                            [ Pl.alias "between_both" (Pl.isBetween Pl.ClosedBoth value (Pl.litInt 2) (Pl.litInt 3))
                            , Pl.alias "between_left" (Pl.isBetween Pl.ClosedLeft value (Pl.litInt 2) (Pl.litInt 3))
                            , Pl.alias "between_right" (Pl.isBetween Pl.ClosedRight value (Pl.litInt 2) (Pl.litInt 3))
                            , Pl.alias "between_none" (Pl.isBetween Pl.ClosedNone value (Pl.litInt 2) (Pl.litInt 3))
                            , Pl.alias "duplicated" (Pl.isDuplicated value)
                            , Pl.alias "unique" (Pl.isUnique value)
                            , Pl.alias "first_distinct" (Pl.isFirstDistinct value)
                            , Pl.alias "last_distinct" (Pl.isLastDistinct value)
                            , Pl.alias "close" (Pl.isClose 0.15 0.0 False value near)
                            , Pl.alias "clip" (Pl.clip value (Pl.litInt 2) (Pl.litInt 2))
                            , Pl.alias "clip_min" (Pl.clipMin value (Pl.litInt 2))
                            , Pl.alias "clip_max" (Pl.clipMax value (Pl.litInt 2))
                            ]
                            lf0
                    case projected of
                        Left err -> expectationFailure (show err)
                        Right lf1 -> do
                            collected <- Pl.collect lf1
                            case collected of
                                Left err -> expectationFailure (show err)
                                Right df -> do
                                    Pl.shape df `shouldReturn` Right (5, 12)
                                    Pl.column @Bool df "between_both" `shouldReturn` Right (V.fromList [Just False, Just True, Just True, Just True, Nothing])
                                    Pl.column @Bool df "between_left" `shouldReturn` Right (V.fromList [Just False, Just True, Just True, Just False, Nothing])
                                    Pl.column @Bool df "between_right" `shouldReturn` Right (V.fromList [Just False, Just False, Just False, Just True, Nothing])
                                    Pl.column @Bool df "between_none" `shouldReturn` Right (V.fromList [Just False, Just False, Just False, Just False, Nothing])
                                    Pl.column @Bool df "duplicated" `shouldReturn` Right (V.fromList [Just False, Just True, Just True, Just False, Just False])
                                    Pl.column @Bool df "unique" `shouldReturn` Right (V.fromList [Just True, Just False, Just False, Just True, Just True])
                                    Pl.column @Bool df "first_distinct" `shouldReturn` Right (V.fromList [Just True, Just True, Just False, Just True, Just True])
                                    Pl.column @Bool df "last_distinct" `shouldReturn` Right (V.fromList [Just True, Just False, Just True, Just True, Just True])
                                    Pl.column @Bool df "close" `shouldReturn` Right (V.fromList [Just True, Just True, Just True, Just False, Nothing])
                                    Pl.column @Int64 df "clip" `shouldReturn` Right (V.fromList [Just 2, Just 2, Just 2, Just 2, Nothing])
                                    Pl.column @Int64 df "clip_min" `shouldReturn` Right (V.fromList [Just 2, Just 2, Just 2, Just 3, Nothing])
                                    Pl.column @Int64 df "clip_max" `shouldReturn` Right (V.fromList [Just 1, Just 2, Just 2, Just 2, Nothing])
        it "evaluates is_in with a text literal against a split list" $ do
            scanResult <- Pl.scanCsv phrasesCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    let split = Pl.strSplit (Pl.col "phrase") (Pl.litText " ")
                    projected <-
                        Pl.select
                            [ Pl.alias "red_in" (Pl.isIn False (Pl.litText "red") split)
                            ]
                            lf0
                    case projected of
                        Left err -> expectationFailure (show err)
                        Right lf1 -> do
                            collected <- Pl.collect lf1
                            case collected of
                                Left err -> expectationFailure (show err)
                                Right df -> do
                                    Pl.shape df `shouldReturn` Right (4, 1)
                                    Pl.column @Bool df "red_in" `shouldReturn` Right (V.fromList [Just True, Just True, Just False, Just False])

    describe "Expression DSL horizontal functions" $ do
        it "computes horizontal stats and coalesce over numeric and boolean columns" $ do
            scanResult <- Pl.scanCsv horizontalCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    let nums = [Pl.col "a", Pl.col "b", Pl.col "c"]
                    let bools = [Pl.col "p", Pl.col "q"]
                    projected <-
                        Pl.select
                            [ Pl.alias "sum_ignore" (Pl.cast Pl.Int64 (Pl.sumHorizontal True nums))
                            , Pl.alias "sum_strict" (Pl.cast Pl.Int64 (Pl.sumHorizontal False nums))
                            , Pl.alias "max_value" (Pl.cast Pl.Int64 (Pl.maxHorizontal nums))
                            , Pl.alias "min_value" (Pl.cast Pl.Int64 (Pl.minHorizontal nums))
                            , Pl.alias "mean_ignore" (Pl.meanHorizontal True nums)
                            , Pl.alias "mean_strict" (Pl.meanHorizontal False nums)
                            , Pl.alias "any_value" (Pl.anyHorizontal bools)
                            , Pl.alias "all_value" (Pl.allHorizontal bools)
                            , Pl.alias "first_non_null" (Pl.cast Pl.Int64 (Pl.coalesce nums))
                            ]
                            lf0
                    case projected of
                        Left err -> expectationFailure (show err)
                        Right lf1 -> do
                            collected <- Pl.collect lf1
                            case collected of
                                Left err -> expectationFailure (show err)
                                Right df -> do
                                    Pl.shape df `shouldReturn` Right (3, 9)
                                    Pl.column @Int64 df "sum_ignore" `shouldReturn` Right (V.fromList [Just 6, Just 10, Just 17])
                                    Pl.column @Int64 df "sum_strict" `shouldReturn` Right (V.fromList [Just 6, Nothing, Nothing])
                                    Pl.column @Int64 df "max_value" `shouldReturn` Right (V.fromList [Just 3, Just 6, Just 9])
                                    Pl.column @Int64 df "min_value" `shouldReturn` Right (V.fromList [Just 1, Just 4, Just 8])
                                    actualMeanIgnore <- Pl.column @Double df "mean_ignore"
                                    case actualMeanIgnore of
                                        Left err -> expectationFailure (show err)
                                        Right vd -> shouldApproximate 1e-12 (V.fromList [Just 2.0, Just 5.0, Just 8.5]) vd
                                    Pl.column @Double df "mean_strict" `shouldReturn` Right (V.fromList [Just 2.0, Nothing, Nothing])
                                    Pl.column @Bool df "any_value" `shouldReturn` Right (V.fromList [Just True, Just True, Just False])
                                    Pl.column @Bool df "all_value" `shouldReturn` Right (V.fromList [Just False, Just True, Just False])
                                    Pl.column @Int64 df "first_non_null" `shouldReturn` Right (V.fromList [Just 1, Just 4, Just 8])

        it "rejects empty horizontal expression list with InvalidArgument" $ do
            scanResult <- Pl.scanCsv horizontalCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    result <- Pl.select [Pl.alias "bad" (Pl.sumHorizontal True [])] lf0
                    case result of
                        Right _ -> expectationFailure "expected InvalidArgument for empty horizontal list"
                        Left err -> Pl.polarsErrorCode err `shouldBe` Pl.InvalidArgument

    describe "Expression DSL name namespace" $ do
        it "keeps, prefixes, suffixes, and transforms column names" $ do
            scanResult <- Pl.scanCsv nameOpsCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    projected <-
                        Pl.select
                            [ Pl.nameKeep (Pl.alias "renamed" (Pl.col "Camel"))
                            , Pl.namePrefix "pre_" (Pl.col "score_value")
                            , Pl.nameSuffix "_suf" (Pl.col "Camel")
                            , Pl.nameToLowercase (Pl.col "Camel")
                            , Pl.nameToUppercase (Pl.col "score_value")
                            , Pl.nameReplace True "score" "points" (Pl.col "score_value")
                            ]
                            lf0
                    case projected of
                        Left err -> expectationFailure (show err)
                        Right lf1 -> do
                            collected <- Pl.collect lf1
                            case collected of
                                Left err -> expectationFailure (show err)
                                Right df -> do
                                    Pl.shape df `shouldReturn` Right (2, 6)
                                    schemaResult <- Pl.schema df
                                    case schemaResult of
                                        Left err -> expectationFailure (show err)
                                        Right schema -> do
                                            let fields = map Pl.fieldName schema
                                            let types = map Pl.fieldType schema
                                            fields `shouldBe` ["Camel", "pre_score_value", "Camel_suf", "camel", "SCORE_VALUE", "points_value"]
                                            types `shouldBe` [Pl.Int64, Pl.Int64, Pl.Int64, Pl.Int64, Pl.Int64, Pl.Int64]

    describe "Expression DSL string more helpers" $ do
        it "uses strip_prefix, strip_suffix, escape_regex, and extract_all" $ do
            scanResult <- Pl.scanCsv stringMoreCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    let caps = Pl.strExtractAll (Pl.col "text") (Pl.litText "[A-Z]")
                    projected <-
                        Pl.select
                            [ Pl.alias "no_prefix_space" (Pl.strStripPrefix (Pl.col "text") (Pl.litText " "))
                            , Pl.alias "no_suffix_space" (Pl.strStripSuffix (Pl.col "text") (Pl.litText " "))
                            , Pl.alias "escaped" (Pl.strEscapeRegex (Pl.col "text"))
                            , Pl.alias "caps_join" (Pl.listJoin True caps (Pl.litText ""))
                            , Pl.alias "caps_len" (Pl.cast Pl.Int64 (Pl.listLen caps))
                            ]
                            lf0
                    case projected of
                        Left err -> expectationFailure (show err)
                        Right lf1 -> do
                            collected <- Pl.collect lf1
                            case collected of
                                Left err -> expectationFailure (show err)
                                Right df -> do
                                    Pl.shape df `shouldReturn` Right (5, 5)
                                    Pl.column @T.Text df "no_prefix_space" `shouldReturn` Right (V.fromList [Just "Alice ", Just "βeta", Just "CAROL", Just "日本語", Just "a.b+c"])
                                    Pl.column @T.Text df "no_suffix_space" `shouldReturn` Right (V.fromList [Just " Alice", Just "βeta", Just "CAROL", Just "日本語", Just "a.b+c"])
                                    Pl.column @T.Text df "escaped" `shouldReturn` Right (V.fromList [Just " Alice ", Just "βeta", Just "CAROL", Just "日本語", Just "a\\.b\\+c"])
                                    Pl.column @T.Text df "caps_join" `shouldReturn` Right (V.fromList [Just "A", Just "", Just "CAROL", Just "", Just ""])
                                    Pl.column @Int64 df "caps_len" `shouldReturn` Right (V.fromList [Just 1, Just 0, Just 5, Just 0, Just 0])

    describe "Expression DSL string concat functions" $ do
        it "concatenates and formats string columns with null handling" $ do
            scanResult <- Pl.scanCsv concatCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    let fullName = [Pl.col "first", Pl.col "last"]
                    projected <-
                        Pl.select
                            [ Pl.alias "full_ignore" (Pl.concatStr True " " fullName)
                            , Pl.alias "full_strict" (Pl.concatStr False " " fullName)
                            , Pl.alias "formatted" (Pl.formatStr "{}:{}" [Pl.col "first", Pl.cast Pl.Utf8 (Pl.col "age")])
                            ]
                            lf0
                    case projected of
                        Left err -> expectationFailure (show err)
                        Right lf1 -> do
                            collected <- Pl.collect lf1
                            case collected of
                                Left err -> expectationFailure (show err)
                                Right df -> do
                                    Pl.shape df `shouldReturn` Right (3, 3)
                                    Pl.column @T.Text df "full_ignore" `shouldReturn` Right (V.fromList [Just "Alice Smith", Just "Bob", Just "Carol Jones"])
                                    Pl.column @T.Text df "full_strict" `shouldReturn` Right (V.fromList [Just "Alice Smith", Nothing, Just "Carol Jones"])
                                    Pl.column @T.Text df "formatted" `shouldReturn` Right (V.fromList [Just "Alice:34", Just "Bob:45", Nothing])

        it "rejects empty expression list for concatStr with InvalidArgument" $ do
            scanResult <- Pl.scanCsv concatCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    result <- Pl.select [Pl.alias "bad" (Pl.concatStr True " " [])] lf0
                    case result of
                        Right _ -> expectationFailure "expected InvalidArgument for empty concatStr expression list"
                        Left err -> Pl.polarsErrorCode err `shouldBe` Pl.InvalidArgument

        it "rejects formatStr with mismatched placeholders" $ do
            scanResult <- Pl.scanCsv concatCsv
            case scanResult of
                Left err -> expectationFailure (show err)
                Right lf0 -> do
                    result <- Pl.select [Pl.alias "bad" (Pl.formatStr "{}:{}" [Pl.col "first"])] lf0
                    case result of
                        Right _ -> expectationFailure "expected PolarsFailure for format placeholder mismatch"
                        Left err -> Pl.polarsErrorCode err `shouldBe` Pl.PolarsFailure

    describe "Polars.IPC" $ do
        it "round-trips a dataframe through IPC bytes" $ do
            result <- Pl.readCsv fixtureCsv
            case result of
                Left err -> expectationFailure (show err)
                Right df0 -> do
                    bytesResult <- Pl.toIpcBytes df0
                    case bytesResult of
                        Left err -> expectationFailure (show err)
                        Right bytes -> do
                            BS.length bytes `shouldSatisfy` (> 0)
                            dfResult <- Pl.fromIpcBytes bytes
                            case dfResult of
                                Left err -> expectationFailure (show err)
                                Right df1 -> Pl.shape df1 `shouldReturn` Right (3, 2)
