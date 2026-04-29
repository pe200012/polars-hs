{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Main (main) where

import Prelude hiding (filter, head)

import qualified Data.ByteString as BS
import Data.Int (Int64)
import Data.Foldable (forM_)
import Data.Maybe (isJust)
import qualified Data.Text as T
import qualified Data.Vector as V
import Foreign.Ptr (nullPtr)
import System.Mem (performGC)
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
                            frameResult <- Pl.seriesToFrame age
                            case frameResult of
                                Left err -> expectationFailure (show err)
                                Right oneColumn -> Pl.shape oneColumn `shouldReturn` Right (3, 1)

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
                            case headResult of
                                Right _ -> expectationFailure "expected InvalidArgument for negative Series head count"
                                Left err -> Pl.polarsErrorCode err `shouldBe` Pl.InvalidArgument
                            tailResult <- Pl.seriesTail (-1) age
                            case tailResult of
                                Right _ -> expectationFailure "expected InvalidArgument for negative Series tail count"
                                Left err -> Pl.polarsErrorCode err `shouldBe` Pl.InvalidArgument

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
                            case sortedResult of
                                Right _ -> expectationFailure "expected InvalidArgument for negative Series sort limit"
                                Left err -> Pl.polarsErrorCode err `shouldBe` Pl.InvalidArgument

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
