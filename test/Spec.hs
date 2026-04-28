{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Main (main) where

import Prelude hiding (filter, head)

import qualified Data.ByteString as BS
import Data.Int (Int64)
import Data.Maybe (isJust)
import qualified Data.Text as T
import qualified Data.Vector as V
import Foreign.Ptr (nullPtr)
import System.Mem (performGC)
import Test.Hspec

import ArrowRecordBatch (withAgeArray, withPeopleRecordBatch)
import qualified Polars as Pl

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
