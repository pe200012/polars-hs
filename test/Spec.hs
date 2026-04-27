{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Prelude hiding (filter, head)

import qualified Data.ByteString as BS
import qualified Data.Text as T
import Test.Hspec

import qualified Polars as Pl

fixtureCsv :: FilePath
fixtureCsv = "test/data/people.csv"

salesCsv :: FilePath
salesCsv = "test/data/sales.csv"

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
