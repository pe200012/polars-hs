{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Prelude hiding (filter, head)

import qualified Data.ByteString as BS
import qualified Data.Text as T
import Test.Hspec

import qualified Polars as Pl

fixtureCsv :: FilePath
fixtureCsv = "test/data/people.csv"

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
