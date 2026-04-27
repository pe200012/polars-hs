{-# LANGUAGE OverloadedStrings #-}

import qualified Data.Text.IO as TIO
import qualified Polars as Pl

main :: IO ()
main = do
    scanResult <- Pl.scanCsv "test/data/sales.csv"
    case scanResult of
        Left err -> print err
        Right lf0 -> do
            groupedResult <-
                Pl.agg
                    [ Pl.alias "salary_sum" (Pl.sum_ (Pl.col "salary"))
                    , Pl.alias "age_mean" (Pl.mean_ (Pl.col "age"))
                    , Pl.alias "people" (Pl.count_ (Pl.col "name"))
                    ]
                    (Pl.groupByStable [Pl.col "department"] lf0)
            case groupedResult of
                Left err -> print err
                Right lf1 -> do
                    collected <- Pl.collect lf1
                    case collected of
                        Left err -> print err
                        Right df -> do
                            shapeResult <- Pl.shape df
                            print shapeResult
                            textResult <- Pl.toText df
                            either print TIO.putStrLn textResult
