{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

import Data.Int (Int64)
import Data.Maybe (isJust)
import qualified Data.Vector as V
import qualified Polars as Pl

nycTaxiParquet :: FilePath
nycTaxiParquet = "test/data/external/nyc_taxi_sample.parquet"

main :: IO ()
main = do
    eager <- Pl.readParquet nycTaxiParquet
    case eager of
        Left err -> fail (show err)
        Right df -> do
            shape <- Pl.shape df
            case shape of
                Right (rows, 4) | rows > 0 -> pure ()
                other -> fail ("unexpected NYC Taxi shape: " <> show other)
            passengerCounts <- Pl.column @Int64 df "passenger_count"
            case passengerCounts of
                Left err -> fail (show err)
                Right values | V.length values > 0 -> pure ()
                Right _ -> fail "empty passenger_count column"

    lazy <- Pl.scanParquet nycTaxiParquet
    case lazy of
        Left err -> fail (show err)
        Right lf -> do
            filtered <- Pl.filter (Pl.col "fare_amount" Pl..> Pl.litDouble 0) lf
            case filtered of
                Left err -> fail (show err)
                Right lf1 -> do
                    limited <- Pl.limit 10 lf1
                    case limited of
                        Left err -> fail (show err)
                        Right lf2 -> do
                            collected <- Pl.collect lf2
                            case collected of
                                Left err -> fail (show err)
                                Right out -> do
                                    resultShape <- Pl.shape out
                                    case resultShape of
                                        Right (rows, 4) | rows >= 0 && rows <= 10 -> pure ()
                                        other -> fail ("unexpected filtered shape: " <> show other)

    groupedLazy <- Pl.scanParquet nycTaxiParquet
    case groupedLazy of
        Left err -> fail (show err)
        Right lf0 -> do
            filtered <- Pl.filter (Pl.col "fare_amount" Pl..> Pl.litDouble 0) lf0
            case filtered of
                Left err -> fail (show err)
                Right lf1 -> do
                    grouped <-
                        Pl.agg
                            [ Pl.alias "mean_fare" (Pl.mean_ (Pl.col "fare_amount"))
                            , Pl.alias "total_distance" (Pl.sum_ (Pl.col "trip_distance"))
                            ]
                            (Pl.groupByStable [Pl.col "payment_type"] lf1)
                    case grouped of
                        Left err -> fail (show err)
                        Right lf2 -> do
                            sorted <- Pl.sort ["payment_type"] lf2
                            case sorted of
                                Left err -> fail (show err)
                                Right lf3 -> do
                                    collected <- Pl.collect lf3
                                    case collected of
                                        Left err -> fail (show err)
                                        Right df -> do
                                            groupedShape <- Pl.shape df
                                            case groupedShape of
                                                Right (rows, 3) | rows > 0 -> pure ()
                                                other -> fail ("unexpected grouped shape: " <> show other)
                                            paymentTypes <- Pl.column @Int64 df "payment_type"
                                            case paymentTypes of
                                                Left err -> fail (show err)
                                                Right values | V.length values > 0 -> pure ()
                                                Right _ -> fail "empty payment_type groups"
                                            meanFares <- Pl.column @Double df "mean_fare"
                                            case meanFares of
                                                Left err -> fail (show err)
                                                Right values | all isJust (V.toList values) -> pure ()
                                                Right values -> fail ("unexpected null mean_fare values: " <> show values)
                                            totalDistances <- Pl.column @Double df "total_distance"
                                            case totalDistances of
                                                Left err -> fail (show err)
                                                Right values | all isJust (V.toList values) -> pure ()
                                                Right values -> fail ("unexpected null total_distance values: " <> show values)
