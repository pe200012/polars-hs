{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

import Data.Int (Int64)
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
