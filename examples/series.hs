{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

import Data.Int (Int64)
import qualified Polars as Pl

main :: IO ()
main = do
    result <- Pl.readCsv "test/data/values.csv"
    case result of
        Left err -> print err
        Right df -> do
            seriesResult <- Pl.column @Pl.Series df "age"
            case seriesResult of
                Left err -> print err
                Right age -> do
                    print =<< Pl.seriesName age
                    print =<< Pl.seriesDataType age
                    print =<< Pl.seriesLength age
                    print =<< Pl.seriesNullCount age
                    print =<< Pl.seriesInt64 age
                    print =<< Pl.column @Int64 df "age"
                    frameResult <- Pl.seriesToFrame age
                    case frameResult of
                        Left err -> print err
                        Right oneColumn -> print =<< Pl.shape oneColumn
