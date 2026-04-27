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
                    castResult <- Pl.seriesCast @Double age
                    case castResult of
                        Left err -> print err
                        Right ageDouble -> print =<< Pl.seriesDouble ageDouble
                    renamedResult <- Pl.seriesRename "age_years" age
                    case renamedResult of
                        Left err -> print err
                        Right renamed -> print =<< Pl.seriesName renamed
                    let sortOptions =
                            Pl.defaultSeriesSortOptions
                                { Pl.seriesSortDescending = True
                                , Pl.seriesSortNullsLast = True
                                }
                    sortedResult <- Pl.seriesSort sortOptions age
                    case sortedResult of
                        Left err -> print err
                        Right sorted -> print =<< Pl.seriesInt64 sorted
                    frameResult <- Pl.seriesToFrame age
                    case frameResult of
                        Left err -> print err
                        Right oneColumn -> print =<< Pl.shape oneColumn
