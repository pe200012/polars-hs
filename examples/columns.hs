{-# LANGUAGE OverloadedStrings #-}

import qualified Polars as Pl

main :: IO ()
main = do
    result <- Pl.readCsv "test/data/values.csv"
    case result of
        Left err -> print err
        Right df -> do
            names <- Pl.columnText df "name"
            ages <- Pl.columnInt64 df "age"
            scores <- Pl.columnDouble df "score"
            active <- Pl.columnBool df "active"
            print names
            print ages
            print scores
            print active
