{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

import Data.Int (Int64)
import qualified Data.Text as T
import qualified Polars as Pl

main :: IO ()
main = do
    result <- Pl.readCsv "test/data/values.csv"
    case result of
        Left err -> print err
        Right df -> do
            names <- Pl.column @T.Text df "name"
            ages <- Pl.column @Int64 df "age"
            scores <- Pl.column @Double df "score"
            active <- Pl.column @Bool df "active"
            print names
            print ages
            print scores
            print active
