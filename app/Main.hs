module Main (main) where

import qualified Polars as Pl

main :: IO ()
main = do
    result <- Pl.readCsv "test/data/people.csv"
    case result of
        Left err -> print err
        Right df -> print =<< Pl.shape df
