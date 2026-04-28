{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

import Data.Int (Int64)
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Polars as Pl

main :: IO ()
main = do
    nameResult <- Pl.series @T.Text "name" (V.fromList [Just "Alice", Just "Bob", Just "Carol"])
    ageResult <- Pl.series @Int64 "age" (V.fromList [Just 34, Nothing, Just 29])
    scoreResult <- Pl.series @Double "score" (V.fromList [Just 9.5, Just 8.25, Nothing])
    activeResult <- Pl.series @Bool "active" (V.fromList [Just True, Just False, Nothing])
    case (nameResult, ageResult, scoreResult, activeResult) of
        (Right name, Right age, Right score, Right active) -> do
            dfResult <- Pl.dataFrame [name, age, score, active]
            case dfResult of
                Left err -> print err
                Right df -> do
                    print =<< Pl.shape df
                    print =<< Pl.column @Int64 df "age"
        (Left err, _, _, _) -> print err
        (_, Left err, _, _) -> print err
        (_, _, Left err, _) -> print err
        (_, _, _, Left err) -> print err
