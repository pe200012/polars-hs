{-# LANGUAGE OverloadedStrings #-}

import qualified Data.Text.IO as TIO
import qualified Polars as Pl

main :: IO ()
main = do
    employeesResult <- Pl.scanCsv "test/data/employees.csv"
    departmentsResult <- Pl.scanCsv "test/data/departments.csv"
    case (employeesResult, departmentsResult) of
        (Right employees, Right departments) -> do
            let options =
                    Pl.defaultJoinOptions
                        { Pl.joinType = Pl.JoinLeft
                        , Pl.leftOn = [Pl.col "department"]
                        , Pl.rightOn = [Pl.col "department"]
                        , Pl.suffix = Just "_dept"
                        }
            joined <- Pl.joinWith options employees departments
            case joined of
                Left err -> print err
                Right lf -> do
                    collected <- Pl.collect lf
                    case collected of
                        Left err -> print err
                        Right df -> do
                            shapeResult <- Pl.shape df
                            print shapeResult
                            textResult <- Pl.toText df
                            either print TIO.putStrLn textResult
        (Left err, _) -> print err
        (_, Left err) -> print err
