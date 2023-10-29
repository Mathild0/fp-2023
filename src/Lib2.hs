{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement
  )
where

import DataFrame (DataFrame(..), Column(..), ColumnType(..), Value(..), Row)
import InMemoryTables (TableName, database)
import Data.Char (toLower)
import Lib1 (findTableByName)

type ErrorMessage = String
type Database = [(TableName, DataFrame)]

type ColumnName = String

-- Keep the type, modify constructors
data ParsedStatement 
  = ParsedStatement
  | SelectSumStatement AggregateFuction ColumnName TableName
  | ShowTables
  deriving Show

data AggregateFuction 
  = Sum
  deriving Show

-- Parses user input into an entity representing a parsed
-- statement
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement input =
  let lowercaseInput = map toLower input
  in case words lowercaseInput of
    ["select", "sum", "(", column, ")", "from", table] -> Right (SelectSumStatement Sum column table)
    ["show", "tables"] -> Right ShowTables
    _ -> Left "Invalid statement"

-- Executes a parsed statement. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement ShowTables = Right $ DataFrame [Column "Tables" StringType] (map (\(name, _) -> [StringValue name]) database)

executeStatement (SelectSumStatement Sum column tableName) = 
  case findTableByName database tableName of
    Nothing -> Left "Table not found"
    Just (DataFrame columns rows) -> do
      let values = extractColumnValues column columns rows
      case calculateSum values of
        Nothing -> Left "Invalid aggregation"
        Just sumValue -> Right $ DataFrame [Column "Sum" IntegerType] [[IntegerValue sumValue]]

executeStatement _ = Left "Not implemented: executeStatement"

columnName :: Column -> String
columnName (Column name _) = name

----------------------------------------------------------------

extractColumnValues :: ColumnName -> [Column] -> [Row] -> [Value]
extractColumnValues columnName columns rows = 
  case findColumnIndex columnName columns of 
    Just index -> map (!! index) rows
    Nothing -> []

findColumnIndex :: ColumnName -> [Column] -> Maybe Int
findColumnIndex name columns = go columns 0
  where 
    go [] _ = Nothing
    go (Column columnName _ : rest) index
      | name == columnName = Just index
      | otherwise = go rest (index + 1)

calculateSum :: [Value] -> Maybe Integer
calculateSum values = case traverse extractIntegerValue values of
  Just integers -> Just (sum integers)
  Nothing -> Nothing

extractIntegerValue :: Value -> Maybe Integer
extractIntegerValue (IntegerValue i) = Just i
extractIntegerValue _ = Nothing
