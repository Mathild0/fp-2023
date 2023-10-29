{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# OPTIONS_GHC -Wno-name-shadowing #-}
{-# OPTIONS_GHC -Wno-unused-matches #-}

module Lib2
  ( calculateSum,
    calculateMinimum,
    parseStatement,
    executeStatement,
    ColumnName,
    ParsedStatement(..),
    AggregateFunction(..),
    Value(..)
  )
where

import DataFrame (DataFrame(..), Column(..), ColumnType(..), Value(..), Row)
import InMemoryTables (TableName, database)
import Lib1 (findTableByName)
import Data.Char (toLower)
import Data.String ()
import Data.Maybe ()
import Data.List (findIndex)

type ErrorMessage = String
type Database = [(TableName, DataFrame)]

type ColumnName = String

data ParsedStatement
  = ParsedStatement
  | SelectSumStatement AggregateFunction ColumnName TableName
  | SelectMinStatement AggregateFunction ColumnName TableName
  | SelectMaxStatement AggregateFunction ColumnName TableName
  | ShowTables
  | ShowTable TableName
  deriving (Show, Eq)

data AggregateFunction
  = Sum
  | Min
  | Max
  deriving (Show)

instance Eq AggregateFunction where
  Sum == Sum = True
  Min == Min = True
  _   == _   = False

-- Parses user input into an entity representing a parsed
-- statement
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement input =
  let tokens = words input
      lowercaseToken t = if t `elem` ["select", "show", "tables", "table", "from", "min", "max", "sum"] then map toLower t else t
      normalizedTokens = map lowercaseToken tokens
  in case normalizedTokens of
    ["show", "tables"] -> Right ShowTables
    ["show", "table", tableName] -> Right (ShowTable tableName)
    ["select", "sum", "(", column, ")", "from", table] ->
      Right (SelectSumStatement Sum column table)
    ["select", "min", "(", column, ")", "from", table] ->
      Right (SelectMinStatement Min column table)
    ["select", "max", "(", column, ")", "from", table] ->
      Right (SelectMaxStatement Max column table)
    _ -> Left "Invalid statement"

columnName :: Column -> String
columnName (Column name _) = name

executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame

executeStatement ShowTables = 
  Right $ DataFrame [Column "Tables" StringType] (map (\(name, _) -> [StringValue name]) database)

executeStatement (ShowTable tableName) = 
  case findTableByName database tableName of
    Just table -> Right table
    Nothing -> Left "Table not found"

executeStatement (SelectSumStatement Sum column tableName) =
  case findTableByName database tableName of
    Just (DataFrame columns rows) -> 
      
      case findColumnIndex column columns of
        Just idx -> do

          let values = extractColumnValues column columns rows

          case calculateSum values of
            Just sumValue -> Right $ DataFrame [Column "Sum" IntegerType] [[IntegerValue sumValue]]
            Nothing -> Left "Column not found or invalid for sum statement"
            
        Nothing -> Left "Column not found or invalid for sum statement"
    
    Nothing -> Left "Table not found"

executeStatement (SelectMinStatement Min columnName tableName) =
  case lookup tableName database of
    Just (DataFrame columns rows) -> do

      let columnIndex = findColumnIndex columnName columns

      case columnIndex of
        Just idx -> do
          -- Extract the column data
          let columnData = [rowData !! idx | rowData <- rows]

          -- Calculate the minimum value
          let minValue = calculateMinimum columnData

          -- Create a result DataFrame
          Right $ DataFrame [Column "Result" (getColumnType minValue)] [[minValue]]

        Nothing -> Left "Column not found"

    Nothing -> Left "Table not found"

executeStatement _ = 
  Left "Not implemented: executeStatement"

-----------------------------------------------------------
-- Helper function to check if a Value is of a valid type
isValidValue :: Value -> Bool
isValidValue value =
  case value of
    IntegerValue _ -> True
    BoolValue _    -> True
    StringValue _  -> True
    _              -> False

calculateMinimum :: [Value] -> Value
calculateMinimum values =
  let validValues = filter isValidValue values in
  case validValues of
    [] -> NullValue
    (x:xs) -> foldr minVal x xs

-- A function to determine the "minimum" of two Value items, considering different data types
minVal :: Value -> Value -> Value
minVal (IntegerValue a) (IntegerValue b) = IntegerValue (min a b)
minVal (StringValue a) (StringValue b) = StringValue (min a b)  -- lexicographical comparison
minVal (BoolValue a) (BoolValue b) = BoolValue (min a b)
minVal _ _ = NullValue

-- Helper function to find the index of a column by name
findColumnIndex :: ColumnName -> [Column] -> Maybe Int
findColumnIndex columnName =
  findIndex (\ (Column name _) -> name == columnName) 

-- Helper function to determine the column type based on a Value
getColumnType :: Value -> ColumnType
getColumnType value =
  case value of
    IntegerValue _ -> IntegerType
    BoolValue _    -> BoolType
    StringValue _  -> StringType
    _              -> StringType

extractColumnValues :: ColumnName -> [Column] -> [Row] -> [Value]
extractColumnValues columnName columns rows =
  case findColumnIndex columnName columns of
    Just index -> map (!! index) rows
    Nothing -> []

calculateSum :: [Value] -> Maybe Integer
calculateSum values = case traverse extractIntegerValue values of
  Just integers -> Just (sum integers)
  Nothing -> Nothing

extractIntegerValue :: Value -> Maybe Integer
extractIntegerValue (IntegerValue i) = Just i
extractIntegerValue _ = Nothing