{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib2
  ( calculateMinimum,
    parseStatement,
    executeStatement,
    selectColumns,
    ParsedStatement(..),
    AggregateFunction(..), 
    Value(..)
  )
where

import DataFrame (ColumnType(..), Row)
import DataFrame (DataFrame(..), Column(..), ColumnType(..), Value(..))
import InMemoryTables (TableName, database)
import Data.Char (toLower, isSpace)
import Data.String (IsString)
import Data.Maybe (fromMaybe, mapMaybe)
import Data.List (find, isSuffixOf, isPrefixOf, group, elemIndex)
import Data.List (findIndex)


type ErrorMessage = String
type Database = [(TableName, DataFrame)]

type ColumnName = String


data ParsedStatement
  = ParsedStatement
  | SelectSumStatement AggregateFunction ColumnName TableName
  | SelectMinStatement AggregateFunction ColumnName TableName
  | SelectMaxStatement AggregateFunction ColumnName TableName
  | SelectColumnListStatement [String] TableName 
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


parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement input =
  let tokens = words input
      lowercaseKeywords = ["select", "show", "table", "tables", "from", "min", "max", "sum"]
      lowercaseToken t = if map toLower t `elem` lowercaseKeywords then map toLower t else t
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
    ("select" : rest) ->
      case break (== "from") rest of
        (columnPart, "from" : tablePart) ->
          (if checkColumnList (unwords columnPart) then (do
          let tableName = unwords tablePart
          let column = parseColumnList (unwords columnPart)
          Right (SelectColumnListStatement column tableName)) else Left "Invalid statement")
        _ -> Left "Invalid statement"
    _ -> Left "Invalid statement"



columnName :: Column -> String
columnName (Column name _) = name


executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement ShowTables = Right $ DataFrame [Column "Tables" StringType] (map (\(name, _) -> [StringValue name]) database)
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

executeStatement (SelectColumnListStatement columns tableName) =
  case lookup tableName database of
    Just table ->
      case selectColumns columns table of
        Left e -> Left e
        Right selectedDf -> Right selectedDf
    Nothing -> Left "Table not found"

executeStatement _ = Left "Not implemented: executeStatement"


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
findColumnIndex columnName columns =
  case findIndex (\(Column name _) -> name == columnName) columns of
    Just index -> Just index
    Nothing -> Nothing



-- Helper function to determine the column type based on a Value
getColumnType :: Value -> ColumnType
getColumnType value =
  case value of
    IntegerValue _ -> IntegerType
    BoolValue _    -> BoolType
    StringValue _  -> StringType
    _              -> StringType 


------------------------------

strip :: String -> String
strip = f . f
   where f = reverse . dropWhile isSpace

parseColumnList :: String -> [String]
parseColumnList [] = []
parseColumnList s = case dropWhile (== ',') s of
  "" -> []
  s' -> strip w : parseColumnList s''
    where (w, s'') = break (== ',') s'

checkColumnList :: String -> Bool
checkColumnList input
  | "," `isPrefixOf` input || "," `isSuffixOf` input = False
  | any (\x -> length x > 1 && head x == ',') (group input) = False
  | otherwise = True

selectColumns :: [String] -> DataFrame -> Either ErrorMessage DataFrame
selectColumns columns (DataFrame dfColumns dfRows) =
  let columnIndices = getAllIndexes columns dfColumns
  in if null columnIndices then Left "No columns were found"
     else
        let selectedColumns = map (\x -> dfColumns !! x) columnIndices
            selectedRows = map (selectCells columnIndices) dfRows
        in Right (DataFrame selectedColumns selectedRows)

getAllIndexes :: [String] -> [Column] -> [Int]
getAllIndexes requestedColumns dfColumns =
  mapMaybe (`elemIndex` map columnName dfColumns) requestedColumns

selectCells :: [Int] -> Row -> [Value]
selectCells indices row = map (row !!) indices
