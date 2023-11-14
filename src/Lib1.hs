{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# OPTIONS_GHC -Wno-name-shadowing #-}

module Lib1
  ( parseSelectAllStatement,
    findTableByName,
    validateDataFrame,
    renderDataFrameAsTable,
  )
where

import DataFrame (DataFrame (..), Column (..), ColumnType (..), Value (..))
import InMemoryTables (TableName)

type ErrorMessage = String

type Database = [(TableName, DataFrame)]

-- Your code modifications go below this comment

toLowerCase :: String -> String
toLowerCase [] = []
toLowerCase (x:xs)
    | 'A' <= x && x <= 'Z' = toEnum (fromEnum x + 32) : toLowerCase xs
    | otherwise = x : toLowerCase xs
-- 1) implement the function which returns a data frame by its name
-- in provided Database list
findTableByName :: Database -> String -> Maybe DataFrame
findTableByName db tableName = lookup (toLowerCase tableName) db

-- 2) implement the function which parses a "select * from ..."
-- sql statement and extracts a table name from the statement
removeLastSemicolon :: String -> String
removeLastSemicolon [] = []
removeLastSemicolon a
  | last a == ';' = init a
  | otherwise = a

parseSelectAllStatement :: String -> Either ErrorMessage TableName
parseSelectAllStatement a = case words (toLowerCase a) of
    ["select", "*", "from", name]
        | null (removeLastSemicolon name) -> Left "Invalid input"
        | otherwise -> Right (removeLastSemicolon name)
    _ -> Left "Invalid input"

-- 3) implement the function which validates tables: checks if
-- columns match value types, if rows sizes match columns,..

validateDataFrame :: DataFrame -> Either ErrorMessage ()
validateDataFrame (DataFrame cols rows)
  | not checkRowSizes = Left "Rows and columns do not match in size"
  | not (checkValueTypes (DataFrame cols rows)) = Left "There is a type mismatch between column types and values"
  | otherwise = Right ()
  where
    checkRowSizes :: Bool
    checkRowSizes = all (\row -> length row == length cols) rows

    checkValueTypes :: DataFrame -> Bool
    checkValueTypes dataFrame = checkColumnValuePairs (zipColumnsAndValues dataFrame)

    checkColumnValuePairs :: [(Column, Value)] -> Bool
    checkColumnValuePairs [] = True
    checkColumnValuePairs ((column, value) : rest) =
      case (column, value) of
        (Column _ IntegerType, IntegerValue _) -> checkColumnValuePairs rest
        (Column _ StringType, StringValue _) -> checkColumnValuePairs rest
        (Column _ BoolType, BoolValue _) -> checkColumnValuePairs rest
        (_, NullValue) -> checkColumnValuePairs rest
        _ -> False

    zipColumnsAndValues :: DataFrame -> [(Column, Value)]
    zipColumnsAndValues (DataFrame cols rows) =
      concatMap (zip cols) rows

-- 4) implement the function which renders a given data frame
-- as ascii-art table (use your imagination, there is no "correct"
-- answer for this task!), it should respect terminal
-- width (in chars, provided as the first argument)
-- Utility function to convert Value to String
valueToString :: Value -> String
valueToString (IntegerValue i) = show i
valueToString (StringValue s) = s
valueToString (BoolValue b) = if b then "True" else "False"
valueToString NullValue = "NULL"

-- Utility function to calculate max width of each column
maxColumnWidths :: DataFrame -> [Int]
maxColumnWidths (DataFrame columns rows) =
  map (\(Column name _, values) -> maximum (length name : map (length . valueToString) values)) $ zip columns (transpose rows)
  where
    transpose [] = repeat []
    transpose (x:xs) = zipWith (:) x (transpose xs)

-- 4) Implement the function which renders a given data frame
-- as ascii-art table
renderDataFrameAsTable :: Integer -> DataFrame -> String
renderDataFrameAsTable maxWidth (DataFrame columns rows) =
  let widths = maxColumnWidths (DataFrame columns rows)
      adjustedWidths = adjustWidths widths maxWidth
  in unlines $ columnHeaders adjustedWidths columns : map (renderRow adjustedWidths) rows
  where
    adjustWidths widths maxWidth =
      let totalWidth = sum widths + length columns + 1
      in if totalWidth > fromIntegral maxWidth then map (\w -> w - (totalWidth - fromIntegral maxWidth) `div` length widths) widths else widths

    columnHeaders widths columns = '|' : concatMap (\(Column name _, width) -> padRight width name ++ "|") (zip columns widths)

    renderRow widths values = '|' : concatMap (\(value, width) -> padRight width (valueToString value) ++ "|") (zip values widths)

    padRight width str = str ++ replicate (width - length str) ' '