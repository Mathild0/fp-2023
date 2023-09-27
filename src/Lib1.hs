{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib1
  ( parseSelectAllStatement,
    findTableByName,
    validateDataFrame,
    renderDataFrameAsTable,
  )
where

import DataFrame (DataFrame (..), Row, Column (..), ColumnType (..), Value (..))
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
parseSelectAllStatement :: String -> Either ErrorMessage TableName
parseSelectAllStatement _ = error "parseSelectAllStatement not implemented"

-- 3) implement the function which validates tables: checks if
-- columns match value types, if rows sizes match columns,..
validateDataFrame :: DataFrame -> Either ErrorMessage ()
validateDataFrame _ = error "validateDataFrame ot implemented"

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
    