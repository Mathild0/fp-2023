{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement
  )
where

import DataFrame (DataFrame(..), Column(..), ColumnType(..), Value(..))
import InMemoryTables (TableName, database)
import Data.Char (toLower)
import Data.String (IsString)

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
    --["select", "sum", "(", column, ")", "from", table] -> Right ()
    ["show", "tables"] -> Right ShowTables
    _ -> Left "Invalid statement"

columnName :: Column -> String
columnName (Column name _) = name

-- Executes a parsed statement. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement ShowTables = Right $ DataFrame [Column "Tables" StringType] (map (\(name, _) -> [StringValue name]) database)

executeStatement _ = Left "Not implemented: executeStatement"
