{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# OPTIONS_GHC -Wno-name-shadowing #-}
{-# OPTIONS_GHC -Wno-unused-matches #-}

module Lib2
  ( calculateSum,
    calculateMinimum,
    parseStatement,
    executeStatement,
    selectColumns,
    ColumnName,
    ParsedStatement(..),
    AggregateFunction(..),
    Value(..),
    whereBool,
    
  )
where

import DataFrame (DataFrame(..), Column(..), ColumnType(..), Value(..), Row)

import Lib1 (findTableByName)
import Data.Char (toLower, isSpace)
import Data.List (findIndex, isPrefixOf, isSuffixOf, group, elemIndex)
import Data.Maybe (fromMaybe, mapMaybe, isJust, catMaybes)
import InMemoryTables (TableName, database)

type ErrorMessage = String
type Database = [(TableName, DataFrame)]

type ColumnName = String

data ParsedStatement
  = ParsedStatement
  | SelectSumStatement AggregateFunction ColumnName TableName
  | SelectSpecificColumnStatement [String] TableName
  | SelectMinStatement AggregateFunction ColumnName TableName
  | SelectColumnListStatement [String] TableName
  | ShowTables
  | ShowTable TableName
  | SelectAndStatement [ParsedCondition] TableName 
  | SelectWhereStatement [ParsedCondition] TableName
  | SelectBoolStatement ColumnName Bool TableName
  | NestedSelectStatement ParsedStatement [String]
  deriving (Show, Eq)

data ParsedCondition
  = EqualCondition ColumnName Value
  | NotEqualCondition ColumnName Value
  | LessThanCondition ColumnName Value
  | LessThanOrEqualCondition ColumnName Value
  | GreaterThanCondition ColumnName Value
  | GreaterThanOrEqualCondition ColumnName Value
  | BoolCondition ColumnName Bool  
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

tableNames :: [TableName]
tableNames = map fst database

commandKeywords :: [String]
commandKeywords = ["select", "*", "from", "show", "table", "tables"]

names :: [String]
names = commandKeywords ++ tableNames

parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement input =
  let tokens = words input
      normalizedTokens = map (map toLower) tokens
  in case normalizedTokens of
    ["show", "tables"] -> Right ShowTables
    ["show", "table", tableName] -> Right (ShowTable tableName)
    ["select", "*", "from", table] -> Right (SelectColumnListStatement ["*"] table)
    ["select", "sum", "(", column, ")", "from", table] ->
      Right (SelectSumStatement Sum column table)
    ["select", "min", "(", column, ")", "from", table] ->
      Right (SelectMinStatement Min column table)
    ["select", "*", "from", table, "where", column, "=", "true"] -> 
      Right (SelectBoolStatement column True table)
    ["select", "*", "from", table, "where", column, "=", "false"] -> 
      Right (SelectBoolStatement column  False table)    
    ["select", "*", "from", table, "where", column, operator, value] -> 
      case parseOperator operator of
        Just op -> 
          case readValue value of
            Just v -> Right (SelectWhereStatement [op column v] table)
            Nothing -> Left "Invalid value"
        Nothing -> Left "Invalid operator"
    ["select", "*", "from", table, "where", column, operator, value, "and", column', operator', value'] -> 
      case (parseOperator operator, parseOperator operator') of
        (Just op1, Just op2) -> 
          case (readValue value, readValue value') of
            (Just v1, Just v2) -> Right (SelectAndStatement [op1 column v1, op2 column' v2] table)
            _ -> Left "Invalid value"
        _ -> Left "Invalid operator"
    "select" : rest ->
      case break (== "from") rest of
        (cols, "from" : "(" : innerRest) ->
          let (innerQueryTokens, remaining) = span (/= ")") innerRest
              innerQuery = unwords innerQueryTokens
              parsedInnerQuery = parseStatement innerQuery
              columns = parseColumnList (unwords cols)
              subqueryAlias = case dropWhile (/= ")") remaining of
                                ")" : "as" : alias : _ -> Just alias
                                _ -> Nothing
          in case parsedInnerQuery of
              Right parsed ->
                if isJust subqueryAlias
                then Right (NestedSelectStatement parsed columns)
                else Left "Subquery alias missing or incorrect"
              Left err -> Left err
        (cols, "from" : table : _) ->
          let columns = parseColumnList (unwords cols)
          in Right (SelectSpecificColumnStatement columns table)
        _ -> Left "Invalid statement"
    _ -> Left "Invalid statement"

parseOperator :: String -> Maybe (ColumnName -> Value -> ParsedCondition)
parseOperator operator
  | operator == "=" = Just EqualCondition
  | operator == "!=" = Just NotEqualCondition
  | operator == "<"  = Just LessThanCondition
  | operator == "<=" = Just LessThanOrEqualCondition
  | operator == ">"  = Just GreaterThanCondition
  | operator == ">=" = Just GreaterThanOrEqualCondition
  | otherwise = Nothing

columnName :: Column -> String
columnName (Column name _) = name

executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement ShowTables =
  Right $ DataFrame [Column "Tables" StringType] (map (\(name, _) -> [StringValue name]) database)

executeStatement (ShowTable tableName) =
  case findTableByName database tableName of
    Just table -> Right table
    Nothing -> Left "Table not found"

executeStatement (SelectSpecificColumnStatement columns tableName) =
  case findTableByName database tableName of
    Just table -> selectColumns columns table

executeStatement (SelectSumStatement Sum column tableName) =
  case findTableByName database tableName of
    Just (DataFrame columns rows) ->
      case findColumnIndex column columns of
        Just idx ->
          let values = extractColumnValues column columns rows
          in case calculateSum values of
            Just sumValue -> Right $ DataFrame [Column "Sum" IntegerType] [[IntegerValue sumValue]]
            Nothing -> Left "Column not found or an invalid sum statement"
        Nothing -> Left "Column not found or an invalid sum statement"
    Nothing -> Left "Table not found"

executeStatement (SelectMinStatement Min columnName tableName) =
  case findTableByName database tableName of
    Just (DataFrame columns rows) ->
      let columnIndex = findColumnIndex columnName columns
      in case columnIndex of
        Just idx ->
          let columnData = [rowData !! idx | rowData <- rows]
              minValue = calculateMinimum columnData
          in Right $ DataFrame [Column "Result" (getColumnType minValue)] [[minValue]]
        Nothing -> Left "Column not found"
    Nothing -> Left "Table not found"

executeStatement (SelectAndStatement conditions tableName) =
  case findTableByName database tableName of
    Just (DataFrame columns rows) ->
      let matchingRows = filter (\row -> any (\condition -> matchesCondition condition columns row) conditions) rows
      in Right (DataFrame columns matchingRows)
    Nothing -> Left "Table not found"

executeStatement (SelectColumnListStatement columns tableName) =
  case findTableByName database tableName of
    Just table -> selectColumns columns table
    Nothing -> Left "Table not found"

executeStatement (SelectWhereStatement conditions tableName) =
  case findTableByName database tableName of
    Just (DataFrame columns rows) ->
      let matchingRows = filter (matchesAllConditions conditions columns) rows
      in Right (DataFrame columns matchingRows)
    Nothing -> Left "Table not found"

executeStatement (SelectBoolStatement columnName boolValue tableName) =
  case findTableByName database tableName of
    Just (DataFrame columns rows) ->
      let matchingRows = filter (\row -> matchesBoolCondition columnName boolValue columns row) rows
      in Right (DataFrame columns matchingRows)
    Nothing -> Left "Table not found"

executeStatement (NestedSelectStatement innerQuery columns) =
  case executeStatement innerQuery of
    Right innerResult -> selectColumns columns innerResult
    Left errorMessage -> Left errorMessage

executeStatement _ = Left "Not implemented: executeStatement"

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

minVal :: Value -> Value -> Value
minVal (IntegerValue a) (IntegerValue b) = IntegerValue (min a b)
minVal (StringValue a) (StringValue b) = StringValue (min a b)  
minVal (BoolValue a) (BoolValue b) = BoolValue (min a b)
minVal _ _ = NullValue

findColumnIndex :: ColumnName -> [Column] -> Maybe Int
findColumnIndex columnName =
  findIndex (\(Column name _) -> map toLower name == map toLower columnName)

getColumnType :: Value -> ColumnType
getColumnType value =
  case value of
    IntegerValue _ -> IntegerType
    BoolValue _    -> BoolType
    StringValue _  -> StringType
    _              -> StringType

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
  if null columns || all (== "*") columns
    then Right (DataFrame dfColumns dfRows)
    else selectColumns' columns (DataFrame dfColumns dfRows)

selectColumns' :: [String] -> DataFrame -> Either ErrorMessage DataFrame
selectColumns' requestedColumns (DataFrame dfColumns dfRows) =
  let columnIndices = getAllIndexes requestedColumns dfColumns
  in if null columnIndices
    then Left "No columns were found"
    else
      let selectedColumns = map (\x -> dfColumns !! x) columnIndices
          selectedRows = map (selectCells columnIndices) dfRows
      in Right (DataFrame selectedColumns selectedRows)

getAllIndexes :: [String] -> [Column] -> [Int]
getAllIndexes requestedColumns dfColumns =
  mapMaybe (`elemIndex` map columnName dfColumns) requestedColumns

selectCells :: [Int] -> Row -> [Value]
selectCells indices row = map (row !!) indices

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

readValue :: String -> Maybe Value
readValue str = case reads str of
  [(val, "")] -> Just (IntegerValue val)
  _           -> Nothing

matchesAllConditions :: [ParsedCondition] -> [Column] -> Row -> Bool
matchesAllConditions conditions columns row =
  all (\condition -> matchesCondition condition columns row) conditions

matchesCondition :: ParsedCondition -> [Column] -> Row -> Bool
matchesCondition (EqualCondition colName value) columns row =
  case findColumnIndex colName columns of
    Just idx -> row !! idx == value
    Nothing  -> False
matchesCondition (NotEqualCondition colName value) columns row =
  case findColumnIndex colName columns of
    Just idx -> row !! idx /= value
    Nothing  -> False
matchesCondition (LessThanCondition colName value) columns row =
  case findColumnIndex colName columns of
    Just idx -> row !! idx < value
    Nothing  -> False
matchesCondition (LessThanOrEqualCondition colName value) columns row =
  case findColumnIndex colName columns of
    Just idx -> row !! idx <= value
    Nothing  -> False
matchesCondition (GreaterThanCondition colName value) columns row =
  case findColumnIndex colName columns of
    Just idx -> row !! idx > value
    Nothing  -> False
matchesCondition (GreaterThanOrEqualCondition colName value) columns row =
  case findColumnIndex colName columns of
    Just idx -> row !! idx >= value
    Nothing  -> False
matchesCondition (BoolCondition colName boolValue) columns row =
  case findColumnIndex colName columns of
    Just idx -> row !! idx == BoolValue boolValue
    Nothing  -> False

instance Ord Value where
    compare (IntegerValue a) (IntegerValue b) = compare a b
    compare (BoolValue a) (BoolValue b) = compare a b
    compare (StringValue a) (StringValue b) = compare a b
    compare _ _ = EQ 

whereAND :: String -> Either ErrorMessage DataFrame
whereAND input =
  case parseStatement input of
    Right (SelectAndStatement conditions tableName) -> 
      case findTableByName database tableName of
        Just (DataFrame columns rows) ->
          let matchingRows = filter (matchesAllConditions conditions columns) rows
          in Right (DataFrame columns matchingRows)
        Nothing -> Left "Table not found"    
    _ -> Left "Invalid statement"

whereBool :: String -> Either ErrorMessage DataFrame
whereBool input =
  case parseStatement input of
    Right (SelectWhereStatement conditions tableName) ->
      case findTableByName database tableName of
        Just (DataFrame columns rows) ->
          let matchingRows = filter (matchesAllConditions conditions columns) rows
          in Right (DataFrame columns matchingRows)
        Nothing -> Left "Table not found"
    _ -> Left "Invalid statement"

matchesBoolCondition :: ColumnName -> Bool -> [Column] -> Row -> Bool
matchesBoolCondition colName boolValue columns row =
  case findColumnIndex colName columns of
    Just idx -> row !! idx == BoolValue boolValue
    Nothing  -> False
