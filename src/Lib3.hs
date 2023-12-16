{-# LANGUAGE DeriveFunctor #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra(..)
  )
where

import Control.Monad.Free (Free (..), liftF)
import DataFrame (DataFrame(..), Column(..), ColumnType(..), Value(..))
import InMemoryTables (database)
import Data.Time (UTCTime)
import qualified DataFrame as DF
import Data.List (isInfixOf, isPrefixOf, tails, findIndex, find)
import Data.Char (toLower)
import Lib2
import Data.Maybe (fromMaybe)
import Data.Maybe (catMaybes, fromJust)
import Data.Char (isDigit, isSpace)
import Data.List (foldl')

import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import Text.Read (readMaybe)
import qualified Data.Text.Read as TR
import System.FilePath ((</>), (<.>))
import Data.Yaml ( encodeFile, decodeFileThrow, decodeThrow )
import Data.Text (Text)

import Debug.Trace (trace)






type TableName = String
type FileContent = String
type ErrorMessage = String
type Condition = String
type JoinCondition = String

data ExecutionAlgebra next
  = LoadFile TableName (FileContent -> next)
  | GetTime (UTCTime -> next)
  | DeleteRows TableName (Maybe Condition) (Either ErrorMessage DataFrame -> next)
  | JoinTables [TableName] JoinCondition (Either ErrorMessage DataFrame -> next)
  | InsertRow TableName Row (Either ErrorMessage DataFrame -> next)
  | UpdateTable TableName [(Lib2.ColumnName, Value)] (Maybe Condition) (Either ErrorMessage DataFrame -> next)
  deriving Functor

type Execution = Free ExecutionAlgebra
type Row = [Value]
type ColumnName = String
type ColumnValue = Value

loadFile :: TableName -> Execution FileContent
loadFile name = liftF $ LoadFile name id

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql = do
    let sqlLower = map toLower sql
    if "select now()" `isInfixOf` sqlLower then
        executeNowStatement
    else if "delete" `isInfixOf` sqlLower then
        executeDeleteStatement sql
    else if "from" `isInfixOf` sqlLower && "where" `isInfixOf` sqlLower && multipleTablesInvolved sqlLower then
        executeJoinSelectStatement sql
    else if "insert into" `isInfixOf` sqlLower then
        executeInsertStatement sql
    else if "update" `isInfixOf` sqlLower then
        executeUpdateStatement sql
    else
        executeGeneralStatement sql

executeNowStatement :: Execution (Either ErrorMessage DataFrame)
executeNowStatement = do
    currentTime <- getTime
    return $ Right (currentTimeDataFrame currentTime)

executeDeleteStatement :: String -> Execution (Either ErrorMessage DataFrame)
executeDeleteStatement sql = do
    let deleteStatement = parseDeleteStatement sql
    case deleteStatement of
        Just (tableName, maybeCondition) ->
            case findTableByName database tableName of
                Just table -> do
                    let updatedTable = deleteRowsFromTable maybeCondition table
                    return $ Right updatedTable
                Nothing -> return $ Left "Table not found"
        Nothing -> return $ Left "Invalid DELETE statement"

executeInsertStatement :: String -> Execution (Either ErrorMessage DataFrame)
executeInsertStatement sql = do
    let insertStatement = parseInsertStatement sql
    case insertStatement of
        Just (tableName, rowValues) ->
            case findTableByName database tableName of
                Just table -> do
                    let updatedTable = insertRowIntoTable rowValues table
                    return $ Right updatedTable
                Nothing -> return $ Left "Table not found"
        Nothing -> return $ Left "Invalid INSERT statement"

executeUpdateStatement :: String -> Execution (Either ErrorMessage DataFrame)
executeUpdateStatement sql = do
    let updateStatement = parseUpdateStatement sql
    case updateStatement of
        Just (tableName, updates, maybeCondition) ->
            case findTableByName database tableName of
                Just table -> do
                    let updatedTable = updateTable maybeCondition updates table
                    return $ Right updatedTable
                Nothing -> return $ Left "Table not found"
        Nothing -> return $ Left "Invalid UPDATE statement"

executeJoinSelectStatement :: String -> Execution (Either ErrorMessage DataFrame)
executeJoinSelectStatement sql = do
    let fromClauseTables = parseFromClause sql
    let whereClause = getJoinCondition (words sql)
    case whereClause of
        Just condition -> do
            let joinedTables = joinTables database condition
            return joinedTables
        _ -> return $ Left "Invalid JOIN statement or join condition not found"


executeGeneralStatement :: String -> Execution (Either ErrorMessage DataFrame)
executeGeneralStatement sql = do
    let parsedStatement = parseStatement sql
    case parsedStatement of
        Right statement -> return $ executeStatement statement
        Left error -> return $ Left error

parseFromClause :: String -> [TableName]
parseFromClause sql =
    let words' = words $ map toLower sql
        tableNames = dropWhile (/= "from") words'
    in case tableNames of
        (_:tables) -> wordsBy (==',') $ unwords $ takeWhile (/= "where") tables
        _ -> []

parseSelectClause :: String -> [(Maybe TableName, Lib2.ColumnName)]
parseSelectClause sql =
    let words' = words $ map toLower sql
        selectClause = takeWhile (/= "from") $ drop 1 $ dropWhile (/= "select") words'
        columnNames = wordsBy (==',') $ unwords selectClause
    in map parseColumn columnNames

parseColumn :: String -> (Maybe TableName, Lib2.ColumnName)
parseColumn column =
    case break (=='.') column of
        (table, '.' : colName) -> (Just table, colName)
        (colName, _)           -> (Nothing, colName)

parseColumnValue :: String -> (Lib2.ColumnName, Value)
parseColumnValue str =
    let (columnName, valueStr) = break (== '=') str
    in (trim columnName, readValue $ trim $ drop 1 valueStr)

parseConditionPart :: [String] -> Maybe Condition
parseConditionPart [] = Nothing
parseConditionPart (_ : cond) = Just $ unwords cond

parseCSV :: String -> [String]
parseCSV input = go input False []
  where
    go [] _ acc = [reverse acc]
    go (c:cs) inQuotes acc
        | c == ',' && not inQuotes = reverse acc : go cs False []
        | c == '\'' = go cs (not inQuotes) acc
        | otherwise = go cs inQuotes (c:acc)

parseValues :: String -> Row
parseValues valuesString =
    let trimmed = dropWhile isSpace . reverse . dropWhile isSpace . reverse $ valuesString
        trimmedParentheses = if head trimmed == '(' && last trimmed == ')' then tail (init trimmed) else trimmed
        values = parseCSV trimmedParentheses
    in map readValue values

parseDeleteStatement :: String -> Maybe (TableName, Maybe Condition)
parseDeleteStatement sql =
    let words' = map toLower <$> words sql
        tableName = getTableName words'
        condition = getCondition words'
    in case tableName of
         Just tn -> Just (tn, condition)
         Nothing -> Nothing

parseInsertStatement :: String -> Maybe (TableName, Row)
parseInsertStatement sql =
    case words (map toLower sql) of
        ("insert" : "into" : tableName : "values" : rest) ->
            Just (tableName, parseValues $ unwords rest)
        _ -> Nothing

parseUpdateStatement :: String -> Maybe (TableName, [(Lib2.ColumnName, Value)], Maybe Condition)
parseUpdateStatement sql =
    let words' = words $ map toLower sql
    in case words' of
        "update" : tableName : "set" : rest ->
            let (columnValues, condition) = break (== "where") rest
                parsedColumnValues = map parseColumnValue $ splitOn "," (unwords columnValues)
            in Just (tableName, parsedColumnValues, parseConditionPart condition)
        _ -> Nothing

parseJoinSelectStatement :: String -> Maybe ([TableName], JoinCondition)
parseJoinSelectStatement sql =
    let words' = words $ map toLower sql
        tableNames = getTableNames words'
        joinCondition = getJoinCondition words'
    in case (tableNames, joinCondition) of
         (Just tns, Just jc) -> Just (tns, jc)
         _ -> Nothing

parseJoinCondition :: JoinCondition -> ((TableName, Lib2.ColumnName), (TableName, Lib2.ColumnName))
parseJoinCondition condition =
    let (left, right) = break (== '=') condition
        leftTableColumn = parseTableColumn $ trim left
        rightTableColumn = parseTableColumn $ trim $ drop 1 right
    in (leftTableColumn, rightTableColumn)

parseTableColumn :: String -> (TableName, Lib2.ColumnName)
parseTableColumn str =
    case break (=='.') str of
        (table, '.' : column) -> (table, column)
        _ -> error "Invalid table.column format in JOIN condition"

parseConditions :: String -> [(Lib2.ColumnName, Value)]
parseConditions condStr = map parseCondition $ splitOn "and" condStr

parseCondition :: String -> (Lib2.ColumnName, Value)
parseCondition cond =
    let (colName, valueStr) = break (== '=') cond
        trimmedColName = trim colName
        trimmedValueStr = trim $ drop 1 valueStr -- Remove '=' and trim
    in (trimmedColName, readValue trimmedValueStr)

getTableName :: [String] -> Maybe TableName
getTableName words' =
    case dropWhile (/= "from") words' of
        (_:tableName:_) -> Just tableName
        _ -> Nothing

getCondition :: [String] -> Maybe String
getCondition words' =
    let conditionParts = dropWhile (/= "where") words'
    in if length conditionParts > 1
       then Just (unwords $ tail conditionParts)
       else Nothing

getColumnValues :: String -> DataFrame -> [Value]
getColumnValues columnName (DataFrame columns rows) =
    case findIndex (\(Column colName _) -> colName == columnName) columns of
        Just colIndex -> map (!! colIndex) rows
        Nothing -> error "Column not found"

getTableNames :: [String] -> Maybe [TableName]
getTableNames words' =
    case dropWhile (/= "from") words' of
        (_:rest) -> Just $ takeWhile (/= "where") rest
        _ -> Nothing

getJoinCondition :: [String] -> Maybe JoinCondition
getJoinCondition words' =
    let conditionParts = dropWhile (/= "where") words'
    in if length conditionParts > 1
       then Just $ unwords $ tail conditionParts
       else Nothing

getTableNamesInFromClause :: [String] -> [TableName]
getTableNamesInFromClause words' =
    case dropWhile (/= "from") words' of
        (_:rest) -> takeWhile (/= "where") $ wordsBy (==',') $ unwords rest
        _ -> []

getColumnIndex :: Lib2.ColumnName -> [Column] -> Int
getColumnIndex colName cols =
    fromMaybe (error "Column not found") $ findIndex (\(Column name _) -> name == colName) cols

findMatch :: Row -> Int -> [Row] -> Int -> Maybe Row
findMatch row1 col1Index rows2 col2Index =
    find (\row2 -> row1 !! col1Index == row2 !! col2Index) rows2

findColumnIndex :: String -> [Column] -> Int
findColumnIndex columnName cols =
    fromMaybe (error "Column not found") $ findIndex (\(Column colName _) -> colName == columnName) cols

findTableByName :: [(TableName, DataFrame)] -> TableName -> Maybe DataFrame
findTableByName db name = lookup name db

joinTables :: [(TableName, DataFrame)] -> JoinCondition -> Either ErrorMessage DataFrame
joinTables database condition =
    let ((table1Name, col1Name), (table2Name, col2Name)) = parseJoinCondition condition
        maybeTable1 = lookup table1Name database
        maybeTable2 = lookup table2Name database
    in case (maybeTable1, maybeTable2) of
        (Just table1, Just table2) ->
            Right $ joinDataFrames table1 col1Name table2 col2Name
        _ -> Left "One or both tables not found2"

joinDataFrames :: DataFrame -> Lib2.ColumnName -> DataFrame -> Lib2.ColumnName -> DataFrame
joinDataFrames (DataFrame cols1 rows1) col1Name (DataFrame cols2 rows2) col2Name =
    let col1Index = getColumnIndex col1Name cols1
        col2Index = getColumnIndex col2Name cols2
        combinedCols = cols1 ++ cols2  -- Include all columns from both tables
        combinedRows = concatMap (\row1 ->
                          case find (\row2 -> row2 !! col2Index == row1 !! col1Index) rows2 of
                            Just row2 -> [row1 ++ row2]  -- Include the entire row from the second table
                            Nothing -> [row1 ++ replicate (length cols2) (StringValue "NULL")])  -- Handle no match case
                        rows1
    in DataFrame combinedCols combinedRows

matchesCondition :: Int -> Value -> Row -> Bool
matchesCondition colIndex value row =
    case row !! colIndex of
        val -> val == value

matchCondition :: Maybe Condition -> DataFrame -> Row -> Bool
matchCondition Nothing _ _ = True
matchCondition (Just condStr) (DataFrame columns _) row =
    let conditions = parseConditions condStr
        colIndices = map (\(colName, _) -> findColumnIndex colName columns) conditions
    in all (\((_, value), colIndex) -> row !! colIndex == value) $ zip conditions colIndices

trim :: String -> String
trim = f . f where f = reverse . dropWhile isSpace

trimQuotes :: String -> String
trimQuotes str =
    case str of
        ('\'':rest) | last rest == '\'' -> init rest
        _ -> str

readValue :: String -> Value
readValue str
    | all isDigit $ filter (/= ' ') str = IntegerValue (read $ filter (/= ' ') str)
    | otherwise = StringValue $ filter (/= '\"') $ trim str

splitOn :: Eq a => [a] -> [a] -> [[a]]
splitOn _ [] = []
splitOn delim str =
    let (first, rest) = break (isPrefixOf delim) (tails str)
    in if null rest
       then [concat first]
       else concat first : splitOn delim (drop (length delim) (head rest))

wordsBy :: (Char -> Bool) -> String -> [String]
wordsBy p s =  case dropWhile p s of
                      "" -> []
                      s' -> w : wordsBy p s''
                            where (w, s'') = break p s'

multipleTablesInvolved :: String -> Bool
multipleTablesInvolved sql =
    let tableNames = getTableNamesInFromClause $ words $ map toLower sql
    in length tableNames > 1

filterRow :: Int -> Row -> Row
filterRow colIndex row = [val | (val, idx) <- zip row [0..], idx /= colIndex]

deleteRowsFromTable :: Maybe Condition -> DataFrame -> DataFrame
deleteRowsFromTable Nothing df = df
deleteRowsFromTable (Just cond) (DataFrame cols rows) =
    let (colName, valueToDelete) = parseCondition cond
        colIndex = findColumnIndex colName cols
        filteredRows = filter (\row -> not $ matchesCondition colIndex valueToDelete row) rows
    in DataFrame cols filteredRows

insertRowIntoTable :: Row -> DataFrame -> DataFrame
insertRowIntoTable row (DataFrame columns rows) =
    DataFrame columns (rows ++ [row])

currentTimeDataFrame :: UTCTime -> DataFrame
currentTimeDataFrame currentTime =
    DataFrame [Column "Current Time" StringType] [[StringValue (show currentTime)]]

updateTable :: Maybe Condition -> [(Lib2.ColumnName, Value)] -> DataFrame -> DataFrame
updateTable maybeCondition updates (DataFrame columns rows) =
    let colIndices = map (\(colName, _) -> findColumnIndex colName columns) updates
        newValues = map snd updates
        updatedRows = map (\row -> if matchCondition maybeCondition (DataFrame columns rows) row
                                  then updateRow colIndices newValues row
                                  else row) rows
    in DataFrame columns updatedRows

updateRow :: [Int] -> [Value] -> Row -> Row
updateRow colIndices newValues row =
    foldl' (\r (index, newValue) -> updateValueAtIndex index newValue r) row (zip colIndices newValues)

updateValueInRow :: Row -> (Int, Value) -> Row
updateValueInRow row (index, newValue) =
    take index row ++ [newValue] ++ drop (index + 1) row

updateValueAtIndex :: Int -> Value -> [Value] -> [Value]
updateValueAtIndex idx newVal row =
    take idx row ++ [newVal] ++ drop (idx + 1) row

findUpdate :: Int -> Value -> [(Lib2.ColumnName, Int)] -> [(Lib2.ColumnName, Value)] -> Value
findUpdate colIdx val colNamesIndices updates =
    case lookup colIdx (map (\(name, idx) -> (idx, fromMaybe val (lookup name updates))) colNamesIndices) of
        Just newVal -> newVal
        Nothing -> val

-----------------------------------------------------------------------------------------------------------

serializeAndSaveDataFrame :: (TableName, DataFrame) -> IO ()
serializeAndSaveDataFrame (tableName, DataFrame columns dataRows) = do
  let fileName = tableName <.> "yaml"
      filePath = "db" </> fileName
      serializedTable =
        T.unlines
          (serializeColumns columns ++ [T.pack "---"] ++ map serializeRow dataRows)
  TIO.writeFile filePath serializedTable

serializeColumnType :: ColumnType -> T.Text
serializeColumnType IntegerType = T.pack "int"
serializeColumnType StringType  = T.pack "string"
serializeColumnType BoolType    = T.pack "bool"


serializeValue :: Value -> T.Text
serializeValue (IntegerValue i) = T.pack (show i)
serializeValue (StringValue s)   = T.pack s
serializeValue (BoolValue b)     = if b then T.pack "true" else T.pack "false"
serializeValue NullValue         = T.pack "null"


serializeColumns :: [Column] -> [Text]
serializeColumns = map serializeColumn

serializeColumn :: Column -> T.Text
serializeColumn (Column name colType) = T.pack name <> T.pack ":" <> serializeColumnType colType

serializeRow :: Row -> T.Text
serializeRow row = T.intercalate (T.pack ", ") (map serializeValue row)



------------

deserializeTable :: T.Text -> Either ErrorMessage DataFrame
deserializeTable input = case T.splitOn (T.pack "---") input of
  [metadataText, dataFrameText] -> do
    let metadataLines = T.lines metadataText
        columns = if not (null metadataLines) then deserializeColumns (head metadataLines) else Left "No metadata found"
        dataRows = map deserializeRow $ dropWhile T.null $ T.lines dataFrameText
    columnsResult <- columns
    return $ DataFrame columnsResult dataRows
  _ -> Left "Invalid input"


deserializeAndLoadTable :: TableName -> IO (Either ErrorMessage DataFrame)
deserializeAndLoadTable tableName = do
  let filePath = "db" </> tableName <.> "yaml"
  fileContent <- TIO.readFile filePath
  return $ deserializeTable fileContent


parseColumnDefinition :: T.Text -> Either ErrorMessage Column
parseColumnDefinition colDef = do
  let parts = T.splitOn (T.pack ":") colDef
  case parts of
    [name, colType] -> do
      t <- parseColumnType (T.unpack colType)
      return $ Column (T.unpack name) t
    _ -> Left "Invalid column definition"


deserializeColumns :: T.Text -> Either ErrorMessage [Column]
deserializeColumns metadataText =
  mapM parseColumnDefinition (T.lines metadataText)


deserializeRow :: T.Text -> Row
deserializeRow rowText = map deserializeValue (T.words rowText)

deserializeValue :: T.Text -> Value
deserializeValue s
  | s == T.pack "null" = NullValue
  | s == T.pack "true" = BoolValue True
  | s == T.pack "false" = BoolValue False
  | otherwise =
      let trimmed = T.strip s in
      case T.stripPrefix (T.pack "\"") trimmed >>= T.stripSuffix (T.pack "\"") of
        Just i' -> StringValue (T.unpack i')
        Nothing ->
          case TR.decimal trimmed of
            Right (i, _) -> IntegerValue i
            Left _ -> trace ("Failed to parse integer value: " ++ T.unpack trimmed) (StringValue $ "InvalidValue: " ++ T.unpack s)

parseColumnType :: String -> Either ErrorMessage ColumnType
parseColumnType colType =
  case map toLower colType of
    "int"    -> Right IntegerType
    "string" -> Right StringType
    "bool"   -> Right BoolType
    _        -> Left "Invalid column type"

