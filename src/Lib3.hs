{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Replace case with fromMaybe" #-}
{-# HLINT ignore "Avoid lambda" #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra(..),
    loadFiles,
    saveTable,
    getTime,
    parseYAMLContent,
    currentTimeDataFrame,
    executeNowStatement,
    runExecuteIO 
  )
where
import Data.Time
import Data.Time (getCurrentTime)
    
import Debug.Trace   
import qualified Data.ByteString as BS
import qualified Data.Yaml as Yaml
import qualified Data.Text.Encoding as TE
import Control.Monad.Free (Free (..), liftF)
import qualified Data.Yaml as Yaml
import DataFrame (DataFrame(..), Column(..), ColumnType(..), Value(..), Row)
import Data.Time (UTCTime)
import Data.List (foldl', isInfixOf, isPrefixOf, tails, findIndex, find)
import Data.Char (toLower, isDigit, isSpace)
import Lib2 
import Data.Maybe (fromMaybe, catMaybes)
import qualified Data.Text as T
import Data.Text (Text)
import GHC.Generics (Generic)
import Data.Aeson (ToJSON, FromJSON) 
import InMemoryTables (database)
type TableName = String
type FileContent = String
type ErrorMessage = String
type Condition = String
type JoinCondition = String

data ExecutionAlgebra next
  = LoadFiles TableName (Either ErrorMessage (TableName, DataFrame) -> next)
  | SaveFiles (TableName, DataFrame) (() -> next)
  | GetTime (UTCTime -> next)
  | DeleteRows TableName (Maybe Condition) (Either ErrorMessage DataFrame -> next)
  | JoinTables [TableName] JoinCondition (Either ErrorMessage DataFrame -> next)
  | InsertRow TableName Row (Either ErrorMessage DataFrame -> next)
  | UpdateTable TableName [(Lib2.ColumnName, Value)] (Maybe Condition) (Either ErrorMessage DataFrame -> next)
  deriving (Functor)

type Execution = Free ExecutionAlgebra

loadFiles :: TableName -> Execution (Either ErrorMessage (TableName, DataFrame))
loadFiles tableNames = liftF $ LoadFiles tableNames id

saveTable :: (TableName, DataFrame) -> Execution ()
saveTable tableData = liftF $ SaveFiles tableData id

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
    let maybeUpdateStatement = parseUpdateStatement sql
    case maybeUpdateStatement of
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
        trimmedColumnName = trim columnName
        trimmedValueStr = trim $ drop 1 valueStr 
    in (trimmedColumnName, readValue trimmedValueStr)

parseConditionPart :: String -> Maybe Condition
parseConditionPart sql =
    let words' = words $ map toLower sql
        conditionPart = dropWhile (/= "where") words'
    in if null conditionPart
       then Nothing
       else Just $ unwords $ tail conditionPart

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
            let (columnValues, conditionPart) = break (== "where") rest
                parsedColumnValues = map parseColumnValue $ splitColumnValues $ unwords columnValues
                parsedCondition = parseConditionPart $ unwords conditionPart
            in Just (tableName, parsedColumnValues, parsedCondition)
        _ -> Nothing

splitColumnValues :: String -> [String]
splitColumnValues = go False ""
  where
    go _ acc [] = [reverse acc]
    go inQuotes acc (c:cs)
      | c == ',' && not inQuotes = reverse acc : go inQuotes "" cs
      | c == '\'' = go (not inQuotes) (c:acc) cs
      | otherwise = go inQuotes (c:acc) cs

parseUpdateCondition :: [String] -> Maybe Condition
parseUpdateCondition conditionPart =
    case conditionPart of
        "where" : colName : "=" : value : _ -> Just (colName ++ " = " ++ value)
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
        trimmedValueStr = trim $ drop 1 valueStr 
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
getColumnIndex colName columns =
    fromMaybe (error "Column not found") $ findIndex (\(Column name _) -> name == colName) columns

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
        combinedCols = cols1 ++ cols2  
        combinedRows = concatMap (\row1 ->
                          case find (\row2 -> row2 !! col2Index == row1 !! col1Index) rows2 of
                            Just row2 -> [row1 ++ row2] 
                            Nothing -> [row1 ++ replicate (length cols2) (StringValue "NULL")])  
                        rows1
    in DataFrame combinedCols combinedRows

matchesCondition :: Int -> Value -> Row -> Bool
matchesCondition colIndex value row =
    case row !! colIndex of
        val -> val == value

matchCondition :: Maybe Condition -> [Column] -> Row -> Bool
matchCondition Nothing _ _ = True
matchCondition (Just condStr) columns row =
    let (colName, expectedValue) = parseCondition condStr
        columnIndex = fromMaybe (error "Column not found") $ findIndex (\(Column name _) -> name == colName) columns
        actualValue = row !! columnIndex
    in actualValue == expectedValue

trim :: String -> String
trim = f . f where f = reverse . dropWhile isSpace

trimQuotes :: String -> String
trimQuotes str =
    case str of
        ('\'':rest) | last rest == '\'' -> init rest
        _ -> str

readValue :: String -> Value
readValue str =
    let cleanedStr = trimQuotes $ trim str
    in if all isDigit $ filter (/= ' ') cleanedStr
       then IntegerValue (read $ filter (/= ' ') cleanedStr)
       else StringValue cleanedStr

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
    DataFrame columns (map (updateRowIfNeeded maybeCondition updates columns) rows)

updateRowIfNeeded :: Maybe Condition -> [(Lib2.ColumnName, Value)] -> [Column] -> Row -> Row
updateRowIfNeeded maybeCondition updates columns row =
    if matchCondition maybeCondition columns row
        then applyUpdates updates columns row
        else row

applyUpdates :: [(Lib2.ColumnName, Value)] -> [Column] -> Row -> Row
applyUpdates updates columns row = foldl' (updateValueInRow columns) row updates

updateRow :: [(Lib2.ColumnName, Value)] -> [Column] -> Row -> Row
updateRow updates columns row = 
    let columnIndices = map (\(Column name _) -> name) columns `zip` [0..]
        indexedUpdates = catMaybes $ map (\(colName, newVal) -> fmap (\idx -> (idx, newVal)) (lookup colName columnIndices)) updates
    in foldl' applyUpdate row indexedUpdates
    where
        applyUpdate r (idx, newVal) = 
            let updatedRow = take idx r ++ [newVal] ++ drop (idx + 1) r
            in trace ("Updating row at index " ++ show idx ++ ": " ++ show r ++ " to " ++ show updatedRow) updatedRow
            
updateValueInRow :: [Column] -> Row -> (Lib2.ColumnName, Value) -> Row
updateValueInRow columns row (colName, newVal) =
    let colIndex = getColumnIndex colName columns
    in take colIndex row ++ [newVal] ++ drop (colIndex + 1) row

updateValueAtIndex :: Int -> Value -> [Value] -> [Value]
updateValueAtIndex idx newVal row =
    take idx row ++ [newVal] ++ drop (idx + 1) row

findUpdate :: Int -> Value -> [(Lib2.ColumnName, Int)] -> [(Lib2.ColumnName, Value)] -> Value
findUpdate colIdx val colNamesIndices updates =
    case lookup colIdx (map (\(name, idx) -> (idx, fromMaybe val (lookup name updates))) colNamesIndices) of
        Just newVal -> newVal
        Nothing -> val

-----------------------------------------------------------------------------------------------------------
data SerializedTable = SerializedTable {
    tableName :: TableName,
    columns :: [SerializedColumn],
    rows :: [[Yaml.Value]]
} deriving (Show, Eq, Generic)

instance ToJSON SerializedTable
instance FromJSON SerializedTable

data SerializedColumn = SerializedColumn {
    columnName :: String,
    dataType :: String
} deriving (Show, Eq, Generic)

instance ToJSON SerializedColumn
instance FromJSON SerializedColumn

columnToSerializedColumn :: Column -> SerializedColumn
columnToSerializedColumn (Column colName colType) = SerializedColumn colName (show colType)


serializeFile :: (TableName, DataFrame) -> SerializedTable
serializeFile (tableName, DataFrame columns dataRows) =
  SerializedTable {
    tableName = tableName,
    columns = map columnToSerializedColumn columns,
    rows = map rowToYamlValues dataRows
  }


serializedRowToRow :: [Yaml.Value] -> Row
serializedRowToRow = map yamlValueToValue


yamlValueToValue :: Yaml.Value -> Value
yamlValueToValue (Yaml.String s) = StringValue (T.unpack s) 
yamlValueToValue (Yaml.Number n) = IntegerValue (round n)
yamlValueToValue (Yaml.Bool b)   = BoolValue b
yamlValueToValue Yaml.Null       = NullValue
yamlValueToValue _               = error "Unsupported Yaml value type"


serializedTableToDataFrame :: SerializedTable -> DataFrame
serializedTableToDataFrame (SerializedTable _ serializedColumns serializedRows) =
    let columns = map serializedColumnToColumn serializedColumns
        rows = map serializedRowToRow serializedRows
    in DataFrame columns rows


serializedColumnToColumn :: SerializedColumn -> Column
serializedColumnToColumn (SerializedColumn colName colType) =
    Column colName (stringToColumnType colType)


stringToColumnType :: String -> ColumnType
stringToColumnType "int" = IntegerType
stringToColumnType "string" = StringType
stringToColumnType "bool" = BoolType
stringToColumnType _ = error "Unsupported column type"


initDatabase :: [FilePath] -> IO [(TableName, DataFrame)]
initDatabase filePaths = do
    tables <- mapM readYamlFile filePaths
    let dataFrames = catMaybes tables
    return $ map (\table -> (tableName table, serializedTableToDataFrame table)) dataFrames

serializeToYAML :: SerializedTable -> T.Text
serializeToYAML st =
    T.unlines
       [ T.pack "table name: " <> T.pack (tableName st),
         T.pack "columns: ",
         T.unlines (map serializeColumn (columns st)),
         T.pack "rows: ",
         T.unlines (map serializeRow (rows st))
       ]
    where
        serializeColumn :: SerializedColumn -> T.Text
        serializeColumn col =
           T.pack "- column name: " <> T.pack (columnName col)

serializeRow :: [Yaml.Value] -> T.Text
serializeRow rowValues = T.intercalate (T.pack ", ") (map yamlValueToText rowValues)

yamlValueToText :: Yaml.Value -> T.Text
yamlValueToText (Yaml.String s) = s
yamlValueToText (Yaml.Number n) = T.pack (show n)
yamlValueToText (Yaml.Bool b)   = T.pack (show b)
yamlValueToText Yaml.Null       = T.pack "NULL"
yamlValueToText _               = error "Unsupported Yaml value type"

rowToYamlValues :: Row -> [Yaml.Value]
rowToYamlValues = map valueToYamlValue

valueToYamlValue :: Value -> Yaml.Value
valueToYamlValue (StringValue s) = Yaml.String (T.pack s)
valueToYamlValue (IntegerValue n) = Yaml.Number (fromIntegral n)
valueToYamlValue (BoolValue b) = Yaml.Bool b

parseYAMLContent :: BS.ByteString -> Either Yaml.ParseException SerializedTable
parseYAMLContent = Yaml.decodeEither'


readYamlFile :: FilePath -> IO (Maybe SerializedTable)
readYamlFile filePath = do
    eitherData <- Yaml.decodeFileEither filePath
    case eitherData of
        Left err -> do
            putStrLn $ "Error reading file: " ++ show err
            return Nothing
        Right tableData -> return $ Just tableData
runExecuteIO :: Lib3.Execution r -> IO r
runExecuteIO (Pure r) = return r
runExecuteIO (Free step) = do
    next <- runStep step
    runExecuteIO next
    where
        runStep :: Lib3.ExecutionAlgebra a -> IO a
        runStep (Lib3.GetTime next) = getCurrentTime >>= return . next
