{-# OPTIONS_GHC -Wno-deferred-out-of-scope-variables #-}
{-# LANGUAGE BlockArguments #-}
import Data.Either
import Data.Either (isRight)
import InMemoryTables qualified as D
import Lib1
import Test.Hspec
import Lib2
import Lib3
import DataFrame
import qualified Data.ByteString as BS
import qualified Data.Yaml as Yaml
import qualified Lib3
import Control.Monad.IO.Class (liftIO)
import Data.Time (getCurrentTime, UTCTime)
import Data.Time.Format (formatTime, defaultTimeLocale)
main :: IO ()
main = hspec $ do
  describe "Lib1.findTableByName" $ do
    it "handles empty lists" $ do
      Lib1.findTableByName [] "" `shouldBe` Nothing
    it "handles empty names" $ do
      Lib1.findTableByName D.database "" `shouldBe` Nothing
    it "can find by name" $ do
      Lib1.findTableByName D.database "employees" `shouldBe` Just (snd D.tableEmployees)
    it "can find by case-insensitive name" $ do
      Lib1.findTableByName D.database "employEEs" `shouldBe` Just (snd D.tableEmployees)

  describe "Lib1.parseSelectAllStatement" $ do
    it "handles empty input" $ do
      Lib1.parseSelectAllStatement "" `shouldSatisfy` isLeft
    it "handles invalid queries" $ do
      Lib1.parseSelectAllStatement "select from dual" `shouldSatisfy` isLeft
    it "returns table name from correct queries" $ do
      Lib1.parseSelectAllStatement "selecT * from dual;" `shouldBe` Right "dual"

  describe "Lib1.validateDataFrame" $ do
    it "finds types mismatch" $ do
      Lib1.validateDataFrame (snd D.tableInvalid1) `shouldSatisfy` isLeft
    it "finds column size mismatch" $ do
      Lib1.validateDataFrame (snd D.tableInvalid2) `shouldSatisfy` isLeft
    it "reports different error messages" $ do
      Lib1.validateDataFrame (snd D.tableInvalid1) `shouldNotBe` Lib1.validateDataFrame (snd D.tableInvalid2)
    it "passes valid tables" $ do
      Lib1.validateDataFrame (snd D.tableWithNulls) `shouldBe` Right ()

  describe "Lib1.renderDataFrameAsTable" $ do
    it "renders a table" $ do
      Lib1.renderDataFrameAsTable 100 (snd D.tableEmployees) `shouldSatisfy` not . null

-----------------------------------------------------------------------------------

  describe "Lib2.parseStatement" $ do
    it "parses show tables statement" $ do
      Lib2.parseStatement "show tables" `shouldBe` Right Lib2.ShowTables
    it "parses SHOW TABLES statement" $ do
      Lib2.parseStatement "SHOW TABLES" `shouldBe` Right Lib2.ShowTables
    it "parses show table name statement" $ do
      Lib2.parseStatement "show table employees" `shouldBe` Right (Lib2.ShowTable "employees")
    it "parses SHOW TABLE name statement" $ do
      Lib2.parseStatement "SHOW TABLE employees" `shouldBe` Right (Lib2.ShowTable "employees")
    it "handles an invalid column list" $ do
      Lib2.parseStatement ",id ,,name," `shouldSatisfy` isLeft
    it "handles a valid sum request statement" $ do
      Lib2.parseStatement "select sum ( id ) from employees" `shouldBe` Right (SelectSumStatement Sum "id" "employees")

  describe "Lib2.whereAND" $ do
    it "parses a valid where AND statement" $ do
      let input = "select * from employees where id = 1"
      case Lib2.parseStatement input of
        Right parsedStatement -> do
          let result = Lib2.executeStatement parsedStatement
          case result of
            Right df -> do
              let expectedDataFrame = DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] [[IntegerValue 1, StringValue "Vi", StringValue "Po"]]
              df `shouldBe` expectedDataFrame
            Left err -> expectationFailure ("Expected a result but got an error: " ++ err)
        Left err -> expectationFailure ("Failed to parse statement: " ++ err)


  describe "Lib2.calculateMinimum" $ do
    it "calculates the minimum with a list of IntegerValues" $ do
      let values = [IntegerValue 3, IntegerValue 1, IntegerValue 2]
      Lib2.calculateMinimum values `shouldBe` IntegerValue 1
    it "returns NullValue for a list with mixed types" $ do
      let values = [IntegerValue 3, StringValue "hello", BoolValue True]
      Lib2.calculateMinimum values `shouldBe` NullValue  

  describe "Lib2.selectColumns" $ do
    it "handles an empty list of columns" $ do
      let inputColumns = []  -- Pass an empty list of columns
      let result = Lib2.selectColumns inputColumns (snd D.tableEmployees)
      case result of
        Right df -> do
          let expectedDataFrame = snd D.tableEmployees  -- The result should be the same as the input DataFrame
          df `shouldBe` expectedDataFrame
        Left err -> expectationFailure ("Expected success but got an error: " ++ err)

  describe "Lib2.calculateSum" $ do
    it "calculates the sum of IntegerValues in a specified column" $ do
      let values = [IntegerValue 1, IntegerValue 2, IntegerValue 3]
      Lib2.calculateSum values `shouldBe` Just 6
    it "returns Nothing for a list with mixed values' types" $ do
      let values = [IntegerValue 1, StringValue "test", BoolValue False]
      Lib2.calculateSum values `shouldBe` Nothing
    it "returns Nothing for a list with no IntegerValue elements" $ do
      let values = [StringValue "1", StringValue "test", StringValue "False"]
      Lib2.calculateSum values `shouldBe` Nothing
    it "calculates the sum of an empty list" $ do
      let values = []
      Lib2.calculateSum values `shouldBe` Just 0

  describe "Lib2.executeStatement" $ do
    it "returns the sum for a valid SelectSumStatement" $ do
      let result = executeStatement (SelectSumStatement Sum "id" "employees")
      result `shouldBe` Right (DataFrame [Column "Sum" IntegerType] [[IntegerValue 3]])
    it "handles an invalid aggregation column for sum" $ do
      let result = executeStatement (SelectSumStatement Sum "invalid_column" "employees")
      result `shouldSatisfy` isLeft
    it "handles an invalid table name for sum" $ do
      let result = executeStatement (SelectSumStatement Sum "id" "invalid_tableName")
      result `shouldSatisfy` isLeft
    
    it "returns the min for a valid SelectMinStatement" $ do
      let result = executeStatement (SelectMinStatement Min "id" "employees")
      result `shouldBe` Right (DataFrame [Column "Result" IntegerType] [[IntegerValue 1]])
    it "returns the min for a valid SelectMinStatement" $ do
      let result = executeStatement (SelectMinStatement Min "name" "employees")
      result `shouldBe` Right (DataFrame [Column "Result" StringType] [[StringValue "Ed"]])
    it "handles an invalid aggregation column for min" $ do
      let result = executeStatement (SelectMinStatement Min "invalid_column" "employees")
      result `shouldSatisfy` isLeft
    it "handles an invalid table name for min" $ do
      let result = executeStatement (SelectMinStatement Min "id" "invalid_tableName")
      result `shouldSatisfy` isLeft
    
    it "returns a table with selected columns" $ do
      let input = ["id", "name", "surname"]
      let parsedStatement = SelectColumnListStatement input (fst D.tableEmployees)
      Lib2.executeStatement parsedStatement `shouldSatisfy` isRight
      
  describe "Lib2.whereBool" $ do
    it "filters rows based on a boolean condition" $ do
      let input = "select * from employees where id = 1"
      case Lib2.whereBool input of
        Right filteredDataFrame -> do
          let expectedDataFrame = DataFrame
                [ Column "id" IntegerType
                , Column "name" StringType
                , Column "surname" StringType
                ]
                [ [ IntegerValue 1, StringValue "Vi", StringValue "Po" ]
                ]
          filteredDataFrame `shouldBe` expectedDataFrame
        Left err -> expectationFailure ("Expected success but got an error: " ++ err)
  
  describe "Lib3.parseYAMLContent" $ do
    it "correctly parses the employees YAML file" $ do
      employeesContent <- liftIO $ BS.readFile "db/employees.yaml"
      let result = Lib3.parseYAMLContent employeesContent
      result `shouldSatisfy` isRight
  
  