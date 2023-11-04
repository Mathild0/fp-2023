{-# OPTIONS_GHC -Wno-deferred-out-of-scope-variables #-}
{-# LANGUAGE BlockArguments #-}
import Data.Either
import Data.Maybe ()
import InMemoryTables qualified as D
import Lib1
import Test.Hspec
import Lib2
import DataFrame

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
      Lib2.parseStatement ",id ,,name,"`shouldSatisfy` isLeft

  describe "Lib2.calculateMinimum" $ do
    it "calculates the minimum with a list of IntegerValues" $ do
      let values = [IntegerValue 3, IntegerValue 1, IntegerValue 2]
      Lib2.calculateMinimum values `shouldBe` IntegerValue 1
    it "returns NullValue for a list with mixed types" $ do
      let values = [IntegerValue 3, StringValue "hello", BoolValue True]
      Lib2.calculateMinimum values `shouldBe` NullValue  

  describe "Lib2.selectColumns" $ do
    it "selects specified columns from a table" $ do
      Lib2.selectColumns ["id", "name"]  (snd D.tableEmployees) `shouldSatisfy` isRight
    it "handles an empty list of columns" $ do
      Lib2.selectColumns [] (snd D.tableEmployees) `shouldSatisfy` isLeft
    it "handles a list with no valid columns" $ do
      Lib2.selectColumns ["invalidColumnName, invalidColumnName2"] (snd D.tableEmployees) `shouldSatisfy` isLeft

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