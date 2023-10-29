{-# OPTIONS_GHC -Wno-deferred-out-of-scope-variables #-}
import Data.Either
import Data.Maybe ()
import InMemoryTables qualified as D
import Lib1
import Test.Hspec
import Lib2
import DataFrame (Value(IntegerValue))

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
    it "parses SHOW TABLES statement" $ do
      Lib2.parseStatement "show tables" `shouldBe` Right Lib2.ShowTables
    it "parses SHOW TABLE name statement" $ do
      Lib2.parseStatement "show table employees" `shouldBe` Right (Lib2.ShowTable "employees")

  describe "Lib2.calculateMinimum" $ do
    it "calculates the minimum with a list of IntegerValues" $ do
      let values = [IntegerValue 3, IntegerValue 1, IntegerValue 2]
      Lib2.calculateMinimum values `shouldBe` IntegerValue 1
    it "returns NullValue for a list with mixed types" $ do
      let values = [IntegerValue 3, StringValue "hello", BoolValue True]
      Lib2.calculateMinimum values `shouldBe` NullValue

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