import org.apache.spark
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType

class TestCases extends  AnyFunSuite {

   val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      //.config("spark.some.config.option", "some-value")
      .master("local[*]")
      .getOrCreate()
   var df = spark.read.format("csv").option("header","true").load("/home/shuvam/Downloads/StudentsPerformance.csv")

   df = df.withColumn("Total Marks",df("math score")+df("reading score")+df("writing score"))

   df = df.withColumn("Percentage",df("Total Marks")/3)

   df = df.withColumn("rounded_score",functions.round(col("Percentage"),2))

   df = df.drop("Percentage")

   df=df.withColumnRenamed("rounded_score","Percentage")
   test("Count of total students"){

      assert(Main.studentCount(df)===1000)
      println("Test Case 1 Passed Successfully!")

   }

   test("Count of female students"){
      assert(Main.getFemaleStudentsCount(df)==518)
      println("Test Case 2 passed successfully!")
   }

   test("Count of male students"){
      assert(Main.getMaleStudentsCount(df)==482)
      println("Test Case 3 passed successfully!")
   }







}
