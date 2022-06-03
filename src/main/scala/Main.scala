import java.io.File
import org.apache.commons.io.FileUtils
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.catalyst.expressions.MonotonicallyIncreasingID
import org.apache.spark.sql.functions.{avg, col, count, max, min, monotonically_increasing_id, when}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.api


//from spark.sql.window import Window as W
  // from pyspark.sql import functions as F
//package com.github.mrpowers.spark.pika



//trait SparkSessionWrapper {
//
//  lazy val spark: SparkSession = {
//    SparkSession
//       .builder()
//       .master("local")
//       .appName("spark pika")
//       .getOrCreate()
//  }
//
//}






object Main {

  def getGenderCount(df: DataFrame): DataFrame = {
    val df1 = df.groupBy("gender").count()
    df1
  }


  def updateRow(df: DataFrame, id: Int): DataFrame = {
    val df2 = df.withColumn("lunch", when(col("StudentID") === id, "standard").otherwise(df("lunch")))
    df2

  }


  def studentCount(df: DataFrame): Long = {
    df.count()
  }

  def updateRows(df: DataFrame, lunchType: String, edu: String): DataFrame = {
    val df2 = df.withColumn("race/ethnicity", when(col("lunch") === lunchType && col("parental level of education") === edu, "Unknown").otherwise(df("race/ethnicity")))
    df2
  }

  def getFemaleData(df: DataFrame): DataFrame = {
    df.filter(df("gender") === "female")
  }

  def getFemaleStudentsCount(df: DataFrame): Long = {
    df.filter(df("gender") === "female").count()
  }

  def getMaleStudentsCount(df: DataFrame): Long = {
    df.filter(df("gender") === "male").count()
  }

  def getMaleData(df: DataFrame): DataFrame = {
    df.filter(df("gender") === "male")
  }

  def getGenderMin(df: DataFrame): DataFrame = {
    df.groupBy("gender").agg(min("math score").as("maths"), min("reading score").as("reading"), min("writing score").as("writing"))
  }

  def getGenderMax(df: DataFrame): DataFrame = {
    df.groupBy("gender").agg(max("math score").as("maths"), max("reading score").as("reading"), max("writing score").as("writing"))
  }

  def getGenderAvg(df: DataFrame): DataFrame = {
    df.groupBy("gender").agg(avg("math score").as("maths"), avg("reading score").as("reading"), avg("writing score").as("writing"))
  }

  //  def Scholarship(df: DataFrame): Boolean = {
  //    val p = df.agg(max("Percentage")).collect()[0]
  //    true
  //  }






  def main(args: Array[String]): Unit = {
    println("Started")

    val spark = SparkSession
       .builder()
       .appName("Spark SQL basic example")
       //.config("spark.some.config.option", "some-value")
       .master("local[*]")
       .getOrCreate()

    val monotonicallyIncreasingID = MonotonicallyIncreasingID()

    spark.sparkContext.setLogLevel("ERROR")



    //  LOADING THE FILE AND CHECKING SCHEMA

    var df = spark.read.format("csv").option("header","true").load("/home/shuvam/Downloads/StudentsPerformance.csv")
    println(df.show())
    println("Schema : ")
    println(df.printSchema())

    //  CHANGING SCHEMA

    println("Maths, Reading and Writing scores are stored as strings, so we cast them to Int")
    df = df.withColumn("math score",df("math score").cast(IntegerType))
    df = df.withColumn("reading score",df("reading score").cast(IntegerType))
    df = df.withColumn("writing score",df("writing score").cast(IntegerType))
    print("Updated Schema : ")
    println(df.printSchema())

    // ADDING A STUDENT ID COLUMN

    df = df.withColumn("StudentID",monotonically_increasing_id())
    println(df.show())

    // CHANGE COLUMN VALUES FOR A SPECIFIC ROW
    // WE PASS THE DATAFRAME AND THE STUDENT ID FOR WHICH LUNCH HAS TO BE CHANGED TO "STANDARD"

    df = updateRow(df,17)
    println("After changing lunch type of StudentID=17")
    println(df.show())


    //CHANGE BASED ON MULTIPLE COLUMNS
    println("Changing Race to 'Unknown' for all whose lunch and education level matches with the input")
    df=updateRows(df,"standard","master's degree")
    println(df.show())


    // FILTER MALE AND FEMALE

    println("Filtering male and female data")
    println(getFemaleData(df).show())
    println(getMaleData(df).show())


    // GET COUNT OF INDIVIDUAL GENDERS
    println("Count of male and female")
    println(getGenderCount(df).show())


    // MIN, MAX, AVG FOR EVERY SUBJECT

    println("Min for individual gender")
     println(getGenderMin(df).show())
    println("Max for individual gender")
    println(getGenderMax(df).show())
    println("Avg for individual gender")
    println(getGenderAvg(df).show())















  }
}