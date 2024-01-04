package ru.otus.spark

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

object DataFrameCreation extends SparkSessionWrapper {

  import spark.implicits._

  val columns = Seq("StudentID", "Course")
  val data = Seq(("1", "Spark"), ("2", "Scala"), ("3", "Java"))

  def fromRDD(): DataFrame = spark.sparkContext.parallelize(data).toDF(columns: _*)

  def fromList(): DataFrame = data.toDF()

  def createDataFrame(): DataFrame = spark.createDataFrame(data)

  def withSchema(): DataFrame = {
    val schema = StructType( Array(
      StructField("StudentID", IntegerType, true),
      StructField("Course", StringType, true)
    ))

    val rdd = (spark.sparkContext.parallelize(Seq(
      Row(1, "Spark"),
      Row(2, "Scala")
    )))

    spark.createDataFrame(rdd, schema)
  }

  def fromFile(): DataFrame = spark.read.format("json")
    .option("mode", "FAILFAST")
    .load("src/main/resources/customer_data.json")
}
