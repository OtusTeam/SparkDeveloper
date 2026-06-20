package ru.otus.spark

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

object DataFrameCreation extends SparkSessionWrapper {

  import spark.implicits._

  private val columns: Seq[String]        = Seq("StudentID", "Course")
  private val data: Seq[(String, String)] = Seq(("1", "Spark"), ("2", "Scala"), ("3", "Java"))

  def fromList(): DataFrame        = data.toDF()
  def fromRDD(): DataFrame         = spark.sparkContext.parallelize(data).toDF(columns: _*)
  def createDataFrame(): DataFrame = spark.createDataFrame(data)

  private val schema = StructType(
    Array(
      StructField("StudentID", IntegerType, nullable = true),
      StructField("Course", StringType, nullable = true)
    )
  )

  def withSchema(): DataFrame = {
    val rdd = spark.sparkContext.parallelize(Seq(Row(1, "Spark"), Row(2, "Scala")))
    spark.createDataFrame(rdd, schema)
  }

  def fromFile(): DataFrame = spark.read
    .format("json")
    .option("mode", "FAILFAST")
    .load("src/main/resources/customer_data.json")
}
