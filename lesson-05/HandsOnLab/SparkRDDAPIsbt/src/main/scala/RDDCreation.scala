package ru.otus.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object RDDCreation extends SparkSessionWrapper {

  val columns = Seq("StudentID", "Course")
  val data = Seq("Spark", "Scala", "Java")

  def fromRange(): RDD[java.lang.Long] = spark.range(500).rdd

  def fromCollection(): RDD[String] = spark.sparkContext.parallelize(data, 2)

  def fromDataFrame(): RDD[Row] = spark.read.format("json")
    .option("mode", "FAILFAST")
    .load("src/main/resources/customer_data.json").rdd

}
