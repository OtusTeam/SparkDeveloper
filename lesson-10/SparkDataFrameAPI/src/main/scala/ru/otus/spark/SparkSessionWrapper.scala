package ru.otus.spark

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {
  lazy val spark: SparkSession = SparkSession
    .builder()
    .appName("SparkDataFrameAPI")
    .master("local[*]")
    .getOrCreate
}
