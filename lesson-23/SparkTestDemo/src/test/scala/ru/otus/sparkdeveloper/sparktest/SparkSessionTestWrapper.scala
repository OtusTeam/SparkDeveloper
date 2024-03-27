package ru.otus.sparkdeveloper.sparktest

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  val spark = SparkSession.builder()
    .appName("SparkTestApp")
    .master("local")
    .getOrCreate

}
