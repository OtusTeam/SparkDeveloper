package ru.otus.sparkdeveloper.sparktest

import org.apache.logging.log4j.scala.Logging

object App extends Logging with SparkSessionWrapper {

  def main(args: Array[String]): Unit = {

    val df = spark.read.format("csv")
      .load("pathTofile")

    val df2 = spark.read.format("csv")
      .load("pathTofile")

  }

}
