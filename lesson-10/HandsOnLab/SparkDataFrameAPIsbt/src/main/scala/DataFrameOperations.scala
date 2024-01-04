package ru.otus.spark

import DataFrameCreation.fromFile

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object DataFrameOperations extends SparkSessionWrapper {

  import spark.implicits._
  val customerDf: DataFrame = fromFile()

  def basicOperations(): Unit = {

    customerDf.printSchema()
    customerDf.cache
    customerDf.head

    //Select
    customerDf.select("Birthdate", "Country").show
    customerDf.select(col("Country")).show
    customerDf.select('Country).show
    customerDf.selectExpr("Birthdate as Date").show
    customerDf.withColumn("Flag", lit(true)).show
    customerDf.withColumnRenamed("Birthdate", "Date").show

    //Filter
    customerDf.filter("Country = 'Norway'").show
    customerDf.where('Country === "Iceland").show

    //Sort
    customerDf.sort('CustomerID.desc).show
    customerDf.orderBy("CustomerID").show

    //Repartition
    println("Num partitions: ", customerDf.rdd.getNumPartitions)
    val repartitionedDf = customerDf.repartition(5, col("Country"))
    println("New num partitions: ", repartitionedDf.rdd.getNumPartitions)
    println("Num partitions after coalesce: ", repartitionedDf.coalesce(1).rdd.getNumPartitions)

    customerDf.unpersist()

  }

  def functions() : Unit = {

    customerDf.printSchema()
    customerDf.cache
    customerDf.head

    customerDf.select(date_format(col("Birthdate"), "yyy-MM-dd")).show

    customerDf.withColumn("Identity", array('Name, 'Username)).printSchema

    customerDf.unpersist()

  }

  def groupBy(): Unit = {

    customerDf.groupBy("Country")
      .agg(count(lit(1)))
      .show

  }

  def join(): Unit = {

    customerDf.printSchema()
    customerDf.cache
    customerDf.head

    val retailDf = spark.read.format("json")
      .option("mode", "FAILFAST")
      .load("src/main/resources/retail_data.json")

    retailDf.printSchema
    retailDf.cache
    retailDf.head

    customerDf.join(retailDf, "CustomerID").show
    customerDf.join(retailDf, customerDf("CustomerID") === retailDf("CustomerID"), "left")
      .select("CustomerID").show

    customerDf.unpersist()
    retailDf.unpersist()

  }

}
