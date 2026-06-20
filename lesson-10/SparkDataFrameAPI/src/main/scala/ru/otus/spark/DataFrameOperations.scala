package ru.otus.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import ru.otus.spark.DataFrameCreation.fromFile

object DataFrameOperations extends SparkSessionWrapper {

  import spark.implicits._
  private val customerDf: DataFrame = fromFile()

  def basicOperations(): Unit = {
    println("\ncustomerDf.printSchema:")
    customerDf.printSchema()

    //Select
    println("\ncustomerDf.select:")
    customerDf.select("Birthdate", "Country").show(10)
    customerDf.select(col("Country")).show(10)
    customerDf.select('Country).show(10)
    customerDf.selectExpr("Birthdate as Date").show(10)
    customerDf.withColumn("Flag", lit(true)).show(10)
    customerDf.withColumnRenamed("Birthdate", "Date").show(10)

    //Filter
    println("\ncustomerDf.filter:")
    customerDf.filter("Country = 'Norway'").show(10)
    customerDf.where('Country === "Iceland").show(10)

    //Sort
    println("\ncustomerDf.sort:")
    customerDf.sort('CustomerID.desc).show(10)
    customerDf.orderBy("CustomerID").show(10)

    //Repartition
    println("\ncustomerDf.repartition:")
    println(s"Num partitions: ${customerDf.rdd.getNumPartitions}")
    val repartitionedDf = customerDf.repartition(5, col("Country"))
    println(s"New num partitions: ${repartitionedDf.rdd.getNumPartitions}")
    println(s"Num partitions after coalesce: ${repartitionedDf.coalesce(1).rdd.getNumPartitions}")
  }

  def functions(): Unit = {
    customerDf.select(date_format(col("Birthdate"), "yyy-MM-dd")).show(10)
    customerDf.withColumn("Identity", array('Name, 'Username)).printSchema
  }

  def groupBy(): Unit = {
    customerDf
      .groupBy("Country")
      .agg(count(lit(1)))
      .show(10)
  }

  def join(): Unit = {
    customerDf.printSchema()

    val retailDf = spark.read
      .format("json")
      .option("mode", "FAILFAST")
      .load("src/main/resources/retail_data.json")

    retailDf.printSchema

    customerDf.join(retailDf, "CustomerID").show(10)
  }
}
