package ru.otus.spark

import org.apache.spark.sql.functions.udaf
import org.apache.spark.sql.SparkSession

object MyUntypedAverage {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("MyUntypedAverage")
      .getOrCreate()

    import spark.implicits._

    val data = Seq(
      ("Michael", 1, 3000),
      ("Michael", 2, 6000),
      ("Andy", 1, 4500),
      ("Andy", 2, 2500),
      ("Justin", 3, 3500),
      ("Berta", 3, 4000)
    )
      .toDF("name", "depId", "salary")
    data.show()

    val myAverageUDAF = udaf(new MyAverage)

    val result1 = data
      .groupBy("depId")
      .agg(myAverageUDAF($"salary").as("average_salary"))
    result1.show()

    val result2 = data
      .groupBy($"name")
      .agg(myAverageUDAF($"salary").as("average_salary"))
    result2.show()

    spark.stop()
  }
}
