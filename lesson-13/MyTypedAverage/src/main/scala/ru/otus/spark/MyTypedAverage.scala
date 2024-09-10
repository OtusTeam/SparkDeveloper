package ru.otus.spark

import org.apache.spark.sql._
import org.apache.spark.sql.Encoders._

object MyTypedAverage {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("MyTypedAverage")
      .getOrCreate()

    import spark.implicits._

    val ds: Dataset[Employee] = Seq(
      Employee("Michael", 1, 3000L),
      Employee("Michael", 2, 6000L),
      Employee("Andy", 1, 4500L),
      Employee("Andy", 2, 2500L),
      Employee("Justin", 3, 3500L),
      Employee("Berta", 3, 4000L)
    ).toDS()

    println("Dataset:")
    ds.show()

    val averageSalary: TypedColumn[Employee, Double] = MyAverage.toColumn.name("average_salary")

    val result0 = ds.select(averageSalary)

    println("Result 0:")
    result0.show()

    val result1 = ds
      .groupByKey(_.depId)
      .agg(averageSalary)

    println("Result 1:")
    result1.show()

    val result11 = ds
      .groupByKey(_.depId)
      .agg(averageSalary)
      .withColumnRenamed("key", "depId")

    println("Result 11:")
    result11.show()

    val result2 = ds
      .groupBy("depId")
      .as[Int, Employee](scalaInt, product)
      .agg(averageSalary)
      .withColumnRenamed("key", "depId")

    println("Result 2:")
    result2.show()

    val result3 = ds
      .groupByKey(_.depId)
      .mapValues(_.salary.toDouble)
      .mapGroups((id, it) => {
        val lst = it.toList
        (id, lst.sum / lst.length)
      })
      .withColumnsRenamed(Map("_1" -> "depId", "_2" -> "average_salary"))

    println("Result 3:")
    result3.show()

    spark.stop()
  }
}
