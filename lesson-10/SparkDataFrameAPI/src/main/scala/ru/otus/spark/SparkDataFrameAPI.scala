package ru.otus.spark

object SparkDataFrameAPI extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {
    //1 - Создание DataFrame
    println("Create DataFrame from List:")
    DataFrameCreation.fromList().show(10)
    println("\nCreate DataFrame from RDD:")
    DataFrameCreation.fromRDD().show(10)
    println("\ncreateDataFrame")
    DataFrameCreation.createDataFrame().show(10)
    println("\nCreate DataFrame withSchema:")
    DataFrameCreation.withSchema().show(10)
    println("\nCreate DataFrame fromFile:")
    DataFrameCreation.fromFile().show(10)

    //2 - Операции
    println("\nbasicOperations:")
    DataFrameOperations.basicOperations()
    println("\nfunctions:")
    DataFrameOperations.functions()
    println("\ngroupBy:")
    DataFrameOperations.groupBy()
    println("\njoin:")
    DataFrameOperations.join()
  }
}
