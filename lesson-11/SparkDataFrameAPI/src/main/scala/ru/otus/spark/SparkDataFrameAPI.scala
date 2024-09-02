package ru.otus.spark

object SparkDataFrameAPI extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {
    //1 - Создание DataFrame
    DataFrameCreation.fromList().show(10)
    DataFrameCreation.fromRDD().show(10)
    DataFrameCreation.createDataFrame().show(10)
    DataFrameCreation.withSchema().show(10)
    DataFrameCreation.fromFile()

    //2 - Операции
    DataFrameOperations.basicOperations()
    DataFrameOperations.functions()
    DataFrameOperations.groupBy()
    DataFrameOperations.join()
  }
}
