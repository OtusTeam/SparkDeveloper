package ru.otus.spark

object App extends SparkSessionWrapper {

  def main(args : Array[String]) = {

    //1 - Создание DataFrame
    DataFrameCreation.fromRDD().show
    DataFrameCreation.fromList().show
    DataFrameCreation.createDataFrame().show
    DataFrameCreation.withSchema().show
    DataFrameCreation.fromFile()

    //2 - Операции
    DataFrameOperations.basicOperations()
    DataFrameOperations.functions()
    DataFrameOperations.groupBy()
    DataFrameOperations.join()


  }

}
