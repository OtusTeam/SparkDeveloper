package ru.otus.spark

object App extends SparkSessionWrapper {

  def main(args : Array[String]) = {

    //1 - Создание DataFrame
    RDDCreation.fromRange().collect.foreach(println)
    RDDCreation.fromCollection().collect.foreach(println)
    RDDCreation.fromDataFrame().take(20).foreach(println)

    //2 - Операции
    RDDOperations.basicOperations()

    //3 - Самописное партицирование
    RDDPartitioning.applyCustomPartitioner()


  }

}
