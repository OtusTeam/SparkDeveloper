package ru.otus.spark

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object RDDAPIExercise extends SparkSessionWrapper {

  /**
   * Задание: чтение и обработка текстовых данных с помощью RDD API.
   *
   * 1. В папке src/main/resources/textFiles лежат тексты.Загрузите их в RDD.
   *    Воспользуйтесь методом spark.sparkContext.wholeTextFiles(), который возвращает RDD c двумя полями:
   *    key - название файла, value - содержимое файла в формате строки.
   * 2. Пользуясь анонимными функциями Scala, посчитайте различные статистики над файлами.
   *    Например, общий размер всех файлов: textFiles.map { case (_, content) => content.length }.sum()
   *    Создайте следующие статистики:
   *      - Название самого большого файла (по количеству символов)
   *      - Название самого маленького файла (по количеству символов)
   *      - Название файла с наибольшим количеством слов (разбивайте слова по пробелу)
   *      - Среднее количество слов в файле.
   */

  import spark.implicits._

  def ex1(): Unit = {

    //1 прочитайте файлы в RDD с помощью wholeTextFiles()
    val textFiles = spark.sparkContext.wholeTextFiles("src/main/resources/textFiles/")

    //2 Пример подсчета статистики
    val totalFileSize = textFiles.map(pair => pair._2.length).sum()

    //НАПИШИТЕ ВАШЕ РЕШЕНИЕ ЗДЕСЬ


  }


}
