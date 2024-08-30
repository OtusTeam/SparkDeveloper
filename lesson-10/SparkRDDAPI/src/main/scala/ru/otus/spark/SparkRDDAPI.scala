package ru.otus.spark

import org.apache.spark.{SparkConf, SparkContext}
import io.circe.parser._
import org.apache.spark.rdd.RDD
import java.io.{File, PrintWriter}
import scala.util.Random

object SparkRDDAPI {
  def main(args: Array[String]): Unit = {

    // Создаём конекст исполнения
    val conf = new SparkConf().setAppName("SparkRDDAPI").setMaster("local[*]")
    val sc   = new SparkContext(conf)

    //1 - Создание RDD
    val data                       = Seq("Spark", "Scala", "Java")
    val collectionRDD: RDD[String] = sc.parallelize(data, 2)
    val studentsRDD: RDD[Student]  = sc.parallelize(Student.getStudentsSample)
    val fileRDD: RDD[String]       = sc.textFile(args(0))

    //2 - Операции
    println("Операции")
    collectionRDD.foreach(println)

    // Filter
    println("\nFilter")
    println(s"Filter John count: ${studentsRDD.filter(_.name == "John").count}")
    println(
      s"Filter Java course count: ${studentsRDD.filter(student => student.courses.contains(Course("Scala"))).count}"
    )

    // Map
    println("\nMap")
    studentsRDD.map(_.copy(name = "ModifiedName")).collect.foreach(println)
    println("\nParse JSON")
    fileRDD.map(parse).take(5).foreach(println)

    // Sort
    println("\nSort")
    studentsRDD.sortBy(_.surname).collect.foreach(println)

    // ForEachPartition
    println("\nForEachPartition")
    studentsRDD.foreachPartition(part => {
      val randomFileName = new Random().nextInt()
      val pw             = new PrintWriter(new File(s"random-file-$randomFileName.txt"))
      while (part.hasNext) {
        pw.write(part.next().toString)
      }
      pw.close()
    })

    //3 - Самописное партицирование
    println("\nСамописное партицирование")
    val keyedRDD: RDD[(Int, Student)]       = studentsRDD.keyBy(_.id)
    val partitionedRDD: RDD[(Int, Student)] = keyedRDD.partitionBy(new CustomPartitioner)
    println(partitionedRDD.map(_._1).glom().collect().map(arr => arr.mkString("|")).mkString(", "))
  }
}
