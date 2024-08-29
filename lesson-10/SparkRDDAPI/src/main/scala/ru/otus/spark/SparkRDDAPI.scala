package ru.otus.spark

import org.apache.spark.{SparkConf, SparkContext}
import io.circe.parser._
import java.io.{File, PrintWriter}
import scala.util.Random

object SparkRDDAPI {
  def main(args: Array[String]): Unit = {

    // Создаём конекст исполнения
    val conf = new SparkConf().setAppName("SparkRDDAPI").setMaster("local[*]")
    val sc   = new SparkContext(conf)

    //1 - Создание RDD
    val data          = Seq("Spark", "Scala", "Java")
    val collectionRDD = sc.parallelize(data, 2)
    val studentsRDD   = sc.parallelize(Student.getStudentsSample)
    val fileRDD       = sc.textFile(args(0))

    //2 - Операции
    collectionRDD.foreach(println)

    // Filter
    println("Filter John count: ", studentsRDD.filter(_.name == "John").count)
    println(
      "Filter Java course count: ",
      studentsRDD.filter(student => student.courses.contains(Course("Scala"))).count
    )

    // Map
    studentsRDD.map(_.copy(name = "ModifiedName")).collect.foreach(println)
    fileRDD.map(parse).take(5).foreach(println)

    // Sort
    studentsRDD.sortBy(_.surname).collect.foreach(println)

    // ForEachPartition
    studentsRDD.foreachPartition(part => {
      val randomFileName = new Random().nextInt()
      val pw             = new PrintWriter(new File(s"random-file-$randomFileName.txt"))
      while (part.hasNext) {
        pw.write(part.next().toString)
      }
      pw.close()
    })

    //3 - Самописное партицирование
    val keyedRDD       = studentsRDD.keyBy(_.id)
    val partitionedRDD = keyedRDD.partitionBy(new CustomPartitioner)
    println(partitionedRDD.map(_._1).glom().collect().map(arr => arr.mkString("|")).mkString(", "))
  }
}
