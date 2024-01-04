package ru.otus.spark

import ru.otus.spark.Student.getStudentsSample

import java.io.{File, PrintWriter}
import scala.util.Random

object RDDOperations extends SparkSessionWrapper {


  def basicOperations(): Unit = {

    val studentsRDD = spark.sparkContext.parallelize(getStudentsSample)

    //Filter
    println("Filter John count: ", studentsRDD.filter(_.name == "John").count)
    println("Filter Java course count: ", studentsRDD.filter(student => student.courses.contains(Course("Java"))).count)

    //Map
    studentsRDD.map(_.copy(name = "ModifiedName")).collect.foreach(println)

    //Sort
    studentsRDD.sortBy(_.surname).collect.foreach(println)

    //ForEachPartition
    studentsRDD.foreachPartition(part => {
      val randomFileName = new Random().nextInt()
      val pw = new PrintWriter(new File(s"random-file-${randomFileName}.txt"))
      while (part.hasNext) {
        pw.write(part.next().toString)
      }
      pw.close()
    })

  }

}
