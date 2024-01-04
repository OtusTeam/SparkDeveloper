package ru.otus.spark

object RDDPartitioning extends SparkSessionWrapper {

  def applyCustomPartitioner(): Unit = {

    val rdd = spark.sparkContext.parallelize(Student.getStudentsSample)

    val keyedRDD = rdd.keyBy(_.id)

    val partitionedRDD = keyedRDD.partitionBy(new CustomPartitioner)

    println(partitionedRDD.map(_._1).glom().collect().map(arr => arr.mkString("|")).mkString(", "))

  }

}
