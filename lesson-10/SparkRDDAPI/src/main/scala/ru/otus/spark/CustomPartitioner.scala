package ru.otus.spark

import org.apache.spark.Partitioner

import java.util.Random

class CustomPartitioner extends Partitioner {
  def numPartitions: Int = 3

  def getPartition(key: Any): Int = {
    val studentID = key.asInstanceOf[Int]
    if (studentID == 1) {
      0
    } else {
      new Random().nextInt(2) + 1
    }
  }
}
