package ru.otus.spark

case class Course(title: String)

object Course {
  def sparkCourse: Course = Course("Spark")
  def scalaCourse: Course = Course("Scala")
  def javaCourse: Course = Course("Java")
}
