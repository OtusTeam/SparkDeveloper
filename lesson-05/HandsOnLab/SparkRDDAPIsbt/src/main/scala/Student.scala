package ru.otus.spark

case class Student(id : Int, name : String, surname : String, courses : Seq[Course])

case class Course(title: String)

object Student {

  def getStudentsSample : Seq[Student] = {
    Seq(
      Student(1, "John", "Doe", Seq(Course.sparkCourse, Course.scalaCourse)),
      Student(2, "Ivan", "Ivanov", Seq(Course.javaCourse)),
      Student(3, "Jack", "Toe", Seq(Course.javaCourse, Course.sparkCourse, Course.scalaCourse))
    )
  }
}

object Course {

  def sparkCourse: Course = Course("Spark")
  def scalaCourse: Course = Course("Scala")
  def javaCourse: Course = Course("Java")

}
