ThisBuild / organization := "ru.otus.spark"
ThisBuild / version := "1.0"
ThisBuild / scalaVersion := "2.12.18"

val sparkVersion = "3.5.0"

ThisBuild / libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
)

lazy val root = (project in file("."))
  .settings(name := "SparkDatasetAPI")
  .settings(assembly / mainClass := Some("ru.otus.spark.SparkDatasetAPI"))
  .settings(assembly / assemblyJarName := "SparkDatasetAPI.jar")
