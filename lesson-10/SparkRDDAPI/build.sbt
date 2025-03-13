ThisBuild / organization := "ru.otus.spark"
ThisBuild / version := "1.0"
ThisBuild / scalaVersion := "2.12.18"

val sparkVersion = "3.5.5"
val circeVersion = "0.14.9"

ThisBuild / libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"    % sparkVersion % "provided",
  "io.circe"         %% "circe-core"    % circeVersion,
  "io.circe"         %% "circe-generic" % circeVersion,
  "io.circe"         %% "circe-parser"  % circeVersion
)

lazy val root = (project in file("."))
  .settings(name := "SparkRDDAPI")
  .settings(assembly / mainClass := Some("ru.otus.spark.SparkRDDAPI"))
  .settings(assembly / assemblyJarName := "SparkRDDAPI.jar")
