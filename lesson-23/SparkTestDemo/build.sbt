ThisBuild / version := "1.0"

ThisBuild / scalaVersion := "2.12.8"

lazy val root = (project in file("."))
  .settings(
    name := "SparkTestDemo"
  )

val sparkVersion = "3.5.0"
lazy val log4jVersion = "2.22.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.16.1",
  "org.scalatest" %% "scalatest" % "3.2.17" % "test",
  "com.github.mrpowers" %% "spark-fast-tests" % "1.3.0" % "test",
  "org.apache.logging.log4j" %% "log4j-api-scala" % "13.0.0",
  "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion
)
