ThisBuild / version := "1.0"

ThisBuild / scalaVersion := "2.12.18"

lazy val sparkVersion = "3.5.5"

lazy val root = (project in file("."))
  .settings(
    name := "HelloSpark"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
)
