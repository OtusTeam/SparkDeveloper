version := "0.1.0-SNAPSHOT"

scalaVersion := "2.12.2"

lazy val root = (project in file("."))
  .settings(
    name := "SparkRDDAPIsbt",
    idePackagePrefix := Some("ru.otus.spark")
  )

val sparkVersion = "3.3.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)
