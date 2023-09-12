name := "spark_custom_connector"

version := "0.1"

scalaVersion := "2.12.13"

val testcontainersScalaVersion = "0.38.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.0.1",
  "org.postgresql" % "postgresql" % "42.2.18",

  "org.scalatest" %% "scalatest" % "3.2.2" % "test",
  "com.dimafeng" %% "testcontainers-scala-scalatest" % testcontainersScalaVersion % "test",
  "com.dimafeng" %% "testcontainers-scala-postgresql" % testcontainersScalaVersion % "test",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.0.1",
  "com.github.housepower" % "clickhouse-native-jdbc" % "2.7.0"
)

Test / fork := true
