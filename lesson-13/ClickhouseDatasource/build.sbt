name := "spark_custom_connector"

version := "0.1"

scalaVersion := "2.12.13"

val testcontainersScalaVersion = "0.38.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.3.2" % "provided",
  "com.github.housepower" % "clickhouse-native-jdbc" % "2.7.0"
)
