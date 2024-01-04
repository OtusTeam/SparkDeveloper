name := "spark_custom_connector"

version := "0.1"

scalaVersion := "2.12.13"
val sparkVersion = "3.4.1"
val testcontainersScalaVersion = "0.41.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.postgresql" % "postgresql" % "42.5.4",
  "org.scalatest" %% "scalatest" % "3.2.15" % "test",
  "com.dimafeng" %% "testcontainers-scala-scalatest" % testcontainersScalaVersion % "test",
  "com.dimafeng" %% "testcontainers-scala-postgresql" % testcontainersScalaVersion % "test"
)

Test / fork := true

assembly / assemblyMergeStrategy := {
  case m if m.toLowerCase.endsWith("manifest.mf")       => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")   => MergeStrategy.discard
  case m if m.toLowerCase.matches("version.conf")       => MergeStrategy.discard
  case "module-info.class"                              => MergeStrategy.first
  case "reference.conf"                                 => MergeStrategy.concat
  case x: String if x.contains("UnusedStubClass.class") => MergeStrategy.first
  case _                                                => MergeStrategy.first
}
