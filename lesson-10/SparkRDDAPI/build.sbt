ThisBuild / organization := "ru.otus.spark"
ThisBuild / version := "1.0"
ThisBuild / scalaVersion := "2.12.18"

val sparkVersion = "3.5.0"
val circeVersion = "0.14.9"

ThisBuild / libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"    % sparkVersion,
  "io.circe"         %% "circe-core"    % circeVersion,
  "io.circe"         %% "circe-generic" % circeVersion,
  "io.circe"         %% "circe-parser"  % circeVersion
)

lazy val root = (project in file("."))
  .settings(name := "SparkRDDAPI")
  .settings(assembly / mainClass := Some("ru.otus.spark.SparkRDDAPI"))
  .settings(assembly / assemblyJarName := "SparkRDDAPI.jar")
  .settings(assembly / assemblyMergeStrategy := {
    case m if m.toLowerCase.endsWith("manifest.mf")       => MergeStrategy.discard
    case m if m.toLowerCase.matches("meta-inf.*\\.sf$")   => MergeStrategy.discard
    case "module-info.class"                              => MergeStrategy.first
    case "version.conf"                                   => MergeStrategy.discard
    case "reference.conf"                                 => MergeStrategy.concat
    case x: String if x.contains("UnusedStubClass.class") => MergeStrategy.first
    case _                                                => MergeStrategy.first
  })
