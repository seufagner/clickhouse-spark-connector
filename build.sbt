
name := """clickhouse-spark-connector"""

version := "1.2"

scalaVersion := "2.11.8"

publishTo := Some("jFrog" at "http://10.2.95.5:8080/artifactory/libs-release")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.1.1" % "provided",
  "ru.yandex.clickhouse" % "clickhouse-jdbc" % "0.1.25" % "provided",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.5"
)

fork in run := true

test in assembly := {}

assemblyMergeStrategy in assembly := {
  case n if n.startsWith("META-INF/MANIFEST.MF") => MergeStrategy.discard
  case "reference.conf"                          => MergeStrategy.concat
  case x => MergeStrategy.first
}

packAutoSettings