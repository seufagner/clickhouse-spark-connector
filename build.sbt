
name := """clickhouse-spark-connector"""
version := "1.2"
scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.2.1" % "provided",
  "ru.yandex.clickhouse" % "clickhouse-jdbc" % "0.1.48" % "provided",
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