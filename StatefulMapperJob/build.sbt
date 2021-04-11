
ThisBuild / resolvers ++= Seq(
  // "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
  Resolver.mavenLocal
)

name := "StatefulMapperJob"

version := "0.1-SNAPSHOT"

organization := "org.tudelft.delta"

ThisBuild / scalaVersion := "2.11.12"

val flinkVersion = "1.7-CLONOS-SNAPSHOT"

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-connector-kafka-0.11" % flinkVersion,
  "org.apache.flink" %% "flink-statebackend-rocksdb" % flinkVersion,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.11.3"
)

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies
  )

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assembly / mainClass := Some("org.tudelft.delta.BenchmarkMapper")

// make run command include the provided dependencies
Compile / run := Defaults.runTask(Compile / fullClasspath,
  Compile / run / mainClass,
  Compile / run / runner
).evaluated

// stays inside the sbt console when we press "ctrl-c" while a Flink programme executes with "run" or "runMain"
Compile / run / fork := true
Global / cancelable := true

// exclude Scala library from assembly
assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = true)

updateOptions := updateOptions.value.withLatestSnapshots(false)
