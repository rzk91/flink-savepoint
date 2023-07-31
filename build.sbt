ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(name := "flink-checkpoints-test", libraryDependencies ++= dependencies)

val flinkVersion = "1.17.1"
val circeVersion = "0.14.5"

val dependencies = Seq(
  "org.apache.flink"           % "flink-streaming-java"      % flinkVersion % "provided",
  "org.apache.flink"           % "flink-connector-kafka"     % flinkVersion,
  "org.apache.flink"           % "flink-runtime-web"         % flinkVersion % "provided",
  "org.apache.flink"           % "flink-state-processor-api" % flinkVersion % "provided",
  "io.findify"                 %% "flink-adt"                % "0.6.1",
  "io.findify"                 %% "flink-scala-api"          % "1.15-2",
  "org.slf4j"                  % "slf4j-log4j12"             % "2.0.5",
  "com.typesafe.scala-logging" %% "scala-logging"            % "3.9.5",
  "commons-io"                 % "commons-io"                % "2.11.0",
  "org.xerial.snappy"          % "snappy-java"               % "1.1.9.1",
  "io.circe"                   %% "circe-core"               % circeVersion,
  "io.circe"                   %% "circe-generic"            % circeVersion,
  "io.circe"                   %% "circe-parser"             % circeVersion
)

scalacOptions ++= Seq(
  "-encoding",
  "utf8",
  "-deprecation",
  "-unchecked",
  "-explaintypes",
  "-Ywarn-unused:imports",
  "-Xfatal-warnings",
  "-language:implicitConversions",
  "-language:higherKinds",
  "-language:existentials",
  "-language:postfixOps"
)
