ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(name := "flink-checkpoints-test", libraryDependencies ++= dependencies)

val flinkVersion = "1.12.1"

val dependencies = Seq(
  "org.apache.flink"           %% "flink-scala"               % flinkVersion % "provided",
  "org.apache.flink"           %% "flink-streaming-scala"     % flinkVersion % "provided",
  "org.apache.flink"           %% "flink-connector-kafka"     % flinkVersion,
  "org.apache.flink"           %% "flink-runtime-web"         % flinkVersion % "provided",
  "org.apache.flink"           %% "flink-state-processor-api" % flinkVersion % "provided",
  "org.slf4j"                  % "slf4j-log4j12"              % "1.7.25",
  "com.typesafe.scala-logging" %% "scala-logging"             % "3.9.2",
  "commons-io"                 % "commons-io"                 % "2.11.0",
  "org.xerial.snappy"          % "snappy-java"                % "1.1.9.0",
  "io.circe"                   %% "circe-core"                % "0.14.1",
  "io.circe"                   %% "circe-generic"             % "0.14.1",
  "io.circe"                   %% "circe-parser"              % "0.14.1"
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