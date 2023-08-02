import Dependencies._
import uk.gov.nationalarchives.sbt.Log4j2MergePlugin.log4j2MergeStrategy

ThisBuild / organization := "uk.gov.nationalarchives"
ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file(".")).
  settings(
    name := "dr2-ingest-parsed-court-document-event-handler",
    libraryDependencies ++= Seq(
      commonsCompress,
      fs2Csv,
      fs2IO,
      log4jSlf4j,
      log4jCore,
      log4jTemplateJson,
      lambdaCore,
      lambdaJavaEvents,
      pureConfig,
      pureConfigCats,
      reactiveStreams,
      s3Client,
      sfnClient,
      upickle,
      mockito % Test,
      reactorTest % Test,
      scalaTest % Test,
      wiremock % Test
    )
  )
(assembly / assemblyJarName) := "dr2-ingest-parsed-court-document-event-handler.jar"

scalacOptions ++= Seq("-Wunused:imports", "-Werror")

(Test / fork) := true
(Test / envVars) := Map("AWS_ACCESS_KEY_ID" -> "accesskey", "AWS_SECRET_ACCESS_KEY" -> "secret")

(assembly / assemblyMergeStrategy) := {
  case PathList(ps@_*) if ps.last == "Log4j2Plugins.dat" => log4j2MergeStrategy
  case _ => MergeStrategy.first
}

