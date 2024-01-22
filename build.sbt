import Dependencies._
import uk.gov.nationalarchives.sbt.Log4j2MergePlugin.log4j2MergeStrategy

ThisBuild / organization := "uk.gov.nationalarchives"
ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file(".")).settings(
  name := "dr2-ingest-parsed-court-document-event-handler",
  resolvers += "s01-oss-sonatype-org-snapshots" at "https://s01.oss.sonatype.org/content/repositories/snapshots",
  libraryDependencies ++= Seq(
    commonsCompress,
    fs2IO,
    circeCore,
    circeParser,
    circeGeneric,
    circeGenericExtras,
    log4jSlf4j,
    log4jCore,
    log4jTemplateJson,
    log4CatsCore,
    log4CatsSlf4j,
    lambdaCore,
    lambdaJavaEvents,
    pureConfig,
    pureConfigCats,
    s3Client,
    sfnClient,
    mockito % Test,
    reactorTest % Test,
    scalaTest % Test,
    wiremock % Test
  )
)
(assembly / assemblyJarName) := "dr2-ingest-parsed-court-document-event-handler.jar"

scalacOptions ++= Seq("-Wunused:imports", "-Werror", "-deprecation")

(Test / fork) := true
(Test / envVars) := Map("AWS_ACCESS_KEY_ID" -> "accesskey", "AWS_SECRET_ACCESS_KEY" -> "secret")

(assembly / assemblyMergeStrategy) := {
  case PathList(ps @ _*) if ps.last == "Log4j2Plugins.dat" => log4j2MergeStrategy
  case _                                                   => MergeStrategy.first
}
