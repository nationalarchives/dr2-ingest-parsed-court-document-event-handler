import sbt._
object Dependencies {
  lazy val logbackVersion = "2.20.0"
  lazy val pureConfigVersion = "0.17.4"
  lazy val daAwsClientsVersion = "0.1.23"
  lazy val pureConfigCats = "com.github.pureconfig" %% "pureconfig-cats-effect" % pureConfigVersion
  lazy val pureConfig = "com.github.pureconfig" %% "pureconfig" % pureConfigVersion
  lazy val log4jSlf4j = "org.apache.logging.log4j" % "log4j-slf4j-impl" % logbackVersion
  lazy val log4jCore = "org.apache.logging.log4j" % "log4j-core" % logbackVersion
  lazy val log4jTemplateJson = "org.apache.logging.log4j" % "log4j-layout-template-json" % logbackVersion
  lazy val lambdaCore = "com.amazonaws" % "aws-lambda-java-core" % "1.2.3"
  lazy val lambdaJavaEvents = "com.amazonaws" % "aws-lambda-java-events" % "3.11.3"
  lazy val reactiveStreams = "co.fs2" %% "fs2-reactive-streams" % "3.9.2"
  lazy val s3Client = "uk.gov.nationalarchives" %% "da-s3-client" % daAwsClientsVersion
  lazy val sfnClient = "uk.gov.nationalarchives" %% "da-sfn-client" % daAwsClientsVersion
  lazy val commonsCompress = "org.apache.commons" % "commons-compress" % "1.24.0"
  lazy val fs2Csv = "org.gnieh" %% "fs2-data-csv" % "1.8.1"
  lazy val fs2IO = "co.fs2" %% "fs2-io" % "3.9.2"
  lazy val upickle = "com.lihaoyi" %% "upickle" % "3.1.3"
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.2.17"
  lazy val mockito = "org.mockito" %% "mockito-scala" % "1.17.22"
  lazy val wiremock = "com.github.tomakehurst" % "wiremock" % "3.0.1"
  lazy val reactorTest = "io.projectreactor" % "reactor-test" % "3.5.10"
}
