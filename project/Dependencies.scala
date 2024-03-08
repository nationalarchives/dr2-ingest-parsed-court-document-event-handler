import sbt._
object Dependencies {
  lazy val logbackVersion = "2.23.0"
  lazy val pureConfigVersion = "0.17.6"
  lazy val daAwsClientsVersion = "0.1.38"
  private val fs2Version = "3.9.4"
  private val circeVersion = "0.14.6"
  private val log4CatsVersion = "2.6.0"

  lazy val awsCrt = "software.amazon.awssdk.crt" % "aws-crt" % "0.29.11"
  lazy val circeCore = "io.circe" %% "circe-core" % circeVersion
  lazy val circeGeneric = "io.circe" %% "circe-generic" % circeVersion
  lazy val circeParser = "io.circe" %% "circe-parser" % circeVersion
  lazy val circeGenericExtras = "io.circe" %% "circe-generic-extras" % "0.14.3"
  lazy val pureConfigCats = "com.github.pureconfig" %% "pureconfig-cats-effect" % pureConfigVersion
  lazy val pureConfig = "com.github.pureconfig" %% "pureconfig" % pureConfigVersion
  lazy val log4jSlf4j = "org.apache.logging.log4j" % "log4j-slf4j-impl" % logbackVersion
  lazy val log4jCore = "org.apache.logging.log4j" % "log4j-core" % logbackVersion
  lazy val log4jTemplateJson = "org.apache.logging.log4j" % "log4j-layout-template-json" % logbackVersion
  lazy val log4CatsCore = "org.typelevel" %% "log4cats-core" % log4CatsVersion;
  lazy val log4CatsSlf4j = "org.typelevel" %% "log4cats-slf4j" % log4CatsVersion
  lazy val lambdaCore = "com.amazonaws" % "aws-lambda-java-core" % "1.2.3"
  lazy val lambdaJavaEvents = "com.amazonaws" % "aws-lambda-java-events" % "3.11.4"
  lazy val s3Client = "uk.gov.nationalarchives" %% "da-s3-client" % daAwsClientsVersion
  lazy val sfnClient = "uk.gov.nationalarchives" %% "da-sfn-client" % daAwsClientsVersion
  lazy val commonsCompress = "org.apache.commons" % "commons-compress" % "1.26.0"
  lazy val fs2IO = "co.fs2" %% "fs2-io" % fs2Version
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.2.18"
  lazy val mockito = "org.mockito" %% "mockito-scala" % "1.17.30"
  lazy val wiremock = "com.github.tomakehurst" % "wiremock" % "3.0.1"
  lazy val reactorTest = "io.projectreactor" % "reactor-test" % "3.6.2"
}
