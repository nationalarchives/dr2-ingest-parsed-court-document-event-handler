package uk.gov.nationalarchives

import cats.effect.IO
import com.amazonaws.services.lambda.runtime.{Context, RequestStreamHandler}
import cats.effect.unsafe.implicits.global
import io.circe.generic.auto._
import pureconfig._
import pureconfig.generic.auto._
import pureconfig.module.catseffect.syntax._
import uk.gov.nationalarchives.FileProcessor._
import upickle.default._

import java.io.{InputStream, OutputStream}
import java.util.UUID
class Lambda extends RequestStreamHandler {
  val s3: DAS3Client[IO] = DAS3Client[IO]()
  val sfn: DASFNClient[IO] = DASFNClient[IO]()
  val uuidGenerator: () => UUID = () => UUID.randomUUID
  val seriesMapper: SeriesMapper = SeriesMapper()

  override def handleRequest(input: InputStream, output: OutputStream, context: Context): Unit = {
    val inputJson = read[TREInput](input.readAllBytes.map(_.toChar).mkString)
    val inputBucket = inputJson.parameters.s3Bucket
    val batchId = inputJson.parameters.reference

    for {
      config <- ConfigSource.default.loadF[IO, Config]()
      outputBucket = config.outputBucket
      fileProcessor = new FileProcessor(inputBucket, outputBucket, batchId, s3, uuidGenerator)
      fileToId <- fileProcessor.copyFilesToBucket(inputJson.parameters.s3Key)

      metadataFileInfo <- IO.fromOption(fileToId.get(s"$batchId/TRE-$batchId-metadata.json"))(
        new RuntimeException(s"Cannot find metadata for $batchId")
      )
      json <- fileProcessor.readJsonFromPackage(metadataFileInfo.id)
      payload = json.parameters.TRE.payload
      cite = json.parameters.PARSER.cite
      fileInfo <- IO.fromOption(fileToId.get(s"$batchId/${payload.filename}"))(
        new RuntimeException("Document not found")
      )
      output <- seriesMapper.createOutput(config.outputBucket, batchId, cite)
      _ <- fileProcessor.createMetadataFiles(
        fileInfo.copy(checksum = payload.sha256),
        metadataFileInfo,
        cite,
        output.department,
        output.series
      )
      _ <- s3.copy(outputBucket, fileInfo.id.toString, outputBucket, s"$batchId/data/${fileInfo.id}")
      _ <- s3.copy(outputBucket, metadataFileInfo.id.toString, outputBucket, s"$batchId/data/${metadataFileInfo.id}")

      _ <- sfn.startExecution(config.sfnArn, output, Option(s"$batchId-${uuidGenerator()}"))
    } yield ()
  }.unsafeRunSync()
}
