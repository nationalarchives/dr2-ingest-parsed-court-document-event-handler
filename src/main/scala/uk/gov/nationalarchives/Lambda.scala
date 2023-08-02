package uk.gov.nationalarchives

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits._
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import io.circe.generic.auto._
import pureconfig._
import pureconfig.generic.auto._
import pureconfig.module.catseffect.syntax._
import uk.gov.nationalarchives.FileProcessor._
import upickle.default._

import java.util.UUID
import scala.jdk.CollectionConverters._
class Lambda extends RequestHandler[SQSEvent, Unit] {
  val s3: DAS3Client[IO] = DAS3Client[IO]()
  val sfn: DASFNClient[IO] = DASFNClient[IO]()
  val uuidGenerator: () => UUID = () => UUID.randomUUID
  val seriesMapper: SeriesMapper = SeriesMapper()

  override def handleRequest(input: SQSEvent, context: Context): Unit = {
    input.getRecords.asScala.toList
      .map(record => {
        val inputJson = read[TREInput](record.getBody)
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
          _ <- s3
            .copy(outputBucket, metadataFileInfo.id.toString, outputBucket, s"$batchId/data/${metadataFileInfo.id}")

          _ <- sfn.startExecution(config.sfnArn, output, Option(s"$batchId-${uuidGenerator()}"))
        } yield ()
      })
      .sequence
  }.unsafeRunSync()
}
