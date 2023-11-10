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
import io.circe.parser.decode
import java.util.UUID
import scala.jdk.CollectionConverters._

class Lambda extends RequestHandler[SQSEvent, Unit] {
  val s3: DAS3Client[IO] = DAS3Client[IO]()
  val sfn: DASFNClient[IO] = DASFNClient[IO]()
  val randomUuidGenerator: () => UUID = () => UUID.randomUUID
  val seriesMapper: SeriesMapper = SeriesMapper()

  override def handleRequest(input: SQSEvent, context: Context): Unit = {
    input.getRecords.asScala.toList.map { record =>
      for {
        treInput <- IO.fromEither(decode[TREInput](record.getBody))
        batchRef = treInput.parameters.reference
        config <- ConfigSource.default.loadF[IO, Config]()
        outputBucket = config.outputBucket
        fileProcessor = new FileProcessor(treInput.parameters.s3Bucket, outputBucket, batchRef, s3, randomUuidGenerator)
        fileNameToFileInfo <- fileProcessor.copyFilesFromDownloadToUploadBucket(treInput.parameters.s3Key)
        metadataFileInfo <- IO.fromOption(fileNameToFileInfo.get(s"$batchRef/TRE-$batchRef-metadata.json"))(
          new RuntimeException(s"Cannot find metadata for $batchRef")
        )

        treMetadata <- fileProcessor.readJsonFromPackage(metadataFileInfo.id)
        potentialUri = treMetadata.parameters.PARSER.uri
        potentialFileName = treMetadata.parameters.PARSER.name
        uriProcessor = new UriProcessor(potentialUri)
        _ <- uriProcessor.verifyFileNameStartsWithPressSummaryOfIfInUri(potentialFileName)

        parsedUri <- uriProcessor.getCiteAndUriWithoutDocType
        payload = treMetadata.parameters.TRE.payload
        potentialCite = treMetadata.parameters.PARSER.cite

        fileInfo <- IO.fromOption(fileNameToFileInfo.get(s"$batchRef/${payload.filename}"))(
          new RuntimeException(s"Document not found for file belonging to $batchRef")
        )

        _ <- IO.raiseWhen(fileInfo.fileSize == 0)(new Exception(s"File id '${fileInfo.id}' size is 0"))
        output <- seriesMapper.createOutput(
          config.outputBucket,
          batchRef,
          parsedUri.flatMap(_.potentialCite),
          treInput.parameters.skipSeriesLookup
        )
        _ <- fileProcessor.createMetadataFiles(
          fileInfo.copy(checksum = treMetadata.parameters.TDR.`Document-Checksum-sha256`),
          metadataFileInfo,
          parsedUri,
          potentialCite,
          potentialFileName,
          output.department,
          output.series
        )
        _ <- s3.copy(outputBucket, fileInfo.id.toString, outputBucket, s"$batchRef/data/${fileInfo.id}")
        _ <- s3
          .copy(outputBucket, metadataFileInfo.id.toString, outputBucket, s"$batchRef/data/${metadataFileInfo.id}")
        _ <- s3.deleteObjects(outputBucket, fileNameToFileInfo.values.map(_.id.toString).toList)
        _ <- sfn.startExecution(config.sfnArn, output, Option(s"$batchRef-${randomUuidGenerator()}"))
      } yield ()
    }.sequence
  }.unsafeRunSync()
}
