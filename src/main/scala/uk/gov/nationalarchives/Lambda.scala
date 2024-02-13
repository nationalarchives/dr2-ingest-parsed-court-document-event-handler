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
import org.typelevel.log4cats.{LoggerName, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j._

import java.util.UUID
import scala.jdk.CollectionConverters._

class Lambda extends RequestHandler[SQSEvent, Unit] {
  val s3: DAS3Client[IO] = DAS3Client[IO]()
  val sfn: DASFNClient[IO] = DASFNClient[IO]()
  val randomUuidGenerator: () => UUID = () => UUID.randomUUID
  val seriesMapper: SeriesMapper = SeriesMapper()

  implicit val loggerName: LoggerName = LoggerName("Ingest Parsed Court Document Event Handler")
  private val logger: SelfAwareStructuredLogger[IO] = Slf4jFactory.create[IO].getLogger

  override def handleRequest(input: SQSEvent, context: Context): Unit = {
    input.getRecords.asScala.toList.map { record =>
      for {
        treInput <- IO.fromEither(decode[TREInput](record.getBody))
        batchRef = treInput.parameters.reference
        logCtx = Map("batchRef" -> batchRef)
        _ <- logger.info(logCtx)(s"Processing batchRef $batchRef")

        config <- ConfigSource.default.loadF[IO, Config]()
        outputBucket = config.outputBucket
        fileProcessor = new FileProcessor(treInput.parameters.s3Bucket, outputBucket, batchRef, s3, randomUuidGenerator)
        fileNameToFileInfo <- fileProcessor.copyFilesFromDownloadToUploadBucket(treInput.parameters.s3Key)
        _ <- logger.info(logCtx)(s"Copied ${treInput.parameters.s3Key} from ${treInput.parameters.s3Bucket} to $outputBucket")

        metadataFileInfo <- IO.fromOption(fileNameToFileInfo.get(s"$batchRef/TRE-$batchRef-metadata.json"))(
          new RuntimeException(s"Cannot find metadata for $batchRef")
        )
        treMetadata <- fileProcessor.readJsonFromPackage(metadataFileInfo.id)
        potentialUri = treMetadata.parameters.PARSER.uri
        potentialJudgmentName = treMetadata.parameters.PARSER.name
        uriProcessor = new UriProcessor(potentialUri)
        _ <- uriProcessor.verifyJudgmentNameStartsWithPressSummaryOfIfInUri(potentialJudgmentName)

        parsedUri <- uriProcessor.getCourtAndUriWithoutDocType
        payload = treMetadata.parameters.TRE.payload
        potentialCite = treMetadata.parameters.PARSER.cite
        fileReference = treMetadata.parameters.TDR.`File-Reference`
        logWithFileRef = logger.info(logCtx ++ Map("fileReference" -> fileReference.orNull))(_)

        fileInfo <- IO.fromOption(fileNameToFileInfo.get(s"$batchRef/${payload.filename}"))(
          new RuntimeException(s"Document not found for file belonging to $batchRef")
        )

        _ <- IO.raiseWhen(fileInfo.fileSize == 0)(new Exception(s"File id '${fileInfo.id}' size is 0"))
        output <- seriesMapper.createOutput(
          config.outputBucket,
          batchRef,
          parsedUri.flatMap(_.potentialCourt),
          treInput.parameters.skipSeriesLookup
        )
        fileInfoWithUpdatedChecksum = fileInfo.copy(checksum = treMetadata.parameters.TDR.`Document-Checksum-sha256`)
        bagitMetadata = fileProcessor.createBagitMetadataObjects(
          fileInfoWithUpdatedChecksum,
          metadataFileInfo,
          parsedUri,
          potentialCite,
          potentialJudgmentName,
          potentialUri,
          treMetadata.parameters.TRE.reference,
          fileReference,
          output.department,
          output.series,
          treMetadata.parameters.TDR.`UUID`.toString
        )
        _ <- fileProcessor.createBagitFiles(
          bagitMetadata,
          fileInfoWithUpdatedChecksum,
          metadataFileInfo,
          treMetadata,
          output.department,
          output.series
        )
        _ <- logWithFileRef(s"Copied bagit files to $outputBucket")

        _ <- s3.copy(outputBucket, fileInfo.id.toString, outputBucket, s"$batchRef/data/${fileInfo.id}")
        _ <- logWithFileRef(s"Copied file with id ${fileInfo.id} to data directory")

        _ <- s3
          .copy(outputBucket, metadataFileInfo.id.toString, outputBucket, s"$batchRef/data/${metadataFileInfo.id}")
        _ <- logWithFileRef(s"Copied metadata file with id ${metadataFileInfo.id} to data directory")

        _ <- s3.deleteObjects(outputBucket, fileNameToFileInfo.values.map(_.id.toString).toList)
        _ <- logWithFileRef("Deleted objects from the root of S3")

        _ <- sfn.startExecution(config.sfnArn, output, Option(batchRef))
        _ <- logWithFileRef("Started step function execution")
      } yield ()
    }.sequence
  }.onError(logLambdaError).unsafeRunSync()

  private def logLambdaError(error: Throwable): IO[Unit] = logger.error(error)("Error running court document event handler")
}
