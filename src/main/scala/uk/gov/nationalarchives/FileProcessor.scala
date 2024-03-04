package uk.gov.nationalarchives

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.implicits._
import fs2.compression.Compression
import fs2.io._
import fs2.{Chunk, Pipe, Stream, text}
import io.circe.Json.Null
import io.circe.generic.auto._
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.parser.decode
import io.circe.syntax._
import io.circe.{Decoder, Encoder, HCursor, Json, Printer}
import org.apache.commons.codec.binary.Hex
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.reactivestreams.{FlowAdapters, Publisher}
import uk.gov.nationalarchives.FileProcessor._
import uk.gov.nationalarchives.UriProcessor.ParsedUri

import java.io.{BufferedInputStream, InputStream}
import java.nio.ByteBuffer
import java.time.OffsetDateTime
import java.util.{Base64, UUID}

class FileProcessor(
    downloadBucket: String,
    uploadBucket: String,
    consignmentRef: String,
    s3: DAS3Client[IO],
    uuidGenerator: () => UUID
) {

  def copyFilesFromDownloadToUploadBucket(downloadBucketKey: String): IO[Map[String, FileInfo]] = {
    s3.download(downloadBucket, downloadBucketKey)
      .flatMap(
        _.publisherToStream
          .flatMap(bf => Stream.chunk(Chunk.byteBuffer(bf)))
          .through(Compression[IO].gunzip())
          .flatMap(_.content)
          .through(unarchiveToS3)
          .compile
          .toList
      )
      .map(_.toMap)
  }

  def readJsonFromPackage(metadataId: UUID): IO[TREMetadata] = {
    for {
      s3Publisher <- s3.download(uploadBucket, metadataId.toString)
      contentString <- s3Publisher.publisherToStream
        .flatMap(bf => Stream.chunk(Chunk.byteBuffer(bf)))
        .through(extractMetadataFromJson)
        .compile
        .toList
      parsedJson <- IO.fromOption(contentString.headOption)(
        new RuntimeException(
          "Error parsing metadata.json.\nPlease check that the JSON is valid and that all required fields are present"
        )
      )
    } yield parsedJson
  }

  def createBagitMetadataObjects(
      fileInfo: FileInfo,
      metadataFileInfo: FileInfo,
      parsedUri: Option[ParsedUri],
      potentialCite: Option[String],
      potentialJudgmentName: Option[String],
      potentialUri: Option[String],
      potentialFileReference: String,
      fileReference: Option[String],
      potentialDepartment: Option[String],
      potentialSeries: Option[String],
      tdrUuid: String
  ): List[BagitMetadataObject] = {
    val potentialCourtFromUri = parsedUri.flatMap(_.potentialCourt)
    val (folderName, potentialFolderTitle, uriIdField) =
      if (potentialDepartment.flatMap(_ => potentialSeries).isEmpty && potentialCourtFromUri.isDefined)
        ("Court Documents (court not matched)", None, Nil)
      else if (potentialCourtFromUri.isEmpty) ("Court Documents (court unknown)", None, Nil)
      else
        (
          parsedUri.get.uriWithoutDocType,
          potentialJudgmentName.map(_.stripPrefix("Press Summary of ")),
          List(IdField("URI", parsedUri.get.uriWithoutDocType))
        )

    val folderMetadataIdFields = potentialDepartment
      .map { _ =>
        val potentialIdFields = potentialCite
          .map(cite => List(IdField("Code", cite), IdField("Cite", cite)) ++ uriIdField)
        potentialIdFields.getOrElse(uriIdField)
      }
      .getOrElse(Nil)

    val assetMetadataIdFields = List(
      Option(IdField("UpstreamSystemReference", potentialFileReference)),
      potentialUri.map(uri => IdField("URI", uri)),
      potentialCite.map(cite => IdField("NeutralCitation", cite)),
      fileReference.map(ref => IdField("BornDigitalRef", ref)),
      Option(IdField("RecordID", tdrUuid))
    ).flatten
    val fileTitle = fileInfo.fileName.split("\\.").dropRight(1).mkString(".")
    val folderId = uuidGenerator()
    val assetId = uuidGenerator()
    val folderMetadataObject = BagitFolderMetadataObject(folderId, None, potentialFolderTitle, folderName, folderMetadataIdFields)
    val assetMetadataObject =
      BagitAssetMetadataObject(
        assetId,
        Option(folderId),
        fileInfo.fileName,
        tdrUuid,
        List(fileInfo.id),
        List(metadataFileInfo.id),
        potentialJudgmentName,
        assetMetadataIdFields
      )
    val fileRowMetadataObject =
      BagitFileMetadataObject(
        fileInfo.id,
        Option(assetId),
        fileTitle,
        1,
        fileInfo.fileName,
        fileInfo.fileSize
      )
    val fileMetadataObject = BagitFileMetadataObject(
      metadataFileInfo.id,
      Option(assetId),
      "",
      2,
      metadataFileInfo.fileName,
      metadataFileInfo.fileSize
    )
    List(folderMetadataObject, assetMetadataObject, fileRowMetadataObject, fileMetadataObject)
  }

  def createBagitFiles(
      bagitMetadata: List[BagitMetadataObject],
      fileInfo: FileInfo,
      metadataFileInfo: FileInfo,
      treMetadata: TREMetadata,
      department: Option[String],
      series: Option[String]
  ): IO[String] = {
    for {
      metadataChecksum <- createMetadataJson(bagitMetadata)
      bagitString = "BagIt-Version: 1.0\nTag-File-Character-Encoding: UTF-8"
      bagitTxtChecksum <- uploadAsFile(bagitString, "bagit.txt")
      manifestString =
        s"${fileInfo.checksum} data/${fileInfo.id}\n${metadataFileInfo.checksum} data/${metadataFileInfo.id}"
      manifestSha256Checksum <- uploadAsFile(manifestString, "manifest-sha256.txt")
      bagInfoChecksum <- createBagInfo(department, series)
      bagInfoJsonChecksum <- createBagInfoJson(treMetadata)
      tagManifest <- createTagManifest(
        metadataChecksum,
        bagitTxtChecksum,
        manifestSha256Checksum,
        bagInfoChecksum,
        bagInfoJsonChecksum
      )
    } yield tagManifest
  }

  private def createBagInfo(departmentOpt: Option[String], seriesOpt: Option[String]): IO[Option[String]] = {
    for {
      department <- departmentOpt
      series <- seriesOpt
    } yield {
      val bagInfoString = s"Department: $department\nSeries: $series"
      uploadAsFile(bagInfoString, "bag-info.txt")
    }
  }.sequence

  private def createBagInfoJson(treMetadata: TREMetadata): IO[String] = {
    val bagInfoIdFields =
      List(
        IdField("ConsignmentReference", treMetadata.parameters.TDR.`Internal-Sender-Identifier`),
        IdField("UpstreamSystemReference", treMetadata.parameters.TRE.reference)
      )
    val bagInfo = BagInfo(
      treMetadata.parameters.TDR.`Source-Organization`,
      treMetadata.parameters.TDR.`Consignment-Export-Datetime`,
      "TRE: FCL Parser workflow",
      "Born Digital",
      "FCL",
      bagInfoIdFields
    )
    uploadAsFile(bagInfo.asJson.printWith(Printer.noSpaces), "bag-info.json")
  }

  private def extractMetadataFromJson(str: Stream[IO, Byte]): Stream[IO, TREMetadata] = {
    str
      .through(text.utf8.decode)
      .flatMap { jsonString =>
        Stream.fromEither[IO](decode[TREMetadata](jsonString))
      }
  }

  private def unarchiveAndUploadToS3(tarInputStream: TarArchiveInputStream): Stream[IO, (String, FileInfo)] = {
    Stream
      .eval(IO.blocking(Option(tarInputStream.getNextEntry)))
      .flatMap(Stream.fromOption[IO](_))
      .flatMap { tarEntry =>
        Stream
          .eval(IO(readInputStream(IO.pure[InputStream](tarInputStream), chunkSize, closeAfterUse = false)))
          .flatMap { stream =>
            if (!tarEntry.isDirectory) {
              val id = uuidGenerator()
              Stream.eval[IO, (String, FileInfo)](
                stream.chunks
                  .map(_.toByteBuffer)
                  .toPublisherResource
                  .use(pub => s3.upload(uploadBucket, id.toString, tarEntry.getSize, FlowAdapters.toPublisher(pub)))
                  .map { res =>
                    val checksum = checksumToString(res.response().checksumSHA256())
                    tarEntry.getName -> FileInfo(id, tarEntry.getSize, tarEntry.getName.split("/").last, checksum)
                  }
              )
            } else Stream.empty
          } ++
          unarchiveAndUploadToS3(tarInputStream)
      }
  }

  private def unarchiveToS3: Pipe[IO, Byte, (String, FileInfo)] = { stream =>
    stream
      .through(toInputStream[IO])
      .map(new BufferedInputStream(_, chunkSize))
      .flatMap(is => Stream.resource(Resource.fromAutoCloseable(IO.blocking(new TarArchiveInputStream(is)))))
      .flatMap(unarchiveAndUploadToS3)
  }

  private def uploadAsFile(fileContent: String, key: String) = {
    Stream
      .eval(IO(fileContent))
      .map(s => ByteBuffer.wrap(s.getBytes()))
      .toPublisherResource
      .use { pub =>
        s3.upload(uploadBucket, s"$consignmentRef/$key", fileContent.getBytes.length, FlowAdapters.toPublisher(pub))
      }
      .map(_.response().checksumSHA256())
      .map(checksumToString)
  }

  private def createMetadataJson(metadata: List[BagitMetadataObject]): IO[String] =
    uploadAsFile(metadata.asJson.printWith(Printer.noSpaces), "metadata.json")

  private def checksumToString(checksum: String): String =
    Option(checksum)
      .map(c => Hex.encodeHex(Base64.getDecoder.decode(c.getBytes())).mkString)
      .getOrElse("")

  private def createTagManifest(
      metadataChecksum: String,
      bagitTxtChecksum: String,
      manifestSha256Checksum: String,
      potentialBagInfoChecksum: Option[String],
      bagInfoJsonChecksum: String
  ): IO[String] = {
    val tagManifestMap = Map(
      "metadata.json" -> metadataChecksum,
      "bagit.txt" -> bagitTxtChecksum,
      "manifest-sha256.txt" -> manifestSha256Checksum,
      "bag-info.json" -> bagInfoJsonChecksum
    )
    val tagManifest = potentialBagInfoChecksum
      .map(cs => tagManifestMap + ("bag-info.txt" -> cs))
      .getOrElse(tagManifestMap)
      .toSeq
      .sortBy(_._1)
      .map { case (file, checksum) => s"$checksum $file" }
      .mkString("\n")
    uploadAsFile(tagManifest, "tagmanifest-sha256.txt")
  }
}

object FileProcessor {
  private val chunkSize: Int = 1024 * 64
  private val convertIdFieldsToJson = (idFields: List[IdField]) =>
    idFields.map { idField =>
      (s"id_${idField.name}", Json.fromString(idField.value))
    }
  implicit val customConfig: Configuration = Configuration.default.withDefaults
  implicit val parserDecoder: Decoder[Parser] = deriveConfiguredDecoder
  implicit val inputParametersDecoder: Decoder[TREInputParameters] = (c: HCursor) =>
    for {
      status <- c.downField("status").as[String]
      reference <- c.downField("reference").as[String]
      s3Bucket <- c.downField("s3Bucket").as[String]
      s3Key <- c.downField("s3Key").as[String]
      skipSeriesLookup <- c.getOrElse("skipSeriesLookup")(false)
    } yield TREInputParameters(status, reference, skipSeriesLookup, s3Bucket, s3Key)
  implicit val bagitMetadataEncoder: Encoder[BagitMetadataObject] = {
    case BagitFolderMetadataObject(id, parentId, title, name, folderMetadataIdFields) =>
      jsonFromMetadataObject(id, parentId, title, ArchiveFolder, name).deepMerge {
        Json.fromFields(convertIdFieldsToJson(folderMetadataIdFields))
      }
    case BagitAssetMetadataObject(
          id,
          parentId,
          title,
          name,
          originalFilesUuids,
          originalMetadataFilesUuids,
          description,
          assetMetadataIdFields
        ) =>
      val convertListOfUuidsToJsonStrArray = (fileUuids: List[UUID]) =>
        fileUuids.map(fileUuid => Json.fromString(fileUuid.toString))

      jsonFromMetadataObject(id, parentId, Option(title), Asset, name)
        .deepMerge {
          Json.fromFields(convertIdFieldsToJson(assetMetadataIdFields))
        }
        .deepMerge {
          Json
            .obj(
              ("originalFiles", Json.fromValues(convertListOfUuidsToJsonStrArray(originalFilesUuids))),
              ("originalMetadataFiles", Json.fromValues(convertListOfUuidsToJsonStrArray(originalMetadataFilesUuids))),
              ("description", description.map(Json.fromString).getOrElse(Null))
            )
            .deepDropNullValues
        }

    case BagitFileMetadataObject(id, parentId, title, sortOrder, name, fileSize) =>
      Json
        .obj(
          ("sortOrder", Json.fromInt(sortOrder)),
          ("fileSize", Json.fromLong(fileSize))
        )
        .deepMerge(jsonFromMetadataObject(id, parentId, Option(title), File, name))
  }
  implicit val bagitInfoEncoder: Encoder[BagInfo] = {
    case BagInfo(transferringBody, transferCompleteDatetime, upstreamSystem, digitalAssetSource, digitalAssetSubtype, idFields) =>
      Json
        .obj(
          ("transferringBody", Json.fromString(transferringBody)),
          ("transferCompleteDatetime", Json.fromString(transferCompleteDatetime.toString)),
          ("upstreamSystem", Json.fromString(upstreamSystem)),
          ("digitalAssetSource", Json.fromString(digitalAssetSource)),
          ("digitalAssetSubtype", Json.fromString(digitalAssetSubtype))
        )
        .deepDropNullValues
        .deepMerge(Json.fromFields(convertIdFieldsToJson(idFields)))
  }

  private def jsonFromMetadataObject(
      id: UUID,
      parentId: Option[UUID],
      title: Option[String],
      objectType: Type,
      name: String
  ) = {
    Json.obj(
      ("id", Json.fromString(id.toString)),
      ("parentId", parentId.map(_.toString).map(Json.fromString).getOrElse(Null)),
      ("title", title.map(Json.fromString).getOrElse(Null)),
      ("type", objectType.asJson),
      ("name", Json.fromString(name))
    )
  }

  implicit val additionalMetadataEncoder: Encoder[AdditionalMetadata] = deriveConfiguredEncoder

  implicit val typeEncoder: Encoder[Type] = {
    case ArchiveFolder => Json.fromString("ArchiveFolder")
    case Asset         => Json.fromString("Asset")
    case File          => Json.fromString("File")
  }

  sealed trait Type

  case object ArchiveFolder extends Type

  case object Asset extends Type

  case object File extends Type

  case class AdditionalMetadata(key: String, value: String)
  sealed trait BagitMetadataObject {
    def id: UUID
    def parentId: Option[UUID]
  }

  case class IdField(name: String, value: String)

  case class BagitFolderMetadataObject(
      id: UUID,
      parentId: Option[UUID],
      title: Option[String],
      name: String,
      idFields: List[IdField] = Nil
  ) extends BagitMetadataObject

  case class BagitAssetMetadataObject(
      id: UUID,
      parentId: Option[UUID],
      title: String,
      name: String,
      originalFiles: List[UUID],
      originalMetadataFiles: List[UUID],
      description: Option[String],
      idFields: List[IdField] = Nil
  ) extends BagitMetadataObject

  case class BagitFileMetadataObject(
      id: UUID,
      parentId: Option[UUID],
      title: String,
      sortOrder: Int,
      name: String,
      fileSize: Long
  ) extends BagitMetadataObject

  case class BagInfo(
      transferringBody: String,
      transferCompleteDatetime: OffsetDateTime,
      upstreamSystem: String,
      digitalAssetSource: String,
      digitalAssetSubtype: String,
      idFields: List[IdField] = Nil
  )

  case class FileInfo(id: UUID, fileSize: Long, fileName: String, checksum: String)

  case class TREInputParameters(status: String, reference: String, skipSeriesLookup: Boolean, s3Bucket: String, s3Key: String)

  case class TREInput(parameters: TREInputParameters)

  case class TREMetadata(parameters: TREMetadataParameters)

  case class Parser(
      uri: Option[String],
      cite: Option[String] = None,
      name: Option[String],
      attachments: List[String] = Nil,
      `error-messages`: List[String] = Nil
  )

  case class Payload(filename: String)

  case class TREParams(reference: String, payload: Payload)

  case class TDRParams(
      `Document-Checksum-sha256`: String,
      `Source-Organization`: String,
      `Internal-Sender-Identifier`: String,
      `Consignment-Export-Datetime`: OffsetDateTime,
      `File-Reference`: Option[String],
      `UUID`: UUID
  )

  case class TREMetadataParameters(PARSER: Parser, TRE: TREParams, TDR: TDRParams)

  implicit class PublisherToStream(publisher: Publisher[ByteBuffer]) {
    def publisherToStream: Stream[IO, ByteBuffer] = Stream.eval(IO.delay(publisher)).flatMap { publisher =>
      fs2.interop.flow.fromPublisher[IO](FlowAdapters.toFlowPublisher(publisher), chunkSize = 16)
    }
  }

  case class Config(outputBucket: String, sfnArn: String)
}
