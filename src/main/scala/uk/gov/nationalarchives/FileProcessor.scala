package uk.gov.nationalarchives

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.implicits._
import fs2.compression.Compression
import fs2.interop.reactivestreams._
import fs2.io._
import fs2.{Chunk, Pipe, Stream, text}
import io.circe.Json.Null
import io.circe.generic.auto._
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.parser.decode
import io.circe.syntax._
import io.circe.{Decoder, Encoder, Json, Printer}
import org.apache.commons.codec.binary.Hex
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import uk.gov.nationalarchives.FileProcessor._

import java.io.{BufferedInputStream, InputStream}
import java.nio.ByteBuffer
import java.time.LocalDate
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
        _.toStreamBuffered[IO](10 * 1024)
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
      s3Stream <- s3.download(uploadBucket, metadataId.toString)
      contentString <- s3Stream
        .toStreamBuffered[IO](chunkSize)
        .flatMap(bf => Stream.chunk(Chunk.byteBuffer(bf)))
        .through(extractMetadataFromJson)
        .compile
        .toList
      parsedJson <- IO.fromOption(contentString.headOption)(new RuntimeException("Error parsing json"))
    } yield parsedJson
  }

  def createMetadataFiles(
      fileInfo: FileInfo,
      metadataFileInfo: FileInfo,
      cite: String,
      judgmentName: Option[String],
      department: Option[String],
      series: Option[String]
  ): IO[String] = {
    val fileTitle = fileInfo.fileName.split("\\.").dropRight(1).mkString(".")
    val folderTitle = judgmentName.map(_.stripPrefix("Press Summary of ")).getOrElse("")
    val assetTitle = judgmentName.getOrElse(fileTitle)
    val folderId = uuidGenerator()
    val assetId = uuidGenerator()
    val folderMetadataObject = BagitFolderMetadataObject(folderId, None, folderTitle, Option(cite))
    val assetMetadataObject = BagitAssetMetadataObject(assetId, Option(folderId), assetTitle)
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
    val metadata = List(folderMetadataObject, assetMetadataObject, fileRowMetadataObject, fileMetadataObject)
    val bagitString = "BagIt-Version: 1.0\nTag-File-Character-Encoding: UTF-8"
    for {
      metadataChecksum <- createAndUploadMetadata(metadata)
      bagitTxtChecksum <- uploadAsFile(bagitString, "bagit.txt")
      manifestString =
        s"${fileInfo.checksum} data/${fileInfo.id}\n${metadataFileInfo.checksum} data/${metadataFileInfo.id}"
      manifestSha256Checksum <- uploadAsFile(manifestString, "manifest-sha256.txt")
      bagInfoChecksum <- createBagInfo(department, series)
      tagManifest <- createTagManifest(
        metadataChecksum,
        bagitTxtChecksum,
        manifestSha256Checksum,
        bagInfoChecksum
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

  private def extractMetadataFromJson(str: Stream[IO, Byte]): Stream[IO, TREMetadata] = {
    str
      .through(text.utf8.decode)
      .flatMap { jsonString =>
        Stream.fromEither[IO](decode[TREMetadata](jsonString))
      }
  }

  private def unarchiveAndUploadToS3(tarInputStream: TarArchiveInputStream): Stream[IO, (String, FileInfo)] = {
    Stream
      .eval(IO.blocking(Option(tarInputStream.getNextTarEntry)))
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
                  .toUnicastPublisher
                  .use(s3.upload(uploadBucket, id.toString, tarEntry.getSize, _))
                  .map { res =>
                    val checksum = checksumToString(res.response().checksumSHA256())
                    tarEntry.getName -> FileInfo(id, tarEntry.getSize, tarEntry.getName.split("/").last, checksum)
                  }
              )
            } else {
              Stream.empty
            }
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
      .toUnicastPublisher
      .use { pub =>
        s3.upload(uploadBucket, s"$consignmentRef/$key", fileContent.getBytes.length, pub)
      }
      .map(_.response().checksumSHA256())
      .map(checksumToString)
  }

  private def createAndUploadMetadata(metadata: List[BagitMetadataObject]): IO[String] = {
    Stream
      .emit[IO, List[BagitMetadataObject]](metadata)
      .through(_.map(_.asJson.printWith(Printer.noSpaces)))
      .compile
      .string
      .flatMap(s => uploadAsFile(s, "metadata.json"))
  }

  private def checksumToString(checksum: String): String =
    Option(checksum)
      .map(c => Hex.encodeHex(Base64.getDecoder.decode(c.getBytes())).mkString)
      .getOrElse("")

  private def createTagManifest(
      metadataChecksum: String,
      bagitTxtChecksum: String,
      manifestSha256Checksum: String,
      potentialBagInfoChecksum: Option[String]
  ): IO[String] = {
    val tagManifestMap = Map(
      "metadata.json" -> metadataChecksum,
      "bagit.txt" -> bagitTxtChecksum,
      "manifest-sha256.txt" -> manifestSha256Checksum
    )
    val tagManifest = potentialBagInfoChecksum
      .map(cs => tagManifestMap + ("bag-info.txt" -> cs))
      .getOrElse(tagManifestMap)
      .toSeq
      .sortBy(_._1)
      .map { case (file, checksum) =>
        s"$checksum $file"
      }
      .mkString("\n")
    uploadAsFile(tagManifest, "tagmanifest-sha256.txt")
  }
}

object FileProcessor {
  private val chunkSize: Int = 1024 * 64
  implicit val customConfig: Configuration = Configuration.default.withDefaults
  implicit val parserDecoder: Decoder[Parser] = deriveConfiguredDecoder
  implicit val bagitMetadataEncoder: Encoder[BagitMetadataObject] = {
    case BagitFolderMetadataObject(id, parentId, title, name) =>
      jsonFromMetadataObject(id, parentId, title, ArchiveFolder, name)
    case BagitAssetMetadataObject(id, parentId, title, name) =>
      jsonFromMetadataObject(id, parentId, title, Asset, name)
    case BagitFileMetadataObject(id, parentId, title, sortOrder, name, fileSize) =>
      Json
        .obj(
          ("sortOrder", Json.fromInt(sortOrder)),
          ("fileSize", Json.fromLong(fileSize))
        )
        .deepMerge(jsonFromMetadataObject(id, parentId, title, File, Option(name)))
  }

  private def jsonFromMetadataObject(
      id: UUID,
      parentId: Option[UUID],
      title: String,
      objectType: Type,
      name: Option[String]
  ) = {
    Json.obj(
      ("id", Json.fromString(id.toString)),
      ("parentId", parentId.map(_.toString).map(Json.fromString).getOrElse(Null)),
      ("title", Json.fromString(title)),
      ("type", objectType.asJson),
      ("name", name.map(Json.fromString).getOrElse(Null))
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
    def title: String
  }

  case class BagitFolderMetadataObject(
      id: UUID,
      parentId: Option[UUID],
      title: String,
      name: Option[String] = None
  ) extends BagitMetadataObject

  case class BagitAssetMetadataObject(
      id: UUID,
      parentId: Option[UUID],
      title: String,
      name: Option[String] = None
  ) extends BagitMetadataObject

  case class BagitFileMetadataObject(
      id: UUID,
      parentId: Option[UUID],
      title: String,
      sortOrder: Int,
      name: String,
      fileSize: Long
  ) extends BagitMetadataObject

  case class FileInfo(id: UUID, fileSize: Long, fileName: String, checksum: String)

  case class TREInputParameters(status: String, reference: String, s3Bucket: String, s3Key: String)

  case class TREInput(parameters: TREInputParameters)

  case class TREMetadata(parameters: TREMetadataParameters)

  case class Parser(
      uri: String,
      court: String,
      cite: Option[String] = None,
      date: LocalDate,
      name: Option[String],
      attachments: List[String] = Nil,
      `error-messages`: List[String] = Nil
  )

  case class Payload(filename: String, sha256: String)

  case class TREParams(payload: Payload)

  case class TREMetadataParameters(PARSER: Parser, TRE: TREParams)

  case class Config(outputBucket: String, sfnArn: String)
}
