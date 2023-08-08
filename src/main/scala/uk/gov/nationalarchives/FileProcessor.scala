package uk.gov.nationalarchives

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.kernel.Resource
import fs2.compression.Compression
import fs2.data.csv.{Row, lowlevel}
import fs2.interop.reactivestreams._
import fs2.io._
import fs2.{Chunk, Pipe, Stream, text}
import org.apache.commons.codec.binary.Hex
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import uk.gov.nationalarchives.FileProcessor._
import upickle.default._

import java.io.{BufferedInputStream, InputStream}
import java.nio.ByteBuffer
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.{Base64, UUID}

class FileProcessor(
    downloadBucket: String,
    uploadBucket: String,
    consignmentRef: String,
    s3: DAS3Client[IO],
    uuidGenerator: () => UUID
) {

  def copyFilesToBucket(key: String): IO[Map[String, FileInfo]] = {
    s3.download(downloadBucket, key)
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
      department: String,
      series: String
  ): IO[String] = {
    val title = fileInfo.fileName.split("\\.").dropRight(1).mkString(".")
    val folderMetadataHeader = NonEmptyList("identifier", List("parentPath", "name", "title"))
    val assetMetadataHeader = NonEmptyList("identifier", List("parentPath", "title"))
    val fileMetadataHeader = NonEmptyList("identifier", List("parentPath", "name", "fileSize", "title"))
    val folderPath = uuidGenerator()
    val assetId = uuidGenerator()
    val assetPath = s"$folderPath/$assetId"
    val folderMetadataRow = Row(NonEmptyList(folderPath.toString, List("", cite, title)))
    val assetMetadataRow = Row(NonEmptyList(assetId.toString, List(folderPath.toString, title)))
    val fileRow = Row(
      NonEmptyList(fileInfo.id.toString, List(assetPath, fileInfo.fileName, fileInfo.fileSize.toString, title))
    )
    val fileMetadataRow = Row(
      NonEmptyList(
        metadataFileInfo.id.toString,
        List(assetPath, metadataFileInfo.fileName, metadataFileInfo.fileSize.toString, "")
      )
    )
    val bagitTxtRow = Row(NonEmptyList("Tag-File-Character-Encoding: UTF-8", Nil))
    val bagitTxtHeader = NonEmptyList("BagIt-Version: 1.0", Nil)

    for {
      folderMetadataChecksum <- createAndUploadFile(
        "folder-metadata.csv",
        folderMetadataRow :: Nil,
        folderMetadataHeader
      )
      assetMetadataChecksum <- createAndUploadFile(
        "asset-metadata.csv",
        assetMetadataRow :: Nil,
        assetMetadataHeader
      )
      fileMetadataChecksum <- createAndUploadFile(
        "file-metadata.csv",
        List(fileRow, fileMetadataRow),
        fileMetadataHeader
      )
      bagitTxtChecksum <- createAndUploadFile("bagit.txt", bagitTxtRow :: Nil, bagitTxtHeader)
      manifestString =
        s"${fileInfo.checksum} data/${fileInfo.id}\n${metadataFileInfo.checksum} data/${metadataFileInfo.id}"
      manifestSha256Checksum <- uploadAsFile(
        manifestString,
        "manifest-sha256.txt",
        manifestString.getBytes.length
      )
      bagInfoString = s"Department: $department\nSeries: $series"
      bagInfoChecksum <- uploadAsFile(bagInfoString, "bag-info.txt", bagInfoString.getBytes.length)
      tagManifest <- createTagManifest(
        folderMetadataChecksum,
        assetMetadataChecksum,
        fileMetadataChecksum,
        bagitTxtChecksum,
        manifestSha256Checksum,
        bagInfoChecksum
      )
    } yield tagManifest
  }

  private def extractMetadataFromJson(str: Stream[IO, Byte]): Stream[IO, TREMetadata] = {
    str
      .through(text.utf8.decode)
      .map { jsonString =>
        read[TREMetadata](jsonString)
      }
  }

  private def readEntriesAndUpload(tarInputStream: TarArchiveInputStream): Stream[IO, (String, FileInfo)] = {
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
          readEntriesAndUpload(tarInputStream)
      }
  }

  private def unarchiveToS3: Pipe[IO, Byte, (String, FileInfo)] = { stream =>
    stream
      .through(toInputStream[IO])
      .map(new BufferedInputStream(_, chunkSize))
      .flatMap(is => Stream.resource(Resource.fromAutoCloseable(IO.blocking(new TarArchiveInputStream(is)))))
      .flatMap(readEntriesAndUpload)
  }

  private def uploadAsFile(fileContent: String, key: String, fileSize: Long) = {
    Stream
      .eval(IO(fileContent))
      .map(s => ByteBuffer.wrap(s.getBytes()))
      .toUnicastPublisher
      .use { pub =>
        s3.upload(uploadBucket, s"$consignmentRef/$key", fileSize, pub)
      }
      .map(_.response().checksumSHA256())
      .map(checksumToString)
  }

  private def createAndUploadFile(key: String, rows: List[Row], header: NonEmptyList[String]): IO[String] = {
    Stream
      .emits[IO, Row](rows)
      .through(lowlevel.writeWithHeaders(header))
      .through(lowlevel.toRowStrings())
      .compile
      .string
      .flatMap(s => uploadAsFile(s, key, s.getBytes.length))
  }

  private def checksumToString(checksum: String): String =
    Option(checksum)
      .map(c => Hex.encodeHex(Base64.getDecoder.decode(c.getBytes())).mkString)
      .getOrElse("")

  private def createTagManifest(
      folderMetadataChecksum: String,
      assetMetadataChecksum: String,
      fileMetadataChecksum: String,
      bagitTxtChecksum: String,
      manifestSha256Checksum: String,
      bagInfoChecksum: String
  ): IO[String] = {
    val tagManifest = Map(
      "folder-metadata.csv" -> folderMetadataChecksum,
      "asset-metadata.csv" -> assetMetadataChecksum,
      "file-metadata.csv" -> fileMetadataChecksum,
      "bagit.txt" -> bagitTxtChecksum,
      "manifest-sha256.txt" -> manifestSha256Checksum,
      "bag-info.txt" -> bagInfoChecksum
    ).toSeq
      .sortBy(_._1)
      .map { case (file, checksum) =>
        s"$checksum $file"
      }
      .mkString("\n")
    uploadAsFile(tagManifest, "tagmanifest-sha256.txt", tagManifest.getBytes.length)
  }
}

object FileProcessor {
  private val chunkSize: Int = 1024 * 64

  implicit val treParamsReader: Reader[TREParams] = macroR[TREParams]
  implicit val jsonReader: Reader[TREMetadata] = macroR[TREMetadata]
  implicit val treInputReader: Reader[TREInput] = macroR[TREInput]
  implicit val treInputParamsReader: Reader[TREInputParameters] = macroR[TREInputParameters]

  implicit val payloadReader: Reader[Payload] = macroR[Payload]
  implicit val parameterReader: Reader[TREMetadataParameters] = macroR
  implicit val parserReader: Reader[Parser] = macroR
  implicit val dateReader: Reader[LocalDate] = readwriter[String].bimap[LocalDate](
    date => date.format(DateTimeFormatter.ISO_LOCAL_DATE),
    dateString => LocalDate.parse(dateString)
  )

  case class FileInfo(id: UUID, fileSize: Long, fileName: String, checksum: String)

  case class TREInputParameters(status: String, reference: String, s3Bucket: String, s3Key: String)

  case class TREInput(parameters: TREInputParameters)

  case class TREMetadata(parameters: TREMetadataParameters)

  case class Parser(
      uri: String,
      court: String,
      cite: String,
      date: LocalDate,
      name: String,
      attachments: List[String] = Nil,
      `error-messages`: List[String] = Nil
  )

  case class Payload(filename: String, sha256: String)

  case class TREParams(payload: Payload)

  case class TREMetadataParameters(PARSER: Parser, TRE: TREParams)

  case class Config(outputBucket: String, sfnArn: String)
}
