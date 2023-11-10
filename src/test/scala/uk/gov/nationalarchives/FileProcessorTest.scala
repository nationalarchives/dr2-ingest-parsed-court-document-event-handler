package uk.gov.nationalarchives

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.interop.reactivestreams._
import fs2.{Chunk, Stream, text}
import io.circe.generic.auto._
import io.circe.parser.decode
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, DecodingFailure, HCursor, ParsingFailure, Printer}
import org.mockito.ArgumentMatchers._
import org.mockito.{ArgumentMatcher, ArgumentMatchers, MockitoSugar}
import org.reactivestreams.Publisher
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor2, TableFor4, TableFor6}
import reactor.core.publisher.Flux
import software.amazon.awssdk.services.s3.model.PutObjectResponse
import software.amazon.awssdk.transfer.s3.model.CompletedUpload
import uk.gov.nationalarchives.FileProcessor._
import uk.gov.nationalarchives.UriProcessor.ParsedUri

import java.nio.ByteBuffer
import java.util.{Base64, HexFormat, UUID}

class FileProcessorTest extends AnyFlatSpec with MockitoSugar with TableDrivenPropertyChecks {
  val testTarGz: Array[Byte] = getClass.getResourceAsStream("/files/test.tar.gz").readAllBytes()
  val publisher: Flux[ByteBuffer] = Flux.just(ByteBuffer.wrap(testTarGz))
  val reference = "TEST-REFERENCE"

  implicit val typeDecoder: Decoder[Type] = (c: HCursor) =>
    for {
      decodedType <- c.downField("type").as[String]
    } yield {
      decodedType match {
        case "ArchiveFolder" => ArchiveFolder
        case "Asset"         => Asset
        case "File"          => File
      }
    }

  val metadataJson: String =
    s"""{"parameters":{"TDR": {"Document-Checksum-sha256": "abcde", "Source-Organization": "test-organisation",
       | "Internal-Sender-Identifier": "test-identifier","Consignment-Export-Datetime": "2023-10-31T13:40:54Z"},
       |"TRE":{"reference":"$reference","payload":{"filename":"Test.docx"}},
       |"PARSER":{"cite":"cite","uri":"https://example.com","court":"test","date":"2023-07-26","name":"test"}}}""".stripMargin

  private val uuids: List[UUID] = List(
    UUID.fromString("6e827e19-6a33-46c3-8730-b242c203d8c1"),
    UUID.fromString("49e4a726-6297-4f8e-8867-fb50bd5acd86")
  )

  case class UUIDGenerator() {
    val uuidsIterator: Iterator[UUID] = uuids.iterator

    val uuidGenerator: () => UUID = () => uuidsIterator.next()
  }

  def convertChecksumToS3Format(cs: Option[String]): String =
    cs.map { c =>
      Base64.getEncoder
        .encode(HexFormat.of().parseHex(c))
        .map(_.toChar)
        .mkString
    }.orNull

  def completedUpload(c: Option[String] = None): CompletedUpload = {
    val putObjectResponse = PutObjectResponse.builder.checksumSHA256(convertChecksumToS3Format(c)).build
    CompletedUpload.builder.response(putObjectResponse).build
  }

  "copyFilesFromDownloadToUploadBucket" should "return the correct file metadata for a valid tar.gz file" in {
    val generator = UUIDGenerator()
    val s3 = mock[DAS3Client[IO]]
    val docxCompletedUpload = completedUpload(Option("abcdef"))

    val metadataCompletedUpload = completedUpload(Option("123456"))

    when(s3.download(ArgumentMatchers.eq("download"), ArgumentMatchers.eq("key"))).thenReturn(IO(publisher))
    when(s3.upload(any[String], ArgumentMatchers.eq(uuids.head.toString), any[Long], any[Publisher[ByteBuffer]]))
      .thenReturn(IO(docxCompletedUpload))
    when(s3.upload(any[String], ArgumentMatchers.eq(uuids.last.toString), any[Long], any[Publisher[ByteBuffer]]))
      .thenReturn(IO(metadataCompletedUpload))

    val fileProcessor = new FileProcessor("download", "upload", "ref", s3, generator.uuidGenerator)
    val res = fileProcessor.copyFilesFromDownloadToUploadBucket("key").unsafeRunSync()

    res.size should equal(2)
    val docx = res.get(s"$reference/Test.docx")
    val metadata = res.get(s"$reference/TRE-$reference-metadata.json")
    docx.isDefined should be(true)
    metadata.isDefined should be(true)

    val docxInfo = docx.get
    docxInfo.id should equal(uuids.head)
    docxInfo.fileName should equal("Test.docx")
    docxInfo.fileSize should equal(15684)
    docxInfo.checksum should equal("abcdef")

    val metadataInfo = metadata.get
    metadataInfo.id should equal(uuids.last)
    metadataInfo.fileName should equal(s"TRE-$reference-metadata.json")
    metadataInfo.fileSize should equal(215)
    metadataInfo.checksum should equal("123456")
  }

  "copyFilesFromDownloadToUploadBucket" should "return an error if the downloaded file is not a valid tar.gz" in {
    val s3 = mock[DAS3Client[IO]]
    when(s3.download(ArgumentMatchers.eq("download"), ArgumentMatchers.eq("key")))
      .thenReturn(IO(Flux.just(ByteBuffer.wrap("invalid".getBytes))))

    val fileProcessor = new FileProcessor("download", "upload", "ref", s3, UUIDGenerator().uuidGenerator)
    val ex = intercept[Exception] {
      fileProcessor.copyFilesFromDownloadToUploadBucket("key").unsafeRunSync()
    }
    ex.getMessage should equal("UpStream failed")
  }

  "copyFilesFromDownloadToUploadBucket" should "return an error if the file download fails" in {
    val s3 = mock[DAS3Client[IO]]
    when(s3.download(ArgumentMatchers.eq("download"), ArgumentMatchers.eq("key")))
      .thenThrow(new RuntimeException("Error downloading files"))

    val fileProcessor = new FileProcessor("download", "upload", "ref", s3, UUIDGenerator().uuidGenerator)
    val ex = intercept[Exception] {
      fileProcessor.copyFilesFromDownloadToUploadBucket("key").unsafeRunSync()
    }
    ex.getMessage should equal("Error downloading files")
  }

  "copyFilesFromDownloadToUploadBucket" should "return an error if the upload fails" in {
    val s3 = mock[DAS3Client[IO]]

    when(s3.download(ArgumentMatchers.eq("download"), ArgumentMatchers.eq("key"))).thenReturn(IO(publisher))
    when(s3.upload(any[String], any[String], any[Long], any[Publisher[ByteBuffer]])) thenThrow new RuntimeException(
      "Upload failed"
    )

    val fileProcessor = new FileProcessor("download", "upload", "ref", s3, UUIDGenerator().uuidGenerator)
    val ex = intercept[Exception] {
      fileProcessor.copyFilesFromDownloadToUploadBucket("key").unsafeRunSync()
    }
    ex.getMessage should equal("Upload failed")
  }

  "copyFilesFromDownloadToUploadBucket" should "return an empty checksum if a checksum is not returned from S3" in {
    val generator = UUIDGenerator()
    val s3 = mock[DAS3Client[IO]]
    val docxCompletedUpload = completedUpload()

    val metadataCompletedUpload = completedUpload()

    when(s3.download(ArgumentMatchers.eq("download"), ArgumentMatchers.eq("key"))).thenReturn(IO(publisher))
    when(s3.upload(any[String], ArgumentMatchers.eq(uuids.head.toString), any[Long], any[Publisher[ByteBuffer]]))
      .thenReturn(IO(docxCompletedUpload))
    when(s3.upload(any[String], ArgumentMatchers.eq(uuids.last.toString), any[Long], any[Publisher[ByteBuffer]]))
      .thenReturn(IO(metadataCompletedUpload))

    val fileProcessor = new FileProcessor("download", "upload", "ref", s3, generator.uuidGenerator)
    val res = fileProcessor.copyFilesFromDownloadToUploadBucket("key").unsafeRunSync()

    res(s"$reference/Test.docx").checksum should equal("")
    res(s"$reference/TRE-$reference-metadata.json").checksum should equal("")
  }

  "readJsonFromPackage" should "return the correct object for valid json" in {
    val s3 = mock[DAS3Client[IO]]
    val downloadResponse = Flux.just(ByteBuffer.wrap(metadataJson.getBytes()))
    val metadataId = UUID.randomUUID()
    when(s3.download(ArgumentMatchers.eq("upload"), ArgumentMatchers.eq(metadataId.toString)))
      .thenReturn(IO(downloadResponse))
    val expectedMetadata = decode[TREMetadata](metadataJson).toOption.get

    val fileProcessor = new FileProcessor("download", "upload", "ref", s3, UUIDGenerator().uuidGenerator)

    val res = fileProcessor.readJsonFromPackage(metadataId).unsafeRunSync()

    res should equal(expectedMetadata)
  }

  "readJsonFromPackage" should "return an error where a non-optional value was expected for a field in the json" in {
    val s3 = mock[DAS3Client[IO]]
    val metadataJsonWithMissingParams: String =
      s"""{"parameters":{"TDR": {"Document-Checksum-sha256": null, "Source-Organization": "test-organisation",
         |"Internal-Sender-Identifier": "test-identifier", "Consignment-Export-Datetime": "2023-10-31T13:40:54Z"},
         |"TRE":{"reference":"$reference","payload":{"filename":"Test.docx"}},
         |"PARSER":{"cite":"cite","uri":"https://example.com","court":"test","date":"2023-07-26","name":"test"}}}""".stripMargin
    val downloadResponse = Flux.just(ByteBuffer.wrap(metadataJsonWithMissingParams.getBytes()))
    val metadataId = UUID.randomUUID()
    when(s3.download(ArgumentMatchers.eq("upload"), ArgumentMatchers.eq(metadataId.toString)))
      .thenReturn(IO(downloadResponse))

    val fileProcessor = new FileProcessor("download", "upload", "ref", s3, UUIDGenerator().uuidGenerator)

    val ex = intercept[DecodingFailure] {
      fileProcessor.readJsonFromPackage(metadataId).unsafeRunSync()
    }

    ex.getMessage should equal(
      """DecodingFailure at .parameters.TDR.Document-Checksum-sha256: Got value 'null' with wrong type, expecting string""".stripMargin
    )
  }

  "readJsonFromPackage" should "return an error for a json that is missing required fields" in {
    val s3 = mock[DAS3Client[IO]]
    val metadataJsonWithMissingParams: String =
      s"""{"parameters":{"TDR": {"Document-Checksum-sha256": "abcde","Internal-Sender-Identifier": "test-identifier",
         |"Consignment-Export-Datetime": "2023-10-31T13:40:54Z"},
         |"TRE":{"reference":"$reference","payload":{"filename":"Test.docx"}},
         |"PARSER":{"cite":"cite","uri":"https://example.com","court":"test","date":"2023-07-26","name":"test"}}}""".stripMargin
    val downloadResponse = Flux.just(ByteBuffer.wrap(metadataJsonWithMissingParams.getBytes()))
    val metadataId = UUID.randomUUID()
    when(s3.download(ArgumentMatchers.eq("upload"), ArgumentMatchers.eq(metadataId.toString)))
      .thenReturn(IO(downloadResponse))

    val fileProcessor = new FileProcessor("download", "upload", "ref", s3, UUIDGenerator().uuidGenerator)

    val ex = intercept[DecodingFailure] {
      fileProcessor.readJsonFromPackage(metadataId).unsafeRunSync()
    }

    ex.getMessage should equal(
      """DecodingFailure at .parameters.Source-Organization: Missing required field""".stripMargin
    )
  }

  "readJsonFromPackage" should "return an error for an invalid json" in {
    val s3 = mock[DAS3Client[IO]]
    val invalidJson = "invalid"
    val downloadResponse = Flux.just(ByteBuffer.wrap(invalidJson.getBytes()))
    val metadataId = UUID.randomUUID()
    when(s3.download(ArgumentMatchers.eq("upload"), ArgumentMatchers.eq(metadataId.toString)))
      .thenReturn(IO(downloadResponse))
    val fileProcessor = new FileProcessor("download", "upload", "ref", s3, UUIDGenerator().uuidGenerator)

    val ex = intercept[ParsingFailure] {
      fileProcessor.readJsonFromPackage(metadataId).unsafeRunSync()
    }

    ex.getMessage should equal(
      """expected json value got 'invali...' (line 1, column 1)""".stripMargin
    )
  }

  "readJsonFromPackage" should "return an error if the download from s3 fails" in {
    val s3 = mock[DAS3Client[IO]]
    val metadataId = UUID.randomUUID()
    when(s3.download(ArgumentMatchers.eq("upload"), ArgumentMatchers.eq(metadataId.toString)))
      .thenThrow(new Exception("Error downloading metadata file"))
    val fileProcessor = new FileProcessor("download", "upload", "ref", s3, UUIDGenerator().uuidGenerator)

    val ex = intercept[Exception] {
      fileProcessor.readJsonFromPackage(metadataId).unsafeRunSync()
    }
    ex.getMessage should equal("Error downloading metadata file")
  }

  val department: Option[String] = Option("Department")
  val series: Option[String] = Option("Series")
  val trimmedUri: String = "http://example.com/id/abcde"
  val withoutCourt: Option[ParsedUri] = Option(ParsedUri(None, trimmedUri))
  val withCourt: Option[ParsedUri] = Option(ParsedUri(Option("TEST-COURT"), trimmedUri))
  val notMatched = "Court Documents (court not matched)"
  val unknown = "Court Documents (court unknown)"

  val citeTable: TableFor2[Option[String], List[IdField]] = Table(
    ("potentialCite", "idFields"),
    (None, Nil),
    (Option("cite"), List(IdField("Code", "cite"), IdField("Cite", "cite")))
  )

  val urlDepartmentAndSeriesTable: TableFor6[Option[String], Option[String], Boolean, Option[ParsedUri], String, Boolean] = Table(
    ("department", "series", "includeBagInfo", "url", "expectedFolderName", "titleExpected"),
    (department, series, true, withCourt, trimmedUri, true),
    (department, None, false, withCourt, notMatched, false),
    (None, series, false, withCourt, notMatched, false),
    (None, None, false, withCourt, notMatched, false),
    (department, series, true, withoutCourt, unknown, false),
    (department, None, false, withoutCourt, unknown, false),
    (None, series, false, withoutCourt, unknown, false),
    (None, None, false, withoutCourt, unknown, false),
    (department, series, true, None, unknown, false),
    (department, None, false, None, unknown, false),
    (None, series, false, None, unknown, false),
    (None, None, false, None, unknown, false)
  )

  val treNameTable: TableFor4[Option[String], String, String, String] = Table(
    ("treName", "treFileName", "expectedFolderTitle", "expectedAssetTitle"),
    (Option("Test title"), "fileName.txt", "Test title", "fileName.txt"),
    (None, "fileName.txt", "", "fileName.txt"),
    (Option("Press Summary of test"), "Press Summary of test.txt", "test", "Press Summary of test.txt")
  )

  forAll(citeTable) { (potentialCite, idFields) =>
    {
      forAll(treNameTable) { (treName, treFileName, expectedFolderTitle, expectedAssetTitle) =>
        forAll(urlDepartmentAndSeriesTable) { (department, series, _, parsedUri, expectedFolderName, titleExpected) =>
          "createMetadataFiles" should s"generate the correct bagit Metadata with $expectedFolderTitle, $expectedAssetTitle and $idFields" +
            s"for $department, $series, $parsedUri and TRE name $treName" in {
              val fileId = UUID.randomUUID()
              val metadataId = UUID.randomUUID()
              val folderId = uuids.head
              val assetId = uuids.last
              val fileName = treFileName.split("\\.").dropRight(1).mkString(".")
              val folderTitle = if (titleExpected) Option(expectedFolderTitle) else None
              val folder =
                BagitFolderMetadataObject(folderId, None, folderTitle, expectedFolderName, idFields)
              val asset = BagitAssetMetadataObject(assetId, Option(folderId), expectedAssetTitle, expectedAssetTitle)
              val files = List(
                BagitFileMetadataObject(fileId, Option(assetId), fileName, 1, treFileName, 1),
                BagitFileMetadataObject(metadataId, Option(assetId), "", 2, "metadataFileName.txt", 2)
              )
              val expectedBagitMetadataObjects: List[BagitMetadataObject] = List(folder, asset) ++ files

              val fileProcessor =
                new FileProcessor("download", "upload", "ref", mock[DAS3Client[IO]], UUIDGenerator().uuidGenerator)
              val fileInfo = FileInfo(fileId, 1, treFileName, "fileChecksum")
              val metadataFileInfo = FileInfo(metadataId, 2, "metadataFileName.txt", "metadataChecksum")

              val bagitMetadataObjects =
                fileProcessor
                  .createMetadataFiles(fileInfo, metadataFileInfo, parsedUri, potentialCite, treName, department, series)

              bagitMetadataObjects should equal(expectedBagitMetadataObjects)
            }
        }
      }
    }
  }

  forAll(citeTable) { (_, idFields) =>
    {
      forAll(treNameTable) { (treName, treFileName, expectedFolderTitle, expectedAssetTitle) =>
        forAll(urlDepartmentAndSeriesTable) {
          (department, series, includeBagInfo, parsedUri, expectedFolderName, titleExpected) =>
            "createBagitFiles" should s"upload the correct bagit files with $expectedFolderTitle, $expectedAssetTitle and $idFields" +
              s"for $department, $series, $parsedUri and TRE name $treName" in {
                val fileId = UUID.randomUUID()
                val metadataId = UUID.randomUUID()
                val s3 = mock[DAS3Client[IO]]
                val folderId = uuids.head
                val assetId = uuids.last
                val fileName = treFileName.split("\\.").dropRight(1).mkString(".")
                val folderTitle = if (titleExpected) Option(expectedFolderTitle) else None
                val folder =
                  BagitFolderMetadataObject(folderId, None, folderTitle, expectedFolderName, idFields)
                val asset = BagitAssetMetadataObject(assetId, Option(folderId), expectedAssetTitle, expectedAssetTitle)
                val files = List(
                  BagitFileMetadataObject(fileId, Option(assetId), fileName, 1, treFileName, 1),
                  BagitFileMetadataObject(metadataId, Option(assetId), "", 2, "metadataFileName.txt", 2)
                )
                val metadataJsonList: List[BagitMetadataObject] = List(folder, asset) ++ files
                val metadataJsonString = metadataJsonList.asJson.printWith(Printer.noSpaces)

                val bagitTxtContent =
                  """BagIt-Version: 1.0
              |Tag-File-Character-Encoding: UTF-8""".stripMargin

                val manifestString =
                  s"""fileChecksum data/$fileId
               |metadataChecksum data/$metadataId""".stripMargin

                val metadataChecksum = "989681"
                val bagitChecksum = "989683"
                val manifestChecksum = "989684"
                val bagInfoChecksum = "989685"
                val tagManifestChecksum = "989686"

                val tagManifest = List(
                  s"$bagitChecksum bagit.txt",
                  s"$manifestChecksum manifest-sha256.txt",
                  s"$metadataChecksum metadata.json"
                )
                val tagManifestString = (if (includeBagInfo) {
                                           s"$bagInfoChecksum bag-info.txt" :: tagManifest
                                         } else {
                                           tagManifest
                                         }).mkString("\n")

                def mockUpload(
                    fileName: String,
                    fileString: String,
                    checksum: String
                ): ArgumentMatcher[Publisher[ByteBuffer]] = {
                  val publisherMatcher = new ArgumentMatcher[Publisher[ByteBuffer]] {
                    override def matches(argument: Publisher[ByteBuffer]): Boolean = {
                      val arg = argument
                        .toStreamBuffered[IO](1024)
                        .flatMap(bf => Stream.chunk(Chunk.byteBuffer(bf)))
                        .through(text.utf8.decode)
                        .compile
                        .string
                        .unsafeRunSync()
                      arg == fileString
                    }
                  }
                  when(
                    s3.upload(
                      ArgumentMatchers.eq("upload"),
                      ArgumentMatchers.eq(s"ref/$fileName"),
                      any[Long],
                      argThat(publisherMatcher)
                    )
                  )
                    .thenReturn(IO(completedUpload(Option(checksum))))
                  publisherMatcher
                }

                mockUpload("metadata.json", metadataJsonString, metadataChecksum)
                mockUpload("bagit.txt", bagitTxtContent, bagitChecksum)
                mockUpload("manifest-sha256.txt", manifestString, manifestChecksum)
                if (includeBagInfo) {
                  val bagInfoString = s"Department: ${department.get}\nSeries: ${series.get}"
                  mockUpload("bag-info.txt", bagInfoString, bagInfoChecksum)
                }
                mockUpload("tagmanifest-sha256.txt", tagManifestString, tagManifestChecksum)

                val fileProcessor = new FileProcessor("download", "upload", "ref", s3, UUIDGenerator().uuidGenerator)
                val fileInfo = FileInfo(fileId, 1, treFileName, "fileChecksum")
                val metadataFileInfo = FileInfo(metadataId, 2, "metadataFileName.txt", "metadataChecksum")

                val tagManifestChecksumResult =
                  fileProcessor
                    .createBagitFiles(metadataJsonList, fileInfo, metadataFileInfo, department, series)
                    .unsafeRunSync()

                tagManifestChecksumResult should equal(tagManifestChecksum)
              }
        }
      }
    }
  }

  "createBagitFiles" should "throw an error if there is an error uploading to s3" in {
    val s3 = mock[DAS3Client[IO]]

    when(s3.upload(any[String], any[String], any[Long], any[Publisher[ByteBuffer]])) thenThrow new RuntimeException(
      "Upload failed"
    )

    val fileProcessor = new FileProcessor("download", "upload", "ref", s3, UUIDGenerator().uuidGenerator)
    val fileInfo = FileInfo(UUID.randomUUID(), 1, "fileName", "fileChecksum")
    val metadataFileInfo = FileInfo(UUID.randomUUID(), 2, "metadataFileName", "metadataChecksum")

    val ex = intercept[Exception] {
      fileProcessor
        .createBagitFiles(
          List(
            BagitFileMetadataObject(
              UUID.randomUUID(),
              Option(UUID.fromString("49e4a726-6297-4f8e-8867-fb50bd5acd86")),
              "",
              2,
              "metadataFileName.txt",
              2
            )
          ),
          fileInfo,
          metadataFileInfo,
          Option("department"),
          Option("series")
        )
        .unsafeRunSync()
    }
    ex.getMessage should equal("Upload failed")
  }

}
