package uk.gov.nationalarchives

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.interop.reactivestreams._
import fs2.{Chunk, Stream, text}
import org.mockito.ArgumentMatchers._
import org.mockito.{ArgumentMatcher, ArgumentMatchers, MockitoSugar}
import org.reactivestreams.Publisher
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatest.prop.TableDrivenPropertyChecks
import reactor.core.publisher.Flux
import software.amazon.awssdk.services.s3.model.PutObjectResponse
import software.amazon.awssdk.transfer.s3.model.CompletedUpload
import uk.gov.nationalarchives.FileProcessor._
import upickle.default._

import java.nio.ByteBuffer
import java.util.{Base64, HexFormat, UUID}

class FileProcessorTest extends AnyFlatSpec with MockitoSugar with TableDrivenPropertyChecks {
  val testTarGz: Array[Byte] = getClass.getResourceAsStream("/files/test.tar.gz").readAllBytes()
  val publisher: Flux[ByteBuffer] = Flux.just(ByteBuffer.wrap(testTarGz))
  val reference = "TEST-REFERENCE"

  val metadataJson =
    s"""{"parameters":{"TRE":{"reference":"$reference","payload":{"filename":"Test.docx","sha256":"abcde"}},"PARSER":{"cite":"cite","uri":"https://example.com","court":"test","date":"2023-07-26","name":"test"}}}"""

  case class UUIDGenerator() {
    var count: Int = -1
    val uuids: List[String] = List("6e827e19-6a33-46c3-8730-b242c203d8c1", "49e4a726-6297-4f8e-8867-fb50bd5acd86")

    val uuidGenerator: () => UUID = () => {
      count = count + 1
      UUID.fromString(uuids(count))
    }
  }

  def checksum(cs: Option[String]): String =
    cs.map(c => Base64.getEncoder.encode(HexFormat.of().parseHex(c)).map(_.toChar).mkString).orNull

  def completedUpload(c: Option[String] = None): CompletedUpload = {
    val putObjectResponse = PutObjectResponse.builder.checksumSHA256(checksum(c)).build
    CompletedUpload.builder.response(putObjectResponse).build
  }

  "copyFilesToBucket" should "return the correct file metadata for a valid tar.gz file" in {
    val generator = UUIDGenerator()
    val uuids = generator.uuids
    val s3 = mock[DAS3Client[IO]]
    val docxCompletedUpload = completedUpload(Option("abcdef"))

    val metadataCompletedUpload = completedUpload(Option("123456"))

    when(s3.download(ArgumentMatchers.eq("download"), ArgumentMatchers.eq("key"))).thenReturn(IO(publisher))
    when(s3.upload(any[String], ArgumentMatchers.eq(uuids.head), any[Long], any[Publisher[ByteBuffer]]))
      .thenReturn(IO(docxCompletedUpload))
    when(s3.upload(any[String], ArgumentMatchers.eq(uuids.last), any[Long], any[Publisher[ByteBuffer]]))
      .thenReturn(IO(metadataCompletedUpload))

    val fileProcessor = new FileProcessor("download", "upload", "ref", s3, generator.uuidGenerator)
    val res = fileProcessor.copyFilesToBucket("key").unsafeRunSync()

    res.size should equal(2)
    val docx = res.get(s"$reference/Test.docx")
    val metadata = res.get(s"$reference/TRE-$reference-metadata.json")
    docx.isDefined should be(true)
    metadata.isDefined should be(true)

    val docxInfo = docx.get
    docxInfo.id should equal(UUID.fromString(uuids.head))
    docxInfo.fileName should equal("Test.docx")
    docxInfo.fileSize should equal(15684)
    docxInfo.checksum should equal("abcdef")

    val metadataInfo = metadata.get
    metadataInfo.id should equal(UUID.fromString(uuids.last))
    metadataInfo.fileName should equal(s"TRE-$reference-metadata.json")
    metadataInfo.fileSize should equal(215)
    metadataInfo.checksum should equal("123456")
  }

  "copyFilesToBucket" should "return an error if the downloaded file is not a valid tar.gz" in {
    val s3 = mock[DAS3Client[IO]]
    when(s3.download(ArgumentMatchers.eq("download"), ArgumentMatchers.eq("key")))
      .thenReturn(IO(Flux.just(ByteBuffer.wrap("invalid".getBytes))))

    val fileProcessor = new FileProcessor("download", "upload", "ref", s3, UUIDGenerator().uuidGenerator)
    val ex = intercept[Exception] {
      fileProcessor.copyFilesToBucket("key").unsafeRunSync()
    }
    ex.getMessage should equal("UpStream failed")
  }

  "copyFilesToBucket" should "return an error if the file download fails" in {
    val s3 = mock[DAS3Client[IO]]
    when(s3.download(ArgumentMatchers.eq("download"), ArgumentMatchers.eq("key")))
      .thenThrow(new RuntimeException("Error downloading files"))

    val fileProcessor = new FileProcessor("download", "upload", "ref", s3, UUIDGenerator().uuidGenerator)
    val ex = intercept[Exception] {
      fileProcessor.copyFilesToBucket("key").unsafeRunSync()
    }
    ex.getMessage should equal("Error downloading files")
  }

  "copyFilesToBucket" should "return an error if the upload fails" in {
    val s3 = mock[DAS3Client[IO]]

    when(s3.download(ArgumentMatchers.eq("download"), ArgumentMatchers.eq("key"))).thenReturn(IO(publisher))
    when(s3.upload(any[String], any[String], any[Long], any[Publisher[ByteBuffer]])) thenThrow new RuntimeException(
      "Upload failed"
    )

    val fileProcessor = new FileProcessor("download", "upload", "ref", s3, UUIDGenerator().uuidGenerator)
    val ex = intercept[Exception] {
      fileProcessor.copyFilesToBucket("key").unsafeRunSync()
    }
    ex.getMessage should equal("Upload failed")
  }

  "copyFilesToBucket" should "return an empty checksum if a checksum is not returned from S3" in {
    val generator = UUIDGenerator()
    val uuids = generator.uuids
    val s3 = mock[DAS3Client[IO]]
    val docxCompletedUpload = completedUpload()

    val metadataCompletedUpload = completedUpload()

    when(s3.download(ArgumentMatchers.eq("download"), ArgumentMatchers.eq("key"))).thenReturn(IO(publisher))
    when(s3.upload(any[String], ArgumentMatchers.eq(uuids.head), any[Long], any[Publisher[ByteBuffer]]))
      .thenReturn(IO(docxCompletedUpload))
    when(s3.upload(any[String], ArgumentMatchers.eq(uuids.last), any[Long], any[Publisher[ByteBuffer]]))
      .thenReturn(IO(metadataCompletedUpload))

    val fileProcessor = new FileProcessor("download", "upload", "ref", s3, generator.uuidGenerator)
    val res = fileProcessor.copyFilesToBucket("key").unsafeRunSync()

    res(s"$reference/Test.docx").checksum should equal("")
    res(s"$reference/TRE-$reference-metadata.json").checksum should equal("")
  }

  "readJsonFromPackage" should "return the correct object for valid json" in {
    val s3 = mock[DAS3Client[IO]]
    val downloadResponse = Flux.just(ByteBuffer.wrap(metadataJson.getBytes()))
    val metadataId = UUID.randomUUID()
    when(s3.download(ArgumentMatchers.eq("upload"), ArgumentMatchers.eq(metadataId.toString)))
      .thenReturn(IO(downloadResponse))
    val expectedMetadata = read[TREMetadata](metadataJson)
    val fileProcessor = new FileProcessor("download", "upload", "ref", s3, UUIDGenerator().uuidGenerator)

    val res = fileProcessor.readJsonFromPackage(metadataId).unsafeRunSync()
    res should equal(expectedMetadata)
  }

  "readJsonFromPackage" should "return an error for invalid json" in {
    val s3 = mock[DAS3Client[IO]]
    val invalidJson = "invalid"
    val downloadResponse = Flux.just(ByteBuffer.wrap(invalidJson.getBytes()))
    val metadataId = UUID.randomUUID()
    when(s3.download(ArgumentMatchers.eq("upload"), ArgumentMatchers.eq(metadataId.toString)))
      .thenReturn(IO(downloadResponse))
    val fileProcessor = new FileProcessor("download", "upload", "ref", s3, UUIDGenerator().uuidGenerator)

    val ex = intercept[Exception] {
      fileProcessor.readJsonFromPackage(metadataId).unsafeRunSync()
    }
    ex.getMessage should equal("""expected json value got "i" at index 0""")
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

  "createMetadataFiles" should "upload the correct bagit files" in {
    val fileId = UUID.randomUUID()
    val generator = UUIDGenerator()
    val metadataId = UUID.randomUUID()
    val department = "TEST"
    val series = "TEST SERIES"
    val s3 = mock[DAS3Client[IO]]
    val folderString =
      s"""identifier,path,name,title
         |${generator.uuids.head},,TEST-CITE,
         |""".stripMargin
    val assetString =
      s"""identifier,parentPath,title
         |${generator.uuids.last},TEST-CITE,
         |""".stripMargin

    val fileString =
      s"""identifier,parentAsset,name,fileSize,title
         |$fileId,${generator.uuids.last},fileName,1,
         |$metadataId,${generator.uuids.last},metadataFileName,2,
         |""".stripMargin

    val bagitString =
      """BagIt-Version: 1.0
        |Tag-File-Character-Encoding: UTF-8
        |""".stripMargin

    val manifestString =
      s"""fileChecksum data/$fileId
         |metadataChecksum data/$metadataId""".stripMargin

    val bagInfoString = s"Department: $department\nSeries: $series"

    val folderMetadataChecksum = "989680"
    val assetMetadataChecksum = "989681"
    val fileMetadataChecksum = "989682"
    val bagitChecksum = "989683"
    val manifestChecksum = "989684"
    val bagInfoChecksum = "989685"
    val tagManifestChecksum = "989686"

    val tagManifest =
      s"""$assetMetadataChecksum asset-metadata.csv
         |$bagInfoChecksum bag-info.txt
         |$bagitChecksum bagit.txt
         |$fileMetadataChecksum file-metadata.csv
         |$folderMetadataChecksum folder-metadata.csv
         |$manifestChecksum manifest-sha256.txt""".stripMargin

    def mockUpload(fileName: String, fileString: String, checksum: String): ArgumentMatcher[Publisher[ByteBuffer]] = {
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

    mockUpload("folder-metadata.csv", folderString, folderMetadataChecksum)
    mockUpload("asset-metadata.csv", assetString, assetMetadataChecksum)
    mockUpload("file-metadata.csv", fileString, fileMetadataChecksum)
    mockUpload("bagit.txt", bagitString, bagitChecksum)
    mockUpload("manifest-sha256.txt", manifestString, manifestChecksum)
    mockUpload("bag-info.txt", bagInfoString, bagInfoChecksum)
    mockUpload("tagmanifest-sha256.txt", tagManifest, tagManifestChecksum)

    val fileProcessor = new FileProcessor("download", "upload", "ref", s3, UUIDGenerator().uuidGenerator)
    val fileInfo = FileInfo(fileId, 1, "fileName", "fileChecksum")
    val metadataFileInfo = FileInfo(metadataId, 2, "metadataFileName", "metadataChecksum")
    val cite = "TEST-CITE"
    val tagManifestChecksumResult =
      fileProcessor.createMetadataFiles(fileInfo, metadataFileInfo, cite, department, series).unsafeRunSync()

    tagManifestChecksumResult should equal(tagManifestChecksum)
  }

  "createMetadataFiles" should "throw an error if there is an error uploading to s3" in {
    val s3 = mock[DAS3Client[IO]]

    when(s3.upload(any[String], any[String], any[Long], any[Publisher[ByteBuffer]])) thenThrow new RuntimeException(
      "Upload failed"
    )

    val fileProcessor = new FileProcessor("download", "upload", "ref", s3, UUIDGenerator().uuidGenerator)
    val fileInfo = FileInfo(UUID.randomUUID(), 1, "fileName", "fileChecksum")
    val metadataFileInfo = FileInfo(UUID.randomUUID(), 2, "metadataFileName", "metadataChecksum")
    val cite = "TEST-CITE"

    val ex = intercept[Exception] {
      fileProcessor.createMetadataFiles(fileInfo, metadataFileInfo, cite, "department", "series").unsafeRunSync()
    }
    ex.getMessage should equal("Upload failed")
  }

}
