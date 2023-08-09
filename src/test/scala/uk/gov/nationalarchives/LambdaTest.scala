package uk.gov.nationalarchives

import cats.effect.IO
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.http.RequestMethod
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.sfn.SfnAsyncClient
import uk.gov.nationalarchives.FileProcessor.{TREInput, TREInputParameters}
import uk.gov.nationalarchives.SeriesMapper.{Court, Output}
import upickle.default._

import java.net.URI
import java.util.UUID
import scala.jdk.CollectionConverters._

class LambdaTest extends AnyFlatSpec with BeforeAndAfterEach {
  implicit val treInputWriter: Writer[TREInput] = macroW[TREInput]
  implicit val treInputParamsWriter: Writer[TREInputParameters] = macroW[TREInputParameters]
  implicit val sfnRequestReader: Reader[SFNRequest] = macroR[SFNRequest]
  implicit val outputReader: Reader[Output] = macroR[Output]

  case class SFNRequest(stateMachineArn: String, name: String, input: String)

  val reference = "TEST-REFERENCE"
  val uuids: List[String] = List(
    "c7e6b27f-5778-4da8-9b83-1b64bbccbd03",
    "61ac0166-ccdf-48c4-800f-29e5fba2efda",
    "4e6bac50-d80a-4c68-bd92-772ac9701f14",
    "c2e7866e-5e94-4b4e-a49f-043ad937c18a",
    "27a9a6bb-a023-4cab-8592-39b44761a30a"
  )

  val metadataFiles: List[String] = List(
    "asset-metadata.csv",
    "bagit.txt",
    "bag-info.txt",
    "file-metadata.csv",
    "folder-metadata.csv",
    "manifest-sha256.txt",
    "tagmanifest-sha256.txt"
  )

  override def beforeEach(): Unit = {
    sfnServer.resetAll()
    s3Server.resetAll()
    sfnServer.start()
    s3Server.start()
  }

  val s3Server = new WireMockServer(9003)
  val sfnServer = new WireMockServer(9004)
  val testOutputBucket = "outputBucket"
  val inputBucket = "inputBucket"
  val packageAvailable: TREInput = TREInput(TREInputParameters("status", "TEST-REFERENCE", inputBucket, "test.tar.gz"))
  val event: SQSEvent = createEvent(write(packageAvailable))

  case class IngestParserTest() extends Lambda {
    private val s3AsyncClient: S3AsyncClient = S3AsyncClient
      .crtBuilder()
      .endpointOverride(URI.create("http://localhost:9003"))
      .region(Region.EU_WEST_2)
      .build()

    private val sfnAsyncClient: SfnAsyncClient = SfnAsyncClient
      .builder()
      .endpointOverride(URI.create("http://localhost:9004"))
      .region(Region.EU_WEST_2)
      .build()

    override val s3: DAS3Client[IO] = DAS3Client[IO](s3AsyncClient)
    override val sfn: DASFNClient[IO] = new DASFNClient(sfnAsyncClient)
    override val seriesMapper: SeriesMapper = new SeriesMapper(Set(Court("cite", "TEST", "TEST SERIES")))
    var count: Int = -1
    val uuidsIterator: Iterator[String] = uuids.iterator

    override val randomUuidGenerator: () => UUID = () => UUID.fromString(uuidsIterator.next())
  }

  def createEvent(body: String): SQSEvent = {
    val sqsEvent = new SQSEvent()
    val record = new SQSMessage()
    record.setBody(body)
    sqsEvent.setRecords(List(record).asJava)
    sqsEvent
  }

  def stubAWSRequests(
      inputBucket: String,
      fileDownloadBytes: Option[Array[Byte]] = None,
      metadataJsonOpt: Option[String] = None
  ): Unit = {
    val bytes = fileDownloadBytes.getOrElse(getClass.getResourceAsStream("/files/test.tar.gz").readAllBytes())
    sfnServer.stubFor(post(urlEqualTo("/")).willReturn(ok()))
    s3Server.stubFor(
      head(urlEqualTo(s"/$inputBucket/test.tar.gz"))
        .willReturn(
          ok()
            .withHeader("Content-Length", bytes.length.toString)
            .withHeader("ETag", "abcde")
        )
    )
    s3Server.stubFor(
      get(urlEqualTo(s"/$inputBucket/test.tar.gz"))
        .willReturn(ok.withBody(bytes))
    )

    val metadataJson: String = metadataJsonOpt.getOrElse(
      s"""{"parameters":{"TRE":{"reference":"$reference","payload":{"filename":"Test.docx","sha256":"abcde"}},"PARSER":{"cite":"cite","uri":"https://example.com","court":"test","date":"2023-07-26","name":"test"}}}"""
    )

    metadataFiles.foreach { file =>
      s3Server.stubFor(
        put(urlEqualTo(s"/$testOutputBucket/$reference/$file"))
          .willReturn(ok())
      )
    }

    uuids.foreach { uuid =>
      s3Server.stubFor(
        put(urlEqualTo(s"/$testOutputBucket/$uuid"))
          .willReturn(ok())
      )
      s3Server.stubFor(
        put(urlEqualTo(s"/$testOutputBucket/$reference/data/$uuid"))
          .willReturn(ok())
      )
      s3Server.stubFor(
        get(urlEqualTo(s"/$testOutputBucket/$uuid"))
          .willReturn(okJson(metadataJson))
      )
      s3Server.stubFor(
        head(urlEqualTo(s"/$testOutputBucket/$uuid"))
          .willReturn(
            ok()
              .withHeader("Content-Length", bytes.length.toString)
              .withHeader("ETag", "abcde")
          )
      )
    }
  }

  "the lambda" should "download the .tar.gz file from the input bucket" in {
    stubAWSRequests(inputBucket)
    IngestParserTest().handleRequest(event, null)
    val serveEvents = s3Server.getAllServeEvents.asScala
    serveEvents.count(e =>
      e.getRequest.getUrl == s"/$inputBucket/test.tar.gz" && e.getRequest.getMethod == RequestMethod.GET
    ) should equal(1)
  }

  "the lambda" should "write the bagit package to the output bucket" in {
    stubAWSRequests(inputBucket)
    IngestParserTest().handleRequest(event, null)
    val serveEvents = s3Server.getAllServeEvents.asScala
    def countPutEvents(name: String) = serveEvents.count(e =>
      e.getRequest.getUrl == s"/$testOutputBucket/$reference/$name" && e.getRequest.getMethod == RequestMethod.PUT
    )
    metadataFiles.foreach(file => countPutEvents(file) should equal(1))
    countPutEvents(s"data/${uuids.head}") should equal(1)
    countPutEvents(s"data/${uuids(1)}") should equal(1)
  }

  "the lambda" should "start the state machine execution with the correct parameters" in {
    stubAWSRequests(inputBucket)
    IngestParserTest().handleRequest(event, null)

    val sfnEvent = sfnServer.getAllServeEvents.asScala.head
    val sfnRequest = read[SFNRequest](sfnEvent.getRequest.getBodyAsString)
    val input = read[Output](sfnRequest.input)

    sfnRequest.stateMachineArn should equal("arn:aws:states:eu-west-2:123456789:stateMachine:StateMachineName")
    sfnRequest.name should equal(s"TEST-REFERENCE-${uuids(4)}")

    input.series should equal("TEST SERIES")
    input.department should equal("TEST")
    input.batchId should equal("TEST-REFERENCE")
    input.s3Prefix should equal("TEST-REFERENCE/")
    input.s3Bucket should equal(testOutputBucket)
  }

  "the lambda" should "error if the input json is invalid" in {
    val event = createEvent("{}")
    val ex = intercept[Exception] {
      IngestParserTest().handleRequest(event, null)
    }
    ex.getMessage should equal("missing keys in dictionary: parameters at index 1")
  }

  "the lambda" should "error if the json in the metadata file is invalid" in {
    stubAWSRequests(inputBucket, metadataJsonOpt = Option("{}"))
    val ex = intercept[Exception] {
      IngestParserTest().handleRequest(event, null)
    }
    ex.getMessage should equal("missing keys in dictionary: parameters at index 1")
  }

  "the lambda" should "error if S3 is unavailable" in {
    s3Server.stop()
    val ex = intercept[Exception] {
      IngestParserTest().handleRequest(event, null)
    }
    ex.getMessage should equal("Failed to send the request: socket connection refused.")
  }
}
