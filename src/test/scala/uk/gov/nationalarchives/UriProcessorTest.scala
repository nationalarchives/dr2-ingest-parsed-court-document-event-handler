package uk.gov.nationalarchives

import cats.effect.unsafe.implicits.global
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor2}
import uk.gov.nationalarchives.UriProcessor.ParsedUri

class UriProcessorTest extends AnyFlatSpec with MockitoSugar with TableDrivenPropertyChecks {
  private val uriTable: TableFor2[Option[String], Option[ParsedUri]] = Table(
    ("uri", "expectedCiteAndUri"),
    (Option("http://example.com/id/abcd/2023/1"), Option(ParsedUri(Option("abcd"), "http://example.com/id/abcd/2023/1"))),
    (
      Option("http://example.com/id/abcd/efgh/2024/123"),
      Option(ParsedUri(Option("abcd"), "http://example.com/id/abcd/efgh/2024/123"))
    ),
    (
      Option("http://example.com/id/ijkl/2025/1/doc-type/3"),
      Option(ParsedUri(Option("ijkl"), "http://example.com/id/ijkl/2025/1"))
    ),
    (
      Option("http://example.com/id/mnop/qrst/2026/567/different-doc-type/8"),
      Option(ParsedUri(Option("mnop"), "http://example.com/id/mnop/qrst/2026/567"))
    ),
    (Option("http://example.com/id/abcd/efgh/2024/"), Option(ParsedUri(Option("abcd"), "http://example.com/id/abcd/efgh/2024/"))),
    (None, None)
  )

  forAll(uriTable) { (uri, expectedCiteAndUri) =>
    "getCiteAndUriWithoutDocType" should s"parse the uri $uri and return the cite and uri without doc type" in {
      val uriProcessor = new UriProcessor(uri)
      uriProcessor.getCiteAndUriWithoutDocType.unsafeRunSync() should equal(expectedCiteAndUri)
    }
  }

  "getCiteAndUriWithoutDocType" should "return an error if the url cannot be trimmed because of a missing year" in {
    val uriProcessor = new UriProcessor(Option("http://example.com/id/mnop/qrst"))
    val ex = intercept[RuntimeException] {
      uriProcessor.getCiteAndUriWithoutDocType.unsafeRunSync()
    }
    ex.getMessage should equal(
      "Failure trying to trim off the doc type for http://example.com/id/mnop/qrst. Is the year missing?"
    )
  }
}
