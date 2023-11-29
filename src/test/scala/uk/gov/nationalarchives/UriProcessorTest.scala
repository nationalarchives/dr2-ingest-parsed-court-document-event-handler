package uk.gov.nationalarchives

import cats.effect.unsafe.implicits.global
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor2}
import uk.gov.nationalarchives.UriProcessor.ParsedUri

class UriProcessorTest extends AnyFlatSpec with MockitoSugar with TableDrivenPropertyChecks {
  private val uriTable: TableFor2[Option[String], Option[ParsedUri]] = Table(
    ("uri", "expectedCourtAndUri"),
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

  private val uriWithoutPressSummaryTable: TableFor2[Option[String], String] = Table(
    ("uri", "description"),
    (Some("http://example.com/id/ijkl/2025/1/doc-type/3"), "does not contain '/press-summary' and"),
    (None, "does not exist")
  )

  private val judgmentNameTable: TableFor2[Option[String], String] = Table(
    ("judgmentName", "description"),
    (Some("This is Press Summary of a test judgment"), "contains but does not start with 'Press Summary of '"),
    (Some("Press Summary office a test judgment"), "starts with 'Press Summary of' without space after"),
    (None, "does not exist"),
    (Some("Press Summary of a test judgment"), "starts with 'Press Summary of '")
  )

  private val nonPressSummaryJudgmentNameTable =
    judgmentNameTable.filter { case (_, judgmentName) => !judgmentName.startsWith("starts with 'Press Summary of '") }

  forAll(nonPressSummaryJudgmentNameTable) { (judgmentNameDoesNotStartWithPressSummaryOf, judgmentNameDescription) =>
    "verifyJudgmentNameStartsWithPressSummaryOfIfInUri" should s"return an exception if uri contains '/press-summary' but " +
      s"judgment name $judgmentNameDescription" in {
        val uriProcessor = new UriProcessor(Some("http://example.com/id/ijkl/2025/1/doc-type/3/press-summary"))
        val ioException =
          uriProcessor.verifyJudgmentNameStartsWithPressSummaryOfIfInUri(judgmentNameDoesNotStartWithPressSummaryOf)

        ioException.attempt
          .unsafeRunSync()
          .left
          .foreach(_.getMessage should be("URI contains '/press-summary' but file does not start with 'Press Summary of '"))
      }
  }

  forAll(uriWithoutPressSummaryTable) { (uri, uriDescription) =>
    forAll(judgmentNameTable) { (judgmentName, judgmentNameDescription) =>
      "verifyJudgmentNameStartsWithPressSummaryOfIfInUri" should s"not throw an exception if uri $uriDescription " +
        s"judgment name $judgmentNameDescription" in {
          val uriProcessor = new UriProcessor(uri)
          uriProcessor.verifyJudgmentNameStartsWithPressSummaryOfIfInUri(judgmentName)
        }
    }
  }

  "verifyJudgmentNameStartsWithPressSummaryOfIfInUri" should "not throw an exception if uri contains '/press-summary' and " +
    "judgment name starts with 'Press Summary of '" in {
      val uri = Some("http://example.com/id/ijkl/2025/1/doc-type/3/press-summary")
      val uriProcessor = new UriProcessor(uri)
      val judgmentNameDoesNotStartWithPressSummaryOf = Some("Press Summary of a test judgment")
      uriProcessor.verifyJudgmentNameStartsWithPressSummaryOfIfInUri(judgmentNameDoesNotStartWithPressSummaryOf)
    }

  forAll(uriTable) { (uri, expectedCourtAndUri) =>
    "getCourtAndUriWithoutDocType" should s"parse the uri $uri and return the court and uri without doc type" in {
      val uriProcessor = new UriProcessor(uri)
      uriProcessor.getCourtAndUriWithoutDocType.unsafeRunSync() should equal(expectedCourtAndUri)
    }
  }

  "getCourtAndUriWithoutDocType" should "return an error if the url cannot be trimmed because of a missing year" in {
    val uriProcessor = new UriProcessor(Option("http://example.com/id/mnop/qrst"))
    val ex = intercept[RuntimeException] {
      uriProcessor.getCourtAndUriWithoutDocType.unsafeRunSync()
    }
    ex.getMessage should equal(
      "Failure trying to trim off the doc type for http://example.com/id/mnop/qrst. Is the year missing?"
    )
  }
}
