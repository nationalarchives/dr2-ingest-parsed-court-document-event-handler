package uk.gov.nationalarchives

import cats.effect.unsafe.implicits.global
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor2}
import uk.gov.nationalarchives.SeriesMapper.seriesMap

class SeriesMapperTest extends AnyFlatSpec with MockitoSugar with TableDrivenPropertyChecks {

  "seriesMap" should "have 10 entries" in {
    seriesMap.size should equal(11)
  }

  val courtToSeries: TableFor2[String, String] = Table(
    ("court", "series"),
    ("EWCA", "J 347"),
    ("EWHC", "J 348"),
    ("EWCOP", "J 349"),
    ("EWFC", "J 350"),
    ("UKPC", "PCAP 16"),
    ("UKSC", "UKSC 2"),
    ("UKUT", "LE 9"),
    ("UKEAT", "LE 10"),
    ("UKFTT", "LE 11"),
    ("UKET", "LE 12")
  )

  forAll(courtToSeries) { (court, series) =>
    "createOutput" should s"return $series for court $court" in {
      val seriesMapper = SeriesMapper()
      val output =
        seriesMapper.createOutput("upload", "batch", Option(court), skipSeriesLookup = false).unsafeRunSync()
      output.department.get should equal(series.split(" ").head)
      output.series.get should equal(series)
    }
  }

  "createOutput" should "return an error if a court does not yield a series and 'skipSeriesLookup' is set to false" in {
    val seriesMapper = SeriesMapper()
    val ex = intercept[Exception] {
      seriesMapper.createOutput("upload", "batch", Option("invalidCourt"), skipSeriesLookup = false).unsafeRunSync()
    }
    val expectedMessage = s"Cannot find series and department for court invalidCourt for batchId batch"
    ex.getMessage should equal(expectedMessage)
  }

  "createOutput" should "return an empty department and series if a court does not yield a series but 'skipSeriesLookup' is set to true" in {
    val seriesMapper = SeriesMapper()
    val output =
      seriesMapper.createOutput("upload", "batch", Option("invalidCourt"), skipSeriesLookup = true).unsafeRunSync()

    output.series should equal(None)
    output.department should equal(None)
  }

  "createOutput" should "return an empty department and series if the court is missing" in {
    val seriesMapper = SeriesMapper()
    val output = seriesMapper.createOutput("upload", "batch", None, skipSeriesLookup = false).unsafeRunSync()

    output.series should equal(None)
    output.department should equal(None)
  }
}
