package uk.gov.nationalarchives

import cats.effect.unsafe.implicits.global
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor2}
import uk.gov.nationalarchives.SeriesMapper.seriesMap

class SeriesMapperTest extends AnyFlatSpec with MockitoSugar with TableDrivenPropertyChecks {

  "seriesMap" should "have 10 entries" in {
    seriesMap.size should equal(10)
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
      val output = seriesMapper.createOutput("upload", "batch", Option(s"2023 $court SUFFIX")).unsafeRunSync()
      output.department.get should equal(series.split(" ").head)
      output.series.get should equal(series)
    }
  }

  "createOutput" should "return the correct output if one series is found" in {
    val seriesMapper = SeriesMapper()
    val ex = intercept[RuntimeException] {
      seriesMapper.createOutput("upload", "batch", Option(s"2023 EWFC UKEAT")).unsafeRunSync()
    }
    val expectedMessage = s"2 entries found when looking up series for cite 2023 EWFC UKEAT and batchId batch"
    ex.getMessage should equal(expectedMessage)
  }

  "createOutput" should "return an error if more than one series is found" in {
    val seriesMapper = SeriesMapper()
    val ex = intercept[RuntimeException] {
      seriesMapper.createOutput("upload", "batch", Option(s"2023 EWFC UKEAT")).unsafeRunSync()
    }
    val expectedMessage = s"2 entries found when looking up series for cite 2023 EWFC UKEAT and batchId batch"
    ex.getMessage should equal(expectedMessage)
  }

  "createOutput" should "return an error if no series are found" in {
    val seriesMapper = SeriesMapper()
    val ex = intercept[RuntimeException] {
      seriesMapper.createOutput("upload", "batch", Option(s"2023 PREFIX SUFFIX")).unsafeRunSync()
    }
    val expectedMessage = s"0 entries found when looking up series for cite 2023 PREFIX SUFFIX and batchId batch"
    ex.getMessage should equal(expectedMessage)
  }
}
