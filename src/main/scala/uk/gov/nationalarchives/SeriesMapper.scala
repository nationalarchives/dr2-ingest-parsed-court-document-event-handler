package uk.gov.nationalarchives

import cats.effect._
import uk.gov.nationalarchives.SeriesMapper._
import upickle.default._

class SeriesMapper(courts: Set[Court]) {
  def createOutput(uploadBucket: String, batchId: String, potentialCite: Option[String]): IO[Output] = {
    potentialCite
      .map { cite =>
        val filteredSeries = courts.filter(court => cite.contains(court.code))
        filteredSeries.size match {
          case 1 =>
            IO {
              val court = filteredSeries.head
              Output(batchId, uploadBucket, s"$batchId/", Option(court.dept), Option(court.series))
            }
          case size: Int =>
            IO.raiseError(
              new RuntimeException(s"$size entries found when looking up series for cite $cite and batchId $batchId")
            )
        }
      }
      .getOrElse(IO(Output(batchId, uploadBucket, s"$batchId/", None, None)))

  }
}

object SeriesMapper {
  implicit val outputWriter: Writer[Output] = macroW[Output]

  case class Output(
      batchId: String,
      s3Bucket: String,
      s3Prefix: String,
      department: Option[String],
      series: Option[String]
  )

  case class Court(code: String, dept: String, series: String)

  def apply(): SeriesMapper = new SeriesMapper(seriesMap)

  val seriesMap: Set[Court] = Set(
    Court("EWCA", "J", "J 347"),
    Court("EWHC", "J", "J 348"),
    Court("EWCOP", "J", "J 349"),
    Court("EWFC", "J", "J 350"),
    Court("UKPC", "PCAP", "PCAP 16"),
    Court("UKSC", "UKSC", "UKSC 2"),
    Court("UKUT", "LE", "LE 9"),
    Court("UKEAT", "LE", "LE 10"),
    Court("UKFTT", "LE", "LE 11"),
    Court("UKET", "LE", "LE 12")
  )
}
