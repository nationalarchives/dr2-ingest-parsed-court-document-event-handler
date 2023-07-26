package uk.gov.nationalarchives

import cats.effect._
import uk.gov.nationalarchives.SeriesMapper.Output
import upickle.default._

class SeriesMapper(seriesMap: Map[String, String]) {
  def createOutput(uploadBucket: String, batchId: String, cite: String): IO[Output] = {
    val filteredSeries = seriesMap.filter(ds => cite.contains(ds._1))
    filteredSeries.size match {
      case 1 =>
        IO {
          val series = filteredSeries.head._2
          val department = series.split(" ").head
          Output(batchId, uploadBucket, s"$batchId/", department, series)
        }
      case size: Int =>
        IO.raiseError(
          new RuntimeException(s"$size entries found when looking up series for cite $cite and batchId $batchId")
        )
    }
  }
}
object SeriesMapper {
  implicit val outputWriter: Writer[Output] = macroW[Output]
  case class Output(batchId: String, s3Bucket: String, s3Prefix: String, department: String, series: String)

  def apply(): SeriesMapper = new SeriesMapper(seriesMap)

  val seriesMap: Map[String, String] = Map(
    "EWCA" -> "J 347",
    "EWHC" -> "J 348",
    "EWCOP" -> "J 349",
    "EWFC" -> "J 350",
    "UKPC" -> "PCAP 16",
    "UKSC" -> "UKSC 2",
    "UKUT" -> "LE 9",
    "UKEAT" -> "LE 10",
    "UKFTT" -> "LE 11",
    "UKET" -> "LE 12"
  )
}
