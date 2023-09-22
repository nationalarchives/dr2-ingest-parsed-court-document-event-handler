package uk.gov.nationalarchives

import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage

import java.io.ByteArrayInputStream
import scala.jdk.CollectionConverters._

object LambdaRunner extends App {

  val json =
    """{"parameters": {"status": "OK", "reference": "TDR-2023-RMX", "s3Bucket": "dp-sam-test-bucket", "s3Key": "TRE-TDR-2023-RMW.tar.gz"}}"""
  val body =
    "{\"parameters\": {\"status\": \"OK\", \"reference\": \"TDR-2023-RMW\", \"s3Bucket\": \"intg-ingest-parsed-court-document-event-handler-test-input\", \"s3Key\": \"TRE-TDR-2023-RMW.tar.gz\"}}"
  val event = new SQSEvent()
  val record = new SQSMessage()
  record.setBody(body)

  event.setRecords(List(record).asJava)
  val is = new ByteArrayInputStream(json.getBytes())
  new Lambda().handleRequest(event, null)
}
