package org.apress.prospark
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}


class FileEventsStream(eventsPerSecond:Int, filePath: String) extends BaseProducer{

  def getDStream(providedSsc: StreamingContext) =
  {
    val fileReceiver = new FileReceiver(eventsPerSecond, filePath)
    providedSsc.receiverStream(fileReceiver)
  }
}

object FileEventsStream {
  def apply(eventsPerSecond: Int, filePath: String): FileEventsStream = new FileEventsStream(eventsPerSecond, filePath)
}

