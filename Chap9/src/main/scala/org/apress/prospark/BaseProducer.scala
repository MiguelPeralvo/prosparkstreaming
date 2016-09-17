package org.apress.prospark

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream

trait BaseProducer {
  def getDStream(ssc: StreamingContext): InputDStream[Array[Byte]]
}
