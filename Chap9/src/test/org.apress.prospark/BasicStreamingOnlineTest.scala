package org.apress.prospark

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{StreamingLinearRegressionWithSGD, LabeledPoint}
import org.apache.spark.rdd.RDD.doubleRDDToDoubleRDDFunctions
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

trait BasicStreamingOnlineTest {
  val lines = mutable.Queue[RDD[String]]()

  def algorithm(ssc: StreamingContext, unfilteredSubstream: DStream[String]): Unit

  def main(args: Array[String]) {

    if (args.length != 2) {
      System.err.println(
        "Usage: BasicManualTest <appname> <batchInterval>")
      System.exit(1)
    }
    val Seq(appName, batchInterval) = args.toSeq

    val conf = new SparkConf().setMaster("local[4]")
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)

    val ssc = new StreamingContext(conf, Seconds(batchInterval.toInt))
    val unfilteredSubstream = ssc.queueStream(lines)
    algorithm(ssc, unfilteredSubstream)
    ssc.start()
    ssc.awaitTermination()
  }

}
