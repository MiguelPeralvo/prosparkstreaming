package org.apress.prospark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.doubleRDDToDoubleRDDFunctions
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

object LinearRegressionApp {

  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println(
        "Usage: LinearRegressionApp <appname> <batchInterval> <hostname> <port>")
      System.exit(1)
    }
    val Seq(appName, batchInterval, hostname, port) = args.toSeq

    val conf = new SparkConf().setMaster("local[4]")
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)

    val ssc = new StreamingContext(conf, Seconds(batchInterval.toInt))

    val substream = ssc.socketTextStream(hostname, port.toInt)
      .filter(!_.contains("NaN"))
      .map(_.split(" "))
      .filter(f => f(1) != "0")

    val datastream = substream.map(f => Array(f(2).toDouble, f(3).toDouble, f(4).toDouble, f(5).toDouble, f(6).toDouble))
      .map(f => LabeledPoint(f(0), Vectors.dense(f.slice(1, 5))))
    val test = datastream.transform(rdd => rdd.randomSplit(Array(0.3, 0.7))(0))
    val train = datastream.transformWith(test, (r1: RDD[LabeledPoint], r2: RDD[LabeledPoint]) => r1.subtract(r2)).cache()
    val model = new StreamingLinearRegressionWithSGD()
      .setInitialWeights(Vectors.zeros(4))
      .setStepSize(0.0001)
      .setNumIterations(1)

    model.trainOn(train)
    model.predictOnValues(test.map(v => (v.label, v.features))).foreachRDD(rdd => println("MSE: %f".format(rdd
      .map(v => math.pow((v._1 - v._2), 2)).mean())))

    ssc.start()
    ssc.awaitTermination()
  }

}