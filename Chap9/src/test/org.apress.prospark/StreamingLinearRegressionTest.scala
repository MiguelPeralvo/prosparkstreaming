package org.apress.prospark

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{StreamingLinearRegressionWithSGD, LabeledPoint}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

object StreamingLinearRegressionTest extends BasicStreamingOnlineTest{

  def algorithm(ssc: StreamingContext, unfilteredSubstream: DStream[String]): Unit = {
    var iterationCounter = 0

    val substream =  unfilteredSubstream.filter(!_.contains("NaN"))
      .map(_.split(" "))
      .filter(f => f(1) != "0")


    val datastream = substream.map(f => Array(f(2).toDouble, f(3).toDouble, f(4).toDouble, f(5).toDouble, f(6).toDouble))
      .map(f => LabeledPoint(f(0), Vectors.dense(f.slice(1, 5))))
    val test = datastream.transform(rdd => rdd.randomSplit(Array(0.3, 0.7), seed = 11L)(0))
    val train = datastream.transformWith(test, (r1: RDD[LabeledPoint], r2: RDD[LabeledPoint]) => r1.subtract(r2)).cache()
    val model = new StreamingLinearRegressionWithSGD()
      .setInitialWeights(Vectors.zeros(4))
      .setStepSize(0.0001)
      .setNumIterations(1)

    model.trainOn(train)
    test.foreachRDD(rdd => println(s"Size of test data: ${rdd.count()}"))
    train.foreachRDD(rdd => println(s"Size of train data: ${rdd.count()}"))


    val rddFiles = RddHelper.calculateRddFiles(ssc)
    lines.enqueue(RddHelper.take(rddFiles, 50000, iterationCounter*50000))

    val prediction = model.predictOnValues(test.map(v => (v.label, v.features)))
    prediction.foreachRDD{
      rdd =>
        println(s"Size: ${rdd.count()}. MSE: %f".format(rdd
          .map(v => math.pow((v._1 - v._2), 2)).mean()))
        iterationCounter +=1
        println(s"Iteration Counter: ${iterationCounter}")
        lines.enqueue(RddHelper.take(rddFiles, 50000, iterationCounter*50000))
    }

    prediction.print()

  }
}
