import org.apache.spark.mllib.clustering.{StreamingKMeans, KMeans, StreamingKMeansModel, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, StreamingLinearRegressionWithSGD}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apress.prospark.{BasicStreamingOnlineTest, RddHelper}

/**
  * Created by Miguel on 17/09/2016.
  */
object StreamingKMeansTest extends BasicStreamingOnlineTest{

  def algorithm(ssc: StreamingContext, unfilteredSubstream: DStream[String]): Unit = {
    var iterationCounter = 0

    val substream =  unfilteredSubstream.filter(!_.contains("NaN"))
      .map(_.split(" "))
      .filter(f => f(1) != "0")


    val orientationStream = substream
      .map(f => Seq(1, 4, 5, 6, 10, 11, 12, 20, 21, 22, 26, 27, 28, 36, 37, 38, 42, 43, 44).map(i => f(i)).toArray)
      .map(arr => arr.map(_.toDouble))
      .filter(f => f(0) == 1.0 || f(0) == 2.0 || f(0) == 3.0)
      .map(f => LabeledPoint(f(0), Vectors.dense(f.slice(1, f.length))))
    val test = orientationStream.transform(rdd => rdd.randomSplit(Array(0.3, 0.7))(0))
    val train = orientationStream.transformWith(test, (r1: RDD[LabeledPoint], r2: RDD[LabeledPoint]) => r1.subtract(r2)).cache()

    val model = new StreamingKMeans()
      .setK(3)
      .setDecayFactor(0)
      .setRandomCenters(18, 0.0)

    model.trainOn(train.map(v => v.features))
    test.foreachRDD(rdd => println(s"Size of test data: ${rdd.count()}"))
    train.foreachRDD(rdd => println(s"Size of train data: ${rdd.count()}"))


    val rddFiles = RddHelper.calculateRddFiles(ssc)
    lines.enqueue(RddHelper.take(rddFiles, 50000, iterationCounter*50000))

    val prediction = model.predictOnValues(test.map(v => (v.label, v.features)))

    prediction.foreachRDD{
      rdd =>
        //println(s"Size: ${rdd.count()}. MSE: %f".format(rdd
          //.map(v => math.pow((v._1 - v._2), 2)).mean()))
        iterationCounter +=1
        println(s"Iteration Counter: ${iterationCounter}")
        lines.enqueue(RddHelper.take(rddFiles, 50000, iterationCounter*50000))
    }

    prediction.print()

  }
}
