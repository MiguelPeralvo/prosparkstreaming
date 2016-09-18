import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apress.prospark.{BasicStreamingOnlineTest, RddHelper}

object ChiSquareTest extends BasicStreamingOnlineTest{

  def algorithm(ssc: StreamingContext, unfilteredSubstream: DStream[String]): Unit = {
    var iterationCounter = 0

    val substream =  unfilteredSubstream.filter(!_.contains("NaN"))
      .map(_.split(" "))
      .filter(f => f(1) != "0")
      .map(f => f.map(f => f.toDouble))

    val rddFiles = RddHelper.calculateRddFiles(ssc)
    lines.enqueue(RddHelper.take(rddFiles, 50000, iterationCounter*50000))

    substream.map(f => Array(f(1).toDouble, f(2).toDouble, f(4).toDouble, f(5).toDouble, f(6).toDouble))
      .filter(f => f(0) == 4.0 || f(0) == 5.0)
      .map(f => LabeledPoint(f(0), Vectors.dense(f.slice(1, 5))))
      .foreachRDD(rdd => {
        if (!rdd.isEmpty()) {
          Statistics.chiSqTest(rdd).zipWithIndex.foreach(v => println("%s, column no. %d".format(v._1, v._2)))
        }
        iterationCounter +=1
        println(s"Iteration Counter: ${iterationCounter}")
        lines.enqueue(RddHelper.take(rddFiles, 50000, iterationCounter*50000))
      })

  }
}
