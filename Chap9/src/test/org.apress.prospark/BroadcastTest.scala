import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apress.prospark.{BasicStreamingOnlineTest, RddHelper}

object BroadcastTest extends BasicStreamingOnlineTest{

  def algorithm(ssc: StreamingContext, unfilteredSubstream: DStream[String]): Unit = {
    var iterationCounter = 0

    val substream =  unfilteredSubstream.filter(!_.contains("NaN"))
      .map(_.split(" "))
      .filter(f => f(1) != "0")
      .map(f => f.map(f => f.toDouble))

    val rddFiles = RddHelper.calculateRddFiles(ssc)
    lines.enqueue(RddHelper.take(rddFiles, 50000, iterationCounter*50000))
    val broadcastVar = ssc.sparkContext.broadcast(iterationCounter)

    substream.map(f => Vectors.dense(f.slice(1, 5))).repartition(2).foreachRDD(rdd => {
      val stats = Statistics.colStats(rdd)
      println("Count: " + stats.count)
      println("Max: " + stats.max.toArray.mkString(" "))
      println("Min: " + stats.min.toArray.mkString(" "))
      println("Mean: " + stats.mean.toArray.mkString(" "))
      println("L1-Norm: " + stats.normL1.toArray.mkString(" "))
      println("L2-Norm: " + stats.normL2.toArray.mkString(" "))
      println("Number of non-zeros: " + stats.numNonzeros.toArray.mkString(" "))
      println("Varience: " + stats.variance.toArray.mkString(" "))
      iterationCounter +=1
      println(s"Iteration Counter: ${iterationCounter}")
      lines.enqueue(RddHelper.take(rddFiles, 50000, iterationCounter*50000))

      rdd.foreachPartition{ partitionOfRecords =>
        println(broadcastVar.value)
      }

    })

  }
}
