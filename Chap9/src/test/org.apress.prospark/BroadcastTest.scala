import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apress.prospark.{BasicStreamingOnlineTest, RddHelper}

object BroadcastTest extends BasicStreamingOnlineTest{

  def algorithm(ssc: StreamingContext, unfilteredSubstream: DStream[String]): Unit = {
    var iterationCounter: Long = 0
    val substream =  unfilteredSubstream.filter(!_.contains("NaN"))
      .map(_.split(" "))
      .filter(f => f(1) != "0")
      .map(f => f.map(f => f.toDouble))

    val rddFiles = RddHelper.calculateRddFiles(ssc)
    lines.enqueue(RddHelper.take(rddFiles, 50000, iterationCounter*50000))
    var broadcastCounter = ssc.sparkContext.broadcast(iterationCounter)
    var broadcastTable = ssc.sparkContext.broadcast(RddHelper.take(rddFiles, 1000, iterationCounter*10000))
    val numInputMessages = ssc.sparkContext.accumulator(0L, "Messages consumed")
    val numOutputMessages = ssc.sparkContext.accumulator(0L, "Messages produced")


    substream.map(f => Vectors.dense(f.slice(1, 5))).map{
      x =>
      numInputMessages += 1
      x
    }.repartition(2).foreachRDD(rdd => {
      val stats = Statistics.colStats(rdd)
      println("Count: " + stats.count)
      println("Max: " + stats.max.toArray.mkString(" "))
      println("Min: " + stats.min.toArray.mkString(" "))
      println("Mean: " + stats.mean.toArray.mkString(" "))
      println("L1-Norm: " + stats.normL1.toArray.mkString(" "))
      println("L2-Norm: " + stats.normL2.toArray.mkString(" "))
      println("Number of non-zeros: " + stats.numNonzeros.toArray.mkString(" "))
      println("Varience: " + stats.variance.toArray.mkString(" "))
      lines.enqueue(RddHelper.take(rddFiles, 50000, iterationCounter*50000))

      println(s"numInputMessages: ${numInputMessages}")
      println(s"numOutputMessages: ${numOutputMessages}")

      iterationCounter +=1
      broadcastCounter.unpersist(true)
      broadcastCounter = ssc.sparkContext.broadcast(iterationCounter)
      println(s"Iteration Counter: ${iterationCounter}")

      broadcastTable.unpersist(true)
      broadcastTable = ssc.sparkContext.broadcast(RddHelper.take(rddFiles, 1000, iterationCounter*10000))
      println(s"Iteration broadcastTable: ${broadcastTable.value}")


      rdd.foreachPartition{ partitionOfRecords =>
        numOutputMessages += 1
        println(s"broadcastCounter.value: ${broadcastCounter.value}")
      }
    })

  }
}
