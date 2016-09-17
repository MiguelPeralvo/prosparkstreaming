package org.apress.prospark
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apress.prospark.Common.IndexedString

object RddHelper {

  def take(rdd: RDD[IndexedString], range: Long , offset: Long):RDD[String] =
  {
    rdd.filter(x=> {x._2>=offset && x._2 < (offset+range)}).map(x=>x._1)
  }

  def calculateRddFiles(ssc: StreamingContext):RDD[IndexedString] = {
    def rddFilesWithoutIndex = for {
      fileSuffix <- (1 to 9)
      rddFile: RDD[String] = ssc.sparkContext.textFile(
        s"src/test/resources/test_data/subject10${fileSuffix}.dat")
    } yield rddFile

    rddFilesWithoutIndex.reduce((a, b) => a.union(b)).zipWithIndex()
  }
}
