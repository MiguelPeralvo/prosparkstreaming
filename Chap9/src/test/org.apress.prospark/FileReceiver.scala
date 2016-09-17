package org.apress.prospark

import java.io.{BufferedReader, InputStreamReader}
import java.io.File
import java.io.FileInputStream
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver._


class FileReceiver(ratePerSec: Int, dataFilePath: String) extends Receiver[Array[Byte]](StorageLevel.MEMORY_AND_DISK_2) {

  def onStart() {
    // Start the thread that receives data over a connection
    new Thread("File Receiver") {
      override def run() { receive() }
    }.start()
  }

  def onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself isStopped() returns false
  }


  private def receive() {

    try {
      val reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(dataFilePath)), "UTF-8"))
      var input = reader.readLine()

      while (!isStopped && input != null) {
        store(input.getBytes("UTF-8"))
        Thread.sleep((1000.toDouble / ratePerSec).toInt)
        input = reader.readLine()
      }
      reader.close()

      //TODO: Improve approach to avoid sleep.
      //TODO: Implement a trait based on actors to send a "finished" stream signal to the main consumer (only on testing)
      while(!isStopped()) Thread.sleep(500)
      // Restart in an attempt to connect again when server is active again
      //restart("Trying to connect again")
    } catch {
      case t: Throwable =>
        // restart if there is any other error
        restart("Error receiving data", t)
    }
  }
}
