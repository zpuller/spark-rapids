package com.nvidia.spark.rapids

import scala.io.Source

object NetworkThroughput {
  var prevBytesReceived: Long = 0
  var prevTimestamp: Long = System.currentTimeMillis()

  def getBytesReceived(interface: String): Option[Long] = {
    val procNetDevFile = "/proc/net/dev"

    try {
      val source = Source.fromFile(procNetDevFile)
      val lines = source.getLines().toList
      source.close()

      // Find the line corresponding to the specified interface
      val interfaceLine = lines.find(_.contains(interface))

      interfaceLine match {
        case Some(line) =>
          // Extract the bytes received value from the line
          val bytesReceived = line.split("\\s+")(1).toLong
          Some(bytesReceived)
        case None =>
          println(s"Interface $interface not found.")
          None
      }
    } catch {
      case e: Exception =>
        println(s"An error occurred: ${e.getMessage}")
        None
    }
  }

  def getThroughput(interface: String): Double = {
    // Extract bytes received from the interface line
    val bytesReceived = getBytesReceived(interface).get / 1024.0 // Convert bytes to kilobytes

    val currentTimestamp = System.currentTimeMillis()
    val timeDiffMillis = currentTimestamp - prevTimestamp
    val bytesDiff = bytesReceived - prevBytesReceived

    // Update previous values
    prevBytesReceived = bytesReceived.toLong
    prevTimestamp = currentTimestamp

    if (timeDiffMillis > 0) {
      bytesDiff / (timeDiffMillis / 1000.0)
    } else {
      0.0
    }
  }
}
