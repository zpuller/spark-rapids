package com.nvidia.spark.rapids

import java.util.concurrent.Semaphore

import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf

object IOSemaphore extends Logging {
  private val threadHoldsSemaphore: ThreadLocal[Boolean] = ThreadLocal.withInitial(() => false)

  private val concurrency = SQLConf.get
    .getConfString(RapidsConf.CONCURRENT_IO_TASKS.key, "4").toInt

  private val semaphore = new Semaphore(concurrency)

  def acquire(): Unit = {
    if (threadHoldsSemaphore.get()) {
      return
    }

    logWarning(s"Thread ${Thread.currentThread.getId} trying to acquire IOSemaphore")
    semaphore.acquire()
    logWarning(s"Thread ${Thread.currentThread.getId} successfully acquired IOSemaphore")
    threadHoldsSemaphore.set(true)
  }

  def release(): Unit = {
    if (!threadHoldsSemaphore.get()) {
      return
    }

    logWarning(s"Thread ${Thread.currentThread.getId} releasing IOSemaphore")
    semaphore.release()
    threadHoldsSemaphore.set(false)
  }
}
