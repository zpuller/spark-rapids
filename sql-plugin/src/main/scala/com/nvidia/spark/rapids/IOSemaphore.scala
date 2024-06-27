/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
