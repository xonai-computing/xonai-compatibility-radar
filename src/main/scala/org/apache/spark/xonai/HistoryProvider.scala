/*
 * Copyright 2026 XONAI LTD
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

package org.apache.spark.xonai

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.EventLogFileReader
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.Status
import org.apache.spark.scheduler.{ReplayListenerBus, SparkListener}
import org.apache.spark.sql.execution.ui.SQLHistoryServerPlugin
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.status.{AppStatusListener, ElementTrackingStore}
import org.apache.spark.util.Utils
import org.apache.spark.util.kvstore.{InMemoryStore, KVStore}

import java.io.FileNotFoundException

/**
 * Based on [[org.apache.spark.deploy.history.FsHistoryProvider.rebuildAppStore]].
 */
class HistoryProvider(fs: FileSystem, path: Path) extends Logging {

  val store: InMemoryStore = new InMemoryStore()

  // Disable async updates to ensure analytics listener can query store.
  // Ensure no data is discarded.
  private val replayConf = new SparkConf(false)
    .set(Status.ASYNC_TRACKING_ENABLED, false)
    .set(Status.MAX_RETAINED_JOBS, Integer.MAX_VALUE)
    .set(Status.MAX_RETAINED_STAGES, Integer.MAX_VALUE)
    .set(Status.MAX_RETAINED_TASKS_PER_STAGE, Integer.MAX_VALUE)
    .set(Status.MAX_RETAINED_DEAD_EXECUTORS, Integer.MAX_VALUE)
    .set(Status.MAX_RETAINED_ROOT_NODES, Integer.MAX_VALUE)
    .set(StaticSQLConf.UI_RETAINED_EXECUTIONS, Integer.MAX_VALUE)
    .set(StaticSQLConf.STREAMING_UI_RETAINED_PROGRESS_UPDATES, Integer.MAX_VALUE)
    .set(StaticSQLConf.STREAMING_UI_RETAINED_QUERIES, Integer.MAX_VALUE)

  private val trackingStore = new ElementTrackingStore(store, replayConf)

  private val replayBus = new ReplayListenerBus()

  private val statusListener = new AppStatusListener(
    trackingStore,
    replayConf,
    live = false
  )
  replayBus.addListener(statusListener)

  private val sqlStatusListener = new SQLHistoryServerPlugin()
    .createListeners(
      replayConf,
      trackingStore
    )
    .head
  replayBus.addListener(sqlStatusListener)

  def addListener(listener: SparkListener): Unit = {
    replayBus.addListener(listener)
  }

  def rebuild[T](build: KVStore => T): Option[T] = {
    try {
      EventLogFileReader(fs, path).map(rebuild(_, build))
    } catch {
      case _: FileNotFoundException =>
        None
    }
  }

  private def rebuild[T](reader: EventLogFileReader, build: KVStore => T): T = {
    var continueReplay = true
    try {
      logInfo(s"Parsing ${reader.rootPath}")

      val maybeTruncated = !reader.completed
      reader.listEventLogFiles.foreach { file =>
        // stop replaying next log files if ReplayListenerBus indicates some error or halt
        if (continueReplay) {
          Utils.tryWithResource(EventLogFileReader.openEventLog(file.getPath, fs)) { in =>
            continueReplay = replayBus.replay(in, file.getPath.toString, maybeTruncated)
          }
        }
      }

      val result = build.apply(store)
      trackingStore.close(closeParent = true)
      logInfo(s"Finished parsing ${reader.rootPath}")

      result
    } catch {
      case e: Exception =>
        Utils.tryLogNonFatalError {
          trackingStore.close(closeParent = true)
        }
        throw e
    }
  }
}
