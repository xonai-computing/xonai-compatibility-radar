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

package com.xonai.spark.sql.status

import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent, SparkListenerTaskEnd}
import org.apache.spark.sql.execution.ui.{SparkListenerSQLAdaptiveExecutionUpdate, SparkListenerSQLExecutionStart}
import org.apache.spark.util.kvstore.KVStore

import java.util

/**
 * [[org.apache.spark.sql.execution.ui.SQLAppStatusListener]].
 *
 * Spark's [[org.apache.spark.sql.execution.ui.SQLExecutionUIData]] class does not keep the
 * `metadata` field from [[org.apache.spark.sql.execution.SparkPlanInfo]].
 */
class ExtendedSQLAppStatusListener(store: KVStore) extends SparkListener {

  val accumulatorStages = new util.HashMap[Long, Set[Int]]

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case e: SparkListenerSQLExecutionStart =>
        onExecutionStart(e)
      case e: SparkListenerSQLAdaptiveExecutionUpdate =>
        onAdaptiveExecutionUpdate(e)
      case _ =>
    }
  }

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
    val stageId = event.stageId
    event.taskInfo.accumulables.foreach { accumulable =>
      val accumulatorId = accumulable.id
      accumulatorStages.computeIfPresent(accumulatorId, (_, set) => set + stageId)
      accumulatorStages.putIfAbsent(accumulatorId, Set(stageId))
    }
  }

  private def onExecutionStart(event: SparkListenerSQLExecutionStart): Unit = {
    val execution = new ExtendedSQLExecution(event.executionId, event.sparkPlanInfo)
    store.write(execution)
  }

  private def onAdaptiveExecutionUpdate(event: SparkListenerSQLAdaptiveExecutionUpdate): Unit = {
    val execution = new ExtendedSQLExecution(event.executionId, event.sparkPlanInfo)
    store.write(execution)
  }
}
