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

package com.xonai.spark.radar.qualification

import com.xonai.spark.sql.parser.SparkPlanParser
import com.xonai.spark.sql.status.{ExtendedSQLAppStatusListener, ExtendedSQLAppStatusStore}
import com.xonai.spark.sql.status.api.v1.StageData
import org.apache.spark.sql.execution.ui.SQLAppStatusStore
import org.apache.spark.util.kvstore.KVStore
import org.apache.spark.xonai.status.TrampolineAppStatusStore

import scala.util.Try

/**
 * Retrieves and derives data to be used by the Xonai Qualification.
 */
class QualificationStore(store: KVStore, listener: ExtendedSQLAppStatusListener) {

  private val appStore = new TrampolineAppStatusStore(store)
  private val sqlStore = new SQLAppStatusStore(store)
  private val extendedSqlStore = new ExtendedSQLAppStatusStore(store)

  def getApplication: QualificationApplication = {
    val application = appStore.applicationInfo()
    val stageAttempts = getStageAttempts
    val sqlQueryExecutions = getSQLQueryExecutions

    QualificationApplication(
      application.id,
      application.name,
      application.attempts.head.appSparkVersion,
      application.attempts.head.completed,
      stageAttempts
        .groupBy(_.stageId)
        .iterator
        .map {
          case (key, values) =>
            (key, values.map(_.executorRunTime).sum)
        }
        .toMap,
      listener.accumulatorStages,
      sqlQueryExecutions
    )
  }

  /**
   * Returns all SQL query executions.
   */
  private def getSQLQueryExecutions: Seq[QualificationSQLQuery] = {
    extendedSqlStore.executionsList().map { extended =>
      val executionId = extended.executionId
      val execution = sqlStore.execution(executionId)

      val parser = new SparkPlanParser(
        execution.map(_.physicalPlanDescription),
        execution.map(_.metricValues).flatMap(Option(_)).getOrElse(Map.empty)
      )
      val plan = Try(parser.parse(extended.sparkPlanInfo))

      val completed = execution.exists(_.completionTime.isDefined)
      val stages = execution.fold(Set.empty[Int])(_.stages)

      QualificationSQLQuery(executionId, completed, stages, plan)
    }
  }

  /**
   * Returns all stage attempts.
   */
  private def getStageAttempts: Seq[StageData] = {
    appStore.jobsList().flatMap { job =>
      job.stageIds.flatMap { stageId =>
        appStore.stageData(stageId)
      }
    }
  }
}
