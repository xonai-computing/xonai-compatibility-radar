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

import com.xonai.spark.sql.parser.plans.SparkPlan

import java.util
import scala.util.Try

/**
 * Summary of a Spark application.
 */
case class QualificationApplicationSummary(
    id: String,
    name: String,
    sparkVersion: String,
    completed: Boolean,
    totalTaskTime: Long,
    sqlTaskTime: Long,
    supportedTaskTime: Option[Long],
    sqlQuerySummaries: Seq[QualificationSQLQuerySummary]
)

/**
 * Summary of a SQL query.
 */
case class QualificationSQLQuerySummary(
    queryId: Long,
    completed: Boolean,
    sqlTaskTime: Long,
    supportedTaskTime: Option[Long]
)

/**
 * Summary of SQL node support.
 */
case class QualificationSQLNodeSupport(
    applicationId: String,
    nodeName: String,
    nodeCount: Int,
    impactTaskTime: Long,
    support: String
)

/**
 * Represents a Spark application.
 */
case class QualificationApplication(
    applicationId: String,
    applicationName: String,
    sparkVersion: String,
    completed: Boolean,
    stageTaskTime: Map[Int, Long],
    accumulatorStages: util.HashMap[Long, Set[Int]],
    sqlQueries: Seq[QualificationSQLQuery]
) {

  def getTaskTime(stageIds: Set[Int]): Long = {
    stageIds.flatMap(stageTaskTime.get).sum
  }
}

/**
 * Represents a Spark SQL query.
 */
case class QualificationSQLQuery(
    queryId: Long,
    completed: Boolean,
    stages: Set[Int],
    physicalPlan: Try[SparkPlan]
)
