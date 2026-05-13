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

import com.xonai.spark.sql.parser.trees.TreeNode
import com.xonai.spark.sql.support.{EngineSupport, NodeSupport, StageSupport}

/**
 * Based on application raw information, checks compatibility with an engine and breaks down task
 * time in: total time, SQL time, and supported time.
 */
class Qualifier(engine: EngineSupport) {

  def tag(application: QualificationApplication): Unit = {
    application.sqlQueries.foreach {
      _.physicalPlan.foreach { plan =>
        engine.tag(plan)
        StageSupport.tag(plan, application.accumulatorStages)
        StageSupport.propagateTags(plan)
      }
    }
  }

  def getNodeSupport(application: QualificationApplication): Seq[QualificationSQLNodeSupport] = {
    application
      .sqlQueries
      .flatMap(_.physicalPlan.toOption.toSeq.flatMap(_.allPlans))
      .flatMap { plan =>
        val stageIds = StageSupport.getStageIds(plan)
        val nodes: Seq[Any] = plan +: plan.expressions.flatMap(_.map(identity))
        nodes.map(_ -> stageIds)
      }
      .groupBy { case (node: TreeNode[_], _) =>
        (
          node.nodeName,
          NodeSupport.getIsolatedSupport(node)
        )
      }
      .toSeq
      .map { case ((nodeName, nodeSupport), nodes) =>
        val support =
          if (nodeSupport.isSupported) {
            "Supported"
          } else {
            nodeSupport.unsupportedReasons.mkString(":")
          }

        QualificationSQLNodeSupport(
          application.applicationId,
          nodeName,
          nodes.length,
          impactTaskTime = application.getTaskTime(nodes.flatMap(_._2).toSet),
          support
        )
      }
  }

  def summarize(application: QualificationApplication): QualificationApplicationSummary = {
    // Tag each SQL node with engine support and associated stages.
    // Compute supported task time based on fully supported stages.
    val querySummaries =
      application.sqlQueries.map { query =>
        val supportedStages =
          query.physicalPlan.toOption.map { plan =>
            query.stages.intersect(StageSupport.getSupportedStages(plan))
          }

        val summary =
          QualificationSQLQuerySummary(
            queryId = query.queryId,
            completed = query.completed,
            sqlTaskTime = application.getTaskTime(query.stages),
            supportedTaskTime = supportedStages.map(application.getTaskTime)
          )

        (summary, supportedStages)
      }

    val sqlStages = application.sqlQueries.flatMap(_.stages).toSet
    val supportedStages =
      if (querySummaries.exists(_._2.isEmpty)) {
        None
      } else {
        Some(querySummaries.flatMap(_._2.get).toSet)
      }

    QualificationApplicationSummary(
      application.applicationId,
      application.applicationName,
      application.sparkVersion,
      application.completed,
      application.stageTaskTime.values.sum,
      sqlTaskTime = application.getTaskTime(sqlStages),
      supportedTaskTime = supportedStages.map(application.getTaskTime),
      querySummaries.map(_._1)
    )
  }
}
