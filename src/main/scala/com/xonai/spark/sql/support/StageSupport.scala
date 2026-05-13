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

package com.xonai.spark.sql.support

import com.xonai.spark.sql.parser.plans.{Exchange, SparkPlan}
import com.xonai.spark.sql.parser.trees.TreeNodeTag

import java.util

/**
 * Enriches [[com.xonai.spark.sql.parser.plans.SparkPlan]] with stage information.
 */
object StageSupport {

  private val StageIdsTag = new TreeNodeTag[Set[Int]]("StageIds")

  /**
   * Returns set of supported stage ids given a plan tagged with [[NodeSupport]] and stage ids.
   */
  def getSupportedStages(plan: SparkPlan): Set[Int] = {
    plan
      .allPlans
      .flatMap { plan =>
        val stageIds = StageSupport.getStageIds(plan)
        val support = NodeSupport.getComposedSupport(plan)
        stageIds.map(_ -> support)
      }
      .groupBy(_._1)
      .map { case (stageId, group) =>
        (
          stageId,
          group.map(_._2).fold(NodeSupport.Supported)(_ + _)
        )
      }
      .filter(_._2.isSupported)
      .keySet
  }

  /**
   * Tags each plan with a set of stage ids, given a mapping between accumulator ids and stage ids.
   */
  def tag(plan: SparkPlan, accumulatorStages: util.HashMap[Long, Set[Int]]): Unit = {
    val stageIds =
      plan
        .metrics
        .values
        .flatMap(metric => accumulatorStages.getOrDefault(metric.accumulatorId, Set.empty))
        .toSet
    setStageIds(plan, stageIds)

    plan.subqueries.foreach(tag(_, accumulatorStages))
    plan.children.foreach(tag(_, accumulatorStages))
  }

  /**
   * Propagates stage ids tags to plans without any stage ids.
   */
  def propagateTags(plan: SparkPlan): Unit = {
    plan.foreachUp { plan =>
      lazy val children = plan.children
      if (getStageIds(plan).isEmpty && !children.exists(_.isInstanceOf[Exchange])) {
        val childStageIds = children.flatMap(getStageIds).toSet
        setStageIds(plan, childStageIds)
      }

      plan.subqueries.foreach(propagateTags)
    }
  }

  def setStageIds(plan: SparkPlan, stageIds: Set[Int]): Unit = {
    plan.setTagValue(StageIdsTag, stageIds)
  }

  def getStageIds(plan: SparkPlan): Set[Int] = {
    plan.getTagValue(StageIdsTag).getOrElse(Set.empty)
  }
}
