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

package com.xonai.spark.sql.parser.plans

import com.xonai.spark.sql.parser.expressions.Expression
import com.xonai.spark.sql.parser.metric.SQLMetric

/**
 * [[org.apache.spark.sql.execution.joins.CartesianProductExec]].
 */
case class CartesianProductExec(
    left: SparkPlan,
    right: SparkPlan,
    condition: Option[Expression],
    override val metrics: Map[String, SQLMetric]
) extends BaseJoinExec {

  override def joinType: JoinType = Inner

  override def leftKeys: Seq[Expression] = Nil

  override def rightKeys: Seq[Expression] = Nil

  override def withNewChildrenInternal(newLeft: SparkPlan, newRight: SparkPlan): SparkPlan = {
    copy(left = newLeft, right = newRight)
  }
}
