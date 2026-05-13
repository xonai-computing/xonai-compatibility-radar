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

import com.xonai.spark.sql.parser.expressions.{Attribute, Expression, SortOrder}
import com.xonai.spark.sql.parser.metric.SQLMetric

sealed trait WindowGroupLimitMode

object WindowGroupLimitMode {

  case object Partial extends WindowGroupLimitMode

  case object Final extends WindowGroupLimitMode
}

/**
 * [[org.apache.spark.sql.execution.window.WindowGroupLimitExec]].
 */
case class WindowGroupLimitExec(
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    rankLikeFunction: Expression,
    limit: Int,
    mode: WindowGroupLimitMode,
    child: SparkPlan,
    override val metrics: Map[String, SQLMetric]
) extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output

  override def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }
}
