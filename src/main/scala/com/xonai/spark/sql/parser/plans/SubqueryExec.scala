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

import com.xonai.spark.sql.parser.expressions.{Attribute, AttributeReference, Cast, Expression, NamedExpression}
import com.xonai.spark.sql.parser.metric.SQLMetric

/**
 * [[org.apache.spark.sql.execution.BaseSubqueryExec]].
 */
abstract class BaseSubqueryExec extends SparkPlan {

  def child: SparkPlan

  override def output: Seq[Attribute] = child.output
}

/**
 * [[org.apache.spark.sql.execution.SubqueryExec]].
 */
case class SubqueryExec(
    name: String,
    child: SparkPlan,
    override val metrics: Map[String, SQLMetric]
) extends BaseSubqueryExec
    with UnaryExecNode {

  override def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.execution.ReusedSubqueryExec]].
 */
case class ReusedSubqueryExec(child: SparkPlan) extends BaseSubqueryExec with LeafExecNode

/**
 * [[org.apache.spark.sql.execution.SubqueryBroadcastExec]].
 */
case class SubqueryBroadcastExec(
    name: String,
    indices: Seq[Int],
    buildKeys: Seq[Expression],
    child: SparkPlan,
    override val metrics: Map[String, SQLMetric],
    legacySingleValue: Boolean
) extends BaseSubqueryExec
    with UnaryExecNode {

  override def output: Seq[Attribute] = {
    indices.map { index =>
      val key = buildKeys(index)
      val name = key match {
        case n: NamedExpression =>
          n.name
        case Cast(n: NamedExpression, _, _) =>
          n.name
        case _ if legacySingleValue =>
          "key"
        case _ =>
          s"key_$index"
      }

      AttributeReference(name, key.dataType)
    }
  }

  override def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }
}
