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

import com.xonai.spark.sql.parser.expressions.{Attribute, ExecSubqueryExpression, Expression}
import com.xonai.spark.sql.parser.metric.SQLMetric
import com.xonai.spark.sql.parser.trees.{BinaryLike, LeafLike, TreeNode, UnaryLike, UnknownNode}
import com.xonai.spark.sql.parser.types.{DataTypeUtils, StructType}

import scala.collection.mutable.ArrayBuffer

/**
 * [[org.apache.spark.sql.execution.SparkPlan]].
 */
abstract class SparkPlan extends TreeNode[SparkPlan] {

  lazy val schema: StructType = DataTypeUtils.fromAttributes(output)

  /**
   * This method is not a complete match to the one in Spark. It may return more or less attributes
   * than it should in order to avoid exceptions and maximize data type propagation across a plan.
   */
  def output: Seq[Attribute]

  def metrics: Map[String, SQLMetric] = Map.empty

  final def expressions: Seq[Expression] = {
    def seqToExpressions(seq: Iterable[Any]): Iterable[Expression] = {
      seq.flatMap {
        case e: Expression =>
          e :: Nil
        case s: Iterable[_] =>
          seqToExpressions(s)
        case _ =>
          Nil
      }
    }

    productIterator
      .flatMap {
        case e: Expression =>
          e :: Nil
        case s: Some[_] =>
          seqToExpressions(s.toSeq)
        case seq: Iterable[_] =>
          seqToExpressions(seq)
        case _ =>
          Nil
      }
      .toSeq
  }

  lazy val subqueries: Seq[SparkPlan] = {
    val subqueries = ArrayBuffer[SparkPlan]()
    expressions.foreach {
      _.collect { case expression: ExecSubqueryExpression =>
        subqueries += expression.plan
      }
    }
    subqueries.toSeq
  }

  def allPlans: Seq[SparkPlan] = {
    this +: (subqueries.flatMap(_.allPlans) ++ children.flatMap(_.allPlans))
  }
}

/**
 * [[org.apache.spark.sql.execution.LeafExecNode]].
 */
trait LeafExecNode extends SparkPlan with LeafLike[SparkPlan]

/**
 * [[org.apache.spark.sql.execution.UnaryExecNode]].
 */
trait UnaryExecNode extends SparkPlan with UnaryLike[SparkPlan]

/**
 * [[org.apache.spark.sql.execution.BinaryExecNode]].
 */
trait BinaryExecNode extends SparkPlan with BinaryLike[SparkPlan]

/**
 * Used when the parser is unable to match a plan nodeName.
 */
case class UnknownExec(
    override val nodeName: String,
    simpleString: String,
    children: IndexedSeq[SparkPlan],
    override val output: Seq[Attribute],
    override val metrics: Map[String, SQLMetric]
) extends SparkPlan
    with UnknownNode {

  override def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan = {
    copy(children = newChildren)
  }
}
