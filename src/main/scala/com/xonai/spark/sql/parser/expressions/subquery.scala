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

package com.xonai.spark.sql.parser.expressions

import com.xonai.spark.sql.parser.plans.SparkPlan
import com.xonai.spark.sql.parser.trees.{LeafLike, UnaryLike}
import com.xonai.spark.sql.parser.types.{AnyType, DataType}

/**
 * [[org.apache.spark.sql.execution.ExecSubqueryExpression]].
 */
abstract class ExecSubqueryExpression extends Expression {

  def plan: SparkPlan
}

/**
 * [[org.apache.spark.sql.execution.ScalarSubquery]].
 */
case class ScalarSubquery(plan: SparkPlan)
    extends ExecSubqueryExpression
    with LeafLike[Expression] {

  override def dataType: DataType = {
    plan.schema.fields.headOption.fold[DataType](AnyType)(_.dataType)
  }
}

/**
 * [[org.apache.spark.sql.execution.InSubqueryExec]].
 */
case class InSubqueryExec(child: Expression, plan: SparkPlan)
    extends ExecSubqueryExpression
    with UnaryLike[Expression]
    with Predicate {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}
