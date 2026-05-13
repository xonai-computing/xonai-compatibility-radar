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

import com.xonai.spark.sql.parser.expressions.Expression
import com.xonai.spark.sql.parser.plans.SparkPlan

/**
 * Provides support information for a Spark execution engine.
 */
trait EngineSupport {

  /**
   * Whether a plan alone is supported by the engine.
   */
  def check(plan: SparkPlan): NodeSupport

  /**
   * Whether an expression alone is supported by the engine.
   */
  def check(expression: Expression): NodeSupport

  /**
   * Recursively adds a [[NodeSupport]] tag to a plan tree.
   */
  def tag(plan: SparkPlan): Unit = {
    val isolatedSupport = check(plan)
    NodeSupport.setIsolatedSupport(plan, isolatedSupport)

    val composedSupport =
      plan.expressions.foldLeft(isolatedSupport) { case (acc, rootExpression) =>
        rootExpression
          .map { expression =>
            val support = check(expression)
            NodeSupport.setIsolatedSupport(expression, support)
            support
          }
          .foldLeft(acc)(_ + _)
      }
    NodeSupport.setComposedSupport(plan, composedSupport)

    plan.subqueries.foreach(tag)
    plan.children.foreach(tag)
  }
}
