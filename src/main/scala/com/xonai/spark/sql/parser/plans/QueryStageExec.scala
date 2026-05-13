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

import com.xonai.spark.sql.parser.expressions.Attribute

/**
 * [[org.apache.spark.sql.execution.adaptive.QueryStageExec]].
 */
abstract class QueryStageExec extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output
}

/**
 * [[org.apache.spark.sql.execution.adaptive.ShuffleQueryStageExec]].
 */
case class ShuffleQueryStageExec(child: SparkPlan) extends QueryStageExec {

  override def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec]].
 */
case class BroadcastQueryStageExec(child: SparkPlan) extends QueryStageExec {

  override def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.execution.adaptive.TableCacheQueryStageExec]].
 */
case class TableCacheQueryStageExec(child: SparkPlan) extends QueryStageExec {

  override def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.execution.adaptive.ResultQueryStageExec]].
 */
case class ResultQueryStageExec(child: SparkPlan) extends QueryStageExec {

  override def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }
}
