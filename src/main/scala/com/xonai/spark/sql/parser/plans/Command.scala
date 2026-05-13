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

import com.xonai.spark.sql.parser.commands.Command
import com.xonai.spark.sql.parser.expressions.Attribute
import com.xonai.utils.Utils

/**
 * [[org.apache.spark.sql.execution.command.ExecutedCommandExec]].
 */
case class ExecutedCommandExec(cmd: Command) extends LeafExecNode {

  override def nodeName: String = "Execute " + Utils.getSimpleName(cmd.getClass)

  override def output: Seq[Attribute] = cmd.output
}

/**
 * [[org.apache.spark.sql.execution.command.DataWritingCommandExec]].
 */
case class DataWritingCommandExec(cmd: Command, child: SparkPlan) extends UnaryExecNode {

  override def output: Seq[Attribute] = cmd.output

  override def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }
}
