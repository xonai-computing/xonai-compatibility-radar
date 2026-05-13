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

import com.xonai.spark.sql.parser.expressions.{Attribute, Expression}

/**
 * [[org.apache.spark.sql.catalyst.plans.JoinType]].
 */
sealed abstract class JoinType

sealed abstract class InnerLike extends JoinType

case object Inner extends InnerLike

case object Cross extends InnerLike

case object LeftOuter extends JoinType

case object RightOuter extends JoinType

case object FullOuter extends JoinType

case object LeftSemi extends JoinType

case object LeftAnti extends JoinType

case class ExistenceJoin(exists: Attribute) extends JoinType

/**
 * [[org.apache.spark.sql.catalyst.optimizer.BuildSide]].
 */
sealed abstract class BuildSide

case object BuildRight extends BuildSide

case object BuildLeft extends BuildSide

/**
 * [[org.apache.spark.sql.execution.joins.BaseJoinExec]].
 */
trait BaseJoinExec extends BinaryExecNode {

  def joinType: JoinType

  def condition: Option[Expression]

  def leftKeys: Seq[Expression]

  def rightKeys: Seq[Expression]

  override def output: Seq[Attribute] = {
    joinType match {
      case j: ExistenceJoin =>
        left.output :+ j.exists
      case LeftSemi | LeftAnti =>
        left.output
      case _ =>
        left.output ++ right.output
    }
  }
}
