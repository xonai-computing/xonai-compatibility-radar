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

import com.xonai.spark.sql.parser.expressions.{Attribute, AttributeReference}
import com.xonai.spark.sql.parser.types.DataType

/**
 * [[org.apache.spark.sql.execution.UnionExec]].
 */
case class UnionExec(children: IndexedSeq[SparkPlan]) extends SparkPlan {

  override def output: Seq[Attribute] = {
    children.map(_.output).transpose.map { attributes =>
      val firstAttribute = attributes.head
      val newDataType = attributes.map(_.dataType).reduce(DataType.intersection)
      if (newDataType == firstAttribute.dataType) {
        firstAttribute
      } else {
        AttributeReference(firstAttribute.name, newDataType, firstAttribute.exprId)
      }
    }
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan = {
    copy(children = newChildren)
  }
}
