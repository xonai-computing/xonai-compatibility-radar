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

import com.xonai.spark.sql.parser.types.{BinaryType, BooleanType, DataType, LongType}

/**
 * [[org.apache.spark.sql.catalyst.expressions.HllSketchEstimate]].
 */
case class HllSketchEstimate(child: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = LongType

  override def inputType: DataType = BinaryType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.HllUnion]].
 */
case class HllUnion(first: Expression, second: Expression, third: Expression)
    extends TernaryExpression
    with ExpectsInputTypes {

  override def dataType: DataType = BinaryType

  override def inputTypes: Seq[DataType] = Seq(BinaryType, BinaryType, BooleanType)

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(first = newFirst, second = newSecond, third = newThird)
  }
}
