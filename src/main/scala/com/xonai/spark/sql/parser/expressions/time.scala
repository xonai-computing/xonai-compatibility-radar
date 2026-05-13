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

import com.xonai.spark.sql.parser.types.{DataType, DecimalType, IntegerType, LongType, StringType, TimeType}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ToTime]].
 */
case class ToTime(str: Expression, format: Option[Expression])
    extends Expression
    with ExpectsInputType {

  override def children: IndexedSeq[Expression] = str +: format.toIndexedSeq

  override def dataType: DataType = TimeType

  override def inputType: DataType = StringType

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(
      str = newChildren.head,
      format = format.map(_ => newChildren(1))
    )
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.MakeTime]].
 */
case class MakeTime(hours: Expression, minutes: Expression, secsAndMicros: Expression)
    extends TernaryExpression
    with ExpectsInputTypes {

  override def first: Expression = hours

  override def second: Expression = minutes

  override def third: Expression = secsAndMicros

  override def dataType: DataType = TimeType

  override def inputTypes: Seq[DataType] = Seq(IntegerType, IntegerType, DecimalType)

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(hours = newFirst, minutes = newSecond, secsAndMicros = newThird)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.TimeDiff]].
 */
case class TimeDiff(unit: Expression, start: Expression, end: Expression)
    extends TernaryExpression
    with ExpectsInputTypes {

  override def first: Expression = unit

  override def second: Expression = start

  override def third: Expression = end

  override def dataType: DataType = LongType

  override def inputTypes: Seq[DataType] = Seq(StringType, TimeType, TimeType)

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(unit = newFirst, start = newSecond, end = newThird)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.TimeTrunc]].
 */
case class TimeTrunc(unit: Expression, time: Expression)
    extends BinaryExpression
    with ExpectsInputTypes {

  override def left: Expression = unit

  override def right: Expression = time

  override def dataType: DataType = time.dataType

  override def inputTypes: Seq[DataType] = Seq(StringType, TimeType)

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(unit = newLeft, time = newRight)
  }
}
