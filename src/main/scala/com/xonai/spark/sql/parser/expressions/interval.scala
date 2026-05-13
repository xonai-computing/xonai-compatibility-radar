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

import com.xonai.spark.sql.parser.types.{ByteType, CalendarIntervalType, DataType, DayTimeIntervalType, DecimalType, DoubleType, IntegerType, TypeSet, YearMonthIntervalType}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ExtractIntervalPart]].
 */
abstract class ExtractIntervalPart(val dataType: DataType) extends UnaryExpression

/**
 * [[org.apache.spark.sql.catalyst.expressions.ExtractIntervalYears]].
 */
case class ExtractIntervalYears(child: Expression)
    extends ExtractIntervalPart(IntegerType)
    with ExpectsInputType {

  override def inputType: DataType = CalendarIntervalType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ExtractIntervalMonths]].
 */
case class ExtractIntervalMonths(child: Expression)
    extends ExtractIntervalPart(ByteType)
    with ExpectsInputType {

  override def inputType: DataType = CalendarIntervalType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ExtractIntervalDays]].
 */
case class ExtractIntervalDays(child: Expression)
    extends ExtractIntervalPart(IntegerType)
    with ExpectsInputType {

  override def inputType: DataType = CalendarIntervalType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ExtractIntervalHours]].
 */
case class ExtractIntervalHours(child: Expression)
    extends ExtractIntervalPart(ByteType)
    with ExpectsInputType {

  override def inputType: DataType = CalendarIntervalType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ExtractIntervalMinutes]].
 */
case class ExtractIntervalMinutes(child: Expression)
    extends ExtractIntervalPart(ByteType)
    with ExpectsInputType {

  override def inputType: DataType = CalendarIntervalType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ExtractIntervalSeconds]].
 */
case class ExtractIntervalSeconds(child: Expression)
    extends ExtractIntervalPart(DecimalType)
    with ExpectsInputType {

  override def inputType: DataType = CalendarIntervalType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ExtractANSIIntervalYears]].
 */
case class ExtractANSIIntervalYears(child: Expression)
    extends ExtractIntervalPart(IntegerType)
    with ExpectsInputType {

  override def inputType: DataType = YearMonthIntervalType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ExtractANSIIntervalMonths]].
 */
case class ExtractANSIIntervalMonths(child: Expression)
    extends ExtractIntervalPart(ByteType)
    with ExpectsInputType {

  override def inputType: DataType = YearMonthIntervalType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ExtractANSIIntervalDays]].
 */
case class ExtractANSIIntervalDays(child: Expression)
    extends ExtractIntervalPart(IntegerType)
    with ExpectsInputType {

  override def inputType: DataType = DayTimeIntervalType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ExtractANSIIntervalHours]].
 */
case class ExtractANSIIntervalHours(child: Expression)
    extends ExtractIntervalPart(ByteType)
    with ExpectsInputType {

  override def inputType: DataType = DayTimeIntervalType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ExtractANSIIntervalMinutes]].
 */
case class ExtractANSIIntervalMinutes(child: Expression)
    extends ExtractIntervalPart(ByteType)
    with ExpectsInputType {

  override def inputType: DataType = DayTimeIntervalType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ExtractANSIIntervalSeconds]].
 */
case class ExtractANSIIntervalSeconds(child: Expression)
    extends ExtractIntervalPart(DecimalType)
    with ExpectsInputType {

  override def inputType: DataType = DayTimeIntervalType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.MultiplyInterval]].
 */
case class MultiplyInterval(interval: Expression, num: Expression)
    extends BinaryExpression
    with ExpectsInputTypes {

  override def left: Expression = interval

  override def right: Expression = num

  override def dataType: DataType = CalendarIntervalType

  override def inputTypes: Seq[DataType] = Seq(CalendarIntervalType, DoubleType)

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(interval = newLeft, num = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.DivideInterval]].
 */
case class DivideInterval(interval: Expression, num: Expression)
    extends BinaryExpression
    with ExpectsInputTypes {

  override def left: Expression = interval

  override def right: Expression = num

  override def dataType: DataType = CalendarIntervalType

  override def inputTypes: Seq[DataType] = Seq(CalendarIntervalType, DoubleType)

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(interval = newLeft, num = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.MakeInterval]].
 */
case class MakeInterval(
    years: Expression,
    months: Expression,
    weeks: Expression,
    days: Expression,
    hours: Expression,
    mins: Expression,
    secs: Expression
) extends Expression
    with ExpectsInputTypes {

  override def dataType: DataType = CalendarIntervalType

  override def children: IndexedSeq[Expression] = {
    IndexedSeq(years, months, weeks, days, hours, mins, secs)
  }

  override def inputTypes: Seq[DataType] = {
    Seq(IntegerType, IntegerType, IntegerType, IntegerType, IntegerType, IntegerType, DecimalType)
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(
      years = newChildren(0),
      months = newChildren(1),
      weeks = newChildren(2),
      days = newChildren(3),
      hours = newChildren(4),
      mins = newChildren(5),
      secs = newChildren(6)
    )
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.MakeDTInterval]].
 */
case class MakeDTInterval(days: Expression, hours: Expression, mins: Expression, secs: Expression)
    extends QuaternaryExpression
    with ExpectsInputTypes {

  override def first: Expression = days

  override def second: Expression = hours

  override def third: Expression = mins

  override def fourth: Expression = secs

  override def dataType: DataType = DayTimeIntervalType

  override def inputTypes: Seq[DataType] = {
    Seq(IntegerType, IntegerType, IntegerType, DecimalType)
  }

  override def withNewChildrenInternal(
      first: Expression,
      second: Expression,
      third: Expression,
      fourth: Expression
  ): Expression = {
    copy(days = first, hours = second, mins = third, secs = fourth)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.MakeYMInterval]].
 */
case class MakeYMInterval(years: Expression, months: Expression)
    extends BinaryExpression
    with ExpectsInputType {

  override def left: Expression = years

  override def right: Expression = months

  override def dataType: DataType = YearMonthIntervalType

  override def inputType: DataType = IntegerType

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(years = newLeft, months = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.MultiplyYMInterval]].
 */
case class MultiplyYMInterval(interval: Expression, num: Expression)
    extends BinaryExpression
    with ExpectsInputTypes {

  override def left: Expression = interval

  override def right: Expression = num

  override def dataType: DataType = YearMonthIntervalType

  override def inputTypes: Seq[DataType] = {
    Seq(YearMonthIntervalType, TypeSet.Numeric)
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(interval = newLeft, num = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.MultiplyDTInterval]].
 */
case class MultiplyDTInterval(interval: Expression, num: Expression)
    extends BinaryExpression
    with ExpectsInputTypes {

  override def left: Expression = interval

  override def right: Expression = num

  override def dataType: DataType = DayTimeIntervalType

  override def inputTypes: Seq[DataType] = {
    Seq(DayTimeIntervalType, TypeSet.Numeric)
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(interval = newLeft, num = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.DivideYMInterval]].
 */
case class DivideYMInterval(interval: Expression, num: Expression)
    extends BinaryExpression
    with ExpectsInputTypes {

  override def left: Expression = interval

  override def right: Expression = num

  override def dataType: DataType = YearMonthIntervalType

  override def inputTypes: Seq[DataType] = {
    Seq(YearMonthIntervalType, TypeSet.Numeric)
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(interval = newLeft, num = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.DivideDTInterval]].
 */
case class DivideDTInterval(interval: Expression, num: Expression)
    extends BinaryExpression
    with ExpectsInputTypes {

  override def left: Expression = interval

  override def right: Expression = num

  override def dataType: DataType = DayTimeIntervalType

  override def inputTypes: Seq[DataType] = {
    Seq(DayTimeIntervalType, TypeSet.Numeric)
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(interval = newLeft, num = newRight)
  }
}
