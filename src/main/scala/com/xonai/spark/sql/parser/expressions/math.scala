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

import com.xonai.spark.sql.parser.types.{BinaryType, DataType, DataTypeMap, DayTimeIntervalType, DecimalType, DoubleType, IntegerType, LongType, StringType, TypeSet, YearMonthIntervalType}

/**
 * [[org.apache.spark.sql.catalyst.expressions.LeafMathExpression]].
 */
abstract class LeafMathExpression extends LeafExpression {

  override def dataType: DataType = DoubleType
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.UnaryMathExpression]].
 */
abstract class UnaryMathExpression extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = DoubleType

  override def inputType: DataType = DoubleType
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.BinaryMathExpression]].
 */
abstract class BinaryMathExpression extends BinaryExpression with ExpectsInputType {

  override def dataType: DataType = DoubleType

  override def inputType: DataType = DoubleType
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.EulerNumber]].
 */
case class EulerNumber() extends LeafMathExpression

/**
 * [[org.apache.spark.sql.catalyst.expressions.Pi]].
 */
case class Pi() extends LeafMathExpression

/**
 * [[org.apache.spark.sql.catalyst.expressions.Acos]].
 */
case class Acos(child: Expression) extends UnaryMathExpression {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Asin]].
 */
case class Asin(child: Expression) extends UnaryMathExpression {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Atan]].
 */
case class Atan(child: Expression) extends UnaryMathExpression {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Cbrt]].
 */
case class Cbrt(child: Expression) extends UnaryMathExpression {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Cos]].
 */
case class Cos(child: Expression) extends UnaryMathExpression {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Sec]].
 */
case class Sec(child: Expression) extends UnaryMathExpression {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Cosh]].
 */
case class Cosh(child: Expression) extends UnaryMathExpression {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Acosh]].
 */
case class Acosh(child: Expression) extends UnaryMathExpression {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Exp]].
 */
case class Exp(child: Expression) extends UnaryMathExpression {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Expm1]].
 */
case class Expm1(child: Expression) extends UnaryMathExpression {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Log]].
 */
case class Log(child: Expression) extends UnaryMathExpression {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Log2]].
 */
case class Log2(child: Expression) extends UnaryMathExpression {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Log10]].
 */
case class Log10(child: Expression) extends UnaryMathExpression {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Log1p]].
 */
case class Log1p(child: Expression) extends UnaryMathExpression {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Rint]].
 */
case class Rint(child: Expression) extends UnaryMathExpression {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Signum]].
 */
case class Signum(child: Expression) extends UnaryMathExpression {

  override def inputType: DataType = {
    TypeSet(DoubleType, YearMonthIntervalType, DayTimeIntervalType)
  }

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Sin]].
 */
case class Sin(child: Expression) extends UnaryMathExpression {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Csc]].
 */
case class Csc(child: Expression) extends UnaryMathExpression {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Sinh]].
 */
case class Sinh(child: Expression) extends UnaryMathExpression {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Asinh]].
 */
case class Asinh(child: Expression) extends UnaryMathExpression {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Sqrt]].
 */
case class Sqrt(child: Expression) extends UnaryMathExpression {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Tan]].
 */
case class Tan(child: Expression) extends UnaryMathExpression {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Cot]].
 */
case class Cot(child: Expression) extends UnaryMathExpression {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Tanh]].
 */
case class Tanh(child: Expression) extends UnaryMathExpression {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Atanh]].
 */
case class Atanh(child: Expression) extends UnaryMathExpression {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ToDegrees]].
 */
case class ToDegrees(child: Expression) extends UnaryMathExpression {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ToRadians]].
 */
case class ToRadians(child: Expression) extends UnaryMathExpression {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Ceil]].
 */
case class Ceil(child: Expression) extends UnaryExpression {

  private lazy val dataTypeMap = DataTypeMap(
    Map(
      DecimalType -> DecimalType,
      DoubleType -> LongType,
      LongType -> LongType
    )
  )

  override def dataType: DataType = {
    dataTypeMap.outputType(child.dataType)
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val inputDataType = dataTypeMap.inputType(outputType)
    val newChildDataType = child.dataType.intersect(inputDataType)
    mapChildren(_.resolveDataType(newChildDataType))
  }

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Floor]].
 */
case class Floor(child: Expression) extends UnaryExpression {

  private lazy val dataTypeMap = DataTypeMap(
    Map(
      DecimalType -> DecimalType,
      DoubleType -> LongType,
      LongType -> LongType
    )
  )

  override def dataType: DataType = {
    dataTypeMap.outputType(child.dataType)
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val inputDataType = dataTypeMap.inputType(outputType)
    val newChildDataType = child.dataType.intersect(inputDataType)
    mapChildren(_.resolveDataType(newChildDataType))
  }

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Factorial]].
 */
case class Factorial(child: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = LongType

  override def inputType: DataType = IntegerType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Bin]].
 */
case class Bin(child: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = StringType

  override def inputType: DataType = LongType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Hex]].
 */
case class Hex(child: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = StringType

  override def inputType: DataType = TypeSet(LongType, BinaryType, StringType)

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Unhex]].
 */
case class Unhex(child: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = BinaryType

  override def inputType: DataType = StringType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Atan2]].
 */
case class Atan2(left: Expression, right: Expression) extends BinaryMathExpression {

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Pow]].
 */
case class Pow(left: Expression, right: Expression) extends BinaryMathExpression {

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Hypot]].
 */
case class Hypot(left: Expression, right: Expression) extends BinaryMathExpression {

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Logarithm]].
 */
case class Logarithm(left: Expression, right: Expression) extends BinaryMathExpression {

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ShiftLeft]].
 */
case class ShiftLeft(left: Expression, right: Expression) extends BinaryExpression {

  override def dataType: DataType = left.dataType

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = outputType
      .intersect(left.dataType)
      .intersect(TypeSet(IntegerType, LongType))

    withNewChildren(
      IndexedSeq(
        left.resolveDataType(newDataType),
        right.resolveDataType(IntegerType)
      )
    )
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ShiftRight]].
 */
case class ShiftRight(left: Expression, right: Expression) extends BinaryExpression {

  override def dataType: DataType = left.dataType

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = outputType
      .intersect(left.dataType)
      .intersect(TypeSet(IntegerType, LongType))

    withNewChildren(
      IndexedSeq(
        left.resolveDataType(newDataType),
        right.resolveDataType(IntegerType)
      )
    )
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ShiftRightUnsigned]].
 */
case class ShiftRightUnsigned(left: Expression, right: Expression) extends BinaryExpression {

  override def dataType: DataType = left.dataType

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = outputType
      .intersect(left.dataType)
      .intersect(TypeSet(IntegerType, LongType))

    withNewChildren(
      IndexedSeq(
        left.resolveDataType(newDataType),
        right.resolveDataType(IntegerType)
      )
    )
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.RoundBase]].
 */
abstract class RoundBase(child: Expression, scale: Expression) extends BinaryExpression {

  override def left: Expression = child

  override def right: Expression = scale

  override def dataType: DataType = child.dataType
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.RoundCeil]].
 */
case class RoundCeil(child: Expression, scale: Expression)
    extends RoundBase(child, scale)
    with ExpectsInputTypes {

  override def inputTypes: Seq[DataType] = Seq(DecimalType, IntegerType)

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(child = newLeft, scale = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.RoundFloor]].
 */
case class RoundFloor(child: Expression, scale: Expression)
    extends RoundBase(child, scale)
    with ExpectsInputTypes {

  override def inputTypes: Seq[DataType] = Seq(DecimalType, IntegerType)

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(child = newLeft, scale = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Round]].
 */
case class Round(child: Expression, scale: Expression) extends RoundBase(child, scale) {

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = outputType.intersect(child.dataType).intersect(TypeSet.Numeric)
    withNewChildren(
      IndexedSeq(
        child.resolveDataType(newDataType),
        scale.resolveDataType(IntegerType)
      )
    )
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(child = newLeft, scale = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.BRound]].
 */
case class BRound(child: Expression, scale: Expression) extends RoundBase(child, scale) {

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = outputType.intersect(child.dataType).intersect(TypeSet.Numeric)
    withNewChildren(
      IndexedSeq(
        child.resolveDataType(newDataType),
        scale.resolveDataType(IntegerType)
      )
    )
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(child = newLeft, scale = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Conv]].
 */
case class Conv(numExpr: Expression, fromBaseExpr: Expression, toBaseExpr: Expression)
    extends TernaryExpression
    with ExpectsInputTypes {

  override def first: Expression = numExpr

  override def second: Expression = fromBaseExpr

  override def third: Expression = toBaseExpr

  override def dataType: DataType = StringType

  override def inputTypes: Seq[DataType] = {
    Seq(StringType, IntegerType, IntegerType)
  }

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(numExpr = newFirst, fromBaseExpr = newSecond, toBaseExpr = newThird)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.WidthBucket]].
 */
case class WidthBucket(
    value: Expression,
    minValue: Expression,
    maxValue: Expression,
    numBucket: Expression
) extends QuaternaryExpression
    with ExpectsInputTypes {

  override def first: Expression = value

  override def second: Expression = minValue

  override def third: Expression = maxValue

  override def fourth: Expression = numBucket

  override def dataType: DataType = LongType

  override def inputTypes: Seq[DataType] = {
    val childDataType = TypeSet(DoubleType, YearMonthIntervalType, DayTimeIntervalType)
    Seq(childDataType, childDataType, childDataType, LongType)
  }

  override def withNewChildrenInternal(
      first: Expression,
      second: Expression,
      third: Expression,
      fourth: Expression
  ): Expression = {
    copy(value = first, minValue = second, maxValue = third, numBucket = fourth)
  }
}
