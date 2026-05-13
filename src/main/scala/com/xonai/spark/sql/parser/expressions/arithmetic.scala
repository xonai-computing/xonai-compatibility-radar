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

import com.xonai.spark.sql.parser.types.{AnyType, DataType, DayTimeIntervalType, DecimalType, DoubleType, LongType, TypeSet, YearMonthIntervalType}

/**
 * [[org.apache.spark.sql.catalyst.expressions.UnaryMinus]].
 */
case class UnaryMinus(child: Expression) extends UnaryExpression with DataTypeIsInputType {

  override def dataType: DataType = child.dataType

  override def inputType: DataType = TypeSet.NumericAndInterval

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.UnaryPositive]].
 */
case class UnaryPositive(child: Expression) extends UnaryExpression with DataTypeIsInputType {

  override def dataType: DataType = child.dataType

  override def inputType: DataType = TypeSet.NumericAndInterval

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Abs]].
 */
case class Abs(child: Expression) extends UnaryExpression with DataTypeIsInputType {

  override def dataType: DataType = child.dataType

  override def inputType: DataType = TypeSet.NumericAndAnsiInterval

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.BinaryArithmetic]].
 */
abstract class BinaryArithmetic extends BinaryOperator with ExpectsInputType {

  private lazy val internalDataType: DataType = left.dataType.intersect(right.dataType)

  override def dataType: DataType = internalDataType

  override def resolveDataType(outputType: DataType): Expression = {
    val newChildDataType = outputType.intersect(internalDataType.intersect(inputType))
    mapChildren(_.resolveDataType(newChildDataType))
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Add]].
 */
case class Add(left: Expression, right: Expression) extends BinaryArithmetic {

  override def inputType: DataType = TypeSet.NumericAndInterval

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Subtract]].
 */
case class Subtract(left: Expression, right: Expression) extends BinaryArithmetic {

  override def inputType: DataType = TypeSet.NumericAndInterval

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Multiply]].
 */
case class Multiply(left: Expression, right: Expression) extends BinaryArithmetic {

  override def inputType: DataType = TypeSet.Numeric

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Divide]].
 */
case class Divide(left: Expression, right: Expression) extends BinaryArithmetic {

  override def inputType: DataType = TypeSet(DoubleType, DecimalType)

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.IntegralDivide]].
 */
case class IntegralDivide(left: Expression, right: Expression) extends BinaryArithmetic {

  override lazy val dataType: DataType = LongType

  override def inputType: DataType = {
    TypeSet(LongType, DecimalType, YearMonthIntervalType, DayTimeIntervalType)
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Remainder]].
 */
case class Remainder(left: Expression, right: Expression) extends BinaryArithmetic {

  override def inputType: DataType = TypeSet.Numeric

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Pmod]].
 */
case class Pmod(left: Expression, right: Expression) extends BinaryArithmetic {

  override def inputType: DataType = TypeSet.Numeric

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Least]].
 */
case class Least(children: IndexedSeq[Expression]) extends ComplexTypeMergingExpression {

  override def inputType: DataType = AnyType

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Greatest]].
 */
case class Greatest(children: IndexedSeq[Expression]) extends ComplexTypeMergingExpression {

  override def inputType: DataType = AnyType

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }
}
