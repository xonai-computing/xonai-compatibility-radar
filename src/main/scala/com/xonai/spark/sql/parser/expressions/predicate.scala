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

import com.xonai.spark.sql.parser.types.{BooleanType, DataType}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Predicate]].
 */
trait Predicate extends Expression {

  override def dataType: DataType = BooleanType
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Not]].
 */
case class Not(child: Expression) extends UnaryExpression with Predicate with ExpectsInputType {

  override def inputType: DataType = BooleanType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.And]].
 */
case class And(left: Expression, right: Expression)
    extends BinaryOperator
    with Predicate
    with ExpectsInputType {

  override def inputType: DataType = BooleanType

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Or]].
 */
case class Or(left: Expression, right: Expression)
    extends BinaryOperator
    with Predicate
    with ExpectsInputType {

  override def inputType: DataType = BooleanType

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.BinaryComparison]].
 */
abstract class BinaryComparison extends BinaryExpression with Predicate {

  override def resolveDataType(outputType: DataType): Expression = {
    val newChildDataType = left.dataType.intersect(right.dataType)
    mapChildren(_.resolveDataType(newChildDataType))
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.EqualTo]].
 */
case class EqualTo(left: Expression, right: Expression) extends BinaryComparison {

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.EqualNullSafe]].
 */
case class EqualNullSafe(left: Expression, right: Expression) extends BinaryComparison {

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.LessThan]].
 */
case class LessThan(left: Expression, right: Expression) extends BinaryComparison {

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.LessThanOrEqual]].
 */
case class LessThanOrEqual(left: Expression, right: Expression) extends BinaryComparison {

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.GreaterThan]].
 */
case class GreaterThan(left: Expression, right: Expression) extends BinaryComparison {

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual]].
 */
case class GreaterThanOrEqual(left: Expression, right: Expression) extends BinaryComparison {

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.In]].
 */
case class In(value: Expression, list: IndexedSeq[Expression]) extends Predicate {

  override def children: IndexedSeq[Expression] = value +: list

  override def resolveDataType(outputType: DataType): Expression = {
    val newChildDataType = list.map(_.dataType).fold(value.dataType)(DataType.intersection)
    mapChildren(_.resolveDataType(newChildDataType))
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(value = newChildren.head, list = newChildren.tail)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.InSet]].
 */
case class InSet(child: Expression, set: IndexedSeq[Literal]) extends Expression with Predicate {

  override def children: IndexedSeq[Expression] = child +: set

  override def resolveDataType(outputType: DataType): Expression = {
    val newChildDataType = set.map(_.dataType).fold(child.dataType)(DataType.intersection)
    mapChildren(_.resolveDataType(newChildDataType))
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(
      child = newChildren.head,
      set = newChildren.tail.asInstanceOf[IndexedSeq[Literal]]
    )
  }
}
