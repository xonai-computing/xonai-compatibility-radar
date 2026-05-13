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

import com.xonai.spark.sql.parser.types.{AnyType, DataType, DoubleType, FloatType, TypeSet}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Coalesce]].
 */
case class Coalesce(children: IndexedSeq[Expression]) extends ComplexTypeMergingExpression {

  override def inputType: DataType = AnyType

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.IsNaN]].
 */
case class IsNaN(child: Expression) extends UnaryExpression with Predicate with ExpectsInputType {

  override def inputType: DataType = TypeSet(FloatType, DoubleType)

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.NaNvl]].
 */
case class NaNvl(left: Expression, right: Expression)
    extends BinaryExpression
    with DataTypeIsInputType {

  override def dataType: DataType = left.dataType

  override def inputType: DataType = TypeSet(FloatType, DoubleType)

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.IsNull]].
 */
case class IsNull(child: Expression) extends UnaryExpression with Predicate {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.IsNotNull]].
 */
case class IsNotNull(child: Expression) extends UnaryExpression with Predicate {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.AtLeastNNonNulls]].
 */
case class AtLeastNNonNulls(n: Int, children: IndexedSeq[Expression]) extends Predicate {

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }
}
