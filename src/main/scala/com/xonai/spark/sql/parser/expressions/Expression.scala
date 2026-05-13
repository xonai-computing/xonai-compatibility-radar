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

import com.xonai.spark.sql.parser.trees.{BinaryLike, LeafLike, QuaternaryLike, TernaryLike, TreeNode, UnaryLike, UnknownNode}
import com.xonai.spark.sql.parser.types.{AnyType, DataType, TypeSet}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Expression]].
 */
abstract class Expression extends TreeNode[Expression] {

  def dataType: DataType

  /**
   * Attempts to narrow down the data type of expressions in tree. Literal expressions are the main
   * reason for this since their string representation does not include a concrete data type.
   */
  def resolveDataType(outputType: DataType): Expression = {
    mapChildren(_.resolveDataType(AnyType))
  }

  def resolvedIsCompatibleWith(dataType: DataType): Boolean = {
    val resolved = resolveDataType(dataType)
    resolved.isCompatibleWith(dataType)
  }

  def isCompatibleWith(dataType: DataType): Boolean = {
    this.dataType.intersect(dataType) != TypeSet.Empty && !hasEmptyDataType
  }

  def hasEmptyDataType: Boolean = {
    dataType == TypeSet.Empty || children.exists(_.hasEmptyDataType)
  }
}

/**
 * Used when the parser is unable to match a string to an expression.
 */
trait UnknownExpression extends Expression with UnknownNode with PinnedDataType

/**
 * Used when the parser cannot match a SQL like string to an expression.
 */
case class UnknownSQLExpression(sql: String, dataType: DataType = AnyType)
    extends LeafExpression
    with UnknownExpression {

  override def nodeName: String = sql

  override def withNewDataTypeInternal(newDataType: DataType): Expression = {
    copy(dataType = newDataType)
  }
}

/**
 * Used when the parser cannot match a function name.
 */
case class UnknownSQLFunction(
    functionName: String,
    children: IndexedSeq[Expression],
    dataType: DataType = AnyType
) extends UnknownExpression {

  override def nodeName: String = functionName

  override def withNewDataTypeInternal(newDataType: DataType): Expression = {
    copy(dataType = newDataType)
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }
}

/**
 * Used when the parser knows there are more expressions but has no information about them.
 */
case class UndefinedExpression(dataType: DataType = AnyType)
    extends UnknownExpression
    with LeafLike[Expression] {

  override def withNewDataTypeInternal(newDataType: DataType): Expression = {
    copy(dataType = newDataType)
  }
}

/**
 * Used when the parser cannot infer the precedence of two expressions.
 */
case class AmbiguousExpression(children: IndexedSeq[Expression]) extends Expression {

  override def dataType: DataType = {
    DataType(children.map(_.dataType).toSet)
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val resolved = mapChildren(_.resolveDataType(outputType))
    val validChildren = resolved.children.filter(_.isCompatibleWith(outputType))

    if (resolved.children.length == validChildren.length) {
      resolved
    } else {
      AmbiguousExpression(validChildren)
    }
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }
}

object AmbiguousExpression {

  def apply(children: IndexedSeq[Expression]): Expression = {
    if (children.length == 1) {
      children.head
    } else {
      new AmbiguousExpression(children)
    }
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.LeafExpression]].
 */
trait LeafExpression extends Expression with LeafLike[Expression]

/**
 * [[org.apache.spark.sql.catalyst.expressions.UnaryExpression]].
 */
trait UnaryExpression extends Expression with UnaryLike[Expression]

/**
 * [[org.apache.spark.sql.catalyst.expressions.BinaryExpression]].
 */
trait BinaryExpression extends Expression with BinaryLike[Expression]

/**
 * [[org.apache.spark.sql.catalyst.expressions.BinaryOperator]].
 */
abstract class BinaryOperator extends BinaryExpression

/**
 * [[org.apache.spark.sql.catalyst.expressions.TernaryExpression]].
 */
trait TernaryExpression extends Expression with TernaryLike[Expression]

/**
 * [[org.apache.spark.sql.catalyst.expressions.QuaternaryExpression]].
 */
trait QuaternaryExpression extends Expression with QuaternaryLike[Expression]

/**
 * [[org.apache.spark.sql.catalyst.expressions.ComplexTypeMergingExpression]].
 */
trait ComplexTypeMergingExpression extends Expression with ExpectsInputType {

  private lazy val internalDataType: DataType = {
    children.map(_.dataType).reduce(DataType.intersection)
  }

  override def dataType: DataType = internalDataType

  override def resolveDataType(outputType: DataType): Expression = {
    val newChildDataType = inputType.intersect(dataType.intersect(outputType))
    mapChildren(_.resolveDataType(newChildDataType))
  }
}

/**
 * Propagates data type resolution to children.
 */
trait PropagatesDataType extends Expression {

  override def resolveDataType(outputType: DataType): Expression = {
    mapChildren(_.resolveDataType(outputType))
  }
}
