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

import com.xonai.spark.sql.parser.types.{AnyType, DataType}

/**
 * [[org.apache.spark.sql.catalyst.expressions.NamedExpression]].
 */
trait NamedExpression extends Expression {

  def name: String

  def exprId: Long

  def toAttribute: Attribute
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Alias]].
 */
case class Alias(child: Expression, name: String, exprId: Long)
    extends UnaryExpression
    with NamedExpression
    with PropagatesDataType {

  override def dataType: DataType = child.dataType

  override def toAttribute: Attribute = {
    AttributeReference(name, child.dataType, exprId)
  }

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Attribute]].
 */
abstract class Attribute extends LeafExpression with NamedExpression {

  override def toAttribute: Attribute = this
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.AttributeReference]].
 */
case class AttributeReference(name: String, dataType: DataType, exprId: Long)
    extends Attribute
    with PinnedDataType {

  override def withNewDataTypeInternal(newDataType: DataType): Expression = {
    copy(dataType = newDataType)
  }
}

object AttributeReference {

  def apply(name: String, exprId: Long): AttributeReference = {
    AttributeReference(name, AnyType, exprId)
  }

  def apply(name: String, dataType: DataType): AttributeReference = {
    AttributeReference(name, dataType, -1)
  }
}
