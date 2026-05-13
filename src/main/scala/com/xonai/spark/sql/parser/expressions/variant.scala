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

import com.xonai.spark.sql.parser.types.{DataType, StringType, VariantType}

/**
 * [[org.apache.spark.sql.catalyst.expressions.variant.ParseJson]].
 */
case class ParseJson(child: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = VariantType

  override def inputType: DataType = StringType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.variant.IsVariantNull]].
 */
case class IsVariantNull(child: Expression)
    extends UnaryExpression
    with Predicate
    with ExpectsInputType {

  override def inputType: DataType = VariantType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.variant.ToVariantObject]].
 */
case class ToVariantObject(child: Expression) extends UnaryExpression {

  override def dataType: DataType = VariantType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.variant.VariantGet]].
 */
case class VariantGet(
    child: Expression,
    path: Expression,
    targetType: DataType,
    failOnError: Boolean
) extends BinaryExpression with ExpectsInputTypes {

  override def dataType: DataType = targetType

  override def left: Expression = child

  override def right: Expression = path

  override def inputTypes: Seq[DataType] = Seq(VariantType, StringType)

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(child = newLeft, path = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.variant.SchemaOfVariant]].
 */
case class SchemaOfVariant(child: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = StringType

  override def inputType: DataType = VariantType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}
