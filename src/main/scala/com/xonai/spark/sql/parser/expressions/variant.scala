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

import com.xonai.spark.sql.parser.types.{AnyType, BooleanType, DataType, StringType, VariantType}

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

/**
 * [[org.apache.spark.sql.catalyst.expressions.variant.IsValidVariant]].
 */
case class IsValidVariant(child: Expression)
    extends UnaryExpression
    with Predicate
    with ExpectsInputType {

  override def inputType: DataType = VariantType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.variant.VariantDelete]].
 */
case class VariantDelete(children: IndexedSeq[Expression])
    extends Expression
    with ExpectsInputTypes {

  override def dataType: DataType = VariantType

  override def inputTypes: Seq[DataType] = {
    VariantType +: Seq.fill(math.max(children.length - 1, 0))(StringType)
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.variant.VariantInsert]].
 */
case class VariantInsert(
    input: Expression,
    path: Expression,
    value: Expression,
    failOnError: Boolean
) extends TernaryExpression with ExpectsInputTypes {

  override def dataType: DataType = VariantType

  override def inputTypes: Seq[DataType] = {
    Seq(VariantType, StringType, AnyType)
  }

  override def first: Expression = input

  override def second: Expression = path

  override def third: Expression = value

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(input = newFirst, path = newSecond, value = newThird)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.variant.VariantSet]].
 */
case class VariantSet(
    input: Expression,
    path: Expression,
    value: Expression,
    createIfMissing: Expression,
    failOnError: Boolean
) extends QuaternaryExpression with ExpectsInputTypes {

  override def dataType: DataType = VariantType

  override def inputTypes: Seq[DataType] = {
    Seq(VariantType, StringType, AnyType, BooleanType)
  }

  override def first: Expression = input

  override def second: Expression = path

  override def third: Expression = value

  override def fourth: Expression = createIfMissing

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression,
      newFourth: Expression
  ): Expression = {
    copy(input = newFirst, path = newSecond, value = newThird, createIfMissing = newFourth)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.variant.VariantArrayAppend]].
 */
case class VariantArrayAppend(
    input: Expression,
    path: Expression,
    value: Expression,
    failOnError: Boolean
) extends TernaryExpression with ExpectsInputTypes {

  override def dataType: DataType = VariantType

  override def inputTypes: Seq[DataType] = {
    Seq(VariantType, StringType, AnyType)
  }

  override def first: Expression = input

  override def second: Expression = path

  override def third: Expression = value

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(input = newFirst, path = newSecond, value = newThird)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.variant.VariantStripNulls]].
 */
case class VariantStripNulls(child: Expression, includeArrays: Expression)
    extends BinaryExpression
    with ExpectsInputTypes {

  override def dataType: DataType = VariantType

  override def inputTypes: Seq[DataType] = {
    Seq(VariantType, BooleanType)
  }

  override def left: Expression = child

  override def right: Expression = includeArrays

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(child = newLeft, includeArrays = newRight)
  }
}
