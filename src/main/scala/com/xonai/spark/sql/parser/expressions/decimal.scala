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

import com.xonai.spark.sql.parser.types.{DataType, DecimalType, LongType}

/**
 * [[org.apache.spark.sql.catalyst.expressions.UnscaledValue]].
 */
case class UnscaledValue(child: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = LongType

  override def inputType: DataType = DecimalType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.MakeDecimal]].
 */
case class MakeDecimal(child: Expression, precision: Int, scale: Int)
    extends UnaryExpression
    with ExpectsInputType {

  override def dataType: DataType = DecimalType

  override def inputType: DataType = LongType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.PromotePrecision]].
 */
case class PromotePrecision(child: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = child.dataType

  override def inputType: DataType = DecimalType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.CheckOverflow]].
 */
case class CheckOverflow(child: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = DecimalType

  override def inputType: DataType = DecimalType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}
