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

import com.xonai.spark.sql.parser.types.{DataType, DecimalType, StringType}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ToNumberBase]].
 */
trait ToNumberBase extends BinaryExpression with ExpectsInputType {

  override def dataType: DataType = DecimalType

  override def inputType: DataType = StringType
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ToNumber]].
 */
case class ToNumber(left: Expression, right: Expression) extends ToNumberBase {

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.TryToNumber]].
 */
case class TryToNumber(left: Expression, right: Expression) extends ToNumberBase {

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ToCharacter]].
 */
case class ToCharacter(left: Expression, right: Expression)
    extends BinaryExpression
    with ExpectsInputTypes {

  override def dataType: DataType = StringType

  override def inputTypes: Seq[DataType] = Seq(DecimalType, StringType)

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}
