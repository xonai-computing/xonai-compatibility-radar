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

import com.xonai.spark.sql.parser.types.{BinaryType, DataType}

/**
 * [[org.apache.spark.sql.catalyst.expressions.TupleDifferenceBase]].
 */
abstract class TupleDifferenceBase
    extends BinaryExpression
    with ExpectsInputType {

  override def inputType: DataType = BinaryType

  override def dataType: DataType = BinaryType
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.TupleDifferenceDouble]].
 */
case class TupleDifferenceDouble(left: Expression, right: Expression) extends TupleDifferenceBase {

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.TupleDifferenceInteger]].
 */
case class TupleDifferenceInteger(left: Expression, right: Expression) extends TupleDifferenceBase {

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.TupleDifferenceThetaDouble]].
 */
case class TupleDifferenceThetaDouble(left: Expression, right: Expression)
    extends TupleDifferenceBase {

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.TupleDifferenceThetaInteger]].
 */
case class TupleDifferenceThetaInteger(left: Expression, right: Expression)
    extends TupleDifferenceBase {

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}
