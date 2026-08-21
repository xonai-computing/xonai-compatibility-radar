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

import com.xonai.spark.sql.parser.types.{BinaryType, DataType, StringType}

/**
 * [[org.apache.spark.sql.catalyst.expressions.TupleIntersectionBase]].
 */
abstract class TupleIntersectionBase
    extends TernaryExpression
    with ExpectsInputTypes {

  override def inputTypes: Seq[DataType] = {
    Seq(BinaryType, BinaryType, StringType)
  }

  override def dataType: DataType = BinaryType
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.TupleIntersectionDouble]].
 */
case class TupleIntersectionDouble(first: Expression, second: Expression, third: Expression)
    extends TupleIntersectionBase {

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(first = newFirst, second = newSecond, third = newThird)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.TupleIntersectionInteger]].
 */
case class TupleIntersectionInteger(first: Expression, second: Expression, third: Expression)
    extends TupleIntersectionBase {

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(first = newFirst, second = newSecond, third = newThird)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.TupleIntersectionThetaDouble]].
 */
case class TupleIntersectionThetaDouble(first: Expression, second: Expression, third: Expression)
    extends TupleIntersectionBase {

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(first = newFirst, second = newSecond, third = newThird)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.TupleIntersectionThetaInteger]].
 */
case class TupleIntersectionThetaInteger(first: Expression, second: Expression, third: Expression)
    extends TupleIntersectionBase {

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(first = newFirst, second = newSecond, third = newThird)
  }
}
