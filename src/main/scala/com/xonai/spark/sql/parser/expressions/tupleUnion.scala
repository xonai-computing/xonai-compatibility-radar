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

import com.xonai.spark.sql.parser.types.{BinaryType, DataType, IntegerType, StringType}

/**
 * [[org.apache.spark.sql.catalyst.expressions.TupleUnionBase]].
 */
abstract class TupleUnionBase
    extends QuaternaryExpression
    with ExpectsInputTypes {

  override def inputTypes: Seq[DataType] = {
    Seq(BinaryType, BinaryType, IntegerType, StringType)
  }

  override def dataType: DataType = BinaryType
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.TupleUnionDouble]].
 */
case class TupleUnionDouble(
    first: Expression,
    second: Expression,
    third: Expression,
    fourth: Expression
) extends TupleUnionBase {

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression,
      newFourth: Expression
  ): Expression = {
    copy(first = newFirst, second = newSecond, third = newThird, fourth = newFourth)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.TupleUnionInteger]].
 */
case class TupleUnionInteger(
    first: Expression,
    second: Expression,
    third: Expression,
    fourth: Expression
) extends TupleUnionBase {

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression,
      newFourth: Expression
  ): Expression = {
    copy(first = newFirst, second = newSecond, third = newThird, fourth = newFourth)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.TupleUnionThetaDouble]].
 */
case class TupleUnionThetaDouble(
    first: Expression,
    second: Expression,
    third: Expression,
    fourth: Expression
) extends TupleUnionBase {

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression,
      newFourth: Expression
  ): Expression = {
    copy(first = newFirst, second = newSecond, third = newThird, fourth = newFourth)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.TupleUnionThetaInteger]].
 */
case class TupleUnionThetaInteger(
    first: Expression,
    second: Expression,
    third: Expression,
    fourth: Expression
) extends TupleUnionBase {

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression,
      newFourth: Expression
  ): Expression = {
    copy(first = newFirst, second = newSecond, third = newThird, fourth = newFourth)
  }
}
