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

import com.xonai.spark.sql.parser.types.{BinaryType, DataType, DoubleType, LongType, StringType}

/**
 * [[org.apache.spark.sql.catalyst.expressions.TupleSketchSummaryDouble]].
 */
case class TupleSketchSummaryDouble(left: Expression, right: Expression)
    extends BinaryExpression
    with ExpectsInputTypes {

  override def inputTypes: Seq[DataType] = Seq(BinaryType, StringType)

  override def dataType: DataType = DoubleType

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.TupleSketchSummaryInteger]].
 */
case class TupleSketchSummaryInteger(left: Expression, right: Expression)
    extends BinaryExpression
    with ExpectsInputTypes {

  override def inputTypes: Seq[DataType] = Seq(BinaryType, StringType)

  override def dataType: DataType = LongType

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}
