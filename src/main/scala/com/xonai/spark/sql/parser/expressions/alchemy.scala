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

import com.xonai.spark.sql.parser.trees.UnaryLike
import com.xonai.spark.sql.parser.types.{BinaryType, DataType, LongType}

/**
 * [[com.swoop.alchemy.spark.expressions.hll.HyperLogLogCardinality]].
 */
case class HyperLogLogCardinality(child: Expression, impl: String)
    extends UnaryExpression
    with ExpectsInputType {

  override def dataType: DataType = LongType

  override def inputType: DataType = BinaryType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[com.swoop.alchemy.spark.expressions.hll.HyperLogLogInitSimpleAgg]].
 */
case class HyperLogLogInitSimpleAgg(child: Expression, relativeSD: Double, impl: String)
    extends AggregateFunction
    with UnaryLike[Expression] {

  override def dataType: DataType = BinaryType

  override def aggBufferDataTypes: IndexedSeq[DataType] = IndexedSeq(BinaryType)

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}
