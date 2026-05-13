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

import com.xonai.spark.sql.parser.types.{BinaryType, DataType, LongType, NullType, TypeSet}

/**
 * [[org.apache.spark.sql.catalyst.expressions.BloomFilterMightContain]].
 */
case class BloomFilterMightContain(bloomFilterExpression: Expression, valueExpression: Expression)
    extends BinaryExpression
    with Predicate {

  override def left: Expression = bloomFilterExpression

  override def right: Expression = valueExpression

  override def resolveDataType(outputType: DataType): Expression = {
    withNewChildren(
      IndexedSeq(
        bloomFilterExpression.resolveDataType(TypeSet(BinaryType, NullType)),
        valueExpression.resolveDataType(TypeSet(LongType, NullType))
      )
    )
  }
  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(bloomFilterExpression = newLeft, valueExpression = newRight)
  }
}
