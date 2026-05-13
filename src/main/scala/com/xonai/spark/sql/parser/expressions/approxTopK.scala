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

import com.xonai.spark.sql.parser.types.{ArrayType, BinaryType, DataType, IntegerType, LongType, StringType, StructField, StructType}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ApproxTopKEstimate]].
 */
case class ApproxTopKEstimate(state: Expression, k: Expression) extends BinaryExpression {

  override def left: Expression = state

  override def right: Expression = k

  override def dataType: DataType = {
    val itemDataType = state.dataType.getStructFields(3).apply(2).dataType
    ArrayType(
      StructType(
        Array(
          StructField("item", itemDataType),
          StructField("count", LongType)
        )
      )
    )
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = outputType.intersect(dataType)
    val itemDataType = newDataType.getArrayElementType.getStructFields(2).head.dataType
    val stateDataType =
      StructType(
        Array(
          StructField("sketch", BinaryType),
          StructField("maxItemsTracked", IntegerType),
          StructField("itemDataType", itemDataType),
          StructField("itemDataTypeDDL", StringType)
        )
      )

    withNewChildren(
      IndexedSeq(
        state.resolveDataType(stateDataType),
        k.resolveDataType(IntegerType)
      )
    )
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(state = newLeft, k = newRight)
  }
}
