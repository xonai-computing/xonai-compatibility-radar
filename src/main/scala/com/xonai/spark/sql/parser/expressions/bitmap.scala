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
 * [[org.apache.spark.sql.catalyst.expressions.BitmapBucketNumber]].
 */
case class BitmapBucketNumber(child: Expression) extends UnaryExpression with DataTypeIsInputType {

  override def inputType: DataType = LongType

  override def dataType: DataType = LongType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.BitmapBitPosition]].
 */
case class BitmapBitPosition(child: Expression) extends UnaryExpression with DataTypeIsInputType {

  override def inputType: DataType = LongType

  override def dataType: DataType = LongType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.BitmapCount]].
 */
case class BitmapCount(child: Expression) extends UnaryExpression {

  override def dataType: DataType = LongType

  override def resolveDataType(outputType: DataType): Expression = {
    mapChildren(_.resolveDataType(BinaryType))
  }

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.BitmapConstructAgg]].
 */
case class BitmapConstructAgg(child: Expression)
    extends AggregateFunction
    with ExpectsInputType
    with UnaryLike[Expression] {

  override def inputType: DataType = LongType

  override def dataType: DataType = BinaryType

  override def aggBufferDataTypes: IndexedSeq[DataType] = IndexedSeq(BinaryType)

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.BitmapOrAgg]].
 */
case class BitmapOrAgg(child: Expression)
    extends AggregateFunction
    with DataTypeIsInputType
    with UnaryLike[Expression] {

  override def inputType: DataType = BinaryType

  override def dataType: DataType = BinaryType

  override def aggBufferDataTypes: IndexedSeq[DataType] = IndexedSeq(BinaryType)

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.BitmapAndAgg]].
 */
case class BitmapAndAgg(child: Expression)
    extends AggregateFunction
    with DataTypeIsInputType
    with UnaryLike[Expression] {

  override def inputType: DataType = BinaryType

  override def dataType: DataType = BinaryType

  override def aggBufferDataTypes: IndexedSeq[DataType] = IndexedSeq(BinaryType)

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}
