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

import com.xonai.spark.sql.parser.types.{AnyType, BinaryType, DataType}

/**
 * [[org.apache.spark.sql.protobuf.ProtobufDataToCatalyst]].
 * Replacement of [[org.apache.spark.sql.catalyst.expressions.FromProtobuf]].
 */
case class ProtobufDataToCatalyst(child: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = AnyType

  override def inputType: DataType = BinaryType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.protobuf.CatalystDataToProtobuf]].
 * Replacement of [[org.apache.spark.sql.catalyst.expressions.ToProtobuf]].
 */
case class CatalystDataToProtobuf(child: Expression) extends UnaryExpression {

  override def dataType: DataType = BinaryType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}
