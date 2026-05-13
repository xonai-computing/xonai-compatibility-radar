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

import com.xonai.spark.sql.parser.DataTypeResolver
import com.xonai.spark.sql.parser.types.{BooleanType, DataType, DoubleType, IntegerType, LongType}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Literal]].
 */
case class Literal(private val value: String, dataType: DataType)
    extends LeafExpression
    with PinnedDataType {

  def booleanValue: Boolean = value.toBoolean

  def intValue: Int = value.toInt

  def doubleValue: Double = value.toDouble

  def stringValue: String = value

  override def withNewDataTypeInternal(newDataType: DataType): Expression = {
    copy(dataType = newDataType)
  }
}

object Literal {

  def apply(value: String, dataType: DataType): Literal = {
    val v =
      if (value == null) {
        "null"
      } else {
        value
      }
    new Literal(v, dataType)
  }

  def apply(value: String): Literal = {
    Literal(value, DataTypeResolver.resolveLiteral(value))
  }

  def apply(value: Boolean): Literal = {
    Literal(value.toString, BooleanType)
  }

  def apply(value: Int): Literal = {
    Literal(value.toString, IntegerType)
  }

  def apply(value: Long): Literal = {
    Literal(value.toString, LongType)
  }

  def apply(value: Double): Literal = {
    Literal(value.toString, DoubleType)
  }
}
