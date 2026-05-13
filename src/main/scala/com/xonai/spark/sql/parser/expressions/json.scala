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

import com.xonai.spark.sql.parser.types.{ArrayType, DataType, IntegerType, StringType, StructField, StructType}

/**
 * [[org.apache.spark.sql.catalyst.expressions.GetJsonObject]].
 */
case class GetJsonObject(json: Expression, path: Expression)
    extends BinaryExpression
    with ExpectsInputType {

  override def left: Expression = json

  override def right: Expression = path

  override def dataType: DataType = StringType

  override def inputType: DataType = StringType

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(json = newLeft, path = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.JsonTuple]].
 */
case class JsonTuple(children: IndexedSeq[Expression]) extends Generator with ExpectsInputType {

  override def inputType: DataType = StringType

  override def elementSchema: StructType = {
    val fields = children.tail.indices.map(index => StructField(s"c$index", StringType))
    StructType(fields.toArray)
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.JsonToStructs]].
 */
case class JsonToStructs(
    schema: DataType,
    options: Map[String, String],
    child: Expression,
    timeZoneId: Option[String]
) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = schema

  override def inputType: DataType = StringType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.StructsToJson]].
 */
case class StructsToJson(
    options: Map[String, String],
    child: Expression,
    timeZoneId: Option[String]
) extends UnaryExpression {

  override def dataType: DataType = StringType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.SchemaOfJson]].
 */
case class SchemaOfJson(child: Expression, options: Map[String, String])
    extends UnaryExpression
    with ExpectsInputType {

  override def dataType: DataType = StringType

  override def inputType: DataType = StringType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.LengthOfJsonArray]].
 */
case class LengthOfJsonArray(child: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = IntegerType

  override def inputType: DataType = StringType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.JsonObjectKeys]].
 */
case class JsonObjectKeys(child: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = ArrayType(StringType)

  override def inputType: DataType = StringType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}
