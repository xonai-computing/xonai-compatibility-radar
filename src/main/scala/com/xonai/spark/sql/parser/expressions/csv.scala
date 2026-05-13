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

import com.xonai.spark.sql.parser.types.{DataType, StringType, StructType}

/**
 * [[org.apache.spark.sql.catalyst.expressions.CsvToStructs]].
 */
case class CsvToStructs(
    schema: StructType,
    options: Map[String, String],
    child: Expression,
    timeZoneId: Option[String],
    requiredSchema: Option[StructType]
) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = requiredSchema.getOrElse(schema)

  override def inputType: DataType = StringType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.SchemaOfCsv]].
 */
case class SchemaOfCsv(child: Expression, options: Map[String, String])
    extends UnaryExpression
    with ExpectsInputType {

  override def dataType: DataType = StringType

  override def inputType: DataType = StringType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.StructsToCsv]].
 */
case class StructsToCsv(options: Map[String, String], child: Expression, timeZoneId: Option[String])
    extends UnaryExpression {

  override def dataType: DataType = StringType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}
