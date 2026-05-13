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

package com.xonai.spark.sql.parser

import com.xonai.spark.sql.parser.expressions.{Attribute, AttributeReference, Expression}
import com.xonai.spark.sql.parser.types.{AnyType, ArrayType, BinaryType, BooleanType, ByteType, CalendarIntervalType, DataType, DateType, DayTimeIntervalType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, ShortType, StringType, StructField, StructType, TimeType, TimestampNTZType, TimestampType, TypeSet, YearMonthIntervalType}

import scala.util.Try

object DataTypeResolver extends Parser {

  private val dateTypePattern =
    "[0-9]{4}-[0-1][0-9]-[0-3][0-9]"

  private val timestampPattern =
    "[0-9]{4}-[0-1][0-9]-[0-3][0-9] [0-2][0-9]:[0-5][0-9]:[0-5][0-9](.[0-9]{0,6})?"

  private val timePattern =
    "[0-2][0-9]:[0-5][0-9]:[0-5][0-9](.[0-9]{0,6})?"

  /**
   * Resolves data types of expression tree given an input and expected output type.
   */
  def resolve[A <: Expression](expression: A, input: Seq[Attribute], outputType: DataType): A = {
    resolveReferences(expression, input).resolveDataType(outputType).asInstanceOf[A]
  }

  /**
   * Replaces the data types in AttributeReference expressions with the data types from the given
   * attributes.
   */
  def resolveReferences[A <: Expression](expression: A, input: Seq[Attribute]): A = {
    val idsToDataType = input.map(a => a.exprId -> a.dataType).toMap

    expression
      .transform { case attribute: AttributeReference =>
        val dataType = idsToDataType.get(attribute.exprId)
        if (dataType.isDefined) {
          attribute.copy(dataType = dataType.get)
        } else {
          attribute
        }
      }
      .asInstanceOf[A]
  }

  /**
   * Infers from the given literal string the possible data types.
   * TODO: VariantType.
   */
  def resolveLiteral(value: String, nested: Boolean = false): DataType = {
    if (value == "null") {
      return AnyType
    }

    if (value == "") {
      return StringType
    }

    // Boolean.
    if (value == "false" || value == "true") {
      return TypeSet(BooleanType, StringType)
    }

    // Numeric.
    if (isNumeric(value) || (value.head == '-' && isNumeric(value.tail))) {
      val longTry = Try(value.toLong)
      if (longTry.isSuccess) {
        val (integerTypes, longTypes) =
          if (nested) {
            (
              TypeSet(IntegerType, DateType, YearMonthIntervalType),
              TypeSet(LongType, TimestampType, TimestampNTZType, DayTimeIntervalType, TimeType)
            )
          } else {
            (IntegerType, LongType)
          }

        val longValue = longTry.get
        if (longValue >= Byte.MinValue && longValue <= Byte.MaxValue) {
          return TypeSet(DecimalType, ShortType, ByteType, StringType) + integerTypes + longTypes
        }

        if (longValue >= Short.MinValue && longValue <= Short.MaxValue) {
          return TypeSet(DecimalType, ShortType, StringType) + integerTypes + longTypes
        }

        if (longValue >= Int.MinValue && longValue <= Int.MaxValue) {
          return TypeSet(DecimalType, StringType) + integerTypes + longTypes
        }

        return TypeSet(DecimalType, StringType) + longTypes
      }

      if (value.length <= 38 || (value.head == '-' && value.length <= 39)) {
        return TypeSet(DecimalType, StringType)
      }

      return StringType
    }

    // Floating-point.
    if (Try(value.toDouble).isSuccess) {
      return TypeSet(DoubleType, DecimalType, FloatType, StringType)
    }

    // Binary.
    if (value.startsWith("0x") && isHexadecimal(value.tail.tail)) {
      return TypeSet(BinaryType, StringType)
    }

    // Date.
    if (value.matches(dateTypePattern)) {
      return TypeSet(DateType, StringType)
    }

    // Timestamp.
    if (value.matches(timestampPattern)) {
      return TypeSet(TimestampType, TimestampNTZType, StringType)
    }

    // Times.
    if (value.matches(timePattern)) {
      return TypeSet(TimeType, StringType)
    }

    // CalendarInterval.
    if (
      (value.head >= '0' && value.head <= '9') && (
        value.endsWith(" years") ||
          value.endsWith(" months") ||
          value.endsWith(" days") ||
          value.endsWith(" hours") ||
          value.endsWith(" minutes") ||
          value.endsWith(" seconds")
      )
    ) {
      return TypeSet(CalendarIntervalType, StringType)
    }

    // DayTimeInterval.
    if (
      value.startsWith("INTERVAL '") && (
        value.endsWith(" DAY") ||
          value.endsWith(" HOUR") ||
          value.endsWith(" MINUTE") ||
          value.endsWith(" SECOND")
      )
    ) {
      return TypeSet(DayTimeIntervalType, StringType)
    }

    // YearMonthInterval.
    if (
      value.startsWith("INTERVAL '") && (
        value.endsWith(" YEAR") ||
          value.endsWith(" MONTH")
      )
    ) {
      return TypeSet(YearMonthIntervalType, StringType)
    }

    // Array or Struct.
    if (value.head == '[' && value.endsWith("]")) {
      val parts = splitSquareBracketsList(value).foldRight(List[String]()) { case (part, parts) =>
        if (
          part.startsWith("keys: [") &&
          parts.headOption.exists(_.startsWith("values: ["))
        ) {
          // Re-group map keys and values into single string.
          (part + ',' + parts.head) :: parts.tail
        } else {
          part :: parts
        }
      }

      if (parts.isEmpty) {
        return TypeSet(
          ArrayType(AnyType),
          StructType(Array(StructField("", StringType))),
          StringType
        )
      }

      val types = parts.map(resolveLiteral(_, nested = true))
      val structType = StructType(types.map(StructField(name = "", _)).toArray)

      val intersectionType = types.reduce(DataType.intersection)
      if (intersectionType == TypeSet.Empty) {
        return TypeSet(structType, StringType)
      }

      return TypeSet(ArrayType(intersectionType), structType, StringType)
    }

    // Map.
    if (value.startsWith("keys: [")) {
      val parts = splitList(value)
      if (
        parts.length == 2 &&
        parts(0).endsWith("]") &&
        parts(1).startsWith("values: [") &&
        parts(1).endsWith("]")
      ) {
        val keyStr = parts(0).substring(6)
        val valueStr = parts(1).substring(8)
        val keyType = resolveLiteral(keyStr, nested = true).getArrayElementType
        val valueType = resolveLiteral(valueStr, nested = true).getArrayElementType
        return MapType(keyType, valueType)
      }
    }

    StringType
  }

  private def isNumeric(str: String): Boolean = {
    str.forall(c => c >= '0' && c <= '9') && str.nonEmpty
  }

  private def isHexadecimal(str: String): Boolean = {
    str.forall { c =>
      (c >= '0' && c <= '9') ||
      (c >= 'a' && c <= 'f') ||
      (c >= 'A' && c <= 'F')
    }
  }
}
