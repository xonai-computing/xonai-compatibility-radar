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

import com.xonai.spark.sql.parser.types.{AnyType, ArrayType, BinaryType, BooleanType, ByteType, CalendarIntervalType, DataType, DataTypeUtils, DateType, DayTimeIntervalType, DecimalType, DoubleType, FloatType, GeographyType, GeometryType, IntegerType, LongType, MapType, NullType, ObjectType, ShortType, StringType, StructField, StructType, TimeType, TimestampNTZType, TimestampType, UnknownType, VariantType, YearMonthIntervalType}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser

import javax.lang.model.SourceVersion
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

object DataTypeParser extends Parser {

  /**
   * [[org.apache.spark.sql.types.DataType.catalogString]].
   *
   * Example:
   * {{{
   * struct<id:int,name:string,timestamp:bigint,values:array<string>>
   * }}}
   */
  def parseCatalogString(str: String): DataType = {
    val escapedStr = escape(str)
    parseCatalogStringInternal(escapedStr)
  }

  private def parseCatalogStringInternal(str: String): DataType = {
    val placeholderPrefix = "xonai_parser_placeholder"
    val rewritten = str
      .replace("variant", s"struct<${placeholderPrefix}_variant:int>")
      .replaceAll("time\\(\\d\\)", s"struct<${placeholderPrefix}_time:int>")
      .replaceAll("geography\\(\\d+\\)", s"struct<${placeholderPrefix}_geography:int>")
      .replaceAll("geometry\\(\\d+\\)", s"struct<${placeholderPrefix}_geometry:int>")

    val sparkDataType = Try(CatalystSqlParser.parseDataType(rewritten))
    if (sparkDataType.isSuccess) {
      DataTypeUtils
        .fromSparkDataType(sparkDataType.get)
        .transformDown {
          case structType: StructType
              if structType.fields.length == 1 &&
                structType.fields.head.name.startsWith(placeholderPrefix) =>
            val placeholder = structType.fields.head.name
            if (placeholder.endsWith("variant")) {
              VariantType
            } else if (placeholder.endsWith("time")) {
              TimeType
            } else if (placeholder.endsWith("geography")) {
              GeographyType
            } else if (placeholder.endsWith("geometry")) {
              GeometryType
            } else {
              structType
            }
          case dataType =>
            dataType
        }
    } else if (SourceVersion.isName(str)) {
      ObjectType(str)
    } else {
      UnknownType(str)
    }
  }

  /**
   * [[org.apache.spark.sql.types.DataType.simpleString]].
   *
   * Similar to [[parseCatalogString]] but the [[org.apache.spark.sql.types.StructType]] fields may
   * be truncated by [[org.apache.spark.sql.catalyst.util.SparkStringUtils.truncatedString]].
   */
  def parseSimpleString(str: String): DataType = {
    // Replace all occurrences of `... X more fields` with placeholders.
    val placeholderName = "XONAI_PARSER_PLACEHOLDER"
    val placeholder = s"$placeholderName:int"
    var i = 0
    var simpleStr = escape(str)
    var replaced = false
    while (i < simpleStr.length) {
      val start = simpleStr.indexOf("... ", i)
      if (start == -1) {
        i = simpleStr.length
      } else {
        replaced = true
        val end = simpleStr.indexOf(" more fields", start)
        val count = simpleStr.substring(start + 4, end).toInt
        val placeholders = (0 until count).map(_ => placeholder).mkString(",")
        simpleStr = simpleStr.substring(0, start) + placeholders + simpleStr.substring(end + 12)
        i = start + placeholders.length
      }
    }

    val dataType = parseCatalogStringInternal(simpleStr)
    if (replaced) {
      dataType.transformDown {
        case structType: StructType =>
          StructType(
            structType.fields.map { field =>
              if (field.name == placeholderName) {
                StructField("", AnyType)
              } else {
                field
              }
            }
          )
        case dataType =>
          dataType
      }
    } else {
      dataType
    }
  }

  /**
   * [[org.apache.spark.sql.types.DataType.toString]].
   */
  def parseToString(str: String): DataType = {
    str match {
      case "NullType" =>
        NullType
      case "BooleanType" =>
        BooleanType
      case "ByteType" =>
        ByteType
      case "ShortType" =>
        ShortType
      case "IntegerType" =>
        IntegerType
      case "LongType" =>
        LongType
      case "FloatType" =>
        FloatType
      case "DoubleType" =>
        DoubleType
      case _ if str.startsWith("DecimalType") =>
        DecimalType
      case "StringType" =>
        StringType
      case "BinaryType" =>
        BinaryType
      case "DateType" =>
        DateType
      case "TimestampType" =>
        TimestampType
      case "TimestampNTZType" =>
        TimestampNTZType
      case "CalendarIntervalType" =>
        CalendarIntervalType
      case _ if str.startsWith("DayTimeIntervalType") =>
        DayTimeIntervalType
      case _ if str.startsWith("YearMonthIntervalType") =>
        YearMonthIntervalType
      case _ if str.startsWith("TimeType") =>
        TimeType
      case "VariantType" =>
        VariantType
      case _ if str.startsWith("GeographyType") =>
        GeographyType
      case _ if str.startsWith("GeometryType") =>
        GeometryType
      case _ if str.startsWith("ArrayType") =>
        val parts = splitParenthesesList(str.substring(9))
        ArrayType(parseToString(parts.head))
      case _ if str.startsWith("StructType") =>
        val fields = splitParenthesesList(str.substring(10)).map(parseStructField)
        StructType(fields.toArray)
      case _ if str.startsWith("MapType") =>
        val parts = splitParenthesesList(str.substring(7))
        MapType(
          parseToString(parts.head),
          parseToString(parts(1))
        )
      case _ if str.startsWith("ObjectType") =>
        val parts = splitParenthesesList(str.substring(10))
        ObjectType(getClassName(parts.head))
      case _ =>
        UnknownType(str)
    }
  }

  /**
   * [[org.apache.spark.sql.types.StructField.toString]].
   */
  def parseStructField(str: String): StructField = {
    val parts = splitParenthesesList(str.substring("StructField".length))
    val name = parts.head
    val dataType = parseToString(parts(1))
    StructField(name, dataType)
  }

  /**
   * StructType field names can be arbitrary which the parser may get confused with.
   */
  def escape(str: String): String = {
    var i = str.length - 1
    val parts = new ArrayBuffer[String]()
    while (i >= 0) {
      val colon = str.lastIndexOf(':', i)
      if (colon == -1) {
        parts += str.substring(0, i + 1)
        i = -1
      } else {
        val less = rootLevelLastIndexOfChar(str, '<', colon)
        val comma = rootLevelLastIndexOfChar(str, ',', colon)
        val start = Math.max(less, comma)
        if (start == -1) {
          throw ParseException(str, "No field name start")
        }
        val fieldName = str.substring(start + 1, colon)
        parts.append(str.substring(colon, i + 1))
        parts.append("`")
        parts.append(fieldName)
        parts.append("`")
        i = start
      }
    }

    parts.reverse.mkString
  }

  private def rootLevelLastIndexOfChar(str: String, char: Char, index: Int): Int = {
    var i = index
    var opened = 0
    while (i >= 0) {
      val current = str.charAt(i)
      if (current == char && opened == 0) {
        return i
      }

      current match {
        case '(' | '[' | '<' =>
          opened -= 1
        case ')' | ']' | '>' =>
          opened += 1
        case _ =>
          ()
      }
      i -= 1
    }

    -1
  }
}
