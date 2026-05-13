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

package com.xonai.spark.sql.parser.types

import com.xonai.spark.sql.parser.expressions.Attribute
import org.apache.spark.sql.types

/**
 * [[org.apache.spark.sql.catalyst.types.DataTypeUtils]].
 */
object DataTypeUtils {

  def fromSparkDataType(dataType: types.DataType): DataType = {
    dataType match {
      case types.NullType =>
        NullType
      case types.BooleanType =>
        BooleanType
      case types.ByteType =>
        ByteType
      case types.ShortType =>
        ShortType
      case types.IntegerType =>
        IntegerType
      case types.LongType =>
        LongType
      case types.FloatType =>
        FloatType
      case types.DoubleType =>
        DoubleType
      case _: types.DecimalType =>
        DecimalType
      case types.StringType =>
        StringType
      case types.BinaryType =>
        BinaryType
      case types.DateType =>
        DateType
      case types.TimestampType =>
        TimestampType
      case types.TimestampNTZType =>
        TimestampNTZType
      case types.CalendarIntervalType =>
        CalendarIntervalType
      case _: types.DayTimeIntervalType =>
        DayTimeIntervalType
      case _: types.YearMonthIntervalType =>
        YearMonthIntervalType
      case arrayType: types.ArrayType =>
        ArrayType(
          fromSparkDataType(arrayType.elementType)
        )
      case structType: types.StructType =>
        StructType(
          structType.fields.map { f =>
            StructField(f.name, fromSparkDataType(f.dataType))
          }
        )
      case mapType: types.MapType =>
        MapType(
          fromSparkDataType(mapType.keyType),
          fromSparkDataType(mapType.valueType)
        )
      case objectType: types.ObjectType =>
        ObjectType(objectType.cls.getName)
      case udt: types.UserDefinedType[_] =>
        UserDefinedType(
          fromSparkDataType(udt.sqlType)
        )
      case _ =>
        val className = dataType.getClass.getSimpleName
        className match {
          case "TimeType" =>
            TimeType
          case "GeographyType" =>
            GeographyType
          case "GeometryType" =>
            GeometryType
          case _ if className.startsWith("VariantType") =>
            VariantType
          case _ =>
            UnknownType(dataType.toString)
        }
    }
  }

  /**
   * [[org.apache.spark.sql.catalyst.types.DataTypeUtils.fromAttributes]].
   */
  def fromAttributes(attributes: Seq[Attribute]): StructType = {
    StructType(
      attributes.map(a => StructField(a.name, a.dataType)).toArray
    )
  }
}
