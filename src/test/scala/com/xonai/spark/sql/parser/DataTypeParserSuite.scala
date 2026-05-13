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

import com.xonai.spark.sql.parser.types._
import org.scalactic.source.Position
import org.scalatest.funsuite.AnyFunSuite

class DataTypeParserSuite extends AnyFunSuite {

  implicit class SuiteDslString(str: String) {

    def parseCatalogStringIs(expected: DataType)(implicit pos: Position): Unit = {
      assert(DataTypeParser.parseCatalogString(str) == expected)
    }

    def parseToStringIs(expected: DataType)(implicit pos: Position): Unit = {
      assert(DataTypeParser.parseToString(str) == expected)
    }
  }

  test("escape") {
    assert(
      DataTypeParser.escape("struct<0:int,1:map<int,struct<2:int,3:string>>>") ==
        "struct<`0`:int,`1`:map<int,struct<`2`:int,`3`:string>>>"
    )
    assert(
      DataTypeParser.escape("struct<map(1, NULL, 3):int,map(1, NULL, 3):map<int,string>>") ==
        "struct<`map(1, NULL, 3)`:int,`map(1, NULL, 3)`:map<int,string>>"
    )
    assert(
      DataTypeParser.escape("struct<map<long,long(a, 2)>:int,map<long,long(a, 2)>:long>") ==
        "struct<`map<long,long(a, 2)>`:int,`map<long,long(a, 2)>`:long>"
    )
  }

  test("parseCatalogString") {
    "void" parseCatalogStringIs NullType
    "boolean" parseCatalogStringIs BooleanType
    "tinyint" parseCatalogStringIs ByteType
    "smallint" parseCatalogStringIs ShortType
    "int" parseCatalogStringIs IntegerType
    "bigint" parseCatalogStringIs LongType
    "float" parseCatalogStringIs FloatType
    "double" parseCatalogStringIs DoubleType
    "decimal(3,2)" parseCatalogStringIs DecimalType
    "string" parseCatalogStringIs StringType
    "binary" parseCatalogStringIs BinaryType
    "date" parseCatalogStringIs DateType
    "timestamp" parseCatalogStringIs TimestampType
    "timestamp_ntz" parseCatalogStringIs TimestampNTZType
    "interval" parseCatalogStringIs CalendarIntervalType
    "interval day to second" parseCatalogStringIs DayTimeIntervalType
    "interval year to month" parseCatalogStringIs YearMonthIntervalType
    "time(6)" parseCatalogStringIs TimeType
    "variant" parseCatalogStringIs VariantType
    "geography(4326)" parseCatalogStringIs GeographyType
    "geometry(3857)" parseCatalogStringIs GeometryType
    "array<bigint>" parseCatalogStringIs ArrayType(LongType)
    "array<variant>" parseCatalogStringIs ArrayType(VariantType)
    "struct<a:bigint,b:string>" parseCatalogStringIs
      StructType(
        Array(
          StructField("a", LongType),
          StructField("b", StringType)
        )
      )
    "map<bigint,string>" parseCatalogStringIs MapType(LongType, StringType)
    "map<variant,variant>" parseCatalogStringIs MapType(VariantType, VariantType)
    "com.xonai.TestClass" parseCatalogStringIs ObjectType("com.xonai.TestClass")
  }

  test("parseSimpleString") {
    val structType = DataTypeParser.parseSimpleString("struct<a:bigint,... 2 more fields>")
    assert(structType.isInstanceOf[StructType])

    val fields = structType.asInstanceOf[StructType].fields
    assert(fields.length == 3)
    assert(fields.head == StructField("a", LongType))
    assert(fields(1) == StructField("", AnyType))
    assert(fields(2) == StructField("", AnyType))
  }

  test("parseToString") {
    "NullType" parseToStringIs NullType
    "BooleanType" parseToStringIs BooleanType
    "ByteType" parseToStringIs ByteType
    "ShortType" parseToStringIs ShortType
    "IntegerType" parseToStringIs IntegerType
    "LongType" parseToStringIs LongType
    "FloatType" parseToStringIs FloatType
    "DoubleType" parseToStringIs DoubleType
    "DecimalType(3,2)" parseToStringIs DecimalType
    "StringType" parseToStringIs StringType
    "BinaryType" parseToStringIs BinaryType
    "DateType" parseToStringIs DateType
    "TimestampType" parseToStringIs TimestampType
    "TimestampNTZType" parseToStringIs TimestampNTZType
    "CalendarIntervalType" parseToStringIs CalendarIntervalType
    "DayTimeIntervalType(0,3)" parseToStringIs DayTimeIntervalType
    "YearMonthIntervalType(0,1)" parseToStringIs YearMonthIntervalType
    "TimeType(6)" parseToStringIs TimeType
    "VariantType" parseToStringIs VariantType
    "GeographyType(4326)" parseToStringIs GeographyType
    "GeometryType(3857)" parseToStringIs GeometryType
    "ArrayType(LongType,true)" parseToStringIs ArrayType(LongType)
    "StructType(StructField(a,LongType,true),StructField(b,StringType,true))" parseToStringIs
      StructType(
        Array(
          StructField("a", LongType),
          StructField("b", StringType)
        )
      )
    "MapType(LongType,StringType,true)" parseToStringIs MapType(LongType, StringType)
    "ObjectType(com.xonai.TestClass)" parseToStringIs ObjectType("com.xonai.TestClass")
  }
}
