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

package com.xonai.spark.test

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, ShortType, StringType, StructField, StructType, TimestampType}

import java.sql.{Date, Timestamp}
import scala.util.Random

trait TestData {

  def exampleAtomicSchema: StructType = {
    val fields =
      Array(
        ("booleans", BooleanType),
        ("bytes", ByteType),
        ("shorts", ShortType),
        ("ints", IntegerType),
        ("longs", LongType),
        ("floats", FloatType),
        ("doubles", DoubleType),
        ("decimals", DecimalType(10, 2)),
        ("decimals_binary", DecimalType(30, 20)),
        ("dates", DateType),
        ("timestamps", TimestampType),
        ("strings", StringType),
        ("binaries", BinaryType)
      ).map { case (name, datatype) =>
        StructField(name, datatype)
      }
    StructType(fields)
  }

  def exampleArraySchema: StructType = {
    val atomicFields = exampleAtomicSchema.fields
    val fields =
      atomicFields.map { field =>
        StructField(s"array_${field.name}", ArrayType(field.dataType))
      } ++ Array(
        StructField("array_structs", ArrayType(StructType(atomicFields))),
        StructField("array_arrays", ArrayType(ArrayType(DecimalType(38, 10)))),
        StructField("array_maps", ArrayType(MapType(StringType, DecimalType(38, 10))))
      )
    StructType(fields)
  }

  def exampleStructSchema: StructType = {
    val atomicFields = exampleAtomicSchema.fields
    val arrayFields = exampleArraySchema.fields
    val fields =
      Array(
        StructField("structs", StructType(atomicFields)),
        StructField("struct_arrays", StructType(arrayFields)),
        StructField(
          "struct_maps",
          StructType(
            Array(
              StructField("maps", MapType(StringType, DecimalType(38, 10)))
            )
          )
        ),
        StructField(
          "struct_structs",
          StructType(
            Array(
              StructField("structs", StructType(atomicFields)),
              StructField("struct_arrays", StructType(arrayFields)),
              StructField(
                "struct_maps",
                StructType(
                  Array(
                    StructField("maps", MapType(StringType, DecimalType(38, 10)))
                  )
                )
              )
            )
          )
        )
      )
    StructType(fields)
  }

  def exampleMapSchema: StructType = {
    val atomicFields = exampleAtomicSchema.fields
    val arrayFields = exampleArraySchema.fields
    val structFields = exampleStructSchema.fields
    val fields =
      Array(
        StructField("maps", MapType(StringType, LongType)),
        StructField("maps_arrays", MapType(StringType, ArrayType(DecimalType(38, 10)))),
        StructField(
          "maps_structs",
          MapType(StringType, StructType(atomicFields ++ arrayFields ++ structFields))
        ),
        StructField(
          "map_maps",
          MapType(StringType, MapType(StringType, DecimalType(38, 10)))
        )
      )
    StructType(fields)
  }

  def exampleSchema: StructType = {
    val atomicFields = exampleAtomicSchema.fields
    val arrayFields = exampleArraySchema.fields
    val structFields = exampleStructSchema.fields
    val mapFields = exampleMapSchema.fields
    StructType(atomicFields ++ arrayFields ++ structFields ++ mapFields)
  }

  def randomDataset(schema: StructType, size: Int): SparkSession => DataFrame = {
    val generator = dataGenerator(schema, nullable = false)
    val rows = new java.util.ArrayList[Row](size)
    (0 until size).foreach { i =>
      rows.add(i, generator().asInstanceOf[Row])
    }
    spark => {
      spark.createDataFrame(rows, schema)
    }
  }

  def dataGenerator(dataType: DataType, nullable: Boolean): () => Any = {
    val generator = dataType match {
      case BooleanType =>
        () => Random.nextBoolean()
      case ByteType =>
        () => (Random.nextInt(Byte.MaxValue - Byte.MinValue) + Byte.MinValue).toByte
      case ShortType =>
        () => (Random.nextInt(Short.MaxValue - Short.MinValue) + Short.MinValue).toShort
      case IntegerType =>
        () => Random.nextInt()
      case LongType =>
        () => Random.nextLong()
      case FloatType =>
        () => Random.nextFloat()
      case DoubleType =>
        () => Random.nextDouble()
      case _: DecimalType =>
        () => new java.math.BigDecimal(Random.nextDouble())
      case DateType =>
        () => {
          val value = Random.nextInt(2147483647)
          new Date(value)
        }
      case TimestampType =>
        () => {
          val value = Math.floorMod(Math.abs(Random.nextLong()), 9223372036854775L)
          new Timestamp(value)
        }
      case StringType =>
        () => {
          val size = Random.nextInt(10)
          Random.nextString(size)
        }
      case BinaryType =>
        () => {
          val size = Random.nextInt(10)
          Random.nextString(size).getBytes
        }
      case arrayType: ArrayType =>
        val arrayDataGenerator = dataGenerator(arrayType.elementType, arrayType.containsNull)
        () => {
          val size = Random.nextInt(10)
          Array.range(0, size).map(_ => arrayDataGenerator())
        }
      case structType: StructType =>
        val dataGenerators = structType.fields.map { field =>
          dataGenerator(field.dataType, field.nullable)
        }
        () => {
          Row(dataGenerators.map(_.apply()).toIndexedSeq: _*)
        }
      case mapType: MapType =>
        val keyDataGenerator = dataGenerator(mapType.keyType, nullable = false)
        val valueDataGenerator = dataGenerator(mapType.valueType, mapType.valueContainsNull)
        () => {
          val size = Random.nextInt(10)
          (0 until size)
            .map(_ => keyDataGenerator() -> valueDataGenerator())
            .toMap
        }
    }

    if (nullable) {
      () =>
        {
          if (Random.nextInt(100) < 25) {
            null
          } else {
            generator()
          }
        }
    } else {
      generator
    }
  }
}
