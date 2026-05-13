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

import com.xonai.spark.sql.parser.types.{AnyType, ArrayType, ByteType, DataType, IntegerType, LongType, MapType, ShortType, StructField, StructType, TypeSet}
import org.scalactic.source.Position
import org.scalatest.funsuite.AnyFunSuite

class DataTypeSuite extends AnyFunSuite {

  test("intersect") {
    assert(IntegerType.intersect(IntegerType) == IntegerType)
    assert(IntegerType.intersect(AnyType) == IntegerType)
    assert(AnyType.intersect(IntegerType) == IntegerType)
    assert(AnyType.intersect(AnyType) == AnyType)
    assert(ShortType.intersect(IntegerType) == TypeSet.Empty)
  }

  test("intersect - TypeSet") {
    assertIntersect(
      TypeSet(ShortType, IntegerType),
      IntegerType,
      expected = IntegerType
    )
    assertIntersect(
      TypeSet(ShortType, IntegerType),
      LongType,
      expected = TypeSet.Empty
    )
    assertIntersect(
      TypeSet(ShortType, IntegerType),
      TypeSet(ByteType, ShortType, IntegerType),
      expected = TypeSet(ShortType, IntegerType)
    )
    assertIntersect(
      TypeSet(ByteType, ShortType, IntegerType),
      TypeSet(ShortType, IntegerType),
      expected = TypeSet(ShortType, IntegerType)
    )
    assertIntersect(
      TypeSet(ShortType, IntegerType),
      TypeSet(ByteType, IntegerType),
      expected = IntegerType
    )
  }

  test("intersect - ArrayType") {
    assertIntersect(
      ArrayType(AnyType),
      IntegerType,
      expected = TypeSet.Empty
    )
    assertIntersect(
      ArrayType(AnyType),
      ArrayType(LongType),
      expected = ArrayType(LongType)
    )
    assertIntersect(
      ArrayType(ShortType),
      ArrayType(LongType),
      expected = TypeSet.Empty
    )
    assertIntersect(
      ArrayType(AnyType),
      ArrayType(ArrayType(LongType)),
      expected = ArrayType(ArrayType(LongType))
    )
  }

  test("intersect - MapType") {
    assertIntersect(
      MapType(AnyType, AnyType),
      IntegerType,
      expected = TypeSet.Empty
    )
    assertIntersect(
      MapType(AnyType, AnyType),
      MapType(LongType, LongType),
      expected = MapType(LongType, LongType)
    )
    assertIntersect(
      MapType(ShortType, LongType),
      MapType(LongType, LongType),
      expected = TypeSet.Empty
    )
    assertIntersect(
      MapType(LongType, ShortType),
      MapType(LongType, LongType),
      expected = TypeSet.Empty
    )
    assertIntersect(
      MapType(AnyType, AnyType),
      MapType(ArrayType(IntegerType), ArrayType(LongType)),
      expected = MapType(ArrayType(IntegerType), ArrayType(LongType))
    )
  }

  test("intersect - StructType") {
    assertIntersect(
      StructType(Array()),
      IntegerType,
      expected = TypeSet.Empty
    )
    assertIntersect(
      StructType(
        Array(
          StructField("a", AnyType),
          StructField("b", AnyType)
        )
      ),
      StructType(
        Array(
          StructField("a", AnyType)
        )
      ),
      expected = TypeSet.Empty
    )
    assertIntersect(
      StructType(
        Array(
          StructField("a", AnyType),
          StructField("b", AnyType)
        )
      ),
      StructType(
        Array(
          StructField("a", LongType),
          StructField("b", IntegerType)
        )
      ),
      expected = StructType(
        Array(
          StructField("a", LongType),
          StructField("b", IntegerType)
        )
      )
    )
    assertIntersect(
      StructType(
        Array(
          StructField("a", ShortType),
          StructField("b", AnyType)
        )
      ),
      StructType(
        Array(
          StructField("a", LongType),
          StructField("b", IntegerType)
        )
      ),
      expected = TypeSet.Empty
    )
    assertIntersect(
      StructType(
        Array(
          StructField("", LongType),
          StructField("b", AnyType)
        )
      ),
      StructType(
        Array(
          StructField("a", AnyType),
          StructField("", IntegerType)
        )
      ),
      expected = StructType(
        Array(
          StructField("a", LongType),
          StructField("b", IntegerType)
        )
      )
    )
  }

  private def assertIntersect(left: DataType, right: DataType, expected: DataType)(implicit
      p: Position
  ): Unit = {
    assert(left.intersect(right) == expected)
    assert(right.intersect(left) == expected)
  }
}
