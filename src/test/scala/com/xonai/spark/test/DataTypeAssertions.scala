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

import com.xonai.spark.sql.parser.types.{ArrayType, DataType, MapType, StructType}
import org.scalatest.Assertions

trait DataTypeAssertions extends Assertions {

  def assertSameTypes(expected: DataType, actual: DataType, path: String = ""): Unit = {
    (expected, actual) match {
      case (expected: StructType, actual: StructType) =>
        val expectedLength = expected.fields.length
        val actualLength = actual.fields.length
        assert(expectedLength == actualLength, s"length in path '$path'")

        expected.fields.indices.foreach { i =>
          val expectedField = expected.fields.apply(i)
          val actualField = actual.fields.apply(i)
          assert(expectedField.name == actualField.name, s"field in path '$path'")
          assertSameTypes(
            expectedField.dataType,
            actualField.dataType,
            s"$path.${expectedField.name}"
          )
        }
      case (expected: ArrayType, actual: ArrayType) =>
        assertSameTypes(expected.elementType, actual.elementType, s"$path[]")
      case (expected: MapType, actual: MapType) =>
        assertSameTypes(expected.keyType, actual.keyType, s"$path[key]")
        assertSameTypes(expected.valueType, actual.valueType, s"$path[value]")
      case _ =>
        assert(expected == actual, s"in path '$path'")
    }
  }
}
