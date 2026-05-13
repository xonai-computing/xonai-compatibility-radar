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

package com.xonai.spark.sql.support

import com.xonai.spark.sql.parser.expressions.{AmbiguousExpression, AttributeReference, DateAdd, Literal, UnknownSQLExpression}
import com.xonai.spark.sql.parser.plans.{FileSourceScanExec, Inner, RangeExec, SortMergeJoinExec, UnknownExec}
import com.xonai.spark.sql.parser.types.{AnyType, ByteType, DateType, FloatType, IntegerType, ShortType, StringType, TypeSet}
import org.scalatest.funsuite.AnyFunSuite

class NodeTypeCheckerSuite extends AnyFunSuite {

  test("Not implemented") {
    val checker = new NodeTypeChecker(Map.empty)
    val expr = Literal("1")
    val plan = RangeExec(0, 10, 1, 5, Seq.empty, Map.empty)
    assert(checker.checkExpression(expr) == NodeSupport.NotImplemented)
    assert(checker.checkPlan(plan) == NodeSupport.NotImplemented)
  }

  test("Unknown") {
    val checker = new NodeTypeChecker(Map.empty)
    val expr = UnknownSQLExpression("abc")
    val childPlan = RangeExec(0, 10, 1, 5, Seq.empty, Map.empty)
    val plan = UnknownExec("Mine", "Mine", IndexedSeq(childPlan), Seq.empty, Map.empty)
    assert(checker.checkExpression(expr) == NodeSupport.Unknown)
    assert(checker.checkPlan(plan) == NodeSupport.Unknown)
  }

  test("Expression - output type") {
    val checker = new NodeTypeChecker(
      Map(
        "Literal" -> Seq(NodeDataTypeSupport(inputs = Seq.empty, output = TypeSet.Numeric))
      )
    )

    // Output type in supported map.
    var expr = Literal("1", TypeSet.Integral)
    assert(checker.checkExpression(expr).isSupported)

    // Output type partially in supported map.
    expr = Literal("1", TypeSet.NumericAndAnsiInterval)
    assert(checker.checkExpression(expr) == NodeSupport.UnsupportedDataType)

    // Output type not in supported map.
    expr = Literal("1", StringType)
    assert(checker.checkExpression(expr) == NodeSupport.UnsupportedDataType)

    // Output type is AnyType.
    expr = Literal("1", AnyType)
    assert(checker.checkExpression(expr) == NodeSupport.UndefinedDataType)
  }

  test("Expression - input type") {
    val checker = new NodeTypeChecker(
      Map(
        "DateAdd" -> Seq(
          NodeDataTypeSupport(
            inputs = Seq(DateType, IntegerType),
            output = DateType
          ),
          NodeDataTypeSupport(
            inputs = Seq(DateType, ByteType),
            output = DateType
          )
        )
      )
    )

    // Input types match first option in supported map.
    var expr = DateAdd(
      AttributeReference("a", DateType),
      AttributeReference("b", IntegerType)
    )
    assert(checker.checkExpression(expr).isSupported)

    // Input type is second option in supported map.
    expr = DateAdd(
      AttributeReference("a", DateType),
      AttributeReference("b", ByteType)
    )
    assert(checker.checkExpression(expr).isSupported)

    // Input type not in supported map.
    expr = DateAdd(
      AttributeReference("a", DateType),
      AttributeReference("b", ShortType)
    )
    assert(checker.checkExpression(expr) == NodeSupport.UnsupportedDataType)

    // Input type is AnyType.
    expr = DateAdd(
      AttributeReference("a", DateType),
      AttributeReference("b", AnyType)
    )
    assert(checker.checkExpression(expr) == NodeSupport.UndefinedDataType)
  }

  test("Expression - ambiguous") {
    val checker = new NodeTypeChecker(Map.empty)
    val expr = AmbiguousExpression(IndexedSeq(Literal("abc"), Literal("ab c")))
    assert(checker.checkExpression(expr) == NodeSupport.Supported)
  }

  test("Plan - output types") {
    val checker = new NodeTypeChecker(
      Map(
        "FileSourceScanExec" -> Seq(
          NodeDataTypeSupport(
            inputs = Seq.empty,
            output = TypeSet.Numeric
          )
        )
      )
    )

    // All output types in supported map.
    var plan = FileSourceScanExec(
      format = "parquet",
      partitionFilters = Seq.empty,
      output = Seq(
        AttributeReference("a", ByteType),
        AttributeReference("b", IntegerType)
      ),
      Map.empty
    )
    assert(checker.checkPlan(plan).isSupported)

    // Only 1 output type not in supported map.
    plan = FileSourceScanExec(
      format = "parquet",
      partitionFilters = Seq.empty,
      output = Seq(
        AttributeReference("a", ByteType),
        AttributeReference("b", IntegerType),
        AttributeReference("c", StringType)
      ),
      Map.empty
    )
    assert(checker.checkPlan(plan) == NodeSupport.UnsupportedDataType)
  }

  test("Plan - input types") {
    val checker = new NodeTypeChecker(
      Map(
        "SortMergeJoinExec" -> Seq(
          NodeDataTypeSupport(
            inputs = Seq(TypeSet.Numeric, TypeSet.Integral),
            output = TypeSet.Numeric
          )
        )
      )
    )

    // All input types in supported map.
    val scan = FileSourceScanExec(
      format = "parquet",
      partitionFilters = Seq.empty,
      output = Seq(
        AttributeReference("a", ByteType),
        AttributeReference("b", IntegerType)
      ),
      Map.empty
    )
    val floatScan = FileSourceScanExec(
      format = "parquet",
      partitionFilters = Seq.empty,
      output = Seq(
        AttributeReference("a", FloatType)
      ),
      Map.empty
    )
    var plan =
      SortMergeJoinExec(Seq.empty, Seq.empty, Inner, None, floatScan, scan, false, Map.empty)
    assert(checker.checkPlan(plan).isSupported)

    // Only 1 input type not in supported map.
    plan = SortMergeJoinExec(Seq.empty, Seq.empty, Inner, None, scan, floatScan, false, Map.empty)
    assert(checker.checkPlan(plan) == NodeSupport.UnsupportedDataType)
  }
}
