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

import com.xonai.spark.sql.parser.dsl.expressions._
import com.xonai.spark.sql.parser.expressions._
import com.xonai.spark.sql.parser.plans.{FileSourceScanExec, ReusedSubqueryExec, SubqueryBroadcastExec, SubqueryExec}
import com.xonai.spark.sql.parser.types.{AnyType, ArrayType, BinaryType, BooleanType, DataTypeUtils, DoubleType, IntegerType, LongType, MapType, NullType, ObjectType, ShortType, StringType, StructField, StructType, TimestampNTZType, TimestampType}
import com.xonai.spark.test.{DataTypeAssertions, TestData}
import org.apache.spark.sql
import org.apache.spark.sql.catalyst
import org.apache.spark.sql.catalyst.expressions.{EvalMode, ExprId}
import org.scalactic.source.Position
import org.scalatest.funsuite.AnyFunSuite

class ExpressionParserSuite extends AnyFunSuite with DataTypeAssertions with TestData {

  private val expressionParser = ExpressionParser.default

  implicit class SuiteDslString(sql: String) {

    def parseIs(expected: Expression)(implicit pos: Position): Unit = {
      assert(expressionParser.parse(sql) == expected)
    }

    def parseIsLiteral(implicit pos: Position): Unit = {
      val actual = expressionParser.parse(sql)
      assert(actual.isInstanceOf[Literal])
      assert(actual.asInstanceOf[Literal].stringValue == sql)
    }

    def parseAggregateIs(expected: Expression)(implicit pos: Position): Unit = {
      assert(expressionParser.parseAggregate(sql) == expected)
    }
  }

  private val id0 = AttributeReference("id", 0)

  test("Literal") {
    "".parseIsLiteral
    "null".parseIsLiteral

    "false".parseIsLiteral
    "true".parseIsLiteral

    "0".parseIsLiteral
    "1".parseIsLiteral
    "-1".parseIsLiteral
    "123456789".parseIsLiteral
    "-123456789".parseIsLiteral

    "1.11".parseIsLiteral
    "0.0".parseIsLiteral
    "-0.0".parseIsLiteral
    "4.9E-324".parseIsLiteral

    "ABCD".parseIsLiteral
    "0x05".parseIsLiteral

    "1800-05-03".parseIsLiteral
    "1997-01-31 09:26:56.123".parseIsLiteral

    "0 seconds".parseIsLiteral
    "1 seconds".parseIsLiteral
    "1 minutes".parseIsLiteral
    "1 hours".parseIsLiteral
    "1 days".parseIsLiteral
    "1 months".parseIsLiteral
    "1 years".parseIsLiteral
    "1 years 1 months".parseIsLiteral
    "1 years 1 months 1 days".parseIsLiteral
    "1 years 1 months 1 days 1 hours".parseIsLiteral
    "1 years 1 months 1 days 1 hours 1 minutes".parseIsLiteral
    "1 years 1 months 1 days 1 hours 1 minutes 1.000001 seconds".parseIsLiteral

    "INTERVAL '100' DAY".parseIsLiteral
    "INTERVAL '100' HOUR".parseIsLiteral
    "INTERVAL '100' MINUTE".parseIsLiteral
    "INTERVAL '100' SECOND".parseIsLiteral
    "INTERVAL '100 05' DAY TO HOUR".parseIsLiteral
    "INTERVAL '100 05:05' DAY TO MINUTE".parseIsLiteral
    "INTERVAL '100 05:05:05' DAY TO SECOND".parseIsLiteral
    "INTERVAL '100 05:05:05.050505' DAY TO SECOND".parseIsLiteral

    "INTERVAL '20' YEAR".parseIsLiteral
    "INTERVAL '20' MONTH".parseIsLiteral
    "INTERVAL '20-3' YEAR TO MONTH".parseIsLiteral

    "[]".parseIsLiteral
    "[1]".parseIsLiteral
    "[21,22]".parseIsLiteral
    "[[21],[22]]".parseIsLiteral
    "[21,[22]]".parseIsLiteral
    "map(keys: [a,b,c], values: [1,5,6])" parseIs
      Literal("keys: [a,b,c], values: [1,5,6]")
    "map(keys: [a,b,c], values: [[1],[5,6],[6]])" parseIs
      Literal("keys: [a,b,c], values: [[1],[5,6],[6]]")
    "[keys: [1,2], values: [a,b],23]".parseIsLiteral
    "[keys: [1,2], values: [keys: [a], values: [10],keys: [b], values: [20]]]".parseIsLiteral

    // Possibly conflicting.
    "isnull".parseIsLiteral
    "-".parseIsLiteral
    "~".parseIsLiteral
    "#".parseIsLiteral
    "NOT".parseIsLiteral
    "(".parseIsLiteral
    ")".parseIsLiteral
    "[".parseIsLiteral
    "]".parseIsLiteral
    ",".parseIsLiteral
    ".".parseIsLiteral
    "'".parseIsLiteral
    ":".parseIsLiteral
    "+".parseIsLiteral
    "-".parseIsLiteral
    "*".parseIsLiteral
    "/".parseIsLiteral
    "%".parseIsLiteral
    "&".parseIsLiteral
    "|".parseIsLiteral
    "^".parseIsLiteral
    "<".parseIsLiteral
    ">".parseIsLiteral
    "=".parseIsLiteral
    "AS".parseIsLiteral
    "if".parseIsLiteral
    "else".parseIsLiteral
    "CASE".parseIsLiteral
    "WHEN".parseIsLiteral
    "THEN".parseIsLiteral
    "END".parseIsLiteral
    "IN".parseIsLiteral
    "INSET".parseIsLiteral
    "LIKE".parseIsLiteral
    "ESCAPE".parseIsLiteral
  }

  test("Attribute") {
    "v#0" parseIs AttributeReference("v", 0)
    "v#1L" parseIs AttributeReference("v", 1)
    "v#9" parseIs AttributeReference("v", 9)
    "value#10L" parseIs AttributeReference("value", 10)
    "value#987654321L" parseIs AttributeReference("value", 987654321)
    "TwoWords#21" parseIs AttributeReference("TwoWords", 21)
    "two_words#21" parseIs AttributeReference("two_words", 21)
    "0value1#1L" parseIs AttributeReference("0value1", 1)
    "data.v#1" parseIs AttributeReference("data.v", 1)
    "(id + 1)#1" parseIs AttributeReference("(id + 1)", 1)
  }

  test("Alias") {
    // Literal and attribute.
    "0 AS v#0" parseIs Alias("0", "v", 0)
    "0 AS v#0L" parseIs Alias("0", "v", 0)
    "100 AS value#1" parseIs Alias("100", "value", 1)
    "100 AS value#123456789L" parseIs Alias("100", "value", 123456789)
    "value#1 AS value#2" parseIs Alias(AttributeReference("value", 1), "value", 2)

    // Unary expressions.
    "-id#0 AS value#1" parseIs Alias(UnaryMinus(id0), "value", 1)
    "-id#0L AS (- id)#1L" parseIs Alias(UnaryMinus(id0), "(- id)", 1)
    "~id#0L AS ~id#1L" parseIs Alias(BitwiseNot(id0), "~id", 1)

    // Binary operators.
    "(1 + 2) AS add#1" parseIs Alias(Add("1", "2"), "add", 1)
    "(1 + id#0) AS (1 + id)#1" parseIs Alias(Add("1", id0), "(1 + id)", 1)
    "(id#0L + 1) AS (id + 1)#1L" parseIs Alias(Add(id0, "1"), "(id + 1)", 1)

    // Function call expressions.
    "isnull(id#0L) AS (id IS NULL)#2" parseIs
      Alias(IsNull(id0), "(id IS NULL)", 2)
    "cast(id#0L as smallint) AS value#1" parseIs
      Alias(Cast(id0, ShortType, isTryCast = false), "value", 1)
    "null AS CAST(NULL AS INT)#2" parseIs
      Alias("null", "CAST(NULL AS INT)", 2)
    "ceil(3.1411, 3) AS value#1" parseIs
      Alias(RoundCeil("3.1411", "3"), "value", 1)
  }

  test("BoundReference") {
    "input[1, long, false]" parseIs BoundReference(1, LongType)
  }

  test("Unknown") {
    "(true ? id#0 : 5)" parseIs
      UnknownSQLExpression("true ? id#0 : 5")

    "some_unknown_FUNCTION2(id#0, 2)" parseIs
      UnknownSQLFunction("some_unknown_FUNCTION2", IndexedSeq(id0, "2"))
  }

  test("UnaryMinus") {
    "-id#0" parseIs UnaryMinus(id0)
    "-(5 + id#0)" parseIs UnaryMinus(Add("5", id0))
  }

  test("UnaryPositive") {
    "positive(1)" parseIs UnaryPositive("1")
    "positive(id#0)" parseIs UnaryPositive(id0)
    "positive((2 + 3))" parseIs UnaryPositive(Add("2", "3"))
  }

  test("BitwiseNot") {
    "~0" parseIs BitwiseNot("0")
    "~id#0" parseIs BitwiseNot(id0)
    "~(id#0 + 4)" parseIs BitwiseNot(Add(id0, "4"))
  }

  test("Arithmetic expressions") {
    "(2 * 3)" parseIs Multiply("2", "3")
    "(2.0 / id#0)" parseIs Divide("2.0", id0)
    "(-2147483648 - -10)" parseIs Subtract("-2147483648", "-10")
    "(2 + id#0)" parseIs Add("2", id0)
    "mod(id#0, 5)" parseIs Remainder(id0, "5")
    "(2 div value#1)" parseIs IntegralDivide("2", AttributeReference("value", 1))
    "(value#2 % 10)" parseIs Remainder(AttributeReference("value", 2), "10")
    "(((2 + id#0) * (id#0 - 1)) / 2)" parseIs
      Divide(
        Multiply(
          Add("2", id0),
          Subtract(id0, "1")
        ),
        "2"
      )
    "abs(id#0)" parseIs Abs(id0)
    "pmod(id#0, 2)" parseIs Pmod(id0, "2")
    "least(id#0, 2)" parseIs Least(IndexedSeq(id0, "2"))
    "greatest(id#0, 2)" parseIs Greatest(IndexedSeq(id0, "2"))

    val num1 = AttributeReference("num", 1)
    "(id#0 - num#1)" parseIs
      AmbiguousExpression(
        IndexedSeq(
          Subtract(id0, num1),
          SubtractDates(id0, num1),
          SubtractTimestamps(id0, num1)
        )
      )
  }

  test("Bitwise operators") {
    "(id#0 | 2)" parseIs BitwiseOr(id0, "2")
    "(2 & id#0)" parseIs BitwiseAnd("2", id0)
    "(5 ^ id#0)" parseIs BitwiseXor("5", id0)
    "(1 | ((5 ^ id#0) & 3))" parseIs
      BitwiseOr(
        "1",
        BitwiseAnd(
          BitwiseXor("5", id0),
          "3"
        )
      )
    "bit_count(id#0)" parseIs BitwiseCount(id0)
    "bit_get(id#0, 1)" parseIs BitwiseGet(id0, "1")
    "getbit(id#0, 1)" parseIs BitwiseGet(id0, "1")
    "bit_reverse(id#0)" parseIs BitwiseReverse(id0)
  }

  test("Binary comparisons") {
    "(id#0L = 5)" parseIs EqualTo(id0, "5")
    "(id#0L <=> 5)" parseIs EqualNullSafe(id0, "5")
    "(id#0L < 5)" parseIs LessThan(id0, "5")
    "(id#0L > 5)" parseIs GreaterThan(id0, "5")
    "(id#0L <= 5)" parseIs LessThanOrEqual(id0, "5")
    "(id#0L >= 5)" parseIs GreaterThanOrEqual(id0, "5")
  }

  test("Binary predicate operators") {
    "NOT id#0" parseIs Not(id0)
    "(true AND id#0)" parseIs And("true", id0)
    "(false OR id#0)" parseIs Or("false", id0)
    "(NOT (id#0 = 0) AND (id#0 = 5))" parseIs
      And(
        Not(id0 === "0"),
        id0 === "5"
      )
    "NOT ((id#0 = 1) OR ((id#0 > 10) AND (id#0 < 20)))" parseIs
      Not(
        Or(
          id0 === "1",
          And(
            id0 > "10",
            id0 < "20"
          )
        )
      )
  }

  test("CaseWhen") {
    "CASE WHEN (id#0 = 1) THEN one WHEN (id#0 = 2) THEN two END" parseIs
      CaseWhen(
        IndexedSeq(
          (id0 === "1", "one".expr),
          (id0 === "2", "two".expr)
        ),
        None
      )
    "CASE WHEN (id#0 = 1) THEN one WHEN (id#0 = 2) THEN two ELSE ? END" parseIs
      CaseWhen(
        IndexedSeq(
          (id0 === "1", "one".expr),
          (id0 === "2", "two".expr)
        ),
        Some(
          "?"
        )
      )
    // Nested in condition.
    "CASE WHEN (id#0 = CASE WHEN ((id#0 + 1) = 1) THEN 10 WHEN (id#0 = 2) THEN 20 END) " +
      "THEN 10x END" parseIs
      CaseWhen(
        IndexedSeq[(Expression, Expression)](
          (
            id0 === CaseWhen(
              IndexedSeq(
                (Add(id0, "1") === "1", "10".expr),
                (id0 === "2", "20".expr)
              ),
              None
            ),
            "10x"
          )
        ),
        None
      )
    // Nested in then value.
    "CASE WHEN (id#0 > 0) THEN " +
      "CASE WHEN (id#0 < 10) THEN low WHEN (id#0 < 20) THEN medium ELSE high END " +
      "ELSE negative END" parseIs
      CaseWhen(
        IndexedSeq(
          (
            id0 > "0",
            CaseWhen(
              IndexedSeq(
                (id0 < "10", "low".expr),
                (id0 < "20", "medium".expr)
              ),
              Some("high")
            )
          )
        ),
        Some("negative")
      )
    // Nested in else value.
    "CASE WHEN (id#0 < 0) " +
      "THEN negative " +
      "ELSE CASE WHEN (id#0 < 10) THEN low WHEN (id#0 < 20) THEN medium ELSE high END " +
      "END" parseIs
      CaseWhen(
        IndexedSeq(
          (id0 < "0", "negative".expr)
        ),
        Some(
          CaseWhen(
            IndexedSeq(
              (id0 < "10", "low".expr),
              (id0 < "20", "medium".expr)
            ),
            Some("high")
          )
        )
      )
  }

  test("If") {
    "if ((id#0 > 0)) id#0 else 0" parseIs
      If(
        id0 > "0",
        id0,
        "0"
      )
    "if ((id#0 < 0)) (id#0 + 2) else (id#0 + 1)" parseIs
      If(
        id0 < "0",
        Add(id0, "2"),
        Add(id0, "1")
      )
    "if ((id#0 < 0)) false else isnull(id#0)" parseIs
      If(
        id0 < "0",
        "false",
        IsNull(id0)
      )

    // Nested in trueValue.
    "if ((id#0 > 0)) if ((id#0 > 10)) id#0 else 10 else 0" parseIs
      If(
        id0 > "0",
        If(
          id0 > "10",
          id0,
          "10"
        ),
        "0"
      )

    // Nested in falseValue.
    "if ((id#0 > 0)) id#0 else if ((id#0 < 0)) -1 else id#0" parseIs
      If(
        id0 > "0",
        id0,
        If(
          id0 < "0",
          "-1",
          id0
        )
      )

    // Nested in condition.
    "if (if ((id#0 > 0)) (id#0 < 5) else false) id#0 else 0" parseIs
      If(
        If(
          id0 > "0",
          id0 < "5",
          "false"
        ),
        id0,
        "0"
      )
  }

  test("In") {
    "id#0 IN (1,2,3,4)" parseIs
      In(
        id0,
        IndexedSeq[Expression]("1", "2", "3", "4")
      )
    "id#0 IN (,,:)" parseIs
      In(
        id0,
        IndexedSeq[Expression]("", "", ":")
      )
    "id#0 IN ( ,,)" parseIs
      In(
        id0,
        IndexedSeq[Expression](" ", "", "")
      )
    "id#0 IN ( , )" parseIs
      In(
        id0,
        IndexedSeq[Expression](" ", " ")
      )
    "-id#0 IN (1,2)" parseIs
      In(
        UnaryMinus(id0),
        IndexedSeq[Expression]("1", "2")
      )
    "~id#0 IN (1,2)" parseIs
      In(
        BitwiseNot(id0),
        IndexedSeq[Expression]("1", "2")
      )
    "(id#0 + 1) IN (id#0,(id#0 + 1))" parseIs
      In(
        Add(id0, "1"),
        IndexedSeq(id0, Add(id0, "1"))
      )

    // Conflicting precedence.
    "if ((id#0 = 1)) 10 else id#0 IN (1,2)" parseIs
      In(
        If(id0 === "1", "10", id0),
        IndexedSeq[Expression]("1", "2")
      )
    "if ((id#0 = 1)) false else id#0 IN (1,2)" parseIs
      AmbiguousExpression(
        IndexedSeq(
          If(
            id0 === "1",
            "false",
            In(id0, IndexedSeq[Expression]("1", "2"))
          ),
          In(
            If(id0 === "1", "false", id0),
            IndexedSeq[Expression]("1", "2")
          )
        )
      )
  }

  test("InSet") {
    "id#0 INSET 1, 10, 11, 2, 3" parseIs
      InSet(
        id0,
        IndexedSeq[Literal]("1", "10", "11", "2", "3")
      )
    "id#0 INSET , (, ), ,, /, :, ;, <, >, [, ], _, {, }" parseIs
      InSet(
        id0,
        IndexedSeq[Literal]("", "(", ")", ",", "/", ":", ";", "<", ">", "[", "]", "_", "{", "}")
      )
    "-id#0 INSET 1, 2" parseIs
      InSet(
        UnaryMinus(id0),
        IndexedSeq[Literal]("1", "2")
      )
    "~id#0 INSET 1, 2" parseIs
      InSet(
        BitwiseNot(id0),
        IndexedSeq[Literal]("1", "2")
      )
    "(id#0 + 1) INSET 1, 10, 11, 2, 3" parseIs
      InSet(
        Add(id0, "1"),
        IndexedSeq[Literal]("1", "10", "11", "2", "3")
      )
    "id#0 INSET [1,2], [10], [3,4,5], [8,9]" parseIs
      InSet(
        id0,
        IndexedSeq[Literal]("[1,2]", "[10]", "[3,4,5]", "[8,9]")
      )
    "cast(id#0 INSET 1, 2 as string) INSET false, true" parseIs
      InSet(
        Cast(
          InSet(id0, IndexedSeq[Literal]("1", "2")),
          StringType,
          isTryCast = false
        ),
        IndexedSeq[Literal]("false", "true")
      )

    // Conflicting precedence.
    "if ((id#0 = 1)) 10 else id#0 INSET 1, 2, 3" parseIs
      InSet(
        If(id0 === "1", "10", id0),
        IndexedSeq[Literal]("1", "2", "3")
      )
    "if ((id#0 = 1)) false else id#0 INSET 1, 2, 3" parseIs
      AmbiguousExpression(
        IndexedSeq(
          If(
            id0 === "1",
            "false",
            InSet(id0, IndexedSeq[Literal]("1", "2", "3"))
          ),
          InSet(
            If(id0 === "1", "false", id0),
            IndexedSeq[Literal]("1", "2", "3")
          )
        )
      )
  }

  test("Like") {
    val defaultEscapeChar = '\\'
    "id#0 LIKE _" parseIs Like(id0, "_", defaultEscapeChar)
    "id#0 LIKE _ ESCAPE '|'" parseIs Like(id0, "_", '|')
    "id#0 LIKE __1\\_" parseIs Like(id0, "__1\\_", defaultEscapeChar)
    "id#0 LIKE __1|_ ESCAPE '|'" parseIs Like(id0, "__1|_", '|')
    "id#0 LIKE pattern#1" parseIs
      Like(id0, AttributeReference("pattern", 1), defaultEscapeChar)

    // Prone to precedence errors.
    "id#0 LIKE concat(_, str#1)" parseIs
      Like(
        id0,
        Concat(IndexedSeq("_".expr, AttributeReference("str", 1))),
        defaultEscapeChar
      )
    "array#1[if (id#0 LIKE _) 0 else 1] LIKE __" parseIs
      Like(
        AmbiguousExpression(
          IndexedSeq(
            GetArrayItem(
              AttributeReference("array", 1),
              If(Like(id0, "_", defaultEscapeChar), "0", "1")
            ),
            GetMapValue(
              AttributeReference("array", 1),
              If(Like(id0, "_", defaultEscapeChar), "0", "1")
            )
          )
        ),
        "__",
        defaultEscapeChar
      )
    "CASE WHEN (id#0 = 1) THEN str#1 END LIKE CASE WHEN (id#0 > 0) THEN __1 END" parseIs
      Like(
        CaseWhen(
          IndexedSeq((id0 === "1", AttributeReference("str", 1))),
          None
        ),
        CaseWhen(
          IndexedSeq((id0 > "0", "__1".expr)),
          None
        ),
        defaultEscapeChar
      )

    // Conflicting precedence.
    "if ((id#0 = 1)) str else other#2 LIKE __1" parseIs
      Like(
        If(id0 === "1", "str", AttributeReference("other", 2)),
        "__1",
        defaultEscapeChar
      )
    "if ((id#0 = 1)) true else str#1 LIKE __1" parseIs
      AmbiguousExpression(
        IndexedSeq(
          If(
            id0 === "1",
            "true",
            Like(AttributeReference("str", 1), "__1", defaultEscapeChar)
          ),
          Like(
            If(id0 === "1", "true", AttributeReference("str", 1)),
            "__1",
            defaultEscapeChar
          )
        )
      )
  }

  test("GetArrayItem") {
    "array#1[0]" parseIs
      AmbiguousExpression(
        IndexedSeq(
          GetArrayItem(AttributeReference("array", 1), "0"),
          GetMapValue(AttributeReference("array", 1), "0")
        )
      )

    // Possibly conflicting.
    "array[0]" parseIs Literal("array[0]")

    val array1 = AttributeReference("array", ArrayType(AnyType), 1)
    val indices2 = AttributeReference("indices", ArrayType(AnyType), 2)

    expressionParser.withInput(Seq(array1, indices2)) { _ =>
      "array#1[0]" parseIs GetArrayItem(array1, "0")
      "array#1[id#0]" parseIs GetArrayItem(array1, id0)
      "array#1[indices#2[id#0]]" parseIs
        GetArrayItem(
          array1,
          GetArrayItem(indices2, id0)
        )

      // Prone to precedence errors.
      "CASE WHEN (id#0 = 1) THEN array#1 END[id#0]" parseIs
        GetArrayItem(
          CaseWhen(IndexedSeq((id0 === "1", array1)), None),
          id0
        )
      "str#3 LIKE array#1[id#0]" parseIs
        Like(
          AttributeReference("str", 3),
          GetArrayItem(array1, id0),
          '\\'
        )

      // Conflicting precedence.
      "if ((id#0 = 1)) 10 else array#1[id#0]" parseIs
        If(
          id0 === "1",
          "10",
          GetArrayItem(array1, id0)
        )
      "if ((id#0 = 1)) null else array#1[id#0]" parseIs
        AmbiguousExpression(
          IndexedSeq(
            If(
              id0 === "1",
              "null",
              GetArrayItem(array1, id0)
            ),
            GetArrayItem(
              If(id0 === "1", "null", array1),
              id0
            )
          )
        )
    }
  }

  test("GetMapValue") {
    "map#1[0]" parseIs
      AmbiguousExpression(
        IndexedSeq(
          GetArrayItem(AttributeReference("map", 1), "0"),
          GetMapValue(AttributeReference("map", 1), "0")
        )
      )

    // Possibly conflicting.
    "map[0]" parseIs Literal("map[0]")

    val map1 = AttributeReference("map", MapType(AnyType, AnyType), 1)
    val keys2 = AttributeReference("keys", ArrayType(AnyType), 2)

    expressionParser.withInput(Seq(map1, keys2)) { _ =>
      "map#1[0]" parseIs GetMapValue(map1, "0")
      "map#1[id#0]" parseIs GetMapValue(map1, id0)
      "map#1[keys#2[id#0]]" parseIs
        GetMapValue(
          map1,
          GetArrayItem(keys2, id0)
        )

      // Prone to precedence errors.
      "CASE WHEN (id#0 = 1) THEN map#1 END[id#0]" parseIs
        GetMapValue(
          CaseWhen(IndexedSeq((id0 === "1", map1)), None),
          id0
        )
      "str#3 LIKE map#1[id#0]" parseIs
        Like(
          AttributeReference("str", 3),
          GetMapValue(map1, id0),
          '\\'
        )

      // Conflicting precedence.
      "if ((id#0 = 1)) 10 else map#1[id#0]" parseIs
        If(
          id0 === "1",
          "10",
          GetMapValue(map1, id0)
        )
      "if ((id#0 = 1)) null else map#1[id#0]" parseIs
        AmbiguousExpression(
          IndexedSeq(
            If(
              id0 === "1",
              "null",
              GetMapValue(map1, id0)
            ),
            GetMapValue(
              If(id0 === "1", "null", map1),
              id0
            )
          )
        )
    }
  }

  test("GetStructField") {
    // Literals.
    "structs.booleans".parseIsLiteral
    "struct_structs.structs.booleans".parseIsLiteral

    // Typed.
    val parser = ExpressionParser.default
    val structType = StructType(
      Array(
        StructField("ints", BooleanType),
        StructField("booleans", BooleanType),
        StructField("array_ints", ArrayType(IntegerType))
      )
    )
    val structStructType = StructType(
      Array(
        StructField("structs", structType),
        StructField("ints", BooleanType)
      )
    )
    val input = Seq(
      AttributeReference("booleans", BooleanType, 0),
      AttributeReference("structs", structType, 1),
      AttributeReference("struct_structs", structStructType, 2)
    )
    parser.setInput(input)

    // Simple
    assert(parser.parse("structs#1.booleans") == GetStructField(input(1), 1, None, 3))
    assert(parser.parse("structs#1.Booleans") == GetStructField(input(1), 1, Some("Booleans"), 3))
    assert(
      parser.parse("struct_structs#2.Structs.booleans") ==
        GetStructField(GetStructField(input(2), 0, Some("Structs"), 2), 1, None, 3)
    )

    // Composed
    assert(
      parser.parse("structs#1.array_ints[0]") ==
        GetArrayItem(GetStructField(input(1), 2, None, 3), "0")
    )

    // Possibly conflicting.
    assert(parser.parse(".....").isInstanceOf[Literal])
    assert(parser.parse(".booleans").isInstanceOf[Literal])
    assert(parser.parse("booleans.").isInstanceOf[Literal])
    assert(parser.parse(".booleans.").isInstanceOf[Literal])
    assert(parser.parse("a.booleans").isInstanceOf[Literal])
    assert(parser.parse("structs.booleans").isInstanceOf[Literal])
  }

  test("GetArrayStructFields") {
    val parser = ExpressionParser.default
    val structType = StructType(
      Array(
        StructField("ints", BooleanType),
        StructField("booleans", BooleanType)
      )
    )
    val input = Seq(
      AttributeReference("booleans", BooleanType, 0),
      AttributeReference("structs", structType, 1),
      AttributeReference("array_structs", ArrayType(structType), 2)
    )
    parser.setInput(input)

    assert(
      parser.parse("array_structs#2.booleans") ==
        GetArrayStructFields(input(2), structType.fields(1), 1, 2)
    )
    assert(
      parser.parse("array_structs#2.Ints") ==
        GetArrayStructFields(input(2), structType.fields(0).copy(name = "Ints"), 0, 2)
    )
  }

  test("ScalarSubquery") {
    val structType = StructType(Array(StructField("array_ints", ArrayType(IntegerType))))
    val output = Seq(AttributeReference("structs", structType, 0))
    val scan = FileSourceScanExec("test", Seq.empty, output, Map.empty)

    val subquery1 = SubqueryExec("subquery#1", scan, Map.empty)
    val subquery2 = SubqueryExec("subquery#254", scan, Map.empty)
    val reusedSubquery1 = ReusedSubqueryExec(subquery1)
    val reusedSubquery2 = ReusedSubqueryExec(subquery2)
    val subqueries = Map(
      "Subquery subquery#1" -> subquery1,
      "Subquery scalar-subquery#254, [id=#3]" -> subquery2,
      "ReusedSubquery Subquery subquery#1" -> reusedSubquery1,
      "ReusedSubquery Subquery scalar-subquery#254, [id=#3]" -> reusedSubquery2
    )
    val parser = new ExpressionParser(subqueries.get)

    assert(
      parser.parse("Subquery subquery#1") ==
        ScalarSubquery(subquery1)
    )
    assert(
      parser.parse("Subquery scalar-subquery#254, [id=#3]") ==
        ScalarSubquery(subquery2)
    )
    assert(
      parser.parse("ReusedSubquery Subquery subquery#1") ==
        ScalarSubquery(reusedSubquery1)
    )
    assert(
      parser.parse("ReusedSubquery Subquery scalar-subquery#254, [id=#3]") ==
        ScalarSubquery(reusedSubquery2)
    )

    // Not found
    assert(
      parser.parse("Subquery subquery#5") ==
        UnknownSQLExpression("Subquery subquery#5")
    )
    assert(
      parser.parse("Subquery scalar-subquery#5, [id=#3]") ==
        UnknownSQLExpression("Subquery scalar-subquery#5, [id=#3]")
    )

    // Composed
    assert(
      parser.parse("(id#0 < Subquery scalar-subquery#254, [id=#3])") ==
        id0 < ScalarSubquery(subquery2)
    )
    assert(
      parser.parse("ReusedSubquery Subquery scalar-subquery#254, [id=#3].array_ints[0]") ==
        GetArrayItem(
          GetStructField(ScalarSubquery(reusedSubquery2), 0, None, 1),
          "0"
        )
    )

    // Inside a named list.
    val expressions =
      parser.parseNamedList("[Subquery scalar-subquery#254, [id=#3] AS scalarsubquery()#1]")
    assert(expressions.length == 1)
    assert(expressions.head == Alias(ScalarSubquery(subquery2), "scalarsubquery()", 1))
  }

  test("InSubquery") {
    val structType = StructType(Array(StructField("ints", IntegerType)))
    val output = Seq(AttributeReference("structs", structType, 0))
    val scan = FileSourceScanExec("test", Seq.empty, output, Map.empty)

    val keys = Seq(AttributeReference("ints", IntegerType, 0))
    val subquery1 = SubqueryBroadcastExec("dynamicpruning#1", Seq(0), keys, scan, Map.empty, true)
    val subqueries = Map(
      "dynamicpruning#1" -> subquery1
    )
    val parser = new ExpressionParser(subqueries.get)

    assert(
      parser.parse("id#0 IN dynamicpruning#1") ==
        InSubqueryExec(id0, subquery1)
    )
  }

  test("Cast") {
    exampleSchema.foreach { field =>
      Seq(false, true).foreach { isTryCast =>
        val cast = catalyst.expressions.Cast(
          catalyst.expressions.Literal(null, sql.types.NullType),
          field.dataType,
          evalMode = if (isTryCast) EvalMode.TRY else EvalMode.LEGACY
        )

        val str = cast.toString()
        val result = expressionParser.parse(str)
        assert(result.isInstanceOf[Cast])
        assert(result.asInstanceOf[Cast].isTryCast == isTryCast)

        val expectedDataType = DataTypeUtils.fromSparkDataType(field.dataType)
        if (field.dataType.simpleString.contains("... ")) {
          // When the data type is incomplete, intersection with original type restores the field
          // names and data types.
          val intersectionType = expectedDataType.intersect(result.dataType)
          assertSameTypes(intersectionType, expectedDataType)
        } else {
          assertSameTypes(result.dataType, expectedDataType)
        }
      }
    }
  }

  test("Aggregate expression") {
    "count(1)" parseAggregateIs
      AggregateExpression(Count("1"), modePrefix = "", isDistinct = false, filter = None)
    "count(1, id#0)" parseAggregateIs
      AggregateExpression(Count("1", id0), modePrefix = "", isDistinct = false, filter = None)
    "count((id#0 + 1))" parseAggregateIs
      AggregateExpression(Count(Add(id0, "1")), modePrefix = "", isDistinct = false, filter = None)

    "partial_count(1)" parseAggregateIs
      AggregateExpression(Count("1"), modePrefix = "partial", isDistinct = false, filter = None)
    "merge_count(1)" parseAggregateIs
      AggregateExpression(Count("1"), modePrefix = "merge", isDistinct = false, filter = None)

    "count(distinct id#0)" parseAggregateIs
      AggregateExpression(Count(id0), modePrefix = "", isDistinct = true, filter = None)

    "count(id#0) FILTER (WHERE (id#0 > 5))" parseAggregateIs
      AggregateExpression(Count(id0), modePrefix = "", isDistinct = false, filter = Some(id0 > "5"))

    "partial_count(distinct id#0) FILTER (WHERE (id#0 > 5))" parseAggregateIs
      AggregateExpression(
        Count(id0),
        modePrefix = "partial",
        isDistinct = true,
        filter = Some(id0 > "5")
      )

    "xyz(id#0, 2)" parseAggregateIs
      AggregateExpression(
        UnknownAggregateFunction("xyz", IndexedSeq(id0, "2")),
        modePrefix = "",
        isDistinct = false,
        filter = None
      )
  }

  test("Aggregate expressions") {
    "count(id#0)" parseIs Count(id0)
    "sum(id#0)" parseIs Sum(id0, isTry = false)
    "try_sum(id#0)" parseIs Sum(id0, isTry = true)
    "avg(id#0)" parseIs Average(id0, isTry = false)
    "try_avg(id#0)" parseIs Average(id0, isTry = true)
    "mean(id#0)" parseIs Average(id0, isTry = false)
    "min(id#0)" parseIs Min(id0)
    "max(id#0)" parseIs Max(id0)

    "first(id#0, false)" parseIs First(id0, ignoreNulls = false)
    "first_value(id#0, false)" parseIs First(id0, ignoreNulls = false)
    "first(id#0, true)" parseIs First(id0, ignoreNulls = true)
    "first_value(id#0, true)" parseIs First(id0, ignoreNulls = true)

    "last(id#0, false)" parseIs Last(id0, ignoreNulls = false)
    "last_value(id#0, false)" parseIs Last(id0, ignoreNulls = false)
    "last(id#0, true)" parseIs Last(id0, ignoreNulls = true)
    "last_value(id#0, true)" parseIs Last(id0, ignoreNulls = true)

    "collect_list(id#0, 0, 0)" parseIs CollectList(id0)
    "collect_set(id#0, 0, 0)" parseIs CollectSet(id0)
    "mode(id#0, 0, 0)" parseIs Mode(id0)
    "product(id#0)" parseIs Product(id0)
    "corr(id#0, 1)" parseIs Corr(id0, "1")
    "covar_pop(id#0, 1)" parseIs CovPopulation(id0, "1")
    "covar_samp(id#0, 1)" parseIs CovSample(id0, "1")
    "stddev_pop(id#0)" parseIs StddevPop(id0)
    "stddev_samp(id#0)" parseIs StddevSamp(id0)
    "stddev(id#0)" parseIs StddevSamp(id0)
    "std(id#0)" parseIs StddevSamp(id0)
    "var_pop(id#0)" parseIs VariancePop(id0)
    "var_samp(id#0)" parseIs VarianceSamp(id0)
    "variance(id#0)" parseIs VarianceSamp(id0)
    "skewness(id#0)" parseIs Skewness(id0)
    "kurtosis(id#0)" parseIs Kurtosis(id0)
    "max_by(id#0, -id#0)" parseIs MaxBy(id0, UnaryMinus(id0))
    "min_by(id#0, -id#0)" parseIs MinBy(id0, UnaryMinus(id0))

    "percentile(id#0, 0.75, 1, 0, 0)" parseIs
      Percentile(id0, "0.75", "1", reverse = false)
    "percentile(id#0, 0.75, 1, 0, 0, false)" parseIs
      Percentile(id0, "0.75", "1", reverse = false)
    "percentile(id#0, 0.75, 1, 0, 0, true)" parseIs
      Percentile(id0, "0.75", "1", reverse = true)

    "percentile_disc(id#0, 0.25, true, 0, 0)" parseIs
      PercentileDisc(id0, "0.25", reverse = true, legacyCalculation = true)
    "percentile_disc(id#0, 0.25, true, 0, 0, false)" parseIs
      PercentileDisc(id0, "0.25", reverse = true, legacyCalculation = false)
    "percentile_disc(id#0, 0.25, false, 0, 0, true)" parseIs
      PercentileDisc(id0, "0.25", reverse = false, legacyCalculation = true)

    "approx_percentile(id#0, [0.3,0.5], 10000, 0, 0)" parseIs
      ApproximatePercentile(id0, "[0.3,0.5]", "10000")
    "percentile_approx(id#0, [0.3,0.5], 10000, 0, 0)" parseIs
      ApproximatePercentile(id0, "[0.3,0.5]", "10000")

    "approx_count_distinct(id#0, 0.05, 0, 0)" parseIs
      HyperLogLogPlusPlus(id0, 0.05)

    "every(id#0)" parseIs Min(AttributeReference("id", BooleanType, 0))
    "any(id#0)" parseIs Max(AttributeReference("id", BooleanType, 0))
    "some(id#0)" parseIs Max(AttributeReference("id", BooleanType, 0))
    "bit_and(id#0)" parseIs BitAndAgg(id0)
    "bit_or(id#0)" parseIs BitOrAgg(id0)
    "bit_xor(id#0)" parseIs BitXorAgg(id0)
    "bloom_filter_agg(id#0L, 1000000, 8388608, 0, 0)" parseIs
      BloomFilterAggregate(id0, "1000000", "8388608")
    "count_min_sketch(id#0L, 0.5, 0.6, 1, 0, 0)" parseIs
      CountMinSketchAgg(id0, "0.5", "0.6", "1")

    "histogram_numeric(id#0, 5, 0, 0)" parseIs HistogramNumeric(id0, "5")
    // Old implementation using Hive.
    "histogram_numeric(histogram_numeric, " +
      "HiveFunctionWrapper(" +
      "org.apache.hadoop.hive.ql.udf.generic.GenericUDAFHistogramNumeric," +
      "org.apache.hadoop.hive.ql.udf.generic.GenericUDAFHistogramNumeric@473613a9," +
      "class org.apache.hadoop.hive.ql.udf.generic.GenericUDAFHistogramNumeric" +
      "), " +
      "id#0, 5, false, 0, 0)" parseIs HistogramNumeric(id0, "5")

    "pivotfirst(id#0, sum(id)#1L, 1, 0, 0)" parseIs
      PivotFirst(id0, AttributeReference("sum(id)", 1), IndexedSeq("1"))
    "pivotfirst(id#0, sum(id)#1L, 1, 3, 5, 0, 0)" parseIs
      PivotFirst(id0, AttributeReference("sum(id)", 1), IndexedSeq("1", "3", "5"))

    "hll_sketch_agg(id#0, 5, 0, 0)" parseIs HllSketchAgg(id0, "5")
    "hll_union_agg(id#0, true, 0, 0)" parseIs HllUnionAgg(id0, "true")

    "regrreplacement(id#0)" parseIs RegrReplacement(id0)
    "regr_r2(id#0, 1.0)" parseIs RegrR2(id0, "1.0")
    "regr_sxy(id#0, 1.0)" parseIs RegrSXY(id0, "1.0")
    "regr_slope(id#0, 1.0)" parseIs RegrSlope(id0, "1.0")
    "regr_intercept(id#0, 1.0)" parseIs RegrIntercept(id0, "1.0")

    "approx_top_k(id#0, 5, 10000, 0, 0)" parseIs ApproxTopK(id0, "5", "10000")
    "approx_top_k_accumulate(id#0, 10000, 0, 0)" parseIs ApproxTopKAccumulate(id0, "10000")
    "approx_top_k_combine(id#0, -1, 0, 0)" parseIs ApproxTopKCombine(id0, "-1")

    "string_agg(id#0, ,, 0, 0)" parseIs ListAgg(id0, ",", IndexedSeq.empty)
    "listagg(id#0, null, 0, 0)" parseIs ListAgg(id0, "null", IndexedSeq.empty)
    "listagg(id#0, null, id#0 DESC NULLS LAST, 0, 0)" parseIs
      ListAgg(id0, "null", IndexedSeq(SortOrder(id0, Descending, NullsLast)))
    "theta_sketch_agg(id#0, 12, 0, 0)" parseIs ThetaSketchAgg(id0, "12")
    "theta_union_agg(id#0, 12, 0, 0)" parseIs ThetaUnionAgg(id0, "12")
    "theta_intersection_agg(id#0, 0, 0)" parseIs ThetaIntersectionAgg(id0)
    "kll_sketch_agg_bigint(id#0, None, 0, 0)" parseIs KllSketchAggBigint(id0, None)
    "kll_sketch_agg_bigint(id#0, Some(400), 0, 0)" parseIs KllSketchAggBigint(id0, Some("400"))
    "kll_sketch_agg_float(id#0, None, 0, 0)" parseIs KllSketchAggFloat(id0, None)
    "kll_sketch_agg_float(id#0, Some(400), 0, 0)" parseIs KllSketchAggFloat(id0, Some("400"))
    "kll_sketch_agg_double(id#0, None, 0, 0)" parseIs KllSketchAggDouble(id0, None)
    "kll_sketch_agg_double(id#0, Some(400), 0, 0)" parseIs KllSketchAggDouble(id0, Some("400"))
    "schema_of_variant_agg(id#0, 0, 0)" parseIs SchemaOfVariantAgg(id0)
  }

  test("Alchemy expressions") {
    "hll_init_agg(id#0, 0.05, AgKn, 0, 0)" parseIs
      HyperLogLogInitSimpleAgg(id0, 0.05, "AgKn")
    "hll_init_agg(id#0, 0.05, StreamLib, 0, 0)" parseIs
      HyperLogLogInitSimpleAgg(id0, 0.05, "StreamLib")

    "hll_cardinality(id#0, AgKn)" parseIs
      HyperLogLogCardinality(id0, "AgKn")
  }

  test("ApproxTopK expressions") {
    "approx_top_k_estimate(id#0, 5)" parseIs ApproxTopKEstimate(id0, "5")
  }

  test("Avro expressions") {
    val schema =
      "{\"type\": \"record\", \"name\": \"struct\", \"fields\": [" +
        "{ \"name\": \"u\", \"type\": [\"int\",\"string\"] }]}"

    s"from_avro(id#0, $schema)" parseIs
      AvroDataToCatalyst(id0)
    s"from_avro(id#0, $schema, (mode,permissive))" parseIs
      AvroDataToCatalyst(id0)

    "to_avro(id#0, None)" parseIs CatalystDataToAvro(id0)
    s"to_avro(id#0, Some($schema))" parseIs CatalystDataToAvro(id0)
  }

  test("Bitmap expressions") {
    "staticinvoke(" +
      "class org.apache.spark.sql.catalyst.expressions.BitmapExpressionUtils, " +
      "LongType, " +
      "bitmapBucketNumber, " +
      "id#0, " +
      "LongType, " +
      "true, " +
      "false, " +
      "true)" parseIs BitmapBucketNumber(id0)
    "static_invoke(BitmapExpressionUtils.bitmapBucketNumber(id#0))" parseIs BitmapBucketNumber(id0)

    "staticinvoke(" +
      "class org.apache.spark.sql.catalyst.expressions.BitmapExpressionUtils, " +
      "LongType, " +
      "bitmapBitPosition, " +
      "id#0, " +
      "LongType, " +
      "true, " +
      "false, " +
      "true)" parseIs BitmapBitPosition(id0)
    "static_invoke(BitmapExpressionUtils.bitmapBitPosition(id#0))" parseIs BitmapBitPosition(id0)

    "staticinvoke(" +
      "class org.apache.spark.sql.catalyst.expressions.BitmapExpressionUtils, " +
      "LongType, " +
      "bitmapCount, " +
      "id#0, " +
      "BinaryType, " +
      "true, " +
      "false, " +
      "true)" parseIs BitmapCount(id0)
    "static_invoke(BitmapExpressionUtils.bitmapCount(id#0))" parseIs BitmapCount(id0)

    "bitmap_construct_agg(id#0, 0, 0)" parseIs BitmapConstructAgg(id0)
    "bitmap_or_agg(id#0, 0, 0)" parseIs BitmapOrAgg(id0)
    "bitmap_and_agg(id#0, 0, 0)" parseIs BitmapAndAgg(id0)
  }

  test("Bloom filter expressions") {
    "might_contain(id#0, 2)" parseIs BloomFilterMightContain(id0, "2")
  }

  test("Collation expressions") {
    "collate(id#0, UTF8_LCASE)" parseIs Collate(id0, "UTF8_LCASE")
  }

  test("Collection expressions") {
    "size(id#0, false)" parseIs Size(id0)
    "size(id#0, true)" parseIs Size(id0)
    "cardinality(id#0, true)" parseIs Size(id0)

    "map_keys(id#0)" parseIs MapKeys(id0)
    "map_values(id#0)" parseIs MapValues(id0)
    "map_entries(id#0)" parseIs MapEntries(id0)
    "map_concat()" parseIs MapConcat(IndexedSeq())
    "map_concat(id#0, map#1)" parseIs MapConcat(IndexedSeq(id0, AttributeReference("map", 1)))
    "map_from_entries(id#0)" parseIs MapFromEntries(id0)

    "sort_array(id#0, false)" parseIs SortArray(id0, "false")
    "shuffle(id#0, Some(-1142780354316839385))" parseIs Shuffle(id0, Some(-1142780354316839385L))
    "reverse(id#0)" parseIs Reverse(id0)
    "array_contains(id#0, 2)" parseIs ArrayContains(id0, "2")
    "arrays_overlap(id#0, array#1)" parseIs ArraysOverlap(id0, AttributeReference("array", 1))
    "slice(id#0, 1, 3)" parseIs Slice(id0, "1", "3")
    "array_join(id#0, _, None)" parseIs ArrayJoin(id0, "_", None)
    "array_join(id#0, ,, None)" parseIs ArrayJoin(id0, ",", None)
    "array_join(id#0, _, Some(,))" parseIs ArrayJoin(id0, "_", Some(","))
    "array_min(id#0)" parseIs ArrayMin(id0)
    "array_max(id#0)" parseIs ArrayMax(id0)
    "array_position(id#0, 2)" parseIs ArrayPosition(id0, "2")

    "element_at(id#0, 1, false)" parseIs ElementAt(id0, "1", None, failOnError = false)
    "element_at(id#0, 1, None, true)" parseIs ElementAt(id0, "1", None, failOnError = true)
    "element_at(id#0, 1, Some(), false)" parseIs ElementAt(id0, "1", Some(""), failOnError = false)

    "concat(_, id#0)" parseIs Concat(IndexedSeq("_", id0))
    "concat(,, id#0)" parseIs Concat(IndexedSeq(",", id0))
    "flatten(id#0)" parseIs Flatten(id0)

    "sequence(1, id#0, None, Some(Europe/Lisbon))" parseIs
      Sequence("1", id0, None, Some("Europe/Lisbon"))
    "sequence(1, id#0, Some(2), Some(Europe/Lisbon))" parseIs
      Sequence("1", id0, Some("2"), Some("Europe/Lisbon"))
    "sequence(id#0, 2018-03-01, Some(INTERVAL '1' MONTH), Some(Europe/Lisbon))" parseIs
      Sequence(id0, "2018-03-01", Some("INTERVAL '1' MONTH"), Some("Europe/Lisbon"))

    "array_insert(id#0, 4, 5)" parseIs
      ArrayInsert(id0, "4", "5", legacyNegativeIndex = true)
    "array_insert(id#0, 4, 5, false)" parseIs
      ArrayInsert(id0, "4", "5", legacyNegativeIndex = false)
    "array_insert(id#0, 4, ,, false)" parseIs
      ArrayInsert(id0, "4", ",", legacyNegativeIndex = false)

    "array_repeat(id#0, 2)" parseIs ArrayRepeat(id0, "2")
    "array_remove(id#0, 2)" parseIs ArrayRemove(id0, "2")
    "array_distinct(id#0)" parseIs ArrayDistinct(id0)
    "array_union(id#0, array#1)" parseIs ArrayUnion(id0, AttributeReference("array", 1))
    "array_intersect(id#0, array#1)" parseIs ArrayIntersect(id0, AttributeReference("array", 1))
    "array_except(id#0, array#1)" parseIs ArrayExcept(id0, AttributeReference("array", 1))
    "array_append(id#0, 2)" parseIs ArrayAppend(id0, "2")

    "arrays_zip(id#0, 0)" parseIs ArraysZip(IndexedSeq(id0), Seq("0"))
    "arrays_zip(id#0, array#1, [], 0, 1, 2)" parseIs
      ArraysZip(
        IndexedSeq(id0, AttributeReference("array", 1), "[]"),
        Seq("0", "1", "2")
      )
  }

  test("Complex expressions") {
    "array()" parseIs CreateArray(IndexedSeq.empty)
    "array(id#0, 2)" parseIs CreateArray(IndexedSeq(id0, "2"))
    "map()" parseIs CreateMap(IndexedSeq.empty)
    "map(id#0, 2, (id#0 + 1), 3)" parseIs CreateMap(IndexedSeq(id0, "2", Add(id0, "1"), "3"))
    "map_from_arrays(id#0, [a,b])" parseIs MapFromArrays(id0, "[a,b]")
    "struct(col1, id#0, col2, (id#0 + 1))" parseIs
      CreateNamedStruct(IndexedSeq("col1", id0, "col2", Add(id0, "1")))
    "named_struct(a, id#0, b, (id#0 + 1))" parseIs
      CreateNamedStruct(IndexedSeq("a", id0, "b", Add(id0, "1")))
    "named_struct(min(id), min#0)" parseIs
      CreateNamedStruct(IndexedSeq("min(id)", AttributeReference("min", 0)))
    "str_to_map(id#0, , )" parseIs StringToMap(id0, "", "")
    "str_to_map(id#0, ,, )" parseIs StringToMap(id0, ",", "")
    "str_to_map(id#0, , ,)" parseIs StringToMap(id0, "", ",")
    "str_to_map(id#0, ,, :)" parseIs StringToMap(id0, ",", ":")
    "str_to_map(id#0, ,, ,)" parseIs StringToMap(id0, ",", ",")
    "str_to_map(id#0, :, ,)" parseIs StringToMap(id0, ":", ",")
    "str_to_map(id#0, :, _)" parseIs StringToMap(id0, ":", "_")
  }

  test("Constraint expressions") {
    "knownnullable(id#0)" parseIs KnownNullable(id0)
    "knownnotnull(id#0)" parseIs KnownNotNull(id0)
    "knownfloatingpointnormalized(id#0)" parseIs KnownFloatingPointNormalized(id0)
    "knownnotcontainsnull(id#0)" parseIs KnownNotContainsNull(id0)
  }

  test("Csv expressions") {
    "from_csv(" +
      "StructField(a,IntegerType,true), " +
      "StructField(b,DoubleType,true), " +
      "1, 0.8, " +
      "Some(Europe/Lisbon))" parseIs
      CsvToStructs(
        StructType(
          Array(
            StructField("a", IntegerType),
            StructField("b", DoubleType)
          )
        ),
        options = Map.empty,
        child = "1, 0.8",
        timeZoneId = Some("Europe/Lisbon"),
        requiredSchema = None
      )

    "from_csv(" +
      "StructField(a,IntegerType,true), " +
      "StructField(b,DoubleType,true), " +
      "1, 0.8, " +
      "Some(Europe/Lisbon), " +
      "Some(StructType(StructField(a,IntegerType,true))))" parseIs
      CsvToStructs(
        StructType(
          Array(
            StructField("a", IntegerType),
            StructField("b", DoubleType)
          )
        ),
        options = Map.empty,
        child = "1, 0.8",
        timeZoneId = Some("Europe/Lisbon"),
        requiredSchema = Some(StructType(Array(StructField("a", IntegerType))))
      )

    "from_csv(" +
      "StructField(time,TimestampType,true), " +
      "(timestampFormat,dd/MM/yyyy), " +
      "26/08/2015, " +
      "Some(Europe/Lisbon), " +
      "None)" parseIs
      CsvToStructs(
        StructType(Array(StructField("time", TimestampType))),
        options = Map("timestampFormat" -> "dd/MM/yyyy"),
        child = "26/08/2015",
        timeZoneId = Some("Europe/Lisbon"),
        requiredSchema = None
      )

    "schema_of_csv(1,abc)" parseIs
      SchemaOfCsv("1,abc", Map.empty)
    "schema_of_csv(1, abc, (codec,lz4))" parseIs
      SchemaOfCsv("1, abc", Map("codec" -> "lz4"))
    "schema_of_csv(1.abc, (codec,lz4), (delimiter,.))" parseIs
      SchemaOfCsv("1.abc", Map("codec" -> "lz4", "delimiter" -> "."))
    "invoke(SchemaOfCsvEvaluator(Map()).evaluate(1,abc))" parseIs
      SchemaOfCsv("1,abc", Map.empty)

    "to_csv(id#0, Some(Europe/Lisbon))" parseIs
      StructsToCsv(
        options = Map.empty,
        child = id0,
        timeZoneId = Some("Europe/Lisbon")
      )
    "to_csv((timestampFormat,dd/MM/yyyy), id#0, Some(Europe/Lisbon))" parseIs
      StructsToCsv(
        options = Map("timestampFormat" -> "dd/MM/yyyy"),
        child = id0,
        timeZoneId = Some("Europe/Lisbon")
      )
  }

  test("Datasketches expressions") {
    "hll_sketch_estimate(id#0)" parseIs HllSketchEstimate(id0)
    "hll_union(id#0, agg#1, false)" parseIs HllUnion(id0, AttributeReference("agg", 1), "false")
  }

  test("Datetime expressions") {
    "current_timezone()" parseIs CurrentTimeZone()
    "current_date()" parseIs CurrentDate()
    "current_timestamp()" parseIs CurrentTimestamp()
    "now()" parseIs Now()
    "localtimestamp()" parseIs LocalTimestamp()
    "year(id#0)" parseIs Year(id0)
    "yearofweek(id#0)" parseIs YearOfWeek(id0)
    "quarter(id#0)" parseIs Quarter(id0)
    "month(id#0)" parseIs Month(id0)
    "day(id#0)" parseIs DayOfMonth(id0)
    "dayofmonth(id#0)" parseIs DayOfMonth(id0)
    "dayofyear(id#0)" parseIs DayOfYear(id0)
    "dayofweek(id#0)" parseIs DayOfWeek(id0)
    "weekday(id#0)" parseIs WeekDay(id0)
    "weekofyear(id#0)" parseIs WeekOfYear(id0)
    "hour(id#0, None)" parseIs Hour(id0, None)
    "hour(id#0, Some(Europe/Lisbon))" parseIs Hour(id0, Some("Europe/Lisbon"))
    "minute(id#0, None)" parseIs Minute(id0, None)
    "minute(id#0, Some(Europe/Lisbon))" parseIs Minute(id0, Some("Europe/Lisbon"))
    "second(id#0, None)" parseIs Second(id0, None)
    "second(id#0, Some(Europe/Lisbon))" parseIs Second(id0, Some("Europe/Lisbon"))
    "secondwithfraction(id#0, None)" parseIs
      SecondWithFraction(id0, None)
    "secondwithfraction(id#0, Some(Europe/Lisbon))" parseIs
      SecondWithFraction(id0, Some("Europe/Lisbon"))
    "unix_date(id#0)" parseIs UnixDate(id0)
    "date_from_unix_date(id#0)" parseIs DateFromUnixDate(id0)
    "last_day(id#0)" parseIs LastDay(id0)
    "timestamp_seconds(id#0)" parseIs SecondsToTimestamp(id0)
    "timestamp_millis(id#0)" parseIs MillisToTimestamp(id0)
    "timestamp_micros(id#0)" parseIs MicrosToTimestamp(id0)
    "unix_seconds(id#0)" parseIs UnixSeconds(id0)
    "cast_timestamp_ntz_to_long(id#0)" parseIs CastTimestampNTZToLong(id0)
    "unix_millis(id#0)" parseIs UnixMillis(id0)
    "unix_micros(id#0)" parseIs UnixMicros(id0)
    "date_add(id#0, 5)" parseIs DateAdd(id0, "5")
    "date_sub(id#0, 5)" parseIs DateSub(id0, "5")
    "next_day(id#0, TU)" parseIs NextDay(id0, "TU")
    "next_day(id#0, TU, false)" parseIs NextDay(id0, "TU")
    "add_months(id#0, 5)" parseIs AddMonths(id0, "5")
    "trunc(id#0, YEAR)" parseIs TruncDate(id0, "YEAR")

    "date_format(id#0, y, None)" parseIs
      DateFormatClass(id0, "y", None)
    "date_format(id#0, y, Some(Europe/Lisbon))" parseIs
      DateFormatClass(id0, "y", Some("Europe/Lisbon"))

    "to_unix_timestamp(id#0, yyyy-MM-dd, None)" parseIs
      ToUnixTimestamp(id0, "yyyy-MM-dd", None)
    "to_unix_timestamp(id#0, yyyy-MM-dd, Some(Europe/Lisbon), false)" parseIs
      ToUnixTimestamp(id0, "yyyy-MM-dd", Some("Europe/Lisbon"))

    "unix_timestamp(id#0, yyyy-MM-dd, None)" parseIs
      UnixTimestamp(id0, "yyyy-MM-dd", None)
    "unix_timestamp(id#0, yyyy-MM-dd, Some(Europe/Lisbon), false)" parseIs
      UnixTimestamp(id0, "yyyy-MM-dd", Some("Europe/Lisbon"))

    "from_unixtime(id#0, yyyy-MM-dd HH:mm:ss, None)" parseIs
      FromUnixTime(id0, "yyyy-MM-dd HH:mm:ss", None)
    "from_unixtime(id#0, yyyy-MM-dd HH:mm:ss, Some(Europe/Lisbon))" parseIs
      FromUnixTime(id0, "yyyy-MM-dd HH:mm:ss", Some("Europe/Lisbon"))

    "id#0 + 1 years" parseIs
      AmbiguousExpression(
        IndexedSeq(
          DateAddInterval(id0, "1 years"),
          TimeAdd(id0, "1 years")
        )
      )
    "id#0 + INTERVAL '1' HOUR" parseIs TimeAdd(id0, "INTERVAL '1' HOUR")
    "id#0 + INTERVAL '1' HOUR + INTERVAL '50' MINUTE" parseIs
      TimeAdd(
        TimeAdd(id0, "INTERVAL '1' HOUR"),
        "INTERVAL '50' MINUTE"
      )
    "id#0 + INTERVAL '1' YEAR" parseIs
      AmbiguousExpression(
        IndexedSeq(
          DateAddYMInterval(id0, "INTERVAL '1' YEAR"),
          TimestampAddYMInterval(id0, "INTERVAL '1' YEAR")
        )
      )
    "2000-01-01 + id#0" parseIs
      AmbiguousExpression(
        IndexedSeq(
          DateAddInterval("2000-01-01", id0),
          DateAddYMInterval("2000-01-01", id0)
        )
      )
    "2000-01-01 00:00:00 + id#0" parseIs
      AmbiguousExpression(
        IndexedSeq(
          TimeAdd("2000-01-01 00:00:00", id0),
          TimestampAddYMInterval("2000-01-01 00:00:00", id0)
        )
      )

    // Conflicting precedence.
    "if (true) id#0 else id#0 + INTERVAL '1' HOUR" parseIs
      AmbiguousExpression(
        IndexedSeq(
          If("true", id0, TimeAdd(id0, "INTERVAL '1' HOUR")),
          TimeAdd(If("true", id0, id0), "INTERVAL '1' HOUR")
        )
      )
    "if (true) id#0 else id#0 + 1 years" parseIs
      AmbiguousExpression(
        IndexedSeq(
          If("true", id0, DateAddInterval(id0, "1 years")),
          DateAddInterval(If("true", id0, id0), "1 years"),
          If("true", id0, TimeAdd(id0, "1 years")),
          TimeAdd(If("true", id0, id0), "1 years")
        )
      )
    "if (true) 2000-01-01 else id#0 + 1 years" parseIs
      AmbiguousExpression(
        IndexedSeq(
          If("true", "2000-01-01", DateAddInterval(id0, "1 years")),
          DateAddInterval(If("true", "2000-01-01", id0), "1 years")
        )
      )
    "if (true) 2000-01-01 else id#0 + INTERVAL '1' YEAR" parseIs
      AmbiguousExpression(
        IndexedSeq(
          If("true", "2000-01-01", DateAddYMInterval(id0, "INTERVAL '1' YEAR")),
          DateAddYMInterval(If("true", "2000-01-01", id0), "INTERVAL '1' YEAR")
        )
      )
    "if (true) 2000-01-01 00:00:00 else id#0 + INTERVAL '1' YEAR" parseIs
      AmbiguousExpression(
        IndexedSeq(
          If("true", "2000-01-01 00:00:00", TimestampAddYMInterval(id0, "INTERVAL '1' YEAR")),
          TimestampAddYMInterval(If("true", "2000-01-01 00:00:00", id0), "INTERVAL '1' YEAR")
        )
      )

    "datediff(id#0, 2000-01-01)" parseIs DateDiff(id0, "2000-01-01")
    "date_diff(id#0, 2000-01-01)" parseIs DateDiff(id0, "2000-01-01")
    "from_utc_timestamp(id#0, Asia/Seoul)" parseIs FromUTCTimestamp(id0, "Asia/Seoul")
    "to_utc_timestamp(id#0, Asia/Seoul)" parseIs ToUTCTimestamp(id0, "Asia/Seoul")

    "date_trunc(DD, id#0, None)" parseIs
      TruncTimestamp("DD", id0, None)
    "date_trunc(DD, id#0, Some(Europe/Lisbon))" parseIs
      TruncTimestamp("DD", id0, Some("Europe/Lisbon"))

    "months_between(id#0, 2000-01-01, false, None)" parseIs
      MonthsBetween(id0, "2000-01-01", "false", None)
    "months_between(id#0, 2000-01-01, true, Some(Europe/Lisbon))" parseIs
      MonthsBetween(id0, "2000-01-01", "true", Some("Europe/Lisbon"))

    "make_date(2000, 1, id#0)" parseIs MakeDate("2000", "1", id0)
    "make_date(2000, 1, id#0, false)" parseIs MakeDate("2000", "1", id0)

    "make_timestamp(2000, 1, 2, 3, 4, id#0, Some(CET), Some(Europe/Lisbon))" parseIs
      MakeTimestamp(
        "2000",
        "1",
        "2",
        "3",
        "4",
        id0,
        Some("CET"),
        Some("Europe/Lisbon"),
        failOnError = false,
        TimestampType
      )
    "make_timestamp(2000, 1, 2, 3, 4, id#0, Some(CET), Some(Europe/Lisbon), false)" parseIs
      MakeTimestamp(
        "2000",
        "1",
        "2",
        "3",
        "4",
        id0,
        Some("CET"),
        Some("Europe/Lisbon"),
        failOnError = false,
        TimestampType
      )
    "make_timestamp(2000, 1, 2, 3, 4, id#0, None, None, false, TimestampType)" parseIs
      MakeTimestamp(
        "2000",
        "1",
        "2",
        "3",
        "4",
        id0,
        None,
        None,
        failOnError = false,
        TimestampType
      )
    "make_timestamp(" +
      "2000, 1, 2, 3, 4, id#0, Some(CET), Some(Europe/Lisbon), true, TimestampType)" parseIs
      MakeTimestamp(
        "2000",
        "1",
        "2",
        "3",
        "4",
        id0,
        Some("CET"),
        Some("Europe/Lisbon"),
        failOnError = true,
        TimestampType
      )
    "make_timestamp_ltz(" +
      "2000, 1, 2, 3, 4, id#0, Some(CET), Some(Europe/Lisbon), false, TimestampType)" parseIs
      MakeTimestamp(
        "2000",
        "1",
        "2",
        "3",
        "4",
        id0,
        Some("CET"),
        Some("Europe/Lisbon"),
        failOnError = false,
        TimestampType
      )
    "make_timestamp_ntz(" +
      "2000, 1, 2, 3, 4, id#0, Some(CET), Some(Europe/Lisbon), false, TimestampNTZType)" parseIs
      MakeTimestamp(
        "2000",
        "1",
        "2",
        "3",
        "4",
        id0,
        Some("CET"),
        Some("Europe/Lisbon"),
        failOnError = false,
        TimestampNTZType
      )
    "try_make_timestamp_ltz(" +
      "2014, 12, 28, 6, 30, id#0, None, Some(Europe/Lisbon), false, TimestampType)" parseIs
      MakeTimestamp(
        "2014",
        "12",
        "28",
        "6",
        "30",
        id0,
        None,
        Some("Europe/Lisbon"),
        failOnError = false,
        TimestampType
      )
    "try_make_timestamp_ntz(" +
      "2014, 12, 28, 6, 30, id#0, None, Some(Europe/Lisbon), false, TimestampNTZType)" parseIs
      MakeTimestamp(
        "2014",
        "12",
        "28",
        "6",
        "30",
        id0,
        None,
        Some("Europe/Lisbon"),
        failOnError = false,
        TimestampNTZType
      )

    "gettimestamp(id#0, yyyy-MM-dd, Some(Europe/Lisbon))" parseIs
      GetTimestamp(id0, "yyyy-MM-dd", TimestampType, Some("Europe/Lisbon"))
    "gettimestamp(id#0, yyyy-MM-dd, Some(Europe/Lisbon), false)" parseIs
      GetTimestamp(id0, "yyyy-MM-dd", TimestampType, Some("Europe/Lisbon"))
    "gettimestamp(id#0, yyyy-MM-dd, TimestampType, None, false)" parseIs
      GetTimestamp(id0, "yyyy-MM-dd", TimestampType, None)

    "(id#0 - 2000-01-01 00:00:00)" parseIs SubtractTimestamps(id0, "2000-01-01 00:00:00")
    "(id#0 - 2000-01-01)" parseIs SubtractDates(id0, "2000-01-01")

    "convert_timezone(Europe/Brussels, America/Los_Angeles, id#0)" parseIs
      ConvertTimezone("Europe/Brussels", "America/Los_Angeles", id0)

    "timestampadd(YEAR, 10, id#0, None)" parseIs
      TimestampAdd("YEAR", "10", id0, None)
    "timestampadd(YEAR, 10, id#0, Some(Europe/Lisbon))" parseIs
      TimestampAdd("YEAR", "10", id0, Some("Europe/Lisbon"))

    "timestampdiff(SECOND, 2000-01-01 00:00:00, id#0, None)" parseIs
      TimestampDiff("SECOND", "2000-01-01 00:00:00", id0, None)
    "timestampdiff(SECOND, 2000-01-01 00:00:00, id#0, Some(Europe/Lisbon))" parseIs
      TimestampDiff("SECOND", "2000-01-01 00:00:00", id0, Some("Europe/Lisbon"))

    "dayname(id#0)" parseIs DayName(id0)
    "monthname(id#0)" parseIs MonthName(id0)
  }

  test("Decimal expressions") {
    "UnscaledValue(id#0)" parseIs UnscaledValue(id0)
    "MakeDecimal(id#0,18,2)" parseIs MakeDecimal(id0, 18, 2)
    "promote_precision(id#0)" parseIs PromotePrecision(id0)
    "CheckOverflow(id#0, DecimalType(9,2))" parseIs CheckOverflow(id0)
    "CheckOverflow(id#0, DecimalType(9,2), true)" parseIs CheckOverflow(id0)
  }

  test("Generators") {
    "explode(id#0)" parseIs Explode(id0)
    "posexplode(id#0)" parseIs PosExplode(id0)
    "inline(id#0)" parseIs Inline(id0)
    "json_tuple(id#0, a, b)" parseIs JsonTuple(IndexedSeq(id0, "a", "b"))
    "stack(2, id#0, 3, 1)" parseIs Stack(IndexedSeq("2", id0, "3", "1"))
    "replicaterows(id#0, s#1)" parseIs ReplicateRows(IndexedSeq(id0, AttributeReference("s", 1)))
    "sql_keywords()" parseIs SQLKeywords()
    "collations()" parseIs Collations()
    "variant_explode(id#0)" parseIs VariantExplode(id0)
  }

  test("Hash expressions") {
    "md5(id#0)" parseIs Md5(id0)
    "sha2(id#0, 256)" parseIs Sha2(id0, "256")
    "sha(id#0)" parseIs Sha1(id0)
    "sha1(id#0)" parseIs Sha1(id0)
    "crc32(id#0)" parseIs Crc32(id0)
    "hash(id#0, 42)" parseIs Murmur3Hash(IndexedSeq(id0), 42)
    "hash(id#0, 21, 42)" parseIs Murmur3Hash(IndexedSeq(id0, "21"), 42)
    "xxhash64(id#0, 42)" parseIs XxHash64(IndexedSeq(id0), 42L)
    "xxhash64(id#0, 21, 42)" parseIs XxHash64(IndexedSeq(id0, "21"), 42L)
  }

  test("High order functions") {
    val x1 = NamedLambdaVariable("x", 1)
    val i2 = NamedLambdaVariable("i", 2)
    "transform(id#0, lambdafunction((lambda x#1 + 2), lambda x#1, false))" parseIs
      ArrayTransform(
        id0,
        LambdaFunction(Add(x1, "2"), IndexedSeq(x1))
      )
    "transform(" +
      "id#0, " +
      "lambdafunction((lambda x#1 + lambda i#2), lambda x#1, lambda i#2, false))" parseIs
      ArrayTransform(
        id0,
        LambdaFunction(Add(x1, i2), IndexedSeq(x1, i2))
      )

    val left1 = NamedLambdaVariable("left", 1)
    val right2 = NamedLambdaVariable("right", 2)
    val defaultSort =
      LambdaFunction(
        If(
          LessThan(left1, right2),
          "-1",
          If(
            GreaterThan(left1, right2),
            "1",
            "0"
          )
        ),
        IndexedSeq(left1, right2)
      )
    "array_sort(id#0)" parseIs ArraySort(id0, UndefinedExpression(IntegerType))
    "array_sort(" +
      "id#0, " +
      "lambdafunction(" +
      "if ((lambda left#1 < lambda right#2)) -1 " +
      "else if ((lambda left#1 > lambda right#2)) 1 " +
      "else 0, " +
      "lambda left#1, " +
      "lambda right#2, " +
      "false))" parseIs ArraySort(id0, defaultSort)
    "array_sort(" +
      "id#0, " +
      "lambdafunction(" +
      "if ((lambda left#1 < lambda right#2)) -1 " +
      "else if ((lambda left#1 > lambda right#2)) 1 " +
      "else 0, " +
      "lambda left#1, " +
      "lambda right#2, " +
      "false), " +
      "false)" parseIs ArraySort(id0, defaultSort)
    "array_sort(" +
      "id#0, " +
      "lambdafunction(" +
      "if ((lambda left#1 = lambda right#2)) 0 else -1, " +
      "lambda left#1, " +
      "lambda right#2, " +
      "false))" parseIs
      ArraySort(
        id0,
        LambdaFunction(
          If(EqualTo(left1, right2), "0", "-1"),
          IndexedSeq(left1, right2)
        )
      )

    val key1 = NamedLambdaVariable("key", 1)
    val value2 = NamedLambdaVariable("value", 2)
    "map_filter(" +
      "id#0, " +
      "lambdafunction((lambda key#1 > 1), lambda key#1, lambda value#2, false))" parseIs
      MapFilter(
        id0,
        LambdaFunction(GreaterThan(key1, "1"), IndexedSeq(key1, value2))
      )

    "filter(id#0, lambdafunction((lambda x#1 > 1), lambda x#1, false))" parseIs
      ArrayFilter(
        id0,
        LambdaFunction(GreaterThan(x1, "1"), IndexedSeq(x1))
      )
    "filter(id#0, lambdafunction((lambda x#1 > lambda i#2), lambda x#1, lambda i#2, false))" parseIs
      ArrayFilter(
        id0,
        LambdaFunction(GreaterThan(x1, i2), IndexedSeq(x1, i2))
      )

    "exists(id#0, lambdafunction((lambda x#1 > 1), lambda x#1, false))" parseIs
      ArrayExists(
        id0,
        LambdaFunction(GreaterThan(x1, "1"), IndexedSeq(x1))
      )

    "forall(id#0, lambdafunction((lambda x#1 > 1), lambda x#1, false))" parseIs
      ArrayForAll(
        id0,
        LambdaFunction(GreaterThan(x1, "1"), IndexedSeq(x1))
      )

    val acc2 = NamedLambdaVariable("acc", 2)
    val id3 = NamedLambdaVariable("id", 3)
    "aggregate(" +
      "id#0, " +
      "0, " +
      "lambdafunction((lambda acc#2 + lambda x#1), lambda acc#2, lambda x#1, false), " +
      "lambdafunction(lambda id#3, lambda id#3, false))" parseIs
      ArrayAggregate(
        id0,
        "0",
        LambdaFunction(Add(acc2, x1), IndexedSeq(acc2, x1)),
        LambdaFunction(id3, IndexedSeq(id3))
      )
    "aggregate(" +
      "id#0, " +
      "1, " +
      "lambdafunction((lambda acc#2 + lambda x#1), lambda acc#2, lambda x#1, false), " +
      "lambdafunction((lambda id#3 + 10), lambda id#3, false))" parseIs
      ArrayAggregate(
        id0,
        "1",
        LambdaFunction(Add(acc2, x1), IndexedSeq(acc2, x1)),
        LambdaFunction(Add(id3, "10"), IndexedSeq(id3))
      )
    "reduce(" +
      "id#0, " +
      "1, " +
      "lambdafunction((lambda acc#2 + lambda x#1), lambda acc#2, lambda x#1, false), " +
      "lambdafunction((lambda id#3 + val#4), lambda id#3, false))" parseIs
      ArrayAggregate(
        id0,
        "1",
        LambdaFunction(Add(acc2, x1), IndexedSeq(acc2, x1)),
        LambdaFunction(Add(id3, AttributeReference("val", 4)), IndexedSeq(id3))
      )

    "transform_keys(" +
      "id#0, " +
      "lambdafunction(-lambda key#1, lambda key#1, lambda value#2, false))" parseIs
      TransformKeys(
        id0,
        LambdaFunction(UnaryMinus(key1), IndexedSeq(key1, value2))
      )
    "transform_keys(" +
      "id#0, " +
      "lambdafunction(" +
      "(lambda key#1 + lambda value#2), " +
      "lambda key#1, " +
      "lambda value#2, " +
      "false))" parseIs
      TransformKeys(
        id0,
        LambdaFunction(Add(key1, value2), IndexedSeq(key1, value2))
      )

    "transform_values(" +
      "id#0, " +
      "lambdafunction(-lambda key#1, lambda key#1, lambda value#2, false))" parseIs
      TransformValues(
        id0,
        LambdaFunction(UnaryMinus(key1), IndexedSeq(key1, value2))
      )
    "transform_values(" +
      "id#0, " +
      "lambdafunction(" +
      "(lambda key#1 + lambda value#2), " +
      "lambda key#1, " +
      "lambda value#2, " +
      "false))" parseIs
      TransformValues(
        id0,
        LambdaFunction(Add(key1, value2), IndexedSeq(key1, value2))
      )

    "map_zip_with(" +
      "id#0, " +
      "map#2, " +
      "lambdafunction(" +
      "coalesce(lambda value1#3, lambda value2#4), " +
      "lambda key#1, " +
      "lambda value1#3, " +
      "lambda value2#4, " +
      "false))" parseIs
      MapZipWith(
        id0,
        AttributeReference("map", 2),
        LambdaFunction(
          Coalesce(
            IndexedSeq(
              NamedLambdaVariable("value1", 3),
              NamedLambdaVariable("value2", 4)
            )
          ),
          IndexedSeq(
            key1,
            NamedLambdaVariable("value1", 3),
            NamedLambdaVariable("value2", 4)
          )
        )
      )

    val y3 = NamedLambdaVariable("y", 3)
    "zip_with(" +
      "id#0, " +
      "array#2, " +
      "lambdafunction((lambda x#1 + lambda y#3), lambda x#1, lambda y#3, false))" parseIs
      ZipWith(
        id0,
        AttributeReference("array", 2),
        LambdaFunction(Add(x1, y3), IndexedSeq(x1, y3))
      )
  }

  test("Input file expressions") {
    "input_file_name()" parseIs InputFileName()
    "input_file_block_start()" parseIs InputFileBlockStart()
    "input_file_block_length()" parseIs InputFileBlockLength()
  }

  test("Interval expressions") {
    "extractintervalyears(id#0)" parseIs ExtractIntervalYears(id0)
    "extractintervalmonths(id#0)" parseIs ExtractIntervalMonths(id0)
    "extractintervaldays(id#0)" parseIs ExtractIntervalDays(id0)
    "extractintervalhours(id#0)" parseIs ExtractIntervalHours(id0)
    "extractintervalminutes(id#0)" parseIs ExtractIntervalMinutes(id0)
    "extractintervalseconds(id#0)" parseIs ExtractIntervalSeconds(id0)

    "extractansiintervalyears(id#0)" parseIs ExtractANSIIntervalYears(id0)
    "extractansiintervalmonths(id#0)" parseIs ExtractANSIIntervalMonths(id0)
    "extractansiintervaldays(id#0)" parseIs ExtractANSIIntervalDays(id0)
    "extractansiintervalhours(id#0)" parseIs ExtractANSIIntervalHours(id0)
    "extractansiintervalminutes(id#0)" parseIs ExtractANSIIntervalMinutes(id0)
    "extractansiintervalseconds(id#0)" parseIs ExtractANSIIntervalSeconds(id0)

    "multiply_interval(id#0, 2.0, false)" parseIs MultiplyInterval(id0, "2.0")
    "divide_interval(id#0, 2.0, false)" parseIs DivideInterval(id0, "2.0")
    "make_interval(id#0, 1, 2, 3, 4, 5, 6.000001, false)" parseIs
      MakeInterval(id0, "1", "2", "3", "4", "5", "6.000001")
    "make_dt_interval(id#0, 1, 2, 3.000001)" parseIs
      MakeDTInterval(id0, "1", "2", "3.000001")
    "make_ym_interval(id#0, 1)" parseIs
      MakeYMInterval(id0, "1")

    "(id#0 * 2)" parseIs
      AmbiguousExpression(
        IndexedSeq(
          Multiply(id0, "2"),
          MultiplyYMInterval(id0, "2"),
          MultiplyDTInterval(id0, "2")
        )
      )
    "(INTERVAL '1' YEAR * id#0)" parseIs MultiplyYMInterval("INTERVAL '1' YEAR", id0)
    "(INTERVAL '1' DAY * id#0)" parseIs MultiplyDTInterval("INTERVAL '1' DAY", id0)

    "(id#0 / 2)" parseIs
      AmbiguousExpression(
        IndexedSeq(
          Divide(id0, "2"),
          DivideYMInterval(id0, "2"),
          DivideDTInterval(id0, "2")
        )
      )
    "(INTERVAL '1' YEAR / id#0)" parseIs DivideYMInterval("INTERVAL '1' YEAR", id0)
    "(INTERVAL '1' DAY / id#0)" parseIs DivideDTInterval("INTERVAL '1' DAY", id0)
  }

  test("Json expressions") {
    "get_json_object({\"a\": 1, \"b\": 2}, $.a)" parseIs
      GetJsonObject("{\"a\": 1, \"b\": 2}", "$.a")

    "from_json(" +
      "StructField(a,IntegerType,true), " +
      "StructField(b,DoubleType,true), " +
      "{\"a\":1, \"b\":0.8}, " +
      "Some(Europe/Lisbon))" parseIs
      JsonToStructs(
        StructType(
          Array(
            StructField("a", IntegerType),
            StructField("b", DoubleType)
          )
        ),
        options = Map.empty,
        child = "{\"a\":1, \"b\":0.8}",
        timeZoneId = Some("Europe/Lisbon")
      )
    "from_json(" +
      "StructField(time,TimestampType,true), " +
      "(timestampFormat,dd/MM/yyyy), " +
      "{\"time\":\"26/08/2015\"}, " +
      "Some(Europe/Lisbon), " +
      "false)" parseIs
      JsonToStructs(
        StructType(Array(StructField("time", TimestampType))),
        options = Map("timestampFormat" -> "dd/MM/yyyy"),
        child = "{\"time\":\"26/08/2015\"}",
        timeZoneId = Some("Europe/Lisbon")
      )
    "from_json(" +
      "StructField(teacher,StringType,true), " +
      "StructField(" +
      "student," +
      "ArrayType(" +
      "StructType(" +
      "StructField(name,StringType,true)," +
      "StructField(rank,IntegerType,true)),true),true), " +
      "{\"teacher\": \"Alice\", \"student\": [" +
      "{\"name\": \"Bob\", \"rank\": 1}, " +
      "{\"name\": \"Charlie\", \"rank\": 2}]}, " +
      "Some(Europe/Lisbon))" parseIs
      JsonToStructs(
        StructType(
          Array(
            StructField("teacher", StringType),
            StructField(
              "student",
              ArrayType(
                StructType(
                  Array(
                    StructField("name", StringType),
                    StructField("rank", IntegerType)
                  )
                )
              )
            )
          )
        ),
        options = Map.empty,
        child =
          "{\"teacher\": \"Alice\", \"student\": [" +
            "{\"name\": \"Bob\", \"rank\": 1}, " +
            "{\"name\": \"Charlie\", \"rank\": 2}]}",
        timeZoneId = Some("Europe/Lisbon")
      )

    "to_json(id#0, Some(Europe/Lisbon))" parseIs
      StructsToJson(
        options = Map.empty,
        child = id0,
        timeZoneId = Some("Europe/Lisbon")
      )
    "to_json((timestampFormat,dd/MM/yyyy), id#0, Some(Europe/Lisbon))" parseIs
      StructsToJson(
        options = Map("timestampFormat" -> "dd/MM/yyyy"),
        child = id0,
        timeZoneId = Some("Europe/Lisbon")
      )
    "invoke(" +
      "StructsToJsonEvaluator(" +
      "Map()," +
      "StructType(StructField(a,IntegerType,false),StructField(b,IntegerType,false))," +
      "Some(Europe/Lisbon))" +
      ".evaluate(id#0))" parseIs
      StructsToJson(
        options = Map.empty,
        child = id0,
        timeZoneId = Some("Europe/Lisbon")
      )

    "schema_of_json({\"col\":0, \"other\": 1})" parseIs
      SchemaOfJson(
        "{\"col\":0, \"other\": 1}",
        options = Map.empty
      )
    "schema_of_json([{\"col\":01}], (allowNumericLeadingZeros,true))" parseIs
      SchemaOfJson(
        "[{\"col\":01}]",
        options = Map("allowNumericLeadingZeros" -> "true")
      )
    "invoke(SchemaOfJsonEvaluator(Map()).evaluate([{\"col\":0}]))" parseIs
      SchemaOfJson(
        "[{\"col\":0}]",
        options = Map.empty
      )
    "invoke(SchemaOfJsonEvaluator(Map(allowNumericLeadingZeros -> true, mode -> permissive))" +
      ".evaluate({}))" parseIs
      SchemaOfJson(
        "{}",
        options = Map(
          "allowNumericLeadingZeros" -> "true",
          "mode" -> "permissive"
        )
      )

    "json_array_length([1,2,3,{\"f1\":1,\"f2\":[5,6]},4])" parseIs
      LengthOfJsonArray("[1,2,3,{\"f1\":1,\"f2\":[5,6]},4]")
    "json_array_length([1,2)" parseIs
      LengthOfJsonArray("[1,2")
    "static_invoke(JsonExpressionUtils.lengthOfJsonArray([1,2,3,4]))" parseIs
      LengthOfJsonArray("[1,2,3,4]")

    "json_object_keys({})" parseIs
      JsonObjectKeys("{}")
    "json_object_keys({\"f1\":\"abc\",\"f2\":{\"f3\":\"a\", \"f4\":\"b\"}})" parseIs
      JsonObjectKeys("{\"f1\":\"abc\",\"f2\":{\"f3\":\"a\", \"f4\":\"b\"}}")
    "static_invoke(JsonExpressionUtils.jsonObjectKeys({}))" parseIs
      JsonObjectKeys("{}")
  }

  test("Kll expressions") {
    "kll_sketch_to_string_bigint(id#0)" parseIs KllSketchToStringBigint(id0)
    "kll_sketch_to_string_float(id#0)" parseIs KllSketchToStringFloat(id0)
    "kll_sketch_to_string_double(id#0)" parseIs KllSketchToStringDouble(id0)
    "kll_sketch_get_n_bigint(id#0)" parseIs KllSketchGetNBigint(id0)
    "kll_sketch_get_n_float(id#0)" parseIs KllSketchGetNFloat(id0)
    "kll_sketch_get_n_double(id#0)" parseIs KllSketchGetNDouble(id0)
    "kll_sketch_merge_bigint(id#0, bin#1)" parseIs
      KllSketchMergeBigint(id0, AttributeReference("bin", 1))
    "kll_sketch_merge_float(id#0, bin#1)" parseIs
      KllSketchMergeFloat(id0, AttributeReference("bin", 1))
    "kll_sketch_merge_double(id#0, bin#1)" parseIs
      KllSketchMergeDouble(id0, AttributeReference("bin", 1))
    "kll_sketch_get_quantile_bigint(id#0, 0.5)" parseIs KllSketchGetQuantileBigint(id0, "0.5")
    "kll_sketch_get_quantile_float(id#0, 0.5)" parseIs KllSketchGetQuantileFloat(id0, "0.5")
    "kll_sketch_get_quantile_double(id#0, 0.5)" parseIs KllSketchGetQuantileDouble(id0, "0.5")
    "kll_sketch_get_rank_bigint(id#0, 42)" parseIs KllSketchGetRankBigint(id0, "42")
    "kll_sketch_get_rank_float(id#0, 42.0)" parseIs KllSketchGetRankFloat(id0, "42.0")
    "kll_sketch_get_rank_double(id#0, 42.0)" parseIs KllSketchGetRankDouble(id0, "42.0")
  }

  test("Mask") {
    "mask(id#0, X, x, n, null)" parseIs Mask(id0, "X", "x", "n", "null")
  }

  test("Math expressions") {
    "E()" parseIs EulerNumber()
    "PI()" parseIs Pi()
    "ACOS(id#0)" parseIs Acos(id0)
    "ASIN(id#0)" parseIs Asin(id0)
    "ATAN(id#0)" parseIs Atan(id0)
    "CBRT(id#0)" parseIs Cbrt(id0)
    "COS(id#0)" parseIs Cos(id0)
    "SEC(id#0)" parseIs Sec(id0)
    "COSH(id#0)" parseIs Cosh(id0)
    "ACOSH(id#0)" parseIs Acosh(id0)
    "EXP(id#0)" parseIs Exp(id0)
    "EXPM1(id#0)" parseIs Expm1(id0)
    "ln(id#0)" parseIs Log(id0)
    "LOG2(id#0)" parseIs Log2(id0)
    "LOG10(id#0)" parseIs Log10(id0)
    "LOG1P(id#0)" parseIs Log1p(id0)
    "rint(id#0)" parseIs Rint(id0)
    "sign(id#0)" parseIs Signum(id0)
    "SIGNUM(id#0)" parseIs Signum(id0)
    "SIN(id#0)" parseIs Sin(id0)
    "CSC(id#0)" parseIs Csc(id0)
    "SINH(id#0)" parseIs Sinh(id0)
    "ASINH(id#0)" parseIs Asinh(id0)
    "SQRT(id#0)" parseIs Sqrt(id0)
    "TAN(id#0)" parseIs Tan(id0)
    "COT(id#0)" parseIs Cot(id0)
    "TANH(id#0)" parseIs Tanh(id0)
    "ATANH(id#0)" parseIs Atanh(id0)
    "DEGREES(id#0)" parseIs ToDegrees(id0)
    "RADIANS(id#0)" parseIs ToRadians(id0)
    "CEIL(id#0)" parseIs Ceil(id0)
    "ceiling(id#0)" parseIs Ceil(id0)
    "FLOOR(id#0)" parseIs Floor(id0)
    "factorial(id#0)" parseIs Factorial(id0)
    "bin(id#0)" parseIs Bin(id0)
    "hex(id#0)" parseIs Hex(id0)
    "unhex(id#0)" parseIs Unhex(id0)
    "ATAN2(id#0, 3.0)" parseIs Atan2(id0, "3.0")
    "pow(id#0, 3.0)" parseIs Pow(id0, "3.0")
    "POWER(id#0, 3.0)" parseIs Pow(id0, "3.0")
    "HYPOT(id#0, 3.0)" parseIs Hypot(id0, "3.0")
    "LOG(id#0, 3.0)" parseIs Logarithm(id0, "3.0")
    "shiftleft(id#0, 3)" parseIs ShiftLeft(id0, "3")
    "(id#0 << 3)" parseIs ShiftLeft(id0, "3")
    "shiftright(id#0, 3)" parseIs ShiftRight(id0, "3")
    "(id#0 >> 3)" parseIs ShiftRight(id0, "3")
    "shiftrightunsigned(id#0, 3)" parseIs ShiftRightUnsigned(id0, "3")
    "(id#0 >>> 3)" parseIs ShiftRightUnsigned(id0, "3")
    "ceil(id#0, 2)" parseIs RoundCeil(id0, "2")
    "ceiling(id#0, 2)" parseIs RoundCeil(id0, "2")
    "floor(id#0, 2)" parseIs RoundFloor(id0, "2")
    "round(id#0, 3)" parseIs Round(id0, "3")
    "bround(id#0, 3)" parseIs BRound(id0, "3")
    "conv(id#0, 2, 10)" parseIs Conv(id0, "2", "10")
    "conv(id#0, 2, 10, false)" parseIs Conv(id0, "2", "10")
    "width_bucket(id#0, 2.0, 10.0, 5)" parseIs WidthBucket(id0, "2.0", "10.0", "5")
  }

  test("Misc expressions") {
    "raise_error(Error, NullType)" parseIs RaiseError("Error", NullType)
    "uuid(Some(-8006362833632075733))" parseIs Uuid(Some(-8006362833632075733L))
    "version()" parseIs SparkVersion()
    "SPARK_PARTITION_ID()" parseIs SparkPartitionID()
    "typeof(id#0)" parseIs TypeOf(id0)
    "monotonically_increasing_id()" parseIs MonotonicallyIncreasingID()
    "reflect(java.util.UUID, randomUUID)" parseIs
      CallMethodViaReflection(IndexedSeq("java.util.UUID", "randomUUID"))
    "java_method(java.util.UUID, fromString, id#0)" parseIs
      CallMethodViaReflection(IndexedSeq("java.util.UUID", "fromString", id0))

    "static_invoke(ExpressionImplUtils.getSparkVersion())" parseIs SparkVersion()

    "staticinvoke(" +
      "class org.apache.spark.sql.catalyst.expressions.ExpressionImplUtils, " +
      "BinaryType, " +
      "aesEncrypt, " +
      "id#0, " +
      "0x30303030313131313232323233333333, " +
      "GCM, " +
      "DEFAULT, " +
      "0x, " +
      "0x, " +
      "BinaryType, " +
      "BinaryType, " +
      "StringType, " +
      "StringType, " +
      "BinaryType, " +
      "BinaryType, " +
      "true, " +
      "true, " +
      "true)" parseIs
      AesEncrypt(id0, "0x30303030313131313232323233333333", "GCM", "DEFAULT", "0x", "0x")
    "static_invoke(" +
      "ExpressionImplUtils.aesEncrypt(" +
      "id#0, 0x30303030313131313232323233333333, GCM, DEFAULT, 0x, 0x))" parseIs
      AesEncrypt(id0, "0x30303030313131313232323233333333", "GCM", "DEFAULT", "0x", "0x")

    "staticinvoke(" +
      "class org.apache.spark.sql.catalyst.expressions.ExpressionImplUtils, " +
      "BinaryType, " +
      "aesDecrypt, " +
      "id#0," +
      "0x30303030313131313232323233333333, " +
      "GCM, " +
      "DEFAULT, " +
      "0x, " +
      "BinaryType, " +
      "BinaryType, " +
      "StringType, " +
      "StringType, " +
      "BinaryType, " +
      "true, " +
      "true, " +
      "true)" parseIs
      AesDecrypt(id0, "0x30303030313131313232323233333333", "GCM", "DEFAULT", "0x")
    "static_invoke(" +
      "ExpressionImplUtils.aesDecrypt(" +
      "id#0, 0x30303030313131313232323233333333, GCM, DEFAULT, 0x))" parseIs
      AesDecrypt(id0, "0x30303030313131313232323233333333", "GCM", "DEFAULT", "0x")
  }

  test("Null expressions") {
    "coalesce(id#0)" parseIs Coalesce(IndexedSeq(id0))
    "coalesce(id#0, 2)" parseIs Coalesce(IndexedSeq(id0, "2"))
    "isnan(id#0)" parseIs IsNaN(id0)
    "nanvl(id#0, 2.0)" parseIs NaNvl(id0, "2.0")
    "isnull(id#0)" parseIs IsNull(id0)
    "isnotnull(id#0)" parseIs IsNotNull(id0)
    "atleastnnonnulls(1)" parseIs AtLeastNNonNulls(1, IndexedSeq.empty)
    "atleastnnonnulls(1, id#0)" parseIs AtLeastNNonNulls(1, IndexedSeq(id0))
    "atleastnnonnulls(1, id#0, 2)" parseIs AtLeastNNonNulls(1, IndexedSeq(id0, "2"))
  }

  test("Number format expressions") {
    "to_number(12,454.8-, 99,999.9S)" parseIs ToNumber("12,454.8-", "99,999.9S")
    "try_to_number(12,454.8-, 99,999.9S)" parseIs TryToNumber("12,454.8-", "99,999.9S")
    "to_char(id#0, 99G999D9S)" parseIs ToCharacter(id0, "99G999D9S")
  }

  test("Object expressions") {
    "staticinvoke(" +
      "class java.lang.Long, " +
      "ObjectType(class java.lang.Long), " +
      "valueOf, " +
      "id#0L, " +
      "true, " +
      "false, " +
      "true)" parseIs
      StaticInvoke(
        "java.lang.Long",
        ObjectType("java.lang.Long"),
        "valueOf",
        IndexedSeq(id0)
      )
    "input[0, scala.Tuple5, true]._3" parseIs
      Invoke(BoundReference(0, ObjectType("scala.Tuple5")), "_3")
    "id#0.booleans" parseIs
      Invoke(id0, "booleans")
    "id#0.structs.booleans" parseIs
      Invoke(Invoke(id0, "structs"), "booleans")
    "newInstance(class com.xonai.spark.sql.parser.SimpleUDT)" parseIs
      NewInstance("com.xonai.spark.sql.parser.SimpleUDT")
    "unwrapoption(IntegerType, id#0)" parseIs
      UnwrapOption(IntegerType, id0)
    "wrapoption(id#0L, LongType)" parseIs
      WrapOption(id0, LongType)
    "mapobjects(" +
      "lambdavariable(MapObject, ObjectType(class java.lang.Object), true, -1), " +
      "lambdavariable(MapObject, ObjectType(class java.lang.Object), true, -1), " +
      "id#0, " +
      "None)" parseIs
      MapObjects(
        LambdaVariable("MapObject", ObjectType("java.lang.Object")),
        LambdaVariable("MapObject", ObjectType("java.lang.Object")),
        id0,
        None
      )
    "catalysttoexternalmap(" +
      "lambdavariable(CatalystToExternalMap_key, LongType, false, -3), " +
      "lambdavariable(CatalystToExternalMap_key, LongType, false, -3), " +
      "lambdavariable(CatalystToExternalMap_value, LongType, false, -4), " +
      "lambdavariable(CatalystToExternalMap_value, LongType, false, -4), " +
      "map(id, id)#2, " +
      "interface scala.collection.immutable.Map)" parseIs
      CatalystToExternalMap(
        LambdaVariable("CatalystToExternalMap_key", LongType),
        LambdaVariable("CatalystToExternalMap_key", LongType),
        LambdaVariable("CatalystToExternalMap_value", LongType),
        LambdaVariable("CatalystToExternalMap_value", LongType),
        AttributeReference("map(id, id)", 2),
        "scala.collection.immutable.Map"
      )
    "externalmaptocatalyst(" +
      "lambdavariable(" +
      "ExternalMapToCatalyst_key, ObjectType(class java.lang.Object), false, -2), " +
      "lambdavariable(" +
      "ExternalMapToCatalyst_key, ObjectType(class java.lang.Object), false, -2), " +
      "lambdavariable(" +
      "ExternalMapToCatalyst_value, ObjectType(class java.lang.Object), false, -3), " +
      "lambdavariable(" +
      "ExternalMapToCatalyst_value, ObjectType(class java.lang.Object), false, -3), " +
      "id#0)" parseIs
      ExternalMapToCatalyst(
        LambdaVariable("ExternalMapToCatalyst_key", ObjectType("java.lang.Object")),
        LambdaVariable("ExternalMapToCatalyst_key", ObjectType("java.lang.Object")),
        LambdaVariable("ExternalMapToCatalyst_value", ObjectType("java.lang.Object")),
        LambdaVariable("ExternalMapToCatalyst_value", ObjectType("java.lang.Object")),
        id0
      )
    "createexternalrow(id#0, StructField(id,LongType,false))" parseIs
      CreateExternalRow(
        IndexedSeq(id0),
        StructType(Array(StructField("id", LongType)))
      )
    "encodeusingserializer(id#0, false)" parseIs
      EncodeUsingSerializer(id0)
    "decodeusingserializer(id#0, com.xonai.spark.sql.parser.SimpleCaseClass, false)" parseIs
      DecodeUsingSerializer(id0, "com.xonai.spark.sql.parser.SimpleCaseClass")
    "initializejavabean(" +
      "newInstance(class com.xonai.spark.sql.parser.SimpleBeanClass), " +
      "(setId,id#0L))" parseIs
      InitializeJavaBean(
        NewInstance("com.xonai.spark.sql.parser.SimpleBeanClass"),
        Map("setId" -> id0)
      )
    "assertnotnull(id#0)" parseIs AssertNotNull(id0)
    "getexternalrowfield(id#0, 0, id)" parseIs
      GetExternalRowField(id0, 0, "id")
    "validateexternaltype(id#0, IntegerType, ObjectType(class java.lang.Integer))" parseIs
      ValidateExternalType(id0, IntegerType, ObjectType("java.lang.Integer"))
  }

  test("Optimizer expressions") {
    "normalizenanandzero(id#0)" parseIs NormalizeNaNAndZero(id0)
  }

  test("Protobuf expressions") {
    "from_protobuf(id#0, Duration, Some([B@ba0d5f4))" parseIs
      ProtobufDataToCatalyst(id0)
    "from_protobuf(id#0, Duration, Some([B@19a052a1), (emit.default.values,true))" parseIs
      ProtobufDataToCatalyst(id0)

    "to_protobuf(id#0, Duration, Some([B@7a508aac))" parseIs
      CatalystDataToProtobuf(id0)
    "to_protobuf(id#0, Duration, Some([B@7a508aac), (emit.default.values,true))" parseIs
      CatalystDataToProtobuf(id0)
  }

  test("Random expressions") {
    "rand(-1907144135226424053)" parseIs Rand("-1907144135226424053")
    "random(4632372474091780078)" parseIs Rand("4632372474091780078")
    "randn(-702549475724243806)" parseIs Randn("-702549475724243806")
    "randstr(20, -7327388463949970848, true)" parseIs RandStr("20", "-7327388463949970848")
  }

  test("Regexp expressions") {
    "RLIKE(id#0, _)" parseIs RLike(id0, "_")
    "likeall(id#0, _)" parseIs LikeAll(id0, Seq("_"))
    "likeall(id#0, _, _*_)" parseIs LikeAll(id0, Seq("_", "_*_"))
    "notlikeall(id#0, _)" parseIs NotLikeAll(id0, Seq("_"))
    "notlikeall(id#0, _, _*_)" parseIs NotLikeAll(id0, Seq("_", "_*_"))
    "likeany(id#0, _)" parseIs LikeAny(id0, Seq("_"))
    "likeany(id#0, _, _*_)" parseIs LikeAny(id0, Seq("_", "_*_"))
    "notlikeany(id#0, _)" parseIs NotLikeAny(id0, Seq("_"))
    "notlikeany(id#0, _, _*_)" parseIs NotLikeAny(id0, Seq("_", "_*_"))
    "split(id#0, ,, -1)" parseIs StringSplit(id0, ",", "-1")
    "split(id#0, _, 2)" parseIs StringSplit(id0, "_", "2")

    Seq(
      "[0-9]",
      "[0-9]*",
      "(def)",
      "abc(def)",
      "([0-9])",
      "a.([0-9])"
    ).foreach { regex =>
      s"regexp_replace(id#0, $regex, x, 1)" parseIs RegExpReplace(id0, regex, "x", "1")
      s"regexp_replace(id#0, $regex, $$1, 3)" parseIs RegExpReplace(id0, regex, "$1", "3")
      s"regexp_extract(id#0, $regex, 1)" parseIs RegExpExtract(id0, regex, "1")
      s"regexp_extract_all(id#0, $regex, 3)" parseIs RegExpExtractAll(id0, regex, "3")
      s"regexp_instr(id#0, $regex, 0)" parseIs RegExpInStr(id0, regex, "0")
    }
  }

  test("Spatial expressions") {
    "static_invoke(STUtils.stAsBinary(id#0))" parseIs ST_AsBinary(id0)
    "static_invoke(STUtils.stGeogFromWKB(id#0))" parseIs ST_GeogFromWKB(id0)
    "static_invoke(STUtils.stGeomFromWKB(id#0))" parseIs ST_GeomFromWKB(id0)
    "static_invoke(STUtils.stSrid(id#0))" parseIs ST_Srid(id0)
    "static_invoke(STUtils.stSetSrid(id#0, 4326))" parseIs ST_SetSrid(id0, "4326")
  }

  test("String expressions") {
    "concat_ws( , id#0)" parseIs ConcatWs(IndexedSeq(" ", id0))
    "concat_ws(_, id#0, str)" parseIs ConcatWs(IndexedSeq("_", id0, "str"))
    "elt(1, id#0)" parseIs Elt(IndexedSeq("1", id0))
    "elt(1, id#0, false)" parseIs Elt(IndexedSeq("1", id0))
    "elt(2, id#0, abc)" parseIs Elt(IndexedSeq("2", id0, "abc"))
    "elt(2, id#0, abc, false)" parseIs Elt(IndexedSeq("2", id0, "abc"))
    "ucase(id#0)" parseIs Upper(id0)
    "upper(id#0)" parseIs Upper(id0)
    "lcase(id#0)" parseIs Lower(id0)
    "lower(id#0)" parseIs Lower(id0)
    "Contains(id#0, a)" parseIs
      Contains(
        AttributeReference("id", StringType, 0),
        Literal("a", StringType)
      )
    "StartsWith(id#0, a)" parseIs
      StartsWith(
        AttributeReference("id", StringType, 0),
        Literal("a", StringType)
      )
    "EndsWith(id#0, a)" parseIs
      EndsWith(
        AttributeReference("id", StringType, 0),
        Literal("a", StringType)
      )
    "replace(id#0, a, )" parseIs StringReplace(id0, "a", "")
    "replace(id#0, a, x)" parseIs StringReplace(id0, "a", "x")
    "overlay(id#0, a, 0, -1)" parseIs Overlay(id0, "a", "0", "-1")
    "translate(id#0, a, b)" parseIs StringTranslate(id0, "a", "b")
    "find_in_set(id#0, a)" parseIs FindInSet(id0, "a")
    "trim(id#0, None)" parseIs StringTrim(id0, None)
    "trim(id#0, Some(-))" parseIs StringTrim(id0, Some("-"))
    "ltrim(id#0, None)" parseIs StringTrimLeft(id0, None)
    "ltrim(id#0, Some(-))" parseIs StringTrimLeft(id0, Some("-"))
    "rtrim(id#0, None)" parseIs StringTrimRight(id0, None)
    "rtrim(id#0, Some(-))" parseIs StringTrimRight(id0, Some("-"))
    "instr(id#0, -)" parseIs StringInstr(id0, "-")
    "substring_index(id#0, ., 2)" parseIs SubstringIndex(id0, ".", "2")
    "locate(id#0, ., 1)" parseIs StringLocate(id0, ".", "1")
    "position(id#0, ., 5)" parseIs StringLocate(id0, ".", "5")
    "lpad(id#0, 10, _)" parseIs StringLPad(id0, "10", "_")
    "rpad(id#0, 10, _)" parseIs StringRPad(id0, "10", "_")
    "printf(Hello %d %s, id#0, days)" parseIs
      FormatString(IndexedSeq("Hello %d %s", id0, "days"))
    "format_string(Hello %d %s, id#0, days)" parseIs
      FormatString(IndexedSeq("Hello %d %s", id0, "days"))
    "initcap(id#0)" parseIs InitCap(id0)
    "repeat(id#0, 5)" parseIs StringRepeat(id0, "5")
    "space(id#0)" parseIs StringSpace(id0)
    "substr(id#0, 1, 2)" parseIs Substring(id0, "1", "2")
    "substring(id#0, 1, 2)" parseIs Substring(id0, "1", "2")
    "len(id#0)" parseIs Length(id0)
    "length(id#0)" parseIs Length(id0)
    "char_length(id#0)" parseIs Length(id0)
    "character_length(id#0)" parseIs Length(id0)
    "bit_length(id#0)" parseIs BitLength(id0)
    "octet_length(id#0)" parseIs OctetLength(id0)
    "levenshtein(id#0, , None)" parseIs Levenshtein(id0, "", None)
    "levenshtein(id#0, 1, Some(2))" parseIs Levenshtein(id0, "1", Some("2"))
    "soundex(id#0)" parseIs SoundEx(id0)
    "ascii(id#0)" parseIs Ascii(id0)
    "chr(id#0)" parseIs Chr(id0)
    "char(id#0)" parseIs Chr(id0)
    "base64(id#0)" parseIs Base64(id0)
    "unbase64(id#0)" parseIs UnBase64(id0)
    "unbase64(id#0, false)" parseIs UnBase64(id0)
    "format_number(id#0, ####.##)" parseIs FormatNumber(id0, "####.##")
    "stringsplitsql(id#0, .)" parseIs StringSplitSQL(id0, ".")

    "decode(id#0, utf-8)" parseIs StringDecode(id0, "utf-8")
    "static_invoke(StringDecode.decode(id#0, UTF-8, false, false))" parseIs
      StringDecode(id0, "UTF-8")

    "encode(id#0, utf-8)" parseIs Encode(id0, "utf-8")
    "static_invoke(Encode.encode(abc, utf-8, false, false))" parseIs
      Encode("abc", "utf-8")

    "sentences(id#0, , )" parseIs Sentences(id0, "", "")
    "static_invoke(ExpressionImplUtils.getSentences(id#0, , ))" parseIs Sentences(id0, "", "")

    "staticinvoke(" +
      "class org.apache.spark.unsafe.array.ByteArrayMethods, " +
      "BooleanType, " +
      "contains, " +
      "id#0, " +
      "0x0010, " +
      "BinaryType, " +
      "BinaryType, " +
      "true, " +
      "true, " +
      "true)" parseIs
      Contains(
        AttributeReference("id", BinaryType, 0),
        Literal("0x0010", BinaryType)
      )

    "staticinvoke(" +
      "class org.apache.spark.unsafe.array.ByteArrayMethods, " +
      "BooleanType, " +
      "startsWith, " +
      "id#0, " +
      "0x0010, " +
      "BinaryType, " +
      "BinaryType, " +
      "true, " +
      "true, " +
      "true)" parseIs
      StartsWith(
        AttributeReference("id", BinaryType, 0),
        Literal("0x0010", BinaryType)
      )

    "staticinvoke(" +
      "class org.apache.spark.unsafe.array.ByteArrayMethods, " +
      "BooleanType, " +
      "endsWith, " +
      "id#0, " +
      "0x0010, " +
      "BinaryType, " +
      "BinaryType, " +
      "true, " +
      "true, " +
      "true)" parseIs
      EndsWith(
        AttributeReference("id", BinaryType, 0),
        Literal("0x0010", BinaryType)
      )

    "staticinvoke(" +
      "class org.apache.spark.unsafe.types.ByteArray, " +
      "BinaryType, " +
      "lpad, " +
      "id#0, " +
      "10, " +
      "0x0010, " +
      "BinaryType, " +
      "IntegerType, " +
      "BinaryType, " +
      "true, " +
      "false, " +
      "true)" parseIs BinaryPad("lpad", id0, "10", "0x0010")
    "static_invoke(ByteArray.lpad(id#0, 10, 0x0010))" parseIs
      BinaryPad("lpad", id0, "10", "0x0010")

    "staticinvoke(" +
      "class org.apache.spark.unsafe.types.ByteArray, " +
      "BinaryType, " +
      "rpad, " +
      "id#0, " +
      "10, " +
      "0x0010, " +
      "BinaryType, " +
      "IntegerType, " +
      "BinaryType, " +
      "true, " +
      "false, " +
      "true)" parseIs BinaryPad("rpad", id0, "10", "0x0010")
    "static_invoke(ByteArray.rpad(id#0, 10, 0x0010))" parseIs
      BinaryPad("rpad", id0, "10", "0x0010")

    "staticinvoke(" +
      "class org.apache.spark.sql.catalyst.expressions.Base64, " +
      "StringType, " +
      "encode, " +
      "id#0, " +
      "true, " +
      "BinaryType, " +
      "BooleanType, " +
      "true, " +
      "false, " +
      "true)" parseIs Base64(id0)
    "static_invoke(Base64.encode(id#0, true))" parseIs
      Base64(id0)

    "staticinvoke(" +
      "class org.apache.spark.sql.catalyst.expressions.ExpressionImplUtils, " +
      "BooleanType, " +
      "isLuhnNumber, " +
      "id#0, " +
      "StringType, " +
      "true, " +
      "true, " +
      "true)" parseIs Luhncheck(id0)
    "static_invoke(ExpressionImplUtils.isLuhnNumber(id#0))" parseIs Luhncheck(id0)

    "invoke(id#0.isValid())" parseIs IsValidUTF8(id0)
    "invoke(id#0.makeValid())" parseIs MakeValidUTF8(id0)
    "static_invoke(ExpressionImplUtils.validateUTF8String(id#0))" parseIs ValidateUTF8(id0)
    "static_invoke(ExpressionImplUtils.tryValidateUTF8String(id#0))" parseIs TryValidateUTF8(id0)
    "static_invoke(ExpressionImplUtils.quote(id#0))" parseIs Quote(id0)
  }

  test("Thetasketches expressions") {
    "theta_sketch_estimate(id#0)" parseIs ThetaSketchEstimate(id0)
    "theta_union(id#0, agg#1, 12)" parseIs ThetaUnion(id0, AttributeReference("agg", 1), "12")
    "theta_difference(id#0, agg#1)" parseIs ThetaDifference(id0, AttributeReference("agg", 1))
    "theta_intersection(id#0, agg#1)" parseIs ThetaIntersection(id0, AttributeReference("agg", 1))
  }

  test("Time expressions") {
    "invoke(ToTimeParser(None).parse(id#0))" parseIs ToTime(id0, None)
    "invoke(ToTimeParser(Some(HH.mm.ss)).parse(id#0))" parseIs ToTime(id0, Some("HH.mm.ss"))
    "static_invoke(DateTimeUtils.makeTime(id#0, 22, 31.23))" parseIs MakeTime(id0, "22", "31.23")
    "static_invoke(DateTimeUtils.timeDiff(HOUR, id#0, 20:25:20.211509))" parseIs
      TimeDiff("HOUR", id0, "20:25:20.211509")
    "static_invoke(DateTimeUtils.timeTrunc(SECOND, id#0))" parseIs TimeTrunc("SECOND", id0)
  }

  test("Time window expressions") {
    "precisetimestampconversion(id#0, TimestampType, LongType)" parseIs
      PreciseTimestampConversion(id0, TimestampType, LongType)
  }

  test("TryEval") {
    "tryeval(id#0)" parseIs TryEval(id0)
  }

  test("Url expressions") {
    "staticinvoke(" +
      "class org.apache.spark.sql.catalyst.expressions.UrlCodec$, " +
      "StringType, " +
      "encode, " +
      "https://spark.apache.org, " +
      "UTF-8, " +
      "StringType, " +
      "StringType, " +
      "true, " +
      "true, " +
      "true)" parseIs UrlEncode("https://spark.apache.org")
    "static_invoke(UrlCodec.encode(https://spark.apache.org))" parseIs
      UrlEncode("https://spark.apache.org")

    "staticinvoke(" +
      "class org.apache.spark.sql.catalyst.expressions.UrlCodec$, " +
      "StringType, " +
      "decode, " +
      "https%3A%2F%2Fspark.apache.org, " +
      "UTF-8, " +
      "StringType, " +
      "StringType, " +
      "true, " +
      "true, " +
      "true)" parseIs UrlDecode("https%3A%2F%2Fspark.apache.org")
    "static_invoke(UrlCodec.decode(https%3A%2F%2Fspark.apache.org, false))" parseIs
      UrlDecode("https%3A%2F%2Fspark.apache.org")

    "parse_url(http://spark.apache.org/path?query=1, HOST, false)" parseIs
      ParseUrl(IndexedSeq("http://spark.apache.org/path?query=1", "HOST"))
    "parse_url(http://spark.apache.org/path?query=1, QUERY, query, false)" parseIs
      ParseUrl(IndexedSeq("http://spark.apache.org/path?query=1", "QUERY", "query"))
    "invoke(" +
      "ParseUrlEvaluator(http://spark.apache.org/path?query=1,HOST,null,true)" +
      ".evaluate(http://spark.apache.org/path?query=1, HOST))" parseIs
      ParseUrl(IndexedSeq("http://spark.apache.org/path?query=1", "HOST"))
  }

  test("Variant expressions") {
    "static_invoke(VariantExpressionEvalUtils.parseJson(id#0, false, true))" parseIs ParseJson(id0)
    "static_invoke(VariantExpressionEvalUtils.isVariantNull(id#0))" parseIs IsVariantNull(id0)
    "to_variant_object(id#0)" parseIs ToVariantObject(id0)
    "variant_get(id#0, $.a, IntegerType, true, Some(Europe/Lisbon))" parseIs
      VariantGet(id0, "$.a", IntegerType, failOnError = true)
    "try_variant_get(id#0, $.a, IntegerType, false, Some(Europe/Lisbon))" parseIs
      VariantGet(id0, "$.a", IntegerType, failOnError = false)
    "static_invoke(SchemaOfVariant.schemaOfVariant(id#0))" parseIs SchemaOfVariant(id0)
  }

  test("Window frame") {
    // Range.
    var windowFrame = catalyst.expressions.SpecifiedWindowFrame(
      catalyst.expressions.RangeFrame,
      catalyst.expressions.UnboundedPreceding,
      catalyst.expressions.CurrentRow
    )
    assert(
      expressionParser.parseWindowFrame(windowFrame.toString) ==
        SpecifiedWindowFrame(RangeFrame, UnboundedPreceding, CurrentRow)
    )

    // Row.
    windowFrame = catalyst.expressions.SpecifiedWindowFrame(
      catalyst.expressions.RowFrame,
      catalyst.expressions.CurrentRow,
      catalyst.expressions.UnboundedFollowing
    )
    assert(
      expressionParser.parseWindowFrame(windowFrame.toString) ==
        SpecifiedWindowFrame(RowFrame, CurrentRow, UnboundedFollowing)
    )

    // Literal bounds.
    windowFrame = catalyst.expressions.SpecifiedWindowFrame(
      catalyst.expressions.RowFrame,
      catalyst.expressions.Literal(1),
      catalyst.expressions.Literal(2)
    )
    assert(
      expressionParser.parseWindowFrame(windowFrame.toString) ==
        SpecifiedWindowFrame(RowFrame, "1", "2")
    )
  }

  test("Window spec") {
    val id0Reference =
      catalyst.expressions.AttributeReference("id", sql.types.IntegerType)(ExprId(0))
    val windowFrame = catalyst.expressions.SpecifiedWindowFrame(
      catalyst.expressions.RangeFrame,
      catalyst.expressions.UnboundedPreceding,
      catalyst.expressions.CurrentRow
    )
    val expectedWindowFrame = SpecifiedWindowFrame(RangeFrame, UnboundedPreceding, CurrentRow)

    // No partition and order.
    var windowSpec = catalyst.expressions.WindowSpecDefinition(
      Seq.empty,
      Seq.empty,
      windowFrame
    )
    assert(
      expressionParser.parseWindowSpec(windowSpec.toString) ==
        WindowSpecDefinition(IndexedSeq.empty, IndexedSeq.empty, expectedWindowFrame)
    )

    // With partition.
    windowSpec = windowSpec.copy(
      partitionSpec = Seq(
        id0Reference,
        catalyst.expressions.Literal(1)
      ),
      orderSpec = Seq.empty
    )
    assert(
      expressionParser.parseWindowSpec(windowSpec.toString) ==
        WindowSpecDefinition(IndexedSeq(id0, "1"), IndexedSeq.empty, expectedWindowFrame)
    )

    // With order.
    windowSpec = windowSpec.copy(
      partitionSpec = Seq.empty,
      orderSpec = Seq(
        catalyst.expressions.SortOrder(
          id0Reference,
          catalyst.expressions.Ascending,
          catalyst.expressions.NullsFirst,
          Seq.empty
        ),
        catalyst.expressions.SortOrder(
          id0Reference,
          catalyst.expressions.Descending,
          catalyst.expressions.NullsLast,
          Seq.empty
        )
      )
    )
    assert(
      expressionParser.parseWindowSpec(windowSpec.toString) ==
        WindowSpecDefinition(
          IndexedSeq.empty,
          IndexedSeq(
            SortOrder(id0, Ascending, NullsFirst),
            SortOrder(id0, Descending, NullsLast)
          ),
          expectedWindowFrame
        )
    )

    // With partition and order.
    windowSpec = windowSpec.copy(
      partitionSpec = Seq(
        id0Reference,
        catalyst.expressions.Literal(1)
      )
    )
    assert(
      expressionParser.parseWindowSpec(windowSpec.toString) ==
        WindowSpecDefinition(
          IndexedSeq(id0, "1"),
          IndexedSeq(
            SortOrder(id0, Ascending, NullsFirst),
            SortOrder(id0, Descending, NullsLast)
          ),
          expectedWindowFrame
        )
    )
  }

  test("Window expressions") {
    val windowStr = "avg(id#0) " +
      "windowspecdefinition(" +
      "id#0, " +
      "id#0 ASC NULLS FIRST, " +
      "specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$()))"
    assert(
      expressionParser.parseWindow(windowStr) ==
        WindowExpression(
          Average(id0, isTry = false),
          WindowSpecDefinition(
            IndexedSeq(id0),
            IndexedSeq(SortOrder(id0, Ascending, NullsFirst)),
            SpecifiedWindowFrame(RangeFrame, UnboundedPreceding, CurrentRow)
          )
        )
    )

    "rank(id#0, 1)" parseIs Rank(IndexedSeq(id0, "1"))
    "dense_rank(id#0, 1)" parseIs DenseRank(IndexedSeq(id0, "1"))
    "percent_rank(id#0, 1)" parseIs PercentRank(IndexedSeq(id0, "1"))
    "row_number()" parseIs RowNumber()
    "ntile(id#0)" parseIs NTile(id0)
    "nth_value(id#0, 5, false)" parseIs NthValue(id0, "5", ignoreNulls = false)
    "nth_value(id#0, 5, true)" parseIs NthValue(id0, "5", ignoreNulls = true)
    "lead(id#0, 5, null)" parseIs Lead(id0, "5", "null")
    "lag(id#0, 2, 27)" parseIs Lag(id0, "2", "27")
    "cume_dist()" parseIs CumeDist()
    "null_index(id#0)" parseIs NullIndex(id0)
    "ewm(id#0, 0.5, true)" parseIs EWM(id0, 0.5, ignoreNA = true)
    "ewm(id#0, 0.5, false)" parseIs EWM(id0, 0.5, ignoreNA = false)
    "last_non_null(id#0)" parseIs LastNonNull(id0)
  }

  test("XML expressions") {
    "from_xml(" +
      "StructField(a,IntegerType,true), " +
      "StructField(b,DoubleType,true), " +
      "<p><a>1</a><b>0.8</b></p>, " +
      "Some(Europe/Lisbon))" parseIs
      XmlToStructs(
        StructType(
          Array(
            StructField("a", IntegerType),
            StructField("b", DoubleType)
          )
        ),
        Map.empty,
        "<p><a>1</a><b>0.8</b></p>",
        Some("Europe/Lisbon")
      )
    "from_xml(" +
      "StructField(time,TimestampType,true), " +
      "(timestampFormat,dd/MM/yyyy), " +
      "<p><time>26/08/2015</time></p>, " +
      "Some(Europe/Lisbon))" parseIs
      XmlToStructs(
        StructType(
          Array(
            StructField("time", TimestampType)
          )
        ),
        Map("timestampFormat" -> "dd/MM/yyyy"),
        "<p><time>26/08/2015</time></p>",
        Some("Europe/Lisbon")
      )

    "static_invoke(" +
      "XmlExpressionEvalUtils.schemaOfXml(" +
      "org.apache.spark.sql.catalyst.xml.XmlInferSchema@16869348, " +
      "<p><time>26/08/2015</time></p>))" parseIs
      SchemaOfXml("<p><time>26/08/2015</time></p>")

    "to_xml(id#0, Some(Europe/Lisbon))" parseIs StructsToXml(id0)
    "to_xml((timestampFormat,dd/MM/yyyy), id#0, Some(Europe/Lisbon))" parseIs StructsToXml(id0)
  }

  test("XPath expressions") {
    "xpath_boolean(<a><b>1</b></a>, a/b)" parseIs
      XPathBoolean("<a><b>1</b></a>", "a/b")
    "xpath_boolean(<a><b>1</b></a>, id#0)" parseIs
      XPathBoolean("<a><b>1</b></a>", id0)
    "xpath_short(<a><b>1</b><b>2</b></a>, sum(a/b))" parseIs
      XPathShort("<a><b>1</b><b>2</b></a>", "sum(a/b)")
    "xpath_int(<a><b>1</b><b>2</b></a>, sum(a/b))" parseIs
      XPathInt("<a><b>1</b><b>2</b></a>", "sum(a/b)")
    "xpath_long(<a><b>1</b><b>2</b></a>, sum(a/b))" parseIs
      XPathLong("<a><b>1</b><b>2</b></a>", "sum(a/b)")
    "xpath_float(<a><b>1</b><b>2</b></a>, sum(a/b))" parseIs
      XPathFloat("<a><b>1</b><b>2</b></a>", "sum(a/b)")
    "xpath_double(<a><b>1</b><b>2</b></a>, sum(a/b))" parseIs
      XPathDouble("<a><b>1</b><b>2</b></a>", "sum(a/b)")
    "xpath_number(<a><b>1</b><b>2</b></a>, sum(a/b))" parseIs
      XPathDouble("<a><b>1</b><b>2</b></a>", "sum(a/b)")
    "xpath_string(<a><b>b</b><c>cc</c></a>, a/c)" parseIs
      XPathString("<a><b>b</b><c>cc</c></a>", "a/c")
    "xpath(<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>, a/b/text())" parseIs
      XPathList("<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>", "a/b/text()")

    // XPath functions.
    "xpath_boolean(id#0, true())" parseIs XPathBoolean(id0, "true()")
    "xpath_boolean(id#0, false())" parseIs XPathBoolean(id0, "false()")
    "xpath_boolean(id#0, not(a))" parseIs XPathBoolean(id0, "not(a)")
    "xpath_boolean(id#0, node())" parseIs XPathBoolean(id0, "node()")
    "xpath_boolean(id#0, text())" parseIs XPathBoolean(id0, "text()")
    "xpath_boolean(id#0, comment())" parseIs XPathBoolean(id0, "comment()")
    "xpath_boolean(id#0, processing-instruction())" parseIs
      XPathBoolean(id0, "processing-instruction()")
    "xpath_boolean(id#0, starts-with(a,1))" parseIs XPathBoolean(id0, "starts-with(a,1)")
    "xpath_boolean(id#0, contains(a,1))" parseIs XPathBoolean(id0, "contains(a,1)")
    "xpath_boolean(id#0, a + b)" parseIs XPathBoolean(id0, "a + b")
    "xpath_boolean(id#0, a - b)" parseIs XPathBoolean(id0, "a - b")
    "xpath_boolean(id#0, a * b)" parseIs XPathBoolean(id0, "a * b")
    "xpath_boolean(id#0, a >= 1)" parseIs XPathBoolean(id0, "a >= 1")
    "xpath_boolean(id#0, a or b)" parseIs XPathBoolean(id0, "a or b")
    "xpath_boolean(id#0, a and b)" parseIs XPathBoolean(id0, "a and b")
    "xpath_short(id#0, sum(a))" parseIs XPathShort(id0, "sum(a)")
    "xpath_short(id#0, count(a))" parseIs XPathShort(id0, "count(a)")
    "xpath_short(id#0, string-length(a))" parseIs XPathShort(id0, "string-length(a)")
    "xpath_short(id#0, position())" parseIs XPathShort(id0, "position()")
    "xpath_short(id#0, last())" parseIs XPathShort(id0, "last()")
    "xpath_short(id#0, a[1])" parseIs XPathShort(id0, "a[1]")
    "xpath_short(id#0, ceiling(a))" parseIs XPathShort(id0, "ceiling(a)")
    "xpath_short(id#0, floor(a))" parseIs XPathShort(id0, "floor(a)")
    "xpath_short(id#0, round(a))" parseIs XPathShort(id0, "round(a)")
    "xpath_short(id#0, a div b)" parseIs XPathShort(id0, "a div b")
    "xpath_short(id#0, a mod b)" parseIs XPathShort(id0, "a mod b")
    "xpath_string(id#0, string(a))" parseIs XPathString(id0, "string(a)")
    "xpath_string(id#0, substring(a,1,2))" parseIs XPathString(id0, "substring(a,1,2)")
    "xpath_string(id#0, substring-before(a,z))" parseIs XPathString(id0, "substring-before(a,z)")
    "xpath_string(id#0, substring-after(a,z))" parseIs XPathString(id0, "substring-after(a,z)")
    "xpath_string(id#0, normalize-space(a))" parseIs XPathString(id0, "normalize-space(a)")
    "xpath_string(id#0, translate(a,b,x))" parseIs XPathString(id0, "translate(a,b,x)")
    "xpath_string(id#0, id(a))" parseIs XPathString(id0, "id(a)")
    "xpath_string(id#0, concat(a/b,a/c))" parseIs
      XPathString(
        id0,
        AmbiguousExpression(
          IndexedSeq(
            "concat(a/b,a/c)",
            Concat(IndexedSeq("a/b,a/c"))
          )
        )
      )

    // Invoke style.
    "invoke(XPathBooleanEvaluator(sum(a/b)).evaluate(<a><b>1</b><b>2</b></a>))" parseIs
      XPathBoolean("<a><b>1</b><b>2</b></a>", "sum(a/b)")
    "invoke(XPathShortEvaluator(sum(a/b)).evaluate(<a><b>1</b><b>2</b></a>))" parseIs
      XPathShort("<a><b>1</b><b>2</b></a>", "sum(a/b)")
    "invoke(XPathIntEvaluator(sum(a/b)).evaluate(<a><b>1</b><b>2</b></a>))" parseIs
      XPathInt("<a><b>1</b><b>2</b></a>", "sum(a/b)")
    "invoke(XPathLongEvaluator(sum(a/b)).evaluate(<a><b>1</b><b>2</b></a>))" parseIs
      XPathLong("<a><b>1</b><b>2</b></a>", "sum(a/b)")
    "invoke(XPathFloatEvaluator(sum(a/b)).evaluate(<a><b>1</b><b>2</b></a>))" parseIs
      XPathFloat("<a><b>1</b><b>2</b></a>", "sum(a/b)")
    "invoke(XPathDoubleEvaluator(sum(a/b)).evaluate(<a><b>1</b><b>2</b></a>))" parseIs
      XPathDouble("<a><b>1</b><b>2</b></a>", "sum(a/b)")
    "invoke(XPathStringEvaluator(a/c).evaluate(<a><b>b</b><c>cc</c></a>))" parseIs
      XPathString("<a><b>b</b><c>cc</c></a>", "a/c")
    "invoke(" +
      "XPathListEvaluator(a/b/text())" +
      ".evaluate(<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>))" parseIs
      XPathList("<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>", "a/b/text()")
  }
}
