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

import com.xonai.spark.sql.parser.expressions.{AggregateExpression, AmbiguousExpression, CaseWhen, ConcatWs, Elt, Expression, In}
import com.xonai.spark.sql.parser.plans.SparkPlan
import com.xonai.spark.sql.parser.trees.{TreeNode, UnknownNode}
import com.xonai.spark.sql.parser.types.TypeSet.NumericAndAnsiInterval
import com.xonai.spark.sql.parser.types.{AnyType, ArrayType, BinaryType, BooleanType, ByteType, CalendarIntervalType, DataType, DateType, DayTimeIntervalType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, NullType, ShortType, StringType, StructType, TimestampNTZType, TimestampType, TypeSet, YearMonthIntervalType}
import com.xonai.utils.Utils

/**
 * Represents a scenario of data types in an [[Expression]] or [[SparkPlan]] which are supported by
 * a Spark engine.
 */
case class NodeDataTypeSupport(inputs: Seq[DataType], output: DataType)

/**
 * Given a mapping between node names and their supported data types, it provides [[NodeSupport]]
 * information of [[Expression]]s and [[SparkPlan]]s.
 */
class NodeTypeChecker(supported: Map[String, Seq[NodeDataTypeSupport]]) {

  def checkPlan(plan: SparkPlan, variation: Option[String] = None): NodeSupport = {
    checkNode(plan, variation, validatePlan(plan, _))
  }

  def checkExpression(expression: Expression, variation: Option[String] = None): NodeSupport = {
    checkNode(expression, variation, validateExpression(expression, _))
  }

  private def checkNode(
      node: TreeNode[_],
      variation: Option[String],
      nodeCheck: NodeDataTypeSupport => NodeSupport
  ): NodeSupport = {
    val nodeName = node.nodeName + variation.map(":" + _).getOrElse("")
    if (!supported.contains(nodeName)) {
      val support =
        node match {
          case _: UnknownNode =>
            NodeSupport.Unknown
          case _: AmbiguousExpression =>
            NodeSupport.Supported
          case _ =>
            NodeSupport.NotImplemented
        }
      return support
    }

    var result: NodeSupport = null
    supported(nodeName).foreach { dataTypeSupport =>
      result = nodeCheck(dataTypeSupport)
      if (result.isSupported) {
        return result
      }
    }
    result
  }

  private def validatePlan(plan: SparkPlan, supported: NodeDataTypeSupport): NodeSupport = {
    val outputSupport = plan.output.foldLeft(NodeSupport.Supported) { case (acc, attribute) =>
      acc + check(attribute.dataType, supported.output)
    }

    plan.children.zip(supported.inputs).foldLeft(outputSupport) {
      case (acc, (child, supportedInput)) =>
        child.output.foldLeft(acc) { case (acc, attribute) =>
          acc + check(attribute.dataType, supportedInput)
        }
    }
  }

  private def validateExpression(
      expression: Expression,
      supported: NodeDataTypeSupport
  ): NodeSupport = {
    // Propagate data type support for expressions with variable number of inputs.
    val inputs = supported.inputs
    val supportedInputs =
      expression match {
        case caseWhen: CaseWhen =>
          require(inputs.size == 3)
          caseWhen.branches.flatMap { _ => Seq(inputs.head, inputs(1)) } ++
            caseWhen.elseValue.map(_ => inputs(2))
        case _: AggregateExpression | _: In | _: ConcatWs | _: Elt =>
          require(inputs.size == 2)
          inputs.head +: expression.children.tail.map(_ => inputs(1))
        case _ =>
          inputs
      }

    val outputSupport = check(expression.dataType, supported.output)
    expression.children.zip(supportedInputs).foldLeft(outputSupport) {
      case (acc, (child, supportedInput)) =>
        acc + check(child.dataType, supportedInput)
    }
  }

  private def check(actual: DataType, expected: DataType): NodeSupport = {
    actual match {
      case AnyType =>
        NodeSupport.UndefinedDataType
      case _: ArrayType if expected == ArrayType =>
        NodeSupport.Supported
      case _: StructType if expected == StructType =>
        NodeSupport.Supported
      case _: MapType if expected == MapType =>
        NodeSupport.Supported
      case _ =>
        if (actual.intersect(expected) == actual) {
          NodeSupport.Supported
        } else {
          NodeSupport.UnsupportedDataType
        }
    }
  }
}

object NodeTypeChecker {

  private val PrimitiveTypeSet =
    TypeSet(NumericAndAnsiInterval.types + BooleanType + DateType + TimestampType)

  private val AtomicTypeSet =
    TypeSet(PrimitiveTypeSet.types + StringType + BinaryType)

  /**
   * Creates a [[NodeTypeChecker]] with a data type support map loaded from a resource file.
   */
  def fromResource(resourcePath: String): NodeTypeChecker = {
    val pairs =
      Utils.readResourceLines(resourcePath).map { line =>
        val parts = line.split(',')
        val nodeName = parts.head
        val nodeSupport = parts.tail.map(parseNodeDataTypeSupport).toSeq
        (nodeName, nodeSupport)
      }

    new NodeTypeChecker(pairs.toMap)
  }

  /**
   * Parses a data type scenario with types separated by the character ';'. The last data type is
   * the output type, the remaining are the input types.
   */
  private def parseNodeDataTypeSupport(str: String): NodeDataTypeSupport = {
    val dataTypes = str.split(';').map(parseDataType)
    val inputs = dataTypes.dropRight(1)
    val output = dataTypes.last

    NodeDataTypeSupport(inputs.toSeq, output)
  }

  /**
   * Parses a single data type or a [[TypeSet]] separated by the character ':'.
   */
  private def parseDataType(str: String): DataType = {
    val types = str.split(':')
    if (types.length > 1) {
      return DataType(types.map(parseDataType).toSet)
    }

    str match {
      case "" => TypeSet.Empty
      case "NULL" => NullType
      case "BOOLEAN" => BooleanType
      case "BYTE" => ByteType
      case "SHORT" => ShortType
      case "INTEGER" => IntegerType
      case "LONG" => LongType
      case "FLOAT" => FloatType
      case "DOUBLE" => DoubleType
      case "DECIMAL" => DecimalType
      case "DATE" => DateType
      case "TIMESTAMP" => TimestampType
      case "TIMESTAMP_NTZ" => TimestampNTZType
      case "STRING" => StringType
      case "BINARY" => BinaryType
      case "CALENDAR" => CalendarIntervalType
      case "YEAR_MONTH" => YearMonthIntervalType
      case "DAY_TIME" => DayTimeIntervalType
      case "ARRAY" => ArrayType
      case "STRUCT" => StructType
      case "MAP" => MapType
      case "ARRAY_PRIMITIVE" =>
        ArrayType(PrimitiveTypeSet)
      case "ARRAY_ATOMIC" => ArrayType(AtomicTypeSet)
      case "UNDEFINED" => TypeSet.Empty
    }
  }
}
