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

package com.xonai.spark.sql.parser.expressions

import com.xonai.spark.sql.parser.trees.{BinaryLike, QuaternaryLike, TernaryLike}
import com.xonai.spark.sql.parser.types.{AnyType, ArrayType, BooleanType, DataType, IntegerType, MapType}

/**
 * [[org.apache.spark.sql.catalyst.expressions.NamedLambdaVariable]].
 */
case class NamedLambdaVariable(name: String, dataType: DataType, exprId: Long)
    extends LeafExpression
    with NamedExpression
    with PinnedDataType {

  override def toAttribute: Attribute = {
    AttributeReference(name, dataType, exprId)
  }

  override def withNewDataTypeInternal(newDataType: DataType): Expression = {
    copy(dataType = newDataType)
  }
}

object NamedLambdaVariable {

  def apply(name: String, exprId: Long): NamedLambdaVariable = {
    NamedLambdaVariable(name, AnyType, exprId)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.LambdaFunction]].
 */
case class LambdaFunction(function: Expression, arguments: IndexedSeq[NamedExpression])
    extends Expression {

  override def dataType: DataType = function.dataType

  override def children: IndexedSeq[Expression] = function +: arguments

  override def resolveDataType(outputType: DataType): Expression = {
    resolveDataType(outputType, arguments.map(_.dataType))
  }

  def resolveDataType(outputType: DataType, argumentInputTypes: Seq[DataType]): Expression = {
    val resolvedArguments = arguments
      .zip(argumentInputTypes)
      .map {
        case (expression, inputType) =>
          expression.resolveDataType(inputType).asInstanceOf[NamedExpression]
      }
    val lambdaTypesInArguments = resolvedArguments
      .map(a => a.exprId -> a.dataType)
      .toMap

    val resolvedFunction = function
      .transform { case lambda: NamedLambdaVariable =>
        lambda.resolveDataType(lambdaTypesInArguments.getOrElse(lambda.exprId, AnyType))
      }
      .resolveDataType(outputType)
    val lambdaTypesInFunction = resolvedFunction
      .collect {
        case lambda: NamedLambdaVariable =>
          lambda.exprId -> lambda.dataType
      }
      .toMap

    withNewChildren(
      resolvedFunction +: resolvedArguments.map { expression =>
        val dataType = lambdaTypesInFunction.getOrElse(expression.exprId, AnyType)
        expression.resolveDataType(dataType)
      }
    )
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(
      function = newChildren.head,
      arguments = newChildren.tail.asInstanceOf[IndexedSeq[NamedExpression]]
    )
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.HigherOrderFunction]].
 */
trait HigherOrderFunction extends Expression {

  def resolveFunction(
      function: Expression,
      outputType: DataType,
      argumentTypes: Seq[DataType]
  ): Expression = {
    function match {
      case function: LambdaFunction =>
        function.resolveDataType(outputType, argumentTypes)
      case _ =>
        function.resolveDataType(outputType)
    }
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.SimpleHigherOrderFunction]].
 */
trait SimpleHigherOrderFunction extends HigherOrderFunction with BinaryLike[Expression] {

  def argument: Expression

  def function: Expression

  override def left: Expression = argument

  override def right: Expression = function
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ArrayTransform]].
 */
case class ArrayTransform(argument: Expression, function: Expression)
    extends SimpleHigherOrderFunction {

  override def dataType: DataType = ArrayType(function.dataType)

  override def resolveDataType(outputType: DataType): Expression = {
    val functionType = outputType.intersect(dataType).getArrayElementType
    val elementType = argument.dataType.getArrayElementType
    val resolvedFunction = resolveFunction(function, functionType, Seq(elementType, IntegerType))
    val resolvedElementType = resolvedFunction.children(1).dataType

    withNewChildren(
      IndexedSeq(
        argument.resolveDataType(ArrayType(resolvedElementType)),
        resolvedFunction
      )
    )
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(argument = newLeft, function = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ArraySort]].
 */
case class ArraySort(argument: Expression, function: Expression) extends SimpleHigherOrderFunction {

  override def dataType: DataType = argument.dataType

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = outputType.intersect(dataType).intersect(ArrayType(AnyType))
    val elementType = newDataType.getArrayElementType
    val resolvedFunction = resolveFunction(function, IntegerType, Seq(elementType, elementType))
    val resolvedElementType = resolvedFunction.children.tail.map(_.dataType).reduce(_ intersect _)

    withNewChildren(
      IndexedSeq(
        argument.resolveDataType(ArrayType(resolvedElementType)),
        resolveFunction(
          resolvedFunction,
          IntegerType,
          Seq(resolvedElementType, resolvedElementType)
        )
      )
    )
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(argument = newLeft, function = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.MapFilter]].
 */
case class MapFilter(argument: Expression, function: Expression) extends SimpleHigherOrderFunction {

  override def dataType: DataType = argument.dataType

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = outputType.intersect(dataType).intersect(MapType(AnyType, AnyType))
    val keyType = newDataType.getMapKeyType
    val valueType = newDataType.getMapValueType
    val resolvedFunction = resolveFunction(function, BooleanType, Seq(keyType, valueType))
    val resolvedKeyType = resolvedFunction.children(1).dataType
    val resolvedValueType = resolvedFunction.children(2).dataType

    withNewChildren(
      IndexedSeq(
        argument.resolveDataType(MapType(resolvedKeyType, resolvedValueType)),
        resolvedFunction
      )
    )
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(argument = newLeft, function = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ArrayFilter]].
 */
case class ArrayFilter(argument: Expression, function: Expression)
    extends SimpleHigherOrderFunction {

  override def dataType: DataType = argument.dataType

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = outputType.intersect(dataType).intersect(ArrayType(AnyType))
    val elementType = newDataType.getArrayElementType
    val resolvedFunction = resolveFunction(function, BooleanType, Seq(elementType, IntegerType))
    val resolvedElementType = resolvedFunction.children(1).dataType

    withNewChildren(
      IndexedSeq(
        argument.resolveDataType(ArrayType(resolvedElementType)),
        resolvedFunction
      )
    )
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(argument = newLeft, function = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ArrayExists]].
 */
case class ArrayExists(argument: Expression, function: Expression)
    extends SimpleHigherOrderFunction
    with Predicate {

  override def resolveDataType(outputType: DataType): Expression = {
    val elementType = argument.dataType.getArrayElementType
    val resolvedFunction = resolveFunction(function, BooleanType, Seq(elementType))
    val resolvedElementType = resolvedFunction.children(1).dataType

    withNewChildren(
      IndexedSeq(
        argument.resolveDataType(ArrayType(resolvedElementType)),
        resolvedFunction
      )
    )
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(argument = newLeft, function = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ArrayForAll]].
 */
case class ArrayForAll(argument: Expression, function: Expression)
    extends SimpleHigherOrderFunction
    with Predicate {

  override def resolveDataType(outputType: DataType): Expression = {
    val elementType = argument.dataType.getArrayElementType
    val resolvedFunction = resolveFunction(function, BooleanType, Seq(elementType))
    val resolvedElementType = resolvedFunction.children(1).dataType

    withNewChildren(
      IndexedSeq(
        argument.resolveDataType(ArrayType(resolvedElementType)),
        resolvedFunction
      )
    )
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(argument = newLeft, function = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ArrayAggregate]].
 */
case class ArrayAggregate(
    argument: Expression,
    zero: Expression,
    merge: Expression,
    finish: Expression
) extends HigherOrderFunction
    with QuaternaryLike[Expression] {

  override def first: Expression = argument

  override def second: Expression = zero

  override def third: Expression = merge

  override def fourth: Expression = finish

  override def dataType: DataType = finish.dataType

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = outputType.intersect(dataType)
    val accumulatorType = zero.dataType.intersect(merge.dataType)
    val arrayType = argument.dataType.intersect(ArrayType(AnyType))
    val resolvedMerge =
      resolveFunction(
        merge,
        accumulatorType,
        Seq(accumulatorType, arrayType.getArrayElementType)
      )
    val resolvedAccumulatorType = resolvedMerge.dataType

    withNewChildren(
      IndexedSeq(
        argument.resolveDataType(arrayType),
        zero.resolveDataType(resolvedAccumulatorType),
        resolveFunction(
          resolvedMerge,
          resolvedAccumulatorType,
          Seq(resolvedAccumulatorType, resolvedMerge.children(2).dataType)
        ),
        resolveFunction(finish, newDataType, Seq(resolvedAccumulatorType))
      )
    )
  }

  override def withNewChildrenInternal(
      first: Expression,
      second: Expression,
      third: Expression,
      fourth: Expression
  ): Expression = {
    copy(argument = first, zero = second, merge = third, finish = fourth)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.TransformKeys]].
 */
case class TransformKeys(argument: Expression, function: Expression)
    extends SimpleHigherOrderFunction {

  override def dataType: DataType = {
    MapType(function.dataType, argument.dataType.getMapValueType)
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = outputType.intersect(dataType)
    val resolvedFunction =
      resolveFunction(
        function,
        newDataType.getMapKeyType,
        Seq(
          argument.dataType.getMapKeyType,
          argument.dataType.getMapValueType.intersect(newDataType.getMapValueType)
        )
      )
    val resolvedKeyType = resolvedFunction.children(1).dataType
    val resolvedValueType = resolvedFunction.children(2).dataType

    withNewChildren(
      IndexedSeq(
        argument.resolveDataType(MapType(resolvedKeyType, resolvedValueType)),
        resolvedFunction
      )
    )
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(argument = newLeft, function = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.TransformValues]].
 */
case class TransformValues(argument: Expression, function: Expression)
    extends SimpleHigherOrderFunction {

  override def dataType: DataType = {
    MapType(argument.dataType.getMapKeyType, function.dataType)
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = outputType.intersect(dataType)
    val resolvedFunction =
      resolveFunction(
        function,
        newDataType.getMapValueType,
        Seq(
          argument.dataType.getMapKeyType.intersect(newDataType.getMapKeyType),
          argument.dataType.getMapValueType
        )
      )
    val resolvedKeyType = resolvedFunction.children(1).dataType
    val resolvedValueType = resolvedFunction.children(2).dataType

    withNewChildren(
      IndexedSeq(
        argument.resolveDataType(MapType(resolvedKeyType, resolvedValueType)),
        resolvedFunction
      )
    )
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(argument = newLeft, function = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.MapZipWith]].
 */
case class MapZipWith(left: Expression, right: Expression, function: Expression)
    extends HigherOrderFunction
    with TernaryLike[Expression] {

  override def first: Expression = left

  override def second: Expression = right

  override def third: Expression = function

  override def dataType: DataType = {
    MapType(left.dataType.getMapKeyType, function.dataType)
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = outputType.intersect(dataType)
    val resolvedFunction =
      resolveFunction(
        function,
        newDataType.getMapValueType,
        Seq(
          newDataType.getMapKeyType,
          left.dataType.getMapValueType,
          right.dataType.getMapValueType
        )
      )
    val resolvedKeyType = resolvedFunction.children(1).dataType
    val resolvedValue1Type = resolvedFunction.children(2).dataType
    val resolvedValue2Type = resolvedFunction.children(3).dataType

    withNewChildren(
      IndexedSeq(
        left.resolveDataType(MapType(resolvedKeyType, resolvedValue1Type)),
        right.resolveDataType(MapType(resolvedKeyType, resolvedValue2Type)),
        resolvedFunction
      )
    )
  }

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(left = newFirst, right = newSecond, function = newThird)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ZipWith]].
 */
case class ZipWith(left: Expression, right: Expression, function: Expression)
    extends HigherOrderFunction
    with TernaryLike[Expression] {

  override def first: Expression = left

  override def second: Expression = right

  override def third: Expression = function

  override def dataType: DataType = ArrayType(function.dataType)

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = outputType.intersect(dataType)
    val resolvedFunction =
      resolveFunction(
        function,
        newDataType.getArrayElementType,
        Seq(
          left.dataType.getArrayElementType,
          right.dataType.getArrayElementType
        )
      )

    withNewChildren(
      IndexedSeq(
        left.resolveDataType(ArrayType(resolvedFunction.children(1).dataType)),
        right.resolveDataType(ArrayType(resolvedFunction.children(2).dataType)),
        resolvedFunction
      )
    )
  }

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(left = newFirst, right = newSecond, function = newThird)
  }
}
