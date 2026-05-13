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

import com.xonai.spark.sql.parser.trees.TernaryLike
import com.xonai.spark.sql.parser.types.{AnyType, ArrayType, BinaryType, DataType, MapType, ObjectType, StructType}

/**
 * [[org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke]].
 */
case class StaticInvoke(
    staticObject: String,
    dataType: DataType,
    functionName: String,
    arguments: IndexedSeq[Expression]
) extends Expression with PinnedDataType {

  override def children: IndexedSeq[Expression] = arguments

  override def withNewDataTypeInternal(newDataType: DataType): Expression = {
    copy(dataType = newDataType)
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(arguments = newChildren)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.objects.Invoke]].
 */
case class Invoke(
    targetObject: Expression,
    functionName: String,
    dataType: DataType,
    arguments: IndexedSeq[Expression]
) extends Expression with PinnedDataType {

  override def children: IndexedSeq[Expression] = targetObject +: arguments

  override def withNewDataTypeInternal(newDataType: DataType): Expression = {
    copy(dataType = newDataType)
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(targetObject = newChildren.head, arguments = newChildren.tail)
  }
}

object Invoke {

  def apply(targetObject: Expression, functionName: String): Invoke = {
    Invoke(targetObject, functionName, AnyType, IndexedSeq.empty)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.objects.NewInstance]].
 */
case class NewInstance(className: String, arguments: IndexedSeq[Expression], dataType: DataType)
    extends Expression
    with PinnedDataType {

  override def children: IndexedSeq[Expression] = arguments

  override def withNewDataTypeInternal(newDataType: DataType): Expression = {
    copy(dataType = newDataType)
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(arguments = newChildren)
  }
}

object NewInstance {

  def apply(className: String): NewInstance = {
    new NewInstance(className, IndexedSeq.empty, AnyType)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.objects.UnwrapOption]].
 */
case class UnwrapOption(dataType: DataType, child: Expression)
    extends UnaryExpression
    with PinnedDataType {

  override def withNewDataTypeInternal(newDataType: DataType): Expression = {
    copy(dataType = newDataType)
  }

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.objects.WrapOption]].
 */
case class WrapOption(child: Expression, optType: DataType)
    extends UnaryExpression
    with ExpectsInputType {

  override def dataType: DataType = ObjectType(classOf[Option[_]].getName)

  override def inputType: DataType = optType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.objects.LambdaVariable]].
 */
case class LambdaVariable(name: String, dataType: DataType)
    extends LeafExpression
    with PinnedDataType {

  override def withNewDataTypeInternal(newDataType: DataType): Expression = {
    copy(dataType = newDataType)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.objects.MapObjects]].
 */
case class MapObjects(
    loopVar: Expression,
    lambdaFunction: Expression,
    inputData: Expression,
    customCollectionCls: Option[String]
) extends Expression
    with TernaryLike[Expression] {

  override def first: Expression = loopVar

  override def second: Expression = lambdaFunction

  override def third: Expression = inputData

  override def dataType: DataType = {
    customCollectionCls
      .map(ObjectType)
      .getOrElse(ArrayType(lambdaFunction.dataType))
  }

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(loopVar = newFirst, lambdaFunction = newSecond, inputData = newThird)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.objects.CatalystToExternalMap]].
 */
case class CatalystToExternalMap(
    keyLoopVar: Expression,
    keyLambdaFunction: Expression,
    valueLoopVar: Expression,
    valueLambdaFunction: Expression,
    inputData: Expression,
    collClass: String
) extends Expression {

  override def dataType: DataType = ObjectType(collClass)

  override def children: IndexedSeq[Expression] = {
    IndexedSeq(keyLoopVar, keyLambdaFunction, valueLoopVar, valueLambdaFunction, inputData)
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val mapType = MapType(keyLoopVar.dataType, valueLoopVar.dataType)
    withNewChildren(
      IndexedSeq(
        keyLoopVar.resolveDataType(AnyType),
        keyLambdaFunction.resolveDataType(AnyType),
        valueLoopVar.resolveDataType(AnyType),
        valueLambdaFunction.resolveDataType(AnyType),
        inputData.resolveDataType(mapType)
      )
    )
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(
      keyLoopVar = newChildren.head,
      keyLambdaFunction = newChildren(1),
      valueLoopVar = newChildren(2),
      valueLambdaFunction = newChildren(3),
      inputData = newChildren(4)
    )
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.objects.ExternalMapToCatalyst]].
 */
case class ExternalMapToCatalyst(
    keyLoopVar: Expression,
    keyConverter: Expression,
    valueLoopVar: Expression,
    valueConverter: Expression,
    inputData: Expression
) extends Expression {

  override def dataType: DataType = {
    MapType(keyConverter.dataType, valueConverter.dataType)
  }

  override def children: IndexedSeq[Expression] = {
    IndexedSeq(keyLoopVar, keyConverter, valueLoopVar, valueConverter, inputData)
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(
      keyLoopVar = newChildren.head,
      keyConverter = newChildren(1),
      valueLoopVar = newChildren(2),
      valueConverter = newChildren(3),
      inputData = newChildren(4)
    )
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.objects.CreateExternalRow]].
 */
case class CreateExternalRow(children: IndexedSeq[Expression], schema: StructType)
    extends Expression
    with ExpectsInputTypes {

  override def dataType: DataType = {
    ObjectType(classOf[org.apache.spark.sql.Row].getName)
  }

  override def inputTypes: Seq[DataType] = {
    schema.fields.map(_.dataType).toSeq
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.objects.EncodeUsingSerializer]].
 */
case class EncodeUsingSerializer(child: Expression) extends UnaryExpression {

  override def dataType: DataType = BinaryType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.objects.DecodeUsingSerializer]].
 */
case class DecodeUsingSerializer(child: Expression, tag: String) extends UnaryExpression {

  override def dataType: DataType = ObjectType(tag)

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.objects.InitializeJavaBean]].
 */
case class InitializeJavaBean(beanInstance: Expression, setters: Map[String, Expression])
    extends Expression {

  override def dataType: DataType = beanInstance.dataType

  override def children: IndexedSeq[Expression] = {
    beanInstance +: setters.toIndexedSeq.map(_._2)
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(
      beanInstance = newChildren.head,
      setters = setters
        .toIndexedSeq
        .zip(newChildren.tail)
        .map {
          case ((property, _), newChild) =>
            (property, newChild)
        }
        .toMap
    )
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull]].
 */
case class AssertNotNull(child: Expression) extends UnaryExpression with PropagatesDataType {

  override def dataType: DataType = child.dataType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.objects.GetExternalRowField]].
 */
case class GetExternalRowField(child: Expression, ordinal: Int, fieldName: String)
    extends UnaryExpression {

  override def dataType: DataType = ObjectType(classOf[Object].getName)

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.objects.ValidateExternalType]].
 */
case class ValidateExternalType(child: Expression, expected: DataType, externalDataType: DataType)
    extends UnaryExpression {

  override val dataType: DataType = externalDataType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}
