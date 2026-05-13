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

import com.xonai.spark.sql.parser.types.{AnyType, ArrayType, DataType, MapType, NullType, StringType, StructField, StructType, TypeSet}

/**
 * [[org.apache.spark.sql.catalyst.expressions.CreateArray]].
 */
case class CreateArray(children: IndexedSeq[Expression], dataType: DataType)
    extends Expression
    with PinnedDataType {

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = dataType.intersect(outputType)
    val newElementType = newDataType.getArrayElementType
    withNewDataType(newDataType).mapChildren(_.resolveDataType(newElementType))
  }

  override def withNewDataTypeInternal(newDataType: DataType): Expression = {
    copy(dataType = newDataType)
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }
}

object CreateArray {

  def apply(children: IndexedSeq[Expression]): CreateArray = {
    val elementType =
      if (children.nonEmpty) {
        children.map(_.dataType).reduce(DataType.intersection)
      } else {
        TypeSet(StringType, NullType)
      }

    new CreateArray(children, ArrayType(elementType))
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.CreateMap]].
 */
case class CreateMap(children: IndexedSeq[Expression], dataType: DataType)
    extends Expression
    with PinnedDataType {

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = dataType.intersect(outputType)
    val keyType = newDataType.getMapKeyType
    val valueType = newDataType.getMapValueType
    withNewDataType(newDataType).withNewChildren(
      children.indices.map { i =>
        if (i % 2 == 0) {
          children(i).resolveDataType(keyType)
        } else {
          children(i).resolveDataType(valueType)
        }
      }
    )
  }

  override def withNewDataTypeInternal(newDataType: DataType): Expression = {
    copy(dataType = newDataType)
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }
}

object CreateMap {

  def apply(children: IndexedSeq[Expression]): CreateMap = {
    val (keyType, valueType) =
      if (children.nonEmpty) {
        val keyTypes = children.indices.filter(_ % 2 == 0).map(children(_).dataType)
        val valueTypes = children.indices.filter(_ % 2 != 0).map(children(_).dataType)
        (
          keyTypes.reduce(DataType.intersection),
          valueTypes.reduce(DataType.intersection)
        )
      } else {
        val elementType = TypeSet(StringType, NullType)
        (elementType, elementType)
      }

    new CreateMap(children, MapType(keyType, valueType))
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.MapFromArrays]].
 */
case class MapFromArrays(left: Expression, right: Expression) extends BinaryExpression {

  override def dataType: DataType = {
    MapType(
      left.dataType.getArrayElementType,
      right.dataType.getArrayElementType
    )
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = dataType.intersect(outputType)
    withNewChildren(
      IndexedSeq(
        left.resolveDataType(ArrayType(newDataType.getMapKeyType)),
        right.resolveDataType(ArrayType(newDataType.getMapValueType))
      )
    )
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.CreateNamedStruct]].
 */
case class CreateNamedStruct(children: IndexedSeq[Expression]) extends Expression {

  override lazy val dataType: StructType = {
    val names = children.indices.filter(_ % 2 == 0).map(children)
    val values = children.indices.filter(_ % 2 == 1).map(children)
    val fields = names.zip(values).map { case (name: Literal, value) =>
      StructField(name.stringValue, value.dataType)
    }
    StructType(fields.toArray)
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val structFields = dataType.intersect(outputType).getStructFields(children.length / 2)
    withNewChildren(
      children.indices.map { i =>
        if (i % 2 == 0) {
          children(i).resolveDataType(StringType)
        } else {
          children(i).resolveDataType(structFields(i / 2).dataType)
        }
      }
    )
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.StringToMap]].
 */
case class StringToMap(text: Expression, pairDelim: Expression, keyValueDelim: Expression)
    extends TernaryExpression
    with ExpectsInputType {

  override def first: Expression = text

  override def second: Expression = pairDelim

  override def third: Expression = keyValueDelim

  override def dataType: DataType = MapType(StringType, StringType)

  override def inputType: DataType = StringType

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(text = newFirst, pairDelim = newSecond, keyValueDelim = newThird)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.GetStructField]].
 */
case class GetStructField(child: Expression, ordinal: Int, name: Option[String], numFields: Int)
    extends UnaryExpression {

  override lazy val dataType: DataType = {
    child.dataType.getStructFields(numFields).apply(ordinal).dataType
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = dataType.intersect(outputType)
    val childDataType = child.dataType
    val newStructType =
      if (dataType == newDataType) {
        childDataType
      } else {
        val fields = childDataType.getStructFields(numFields)
        val newFields = fields.indices.map { i =>
          if (i == ordinal) {
            fields(i).copy(dataType = newDataType)
          } else {
            fields(i)
          }
        }

        StructType(newFields.toArray)
      }

    mapChildren(_.resolveDataType(newStructType))
  }

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.GetArrayStructFields]].
 */
case class GetArrayStructFields(child: Expression, field: StructField, ordinal: Int, numFields: Int)
    extends UnaryExpression {

  override def dataType: DataType = ArrayType(field.dataType)

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = dataType.intersect(outputType)
    val arrayType = child.dataType
    val (newArrayType, updated) =
      if (dataType == newDataType) {
        (arrayType, this)
      } else {
        val fields = arrayType.getArrayElementType.getStructFields(numFields)
        val newFields = fields.indices.map { i =>
          if (i == ordinal) {
            fields(i).copy(dataType = newDataType.getArrayElementType)
          } else {
            fields(i)
          }
        }
        val newStructType = StructType(newFields.toArray)

        (
          ArrayType(newStructType),
          copy(field = newStructType.fields(ordinal))
        )
      }

    updated.mapChildren(_.resolveDataType(newArrayType))
  }

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.GetArrayItem]].
 */
case class GetArrayItem(child: Expression, ordinal: Expression) extends BinaryExpression {

  override def left: Expression = child

  override def right: Expression = ordinal

  override lazy val dataType: DataType = child.dataType.getArrayElementType

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = outputType.intersect(dataType)
    withNewChildren(
      IndexedSeq(
        child.resolveDataType(ArrayType(newDataType)),
        ordinal.resolveDataType(TypeSet.Integral)
      )
    )
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(child = newLeft, ordinal = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.GetMapValue]].
 */
case class GetMapValue(child: Expression, key: Expression) extends BinaryExpression {

  override def left: Expression = child

  override def right: Expression = key

  override def dataType: DataType = {
    child.dataType.getMapValueType
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val newChildType = child.dataType.intersect(MapType(AnyType, AnyType))
    val keyType = newChildType.getMapKeyType.intersect(key.dataType)
    val valueType = newChildType.getMapValueType.intersect(outputType)

    withNewChildren(
      IndexedSeq(
        child.resolveDataType(MapType(keyType, valueType)),
        key.resolveDataType(keyType)
      )
    )
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(child = newLeft, key = newRight)
  }
}
