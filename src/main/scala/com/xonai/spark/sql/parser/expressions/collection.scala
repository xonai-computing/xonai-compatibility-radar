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

import com.xonai.spark.sql.parser.types.{AnyType, ArrayType, BinaryType, BooleanType, ByteType, CalendarIntervalType, DataType, DateType, DayTimeIntervalType, IntegerType, LongType, MapType, ShortType, StringType, StructField, StructType, TimestampNTZType, TimestampType, TypeSet, YearMonthIntervalType}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Size]].
 */
case class Size(child: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = IntegerType

  override def inputType: DataType = {
    TypeSet(ArrayType(AnyType), MapType(AnyType, AnyType))
  }

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.MapKeys]].
 */
case class MapKeys(child: Expression) extends UnaryExpression {

  override def dataType: DataType = {
    ArrayType(child.dataType.getMapKeyType)
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = dataType.intersect(outputType)
    val newMapType = MapType(newDataType.getArrayElementType, AnyType)
    mapChildren(_.resolveDataType(newMapType))
  }

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.MapValues]].
 */
case class MapValues(child: Expression) extends UnaryExpression {

  override def dataType: DataType = {
    ArrayType(child.dataType.getMapValueType)
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = dataType.intersect(outputType)
    val newMapType = MapType(AnyType, newDataType.getArrayElementType)
    mapChildren(_.resolveDataType(newMapType))
  }

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.MapEntries]].
 */
case class MapEntries(child: Expression) extends UnaryExpression {

  override lazy val dataType: DataType = {
    ArrayType(
      StructType(
        Array(
          StructField("key", child.dataType.getMapKeyType),
          StructField("value", child.dataType.getMapValueType)
        )
      )
    )
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = dataType.intersect(outputType)
    val structFields = newDataType.getArrayElementType.getStructFields(2)
    val newMapType = MapType(
      structFields.head.dataType,
      structFields(1).dataType
    )
    mapChildren(_.resolveDataType(newMapType))
  }

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.MapConcat]].
 */
case class MapConcat(children: IndexedSeq[Expression]) extends ComplexTypeMergingExpression {

  override lazy val dataType: DataType = {
    if (children.isEmpty) {
      MapType(StringType, StringType)
    } else {
      super.dataType
    }
  }

  override def inputType: DataType = {
    if (children.isEmpty) {
      dataType
    } else {
      MapType(AnyType, AnyType)
    }
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.MapFromEntries]].
 */
case class MapFromEntries(child: Expression) extends UnaryExpression {

  override def dataType: DataType = {
    val (keyType, valueType) =
      child
        .dataType
        .getArrayElementType
        .collectFirst { case structType: StructType =>
          (
            structType.fields.head.dataType,
            structType.fields(1).dataType
          )
        }
        .getOrElse((AnyType, AnyType))

    MapType(keyType, valueType)
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = dataType.intersect(outputType)
    val newChildType =
      ArrayType(
        StructType(
          Array(
            StructField("key", newDataType.getMapKeyType),
            StructField("value", newDataType.getMapValueType)
          )
        )
      )
    mapChildren(_.resolveDataType(newChildType))
  }

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.SortArray]].
 */
case class SortArray(base: Expression, ascendingOrder: Expression) extends BinaryExpression {

  override def left: Expression = base

  override def right: Expression = ascendingOrder

  override def dataType: DataType = base.dataType

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = dataType.intersect(outputType).intersect(ArrayType(AnyType))
    withNewChildren(
      IndexedSeq(
        base.resolveDataType(newDataType),
        ascendingOrder.resolveDataType(BooleanType)
      )
    )
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(base = newLeft, ascendingOrder = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Shuffle]].
 */
case class Shuffle(child: Expression, randomSeed: Option[Long] = None)
    extends UnaryExpression
    with DataTypeIsInputType {

  override def dataType: DataType = child.dataType

  override def inputType: DataType = ArrayType(AnyType)

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Reverse]].
 */
case class Reverse(child: Expression) extends UnaryExpression with DataTypeIsInputType {

  override def dataType: DataType = child.dataType

  override def inputType: DataType = {
    TypeSet(StringType, ArrayType(AnyType))
  }

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ArrayContains]].
 */
case class ArrayContains(left: Expression, right: Expression)
    extends BinaryExpression
    with Predicate {

  override def resolveDataType(outputType: DataType): Expression = {
    val arrayType = left.dataType.intersect(ArrayType(AnyType))
    val newElementType = right.dataType.intersect(arrayType.getArrayElementType)
    withNewChildren(
      IndexedSeq(
        left.resolveDataType(ArrayType(newElementType)),
        right.resolveDataType(newElementType)
      )
    )
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ArraysOverlap]].
 */
case class ArraysOverlap(left: Expression, right: Expression)
    extends BinaryExpression
    with Predicate {

  override def resolveDataType(outputType: DataType): Expression = {
    val newArrayType = ArrayType(AnyType)
      .intersect(left.dataType)
      .intersect(right.dataType)

    withNewChildren(
      IndexedSeq(
        left.resolveDataType(newArrayType),
        right.resolveDataType(newArrayType)
      )
    )
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Slice]].
 */
case class Slice(x: Expression, start: Expression, length: Expression) extends TernaryExpression {

  override def first: Expression = x

  override def second: Expression = start

  override def third: Expression = length

  override def dataType: DataType = x.dataType

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = outputType.intersect(dataType).intersect(ArrayType(AnyType))
    withNewChildren(
      IndexedSeq(
        x.resolveDataType(newDataType),
        start.resolveDataType(IntegerType),
        length.resolveDataType(IntegerType)
      )
    )
  }

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(x = newFirst, start = newSecond, length = newThird)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ArrayJoin]].
 */
case class ArrayJoin(array: Expression, delimiter: Expression, nullReplacement: Option[Expression])
    extends Expression {

  override def dataType: DataType = StringType

  override def children: IndexedSeq[Expression] = {
    IndexedSeq(array, delimiter) ++ nullReplacement.toIndexedSeq
  }

  override def resolveDataType(outputType: DataType): Expression = {
    withNewChildren(
      IndexedSeq(
        array.resolveDataType(ArrayType(StringType)),
        delimiter.resolveDataType(StringType)
      ) ++ nullReplacement.map(_.resolveDataType(StringType))
    )
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    val newNullReplacement = if (nullReplacement.isDefined) Some(newChildren(2)) else None
    copy(
      array = newChildren(0),
      delimiter = newChildren(1),
      nullReplacement = newNullReplacement
    )
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ArrayMin]].
 */
case class ArrayMin(child: Expression) extends UnaryExpression {

  override def dataType: DataType = {
    child.dataType.getArrayElementType
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = outputType.intersect(dataType)
    mapChildren(_.resolveDataType(ArrayType(newDataType)))
  }

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ArrayMax]].
 */
case class ArrayMax(child: Expression) extends UnaryExpression {

  override def dataType: DataType = {
    child.dataType.getArrayElementType
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = outputType.intersect(dataType)
    mapChildren(_.resolveDataType(ArrayType(newDataType)))
  }

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ArrayPosition]].
 */
case class ArrayPosition(left: Expression, right: Expression) extends BinaryExpression {

  override def dataType: DataType = LongType

  override def resolveDataType(outputType: DataType): Expression = {
    val arrayType = left.dataType.intersect(ArrayType(AnyType))
    val elementType = right.dataType.intersect(arrayType.getArrayElementType)
    withNewChildren(
      IndexedSeq(
        left.resolveDataType(ArrayType(elementType)),
        right.resolveDataType(elementType)
      )
    )
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ElementAt]].
 */
case class ElementAt(
    left: Expression,
    right: Expression,
    defaultValueOutOfBound: Option[Literal] = None,
    failOnError: Boolean
) extends BinaryExpression {

  override def dataType: DataType = {
    val leftDataType = left.dataType
    val arrayElementType =
      leftDataType.collectFirst { case arrayType: ArrayType =>
        arrayType.elementType
      }
    val mapValueType =
      leftDataType.collectFirst { case mapType: MapType =>
        mapType.valueType
      }

    if (arrayElementType.isEmpty && mapValueType.isEmpty) {
      AnyType
    } else {
      DataType(arrayElementType.toSet ++ mapValueType.toSet)
    }
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val leftDataType = left.dataType
    val arrayType = leftDataType.collectFirst { case arrayType: ArrayType => arrayType }
    val mapType = leftDataType.collectFirst { case mapType: MapType => mapType }

    if (arrayType.nonEmpty && mapType.isEmpty) {
      val newArrayType = ArrayType(arrayType.get.elementType.intersect(outputType))
      withNewChildren(
        IndexedSeq(
          left.resolveDataType(newArrayType),
          right.resolveDataType(IntegerType)
        )
      )
    } else if (arrayType.isEmpty && mapType.nonEmpty) {
      val newMapType = MapType(
        mapType.get.keyType,
        mapType.get.valueType.intersect(outputType)
      )
      withNewChildren(
        IndexedSeq(
          left.resolveDataType(newMapType),
          right.resolveDataType(newMapType.keyType)
        )
      )
    } else {
      mapChildren(_.resolveDataType(AnyType))
    }
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Concat]].
 */
case class Concat(children: IndexedSeq[Expression]) extends ComplexTypeMergingExpression {

  override def inputType: DataType = TypeSet(StringType, BinaryType, ArrayType(AnyType))

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Flatten]].
 */
case class Flatten(child: Expression) extends UnaryExpression {

  override def dataType: DataType = {
    child.dataType.getArrayElementType
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = outputType.intersect(dataType).intersect(ArrayType(AnyType))
    mapChildren(_.resolveDataType(ArrayType(newDataType)))
  }

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Sequence]].
 */
case class Sequence(
    start: Expression,
    stop: Expression,
    step: Option[Expression],
    timeZoneId: Option[String]
) extends Expression {

  override def dataType: DataType = ArrayType(start.dataType)

  override def children: IndexedSeq[Expression] = {
    IndexedSeq(start, stop) ++ step.toIndexedSeq
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val elementTypeFromStep =
      step.map(_.dataType) match {
        case Some(CalendarIntervalType | TimestampType | TimestampNTZType) =>
          TypeSet(TimestampType, TimestampNTZType, DateType)
        case Some(ByteType | ShortType | IntegerType | LongType) =>
          step.get.dataType
        case _ =>
          TypeSet.Integral + TimestampType + TimestampNTZType + DateType
      }

    val elementType = outputType
      .intersect(dataType)
      .getArrayElementType
      .intersect(stop.dataType)
      .intersect(elementTypeFromStep)

    val stepType =
      elementType match {
        case TimestampType | TimestampNTZType | DateType =>
          TypeSet(CalendarIntervalType, YearMonthIntervalType, DayTimeIntervalType)
        case ByteType | ShortType | IntegerType | LongType =>
          elementType
        case _ =>
          AnyType
      }

    withNewChildren(
      IndexedSeq(
        start.resolveDataType(elementType),
        stop.resolveDataType(elementType)
      ) ++ step.map(_.resolveDataType(stepType))
    )
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    val newStep = if (step.isDefined) Some(newChildren(2)) else None
    copy(start = newChildren(0), stop = newChildren(1), step = newStep)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ArrayInsert]].
 */
case class ArrayInsert(
    src: Expression,
    pos: Expression,
    item: Expression,
    legacyNegativeIndex: Boolean
) extends TernaryExpression {

  override def first: Expression = src

  override def second: Expression = pos

  override def third: Expression = item

  override def dataType: DataType = src.dataType

  override def resolveDataType(outputType: DataType): Expression = {
    val elementType = outputType
      .intersect(dataType)
      .intersect(ArrayType(AnyType))
      .getArrayElementType
      .intersect(item.dataType)

    withNewChildren(
      IndexedSeq(
        src.resolveDataType(ArrayType(elementType)),
        pos.resolveDataType(IntegerType),
        item.resolveDataType(elementType)
      )
    )
  }

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(src = newFirst, pos = newSecond, item = newThird)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ArrayRepeat]].
 */
case class ArrayRepeat(left: Expression, right: Expression) extends BinaryExpression {

  override def dataType: DataType = ArrayType(left.dataType)

  override def resolveDataType(outputType: DataType): Expression = {
    val elementType = outputType.intersect(dataType).getArrayElementType
    withNewChildren(
      IndexedSeq(
        left.resolveDataType(elementType),
        right.resolveDataType(IntegerType)
      )
    )
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ArrayRemove]].
 */
case class ArrayRemove(left: Expression, right: Expression) extends BinaryExpression {

  override def dataType: DataType = left.dataType

  override def resolveDataType(outputType: DataType): Expression = {
    val elementType = outputType
      .intersect(dataType)
      .intersect(ArrayType(AnyType))
      .getArrayElementType
      .intersect(right.dataType)

    withNewChildren(
      IndexedSeq(
        left.resolveDataType(ArrayType(elementType)),
        right.resolveDataType(elementType)
      )
    )
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ArrayDistinct]].
 */
case class ArrayDistinct(child: Expression) extends UnaryExpression with DataTypeIsInputType {

  override def dataType: DataType = child.dataType

  override def inputType: DataType = ArrayType(AnyType)

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ArrayUnion]].
 */
case class ArrayUnion(left: Expression, right: Expression)
    extends BinaryExpression
    with ComplexTypeMergingExpression {

  override def inputType: DataType = ArrayType(AnyType)

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ArrayIntersect]].
 */
case class ArrayIntersect(left: Expression, right: Expression)
    extends BinaryExpression
    with ComplexTypeMergingExpression {

  override def inputType: DataType = ArrayType(AnyType)

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ArrayExcept]].
 */
case class ArrayExcept(left: Expression, right: Expression)
    extends BinaryExpression
    with ComplexTypeMergingExpression {

  override def inputType: DataType = ArrayType(AnyType)

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ArrayAppend]].
 */
case class ArrayAppend(left: Expression, right: Expression) extends BinaryExpression {

  override def dataType: DataType = left.dataType

  override def resolveDataType(outputType: DataType): Expression = {
    val elementType = outputType
      .intersect(dataType)
      .intersect(ArrayType(AnyType))
      .getArrayElementType
      .intersect(right.dataType)

    withNewChildren(
      IndexedSeq(
        left.resolveDataType(ArrayType(elementType)),
        right.resolveDataType(elementType)
      )
    )
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ArraysZip]].
 */
case class ArraysZip(children: IndexedSeq[Expression], names: Seq[String]) extends Expression {

  override lazy val dataType: DataType = {
    val fields = children
      .map(_.dataType.getArrayElementType)
      .zip(names)
      .map { case (elementType, name) => StructField(name, elementType) }
    ArrayType(StructType(fields.toArray))
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val structFields = outputType
      .intersect(dataType)
      .getArrayElementType
      .getStructFields(children.length)

    withNewChildren(
      children.zipWithIndex.map { case (child, i) =>
        child.resolveDataType(ArrayType(structFields(i).dataType))
      }
    )
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }
}
