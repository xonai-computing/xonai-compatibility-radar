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

import com.xonai.spark.sql.parser.types.{AnyType, ArrayType, BooleanType, DataType, IntegerType, LongType, MapType, StringType, StructField, StructType, TypeSet, VariantType}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Generator]].
 */
trait Generator extends Expression {

  def elementSchema: DataType

  override def dataType: DataType = ArrayType(elementSchema)
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Explode]].
 */
case class Explode(child: Expression) extends UnaryExpression with Generator {

  override lazy val elementSchema: DataType = {
    val childDataType = child.dataType
    val arrayElementType =
      childDataType.collectFirst { case arrayType: ArrayType =>
        StructType(
          Array(
            StructField("col", arrayType.elementType)
          )
        )
      }
    val mapElementType =
      childDataType.collectFirst { case mapType: MapType =>
        StructType(
          Array(
            StructField("key", mapType.keyType),
            StructField("value", mapType.valueType)
          )
        )
      }

    if (arrayElementType.isEmpty && mapElementType.isEmpty) {
      Explode.defaultElementType
    } else {
      DataType(arrayElementType.toSet ++ mapElementType.toSet)
    }
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = dataType.intersect(outputType)

    val childTypes = newDataType.getArrayElementType.collect { case structType: StructType =>
      if (structType.fields.length == 1) {
        ArrayType(structType.fields.head.dataType)
      } else {
        MapType(
          structType.fields.head.dataType,
          structType.fields(1).dataType
        )
      }
    }
    val newChildType = DataType(childTypes.toSet)

    mapChildren(_.resolveDataType(newChildType))
  }

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

object Explode {

  def defaultElementType: DataType = {
    TypeSet(
      StructType(
        Array(
          StructField("col", AnyType)
        )
      ),
      StructType(
        Array(
          StructField("key", AnyType),
          StructField("value", AnyType)
        )
      )
    )
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.PosExplode]].
 */
case class PosExplode(child: Expression) extends UnaryExpression with Generator {

  override lazy val elementSchema: DataType = {
    val childDataType = child.dataType
    val arrayElementType =
      childDataType.collectFirst { case arrayType: ArrayType =>
        StructType(
          Array(
            StructField("pos", IntegerType),
            StructField("col", arrayType.elementType)
          )
        )
      }
    val mapElementType =
      childDataType.collectFirst { case mapType: MapType =>
        StructType(
          Array(
            StructField("pos", IntegerType),
            StructField("key", mapType.keyType),
            StructField("value", mapType.valueType)
          )
        )
      }

    if (arrayElementType.isEmpty && mapElementType.isEmpty) {
      PosExplode.defaultElementType
    } else {
      DataType(arrayElementType.toSet ++ mapElementType.toSet)
    }
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = dataType.intersect(outputType)

    val childTypes = newDataType.getArrayElementType.collect { case structType: StructType =>
      if (structType.fields.length == 2) {
        ArrayType(structType.fields(1).dataType)
      } else {
        MapType(
          structType.fields(1).dataType,
          structType.fields(2).dataType
        )
      }
    }
    val newChildType = DataType(childTypes.toSet)

    mapChildren(_.resolveDataType(newChildType))
  }

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

object PosExplode {

  def defaultElementType: DataType = {
    TypeSet(
      StructType(
        Array(
          StructField("pos", IntegerType),
          StructField("col", AnyType)
        )
      ),
      StructType(
        Array(
          StructField("pos", IntegerType),
          StructField("key", AnyType),
          StructField("value", AnyType)
        )
      )
    )
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Inline]].
 */
case class Inline(child: Expression) extends UnaryExpression with Generator {

  override def elementSchema: DataType = {
    child.dataType.getArrayElementType
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = dataType.intersect(outputType)
    mapChildren(_.resolveDataType(newDataType))
  }

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Stack]].
 */
case class Stack(children: IndexedSeq[Expression]) extends Generator {

  override lazy val elementSchema: StructType = {
    val numRows = children.head.asInstanceOf[Literal].intValue
    val numFields = Math.ceil((children.length - 1.0) / numRows).toInt
    val grouped = children.tail.grouped(numFields).toIndexedSeq
    val fields = (0 until numFields).map { index =>
      val expressions = grouped.flatMap(_.lift(index))
      val dataType = expressions.map(_.dataType).reduce(DataType.intersection)
      StructField(s"col$index", dataType)
    }
    StructType(fields.toArray)
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = dataType.intersect(outputType)
    val numFields = elementSchema.fields.length
    val newFields = newDataType.getArrayElementType.getStructFields(numFields)

    withNewChildren(
      children.head.resolveDataType(IntegerType) +:
        children.tail.zipWithIndex.map {
          case (child, index) =>
            child.resolveDataType(newFields(index % numFields).dataType)
        }
    )
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ReplicateRows]].
 */
case class ReplicateRows(children: IndexedSeq[Expression]) extends Generator {

  override def elementSchema: DataType = {
    val fields = children
      .tail
      .zipWithIndex
      .map { case (e, index) => StructField(s"col$index", e.dataType) }
    StructType(fields.toArray)
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = dataType.intersect(outputType)
    val newFields = newDataType.getArrayElementType.getStructFields(children.tail.length)

    withNewChildren(
      children.head.resolveDataType(LongType) +:
        children.tail.zip(newFields).map {
          case (child, field) =>
            child.resolveDataType(field.dataType)
        }
    )
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.SQLKeywords]].
 */
case class SQLKeywords() extends LeafExpression with Generator {

  override def elementSchema: StructType = {
    StructType(
      Array(
        StructField("keyword", StringType),
        StructField("reserved", BooleanType)
      )
    )
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Collations]].
 */
case class Collations() extends LeafExpression with Generator {

  override def elementSchema: StructType = {
    StructType(
      Array(
        StructField("CATALOG", StringType),
        StructField("SCHEMA", StringType),
        StructField("NAME", StringType),
        StructField("LANGUAGE", StringType),
        StructField("COUNTRY", StringType),
        StructField("ACCENT_SENSITIVITY", StringType),
        StructField("CASE_SENSITIVITY", StringType),
        StructField("PAD_ATTRIBUTE", StringType),
        StructField("ICU_VERSION", StringType)
      )
    )
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.variant.VariantExplode]].
 */
case class VariantExplode(child: Expression)
    extends UnaryExpression
    with Generator
    with ExpectsInputType {

  override def elementSchema: StructType = {
    StructType(
      Array(
        StructField("pos", IntegerType),
        StructField("key", StringType),
        StructField("value", VariantType)
      )
    )
  }

  override def inputType: DataType = VariantType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}
