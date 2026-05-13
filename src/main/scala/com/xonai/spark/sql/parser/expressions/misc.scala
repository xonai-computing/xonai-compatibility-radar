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

import com.xonai.spark.sql.parser.types.{BinaryType, BooleanType, ByteType, DataType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TypeSet}

/**
 * [[org.apache.spark.sql.catalyst.expressions.PrintToStderr]].
 */
case class PrintToStderr(child: Expression) extends UnaryExpression {

  override def dataType: DataType = child.dataType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.RaiseError]].
 */
case class RaiseError(child: Expression, dataType: DataType)
    extends UnaryExpression
    with ExpectsInputType {

  override def inputType: DataType = StringType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Uuid]].
 */
case class Uuid(randomSeed: Option[Long]) extends LeafExpression {

  override def dataType: DataType = StringType
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.SparkVersion]].
 */
case class SparkVersion() extends LeafExpression {

  override def dataType: DataType = StringType
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.SparkPartitionID]].
 */
case class SparkPartitionID() extends LeafExpression {

  override def dataType: DataType = IntegerType
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.TypeOf]].
 */
case class TypeOf(child: Expression) extends UnaryExpression {

  override def dataType: DataType = StringType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.MonotonicallyIncreasingID]].
 */
case class MonotonicallyIncreasingID() extends LeafExpression {

  override def dataType: DataType = LongType
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.AesEncrypt]].
 */
case class AesEncrypt(
    input: Expression,
    key: Expression,
    mode: Expression,
    padding: Expression,
    iv: Expression,
    aad: Expression
) extends Expression
    with ExpectsInputTypes {

  override def dataType: DataType = BinaryType

  override def children: IndexedSeq[Expression] = {
    IndexedSeq(input, key, mode, padding, iv, aad)
  }

  override def inputTypes: Seq[DataType] = {
    Seq(BinaryType, BinaryType, StringType, StringType, BinaryType, BinaryType)
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(
      input = newChildren(0),
      key = newChildren(1),
      mode = newChildren(2),
      padding = newChildren(3),
      iv = newChildren(4),
      aad = newChildren(5)
    )
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.AesDecrypt]].
 */
case class AesDecrypt(
    input: Expression,
    key: Expression,
    mode: Expression,
    padding: Expression,
    aad: Expression
) extends Expression
    with ExpectsInputTypes {

  override def dataType: DataType = BinaryType

  override def children: IndexedSeq[Expression] = {
    IndexedSeq(input, key, mode, padding, aad)
  }

  override def inputTypes: Seq[DataType] = {
    Seq(BinaryType, BinaryType, StringType, StringType, BinaryType)
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(
      input = newChildren(0),
      key = newChildren(1),
      mode = newChildren(2),
      padding = newChildren(3),
      aad = newChildren(4)
    )
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.CallMethodViaReflection]].
 */
case class CallMethodViaReflection(children: IndexedSeq[Expression]) extends Expression {

  override def dataType: DataType = StringType

  override def resolveDataType(outputType: DataType): Expression = {
    val argumentsType = TypeSet(
      BooleanType,
      ByteType,
      ShortType,
      IntegerType,
      LongType,
      FloatType,
      DoubleType,
      StringType
    )

    withNewChildren(
      children.head.resolveDataType(StringType) +:
        children.tail.head.resolveDataType(StringType) +:
        children.tail.tail.map(_.resolveDataType(argumentsType))
    )
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }
}
