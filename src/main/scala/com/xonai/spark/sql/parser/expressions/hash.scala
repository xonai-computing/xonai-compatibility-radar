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

import com.xonai.spark.sql.parser.types.{BinaryType, DataType, IntegerType, LongType, StringType}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Md5]].
 */
case class Md5(child: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = StringType

  override def inputType: DataType = BinaryType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Sha2]].
 */
case class Sha2(left: Expression, right: Expression)
    extends BinaryExpression
    with ExpectsInputTypes {

  override def dataType: DataType = StringType

  override def inputTypes: Seq[DataType] = Seq(BinaryType, IntegerType)

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Sha1]].
 */
case class Sha1(child: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = StringType

  override def inputType: DataType = BinaryType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Crc32]].
 */
case class Crc32(child: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = LongType

  override def inputType: DataType = BinaryType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Murmur3Hash]].
 */
case class Murmur3Hash(children: IndexedSeq[Expression], seed: Int) extends Expression {

  override def dataType: DataType = IntegerType

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.XxHash64]].
 */
case class XxHash64(children: IndexedSeq[Expression], seed: Long) extends Expression {

  override def dataType: DataType = LongType

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.HiveHash]].
 */
case class HiveHash(children: IndexedSeq[Expression]) extends Expression {

  override def dataType: DataType = IntegerType

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }
}
