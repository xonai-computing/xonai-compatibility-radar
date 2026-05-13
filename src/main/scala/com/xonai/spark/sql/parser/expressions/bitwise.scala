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

import com.xonai.spark.sql.parser.types.{BooleanType, ByteType, DataType, IntegerType, TypeSet}

/**
 * [[org.apache.spark.sql.catalyst.expressions.BitwiseAnd]].
 */
case class BitwiseAnd(left: Expression, right: Expression) extends BinaryArithmetic {

  override def inputType: DataType = TypeSet.Integral

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.BitwiseOr]].
 */
case class BitwiseOr(left: Expression, right: Expression) extends BinaryArithmetic {

  override def inputType: DataType = TypeSet.Integral

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.BitwiseXor]].
 */
case class BitwiseXor(left: Expression, right: Expression) extends BinaryArithmetic {

  override def inputType: DataType = TypeSet.Integral

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.BitwiseNot]].
 */
case class BitwiseNot(child: Expression) extends UnaryExpression with DataTypeIsInputType {

  override def dataType: DataType = child.dataType

  override def inputType: DataType = TypeSet.Integral

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.BitwiseCount]].
 */
case class BitwiseCount(child: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = IntegerType

  override def inputType: DataType = TypeSet.Integral + BooleanType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.BitwiseGet]].
 */
case class BitwiseGet(left: Expression, right: Expression) extends BinaryExpression {

  override def dataType: DataType = ByteType

  override def resolveDataType(outputType: DataType): Expression = {
    withNewChildren(
      IndexedSeq(
        left.resolveDataType(TypeSet.Integral),
        right.resolveDataType(IntegerType)
      )
    )
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.BitwiseReverse]].
 */
case class BitwiseReverse(child: Expression) extends UnaryExpression with DataTypeIsInputType {

  override def dataType: DataType = child.dataType

  override def inputType: DataType = TypeSet.Integral

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}
