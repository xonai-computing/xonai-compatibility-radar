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

import com.xonai.spark.sql.parser.types.{ArrayType, DataType, FloatType}

/**
 * [[org.apache.spark.sql.catalyst.expressions.VectorCosineSimilarity]].
 */
case class VectorCosineSimilarity(left: Expression, right: Expression)
    extends BinaryExpression
    with ExpectsInputType {

  override def dataType: DataType = FloatType

  override def inputType: DataType = ArrayType(FloatType)

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.VectorInnerProduct]].
 */
case class VectorInnerProduct(left: Expression, right: Expression)
    extends BinaryExpression
    with ExpectsInputType {

  override def dataType: DataType = FloatType

  override def inputType: DataType = ArrayType(FloatType)

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.VectorL2Distance]].
 */
case class VectorL2Distance(left: Expression, right: Expression)
    extends BinaryExpression
    with ExpectsInputType {

  override def dataType: DataType = FloatType

  override def inputType: DataType = ArrayType(FloatType)

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.VectorNorm]].
 */
case class VectorNorm(vector: Expression, degree: Expression)
    extends BinaryExpression
    with ExpectsInputTypes {

  override def left: Expression = vector

  override def right: Expression = degree

  override def dataType: DataType = FloatType

  override def inputTypes: Seq[DataType] = Seq(ArrayType(FloatType), FloatType)

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(vector = newLeft, degree = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.VectorNormalize]].
 */
case class VectorNormalize(vector: Expression, degree: Expression)
    extends BinaryExpression
    with ExpectsInputTypes {

  override def left: Expression = vector

  override def right: Expression = degree

  override def dataType: DataType = ArrayType(FloatType)

  override def inputTypes: Seq[DataType] = Seq(ArrayType(FloatType), FloatType)

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(vector = newLeft, degree = newRight)
  }
}
