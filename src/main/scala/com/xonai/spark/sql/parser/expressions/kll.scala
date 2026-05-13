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

import com.xonai.spark.sql.parser.types.{ArrayType, BinaryType, DataType, DoubleType, FloatType, LongType, StringType, TypeSet}

/**
 * [[org.apache.spark.sql.catalyst.expressions.KllSketchToStringBase]].
 */
abstract class KllSketchToStringBase extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = StringType

  override def inputType: DataType = BinaryType
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.KllSketchToStringBigint]].
 */
case class KllSketchToStringBigint(child: Expression) extends KllSketchToStringBase {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.KllSketchToStringFloat]].
 */
case class KllSketchToStringFloat(child: Expression) extends KllSketchToStringBase {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.KllSketchToStringDouble]].
 */
case class KllSketchToStringDouble(child: Expression) extends KllSketchToStringBase {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.KllSketchGetNBase]].
 */
abstract class KllSketchGetNBase extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = LongType

  override def inputType: DataType = BinaryType
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.KllSketchGetNBigint]].
 */
case class KllSketchGetNBigint(child: Expression) extends KllSketchGetNBase {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.KllSketchGetNFloat]].
 */
case class KllSketchGetNFloat(child: Expression) extends KllSketchGetNBase {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.KllSketchGetNDouble]].
 */
case class KllSketchGetNDouble(child: Expression) extends KllSketchGetNBase {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.KllSketchMergeBase]].
 */
abstract class KllSketchMergeBase extends BinaryExpression with ExpectsInputType {

  override def dataType: DataType = BinaryType

  override def inputType: DataType = BinaryType
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.KllSketchMergeBigint]].
 */
case class KllSketchMergeBigint(left: Expression, right: Expression) extends KllSketchMergeBase {

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.KllSketchMergeFloat]].
 */
case class KllSketchMergeFloat(left: Expression, right: Expression) extends KllSketchMergeBase {

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.KllSketchMergeDouble]].
 */
case class KllSketchMergeDouble(left: Expression, right: Expression) extends KllSketchMergeBase {

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.KllSketchGetQuantileBase]].
 */
abstract class KllSketchGetQuantileBase extends BinaryExpression {

  protected def outputDataType: DataType

  override def dataType: DataType = {
    val quantileDataType =
      right
        .dataType
        .intersect(TypeSet(DoubleType, ArrayType(DoubleType)))

    if (quantileDataType.isInstanceOf[ArrayType]) {
      ArrayType(outputDataType)
    } else if (quantileDataType == DoubleType) {
      outputDataType
    } else {
      outputDataType + ArrayType(outputDataType)
    }
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = outputType.intersect(dataType)
    val quantileDataType =
      if (newDataType.isInstanceOf[ArrayType]) {
        ArrayType(DoubleType)
      } else if (newDataType == outputDataType) {
        DoubleType
      } else {
        DoubleType + ArrayType(DoubleType)
      }

    withNewChildren(
      IndexedSeq(
        left.resolveDataType(BinaryType),
        right.resolveDataType(quantileDataType)
      )
    )
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.KllSketchGetQuantileBigint]].
 */
case class KllSketchGetQuantileBigint(left: Expression, right: Expression)
    extends KllSketchGetQuantileBase {

  override def outputDataType: DataType = LongType

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.KllSketchGetQuantileFloat]].
 */
case class KllSketchGetQuantileFloat(left: Expression, right: Expression)
    extends KllSketchGetQuantileBase {

  override def outputDataType: DataType = FloatType

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.KllSketchGetQuantileDouble]].
 */
case class KllSketchGetQuantileDouble(left: Expression, right: Expression)
    extends KllSketchGetQuantileBase {

  override def outputDataType: DataType = DoubleType

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.KllSketchGetRankBase]].
 */
abstract class KllSketchGetRankBase extends BinaryExpression {

  protected def inputDataType: DataType

  override def dataType: DataType = {
    val rightDataType =
      right
        .dataType
        .intersect(TypeSet(inputDataType, ArrayType(inputDataType)))

    if (rightDataType.isInstanceOf[ArrayType]) {
      ArrayType(DoubleType)
    } else if (rightDataType == inputDataType) {
      DoubleType
    } else {
      DoubleType + ArrayType(DoubleType)
    }
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = outputType.intersect(dataType)
    val rightDataType =
      if (newDataType.isInstanceOf[ArrayType]) {
        ArrayType(inputDataType)
      } else if (newDataType == DoubleType) {
        inputDataType
      } else {
        inputDataType + ArrayType(inputDataType)
      }

    withNewChildren(
      IndexedSeq(
        left.resolveDataType(BinaryType),
        right.resolveDataType(rightDataType)
      )
    )
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.KllSketchGetRankBigint]].
 */
case class KllSketchGetRankBigint(left: Expression, right: Expression)
    extends KllSketchGetRankBase {

  override def inputDataType: DataType = LongType

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.KllSketchGetRankFloat]].
 */
case class KllSketchGetRankFloat(left: Expression, right: Expression)
    extends KllSketchGetRankBase {

  override def inputDataType: DataType = FloatType

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.KllSketchGetRankDouble]].
 */
case class KllSketchGetRankDouble(left: Expression, right: Expression)
    extends KllSketchGetRankBase {

  override def inputDataType: DataType = DoubleType

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}
