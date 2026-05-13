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

import com.xonai.spark.sql.parser.trees.{BinaryLike, LeafLike, TernaryLike, UnaryLike}
import com.xonai.spark.sql.parser.types.{AnyType, DataType, DoubleType, IntegerType, LongType, NullType, TypeSet}

/**
 * [[org.apache.spark.sql.catalyst.expressions.FrameType]].
 */
sealed trait FrameType

/**
 * [[org.apache.spark.sql.catalyst.expressions.RowFrame]].
 */
case object RowFrame extends FrameType

/**
 * [[org.apache.spark.sql.catalyst.expressions.RangeFrame]].
 */
case object RangeFrame extends FrameType

/**
 * [[org.apache.spark.sql.catalyst.expressions.WindowFrame]].
 */
sealed trait WindowFrame extends Expression {

  override def dataType: DataType = {
    throw new RuntimeException("The operation `dataType` is not supported.")
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.UnspecifiedFrame]].
 */
case object UnspecifiedFrame extends WindowFrame with LeafExpression

/**
 * [[org.apache.spark.sql.catalyst.expressions.SpecialFrameBoundary]].
 */
sealed trait SpecialFrameBoundary extends LeafExpression {

  override def dataType: DataType = NullType
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.UnboundedPreceding]].
 */
case object UnboundedPreceding extends SpecialFrameBoundary

/**
 * [[org.apache.spark.sql.catalyst.expressions.UnboundedFollowing]].
 */
case object UnboundedFollowing extends SpecialFrameBoundary

/**
 * [[org.apache.spark.sql.catalyst.expressions.CurrentRow]].
 */
case object CurrentRow extends SpecialFrameBoundary

/**
 * [[org.apache.spark.sql.catalyst.expressions.SpecifiedWindowFrame]].
 */
case class SpecifiedWindowFrame(frameType: FrameType, lower: Expression, upper: Expression)
    extends WindowFrame
    with BinaryExpression {

  override def left: Expression = lower

  override def right: Expression = upper

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(lower = newLeft, upper = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.WindowSpecDefinition]].
 */
case class WindowSpecDefinition(
    partitionSpec: IndexedSeq[Expression],
    orderSpec: IndexedSeq[SortOrder],
    frameSpecification: WindowFrame
) extends Expression {

  override def dataType: DataType = TypeSet.Empty

  override def children: IndexedSeq[Expression] = {
    partitionSpec ++ orderSpec :+ frameSpecification
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(
      partitionSpec = newChildren.take(partitionSpec.size),
      orderSpec = newChildren
        .drop(partitionSpec.size)
        .dropRight(1)
        .asInstanceOf[IndexedSeq[SortOrder]],
      frameSpecification = newChildren.last.asInstanceOf[WindowFrame]
    )
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.WindowExpression]].
 */
case class WindowExpression(windowFunction: Expression, windowSpec: WindowSpecDefinition)
    extends Expression
    with BinaryLike[Expression] {

  override def left: Expression = windowFunction

  override def right: Expression = windowSpec

  override def dataType: DataType = windowFunction.dataType

  override def resolveDataType(outputType: DataType): Expression = {
    withNewChildren(
      IndexedSeq(
        windowFunction.resolveDataType(outputType),
        windowSpec.resolveDataType(AnyType)
      )
    )
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(windowFunction = newLeft, windowSpec = newRight.asInstanceOf[WindowSpecDefinition])
  }
}

/**
 * Used when the parser knows there are more window expressions but has no information about them.
 */
case class UndefinedWindowExpression(dataType: DataType)
    extends UnknownExpression
    with LeafLike[Expression] {

  override def withNewDataTypeInternal(newDataType: DataType): Expression = {
    copy(dataType = newDataType)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.WindowFunction]].
 */
trait WindowFunction extends Expression

/**
 * [[org.apache.spark.sql.catalyst.expressions.OffsetWindowFunction]].
 */
trait OffsetWindowFunction extends WindowFunction {

  def input: Expression

  def offset: Expression

  def default: Expression
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.FrameLessOffsetWindowFunction]].
 */
sealed abstract class FrameLessOffsetWindowFunction extends OffsetWindowFunction {

  override def dataType: DataType = input.dataType

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = outputType.intersect(dataType).intersect(default.dataType)
    withNewChildren(
      IndexedSeq(
        input.resolveDataType(newDataType),
        offset.resolveDataType(IntegerType),
        default.resolveDataType(newDataType)
      )
    )
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Lead]].
 */
case class Lead(input: Expression, offset: Expression, default: Expression)
    extends FrameLessOffsetWindowFunction
    with TernaryLike[Expression] {

  override def first: Expression = input

  override def second: Expression = offset

  override def third: Expression = default

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(input = newFirst, offset = newSecond, default = newThird)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Lag]].
 */
case class Lag(input: Expression, offset: Expression, default: Expression)
    extends FrameLessOffsetWindowFunction
    with TernaryLike[Expression] {

  override def first: Expression = input

  override def second: Expression = offset

  override def third: Expression = default

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(input = newFirst, offset = newSecond, default = newThird)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.AggregateWindowFunction]].
 */
abstract class AggregateWindowFunction extends AggregateFunction with WindowFunction {

  override def dataType: DataType = IntegerType
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.RowNumberLike]].
 */
abstract class RowNumberLike extends AggregateWindowFunction {

  override def aggBufferDataTypes: IndexedSeq[DataType] = IndexedSeq(IntegerType)
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.RowNumber]].
 */
case class RowNumber() extends RowNumberLike with LeafExpression

/**
 * [[org.apache.spark.sql.catalyst.expressions.SizeBasedWindowFunction]].
 */
trait SizeBasedWindowFunction extends AggregateWindowFunction

/**
 * [[org.apache.spark.sql.catalyst.expressions.CumeDist]].
 */
case class CumeDist() extends RowNumberLike with SizeBasedWindowFunction with LeafExpression {

  override def dataType: DataType = DoubleType
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.NTile]].
 */
case class NTile(buckets: Expression)
    extends RowNumberLike
    with SizeBasedWindowFunction
    with UnaryLike[Expression]
    with DataTypeIsInputType {

  override def child: Expression = buckets

  override def inputType: DataType = IntegerType

  override def aggBufferDataTypes: IndexedSeq[DataType] = {
    IndexedSeq(IntegerType, IntegerType, IntegerType, IntegerType, IntegerType)
  }

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(buckets = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.RankLike]].
 */
abstract class RankLike extends AggregateWindowFunction {

  override def aggBufferDataTypes: IndexedSeq[DataType] = {
    IndexedSeq(IntegerType, IntegerType) ++ children.map(_.dataType)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Rank]].
 */
case class Rank(children: IndexedSeq[Expression]) extends RankLike {

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.DenseRank]].
 */
case class DenseRank(children: IndexedSeq[Expression]) extends RankLike {

  override def aggBufferDataTypes: IndexedSeq[DataType] = {
    IndexedSeq(IntegerType) ++ children.map(_.dataType)
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.PercentRank]].
 */
case class PercentRank(children: IndexedSeq[Expression]) extends RankLike {

  override def dataType: DataType = DoubleType

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.NthValue]].
 */
case class NthValue(input: Expression, offset: Expression, ignoreNulls: Boolean)
    extends AggregateWindowFunction
    with OffsetWindowFunction
    with BinaryLike[Expression] {

  override lazy val default = Literal(null, input.dataType)

  override def left: Expression = input

  override def right: Expression = offset

  override def dataType: DataType = input.dataType

  override def aggBufferDataTypes: IndexedSeq[DataType] = {
    IndexedSeq(input.dataType, LongType)
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = outputType.intersect(dataType)
    withNewChildren(
      IndexedSeq(
        input.resolveDataType(newDataType),
        offset.resolveDataType(IntegerType)
      )
    )
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(input = newLeft, offset = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.EWM]].
 */
case class EWM(input: Expression, alpha: Double, ignoreNA: Boolean)
    extends AggregateWindowFunction
    with UnaryLike[Expression] {

  override def child: Expression = input

  override def dataType: DataType = DoubleType

  override def aggBufferDataTypes: IndexedSeq[DataType] = {
    IndexedSeq(DoubleType, DoubleType, DoubleType)
  }

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(input = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.LastNonNull]].
 */
case class LastNonNull(input: Expression)
    extends AggregateWindowFunction
    with UnaryLike[Expression]
    with PropagatesDataType {

  override def child: Expression = input

  override def dataType: DataType = input.dataType

  override def aggBufferDataTypes: IndexedSeq[DataType] = IndexedSeq(input.dataType)

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(input = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.NullIndex]].
 */
case class NullIndex(input: Expression) extends AggregateWindowFunction with UnaryLike[Expression] {

  override def child: Expression = input

  override def dataType: DataType = IntegerType

  override def aggBufferDataTypes: IndexedSeq[DataType] = IndexedSeq(IntegerType)

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(input = newChild)
  }
}
