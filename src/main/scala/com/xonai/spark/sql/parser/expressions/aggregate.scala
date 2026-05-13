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

import com.xonai.spark.sql.parser.trees.{BinaryLike, QuaternaryLike, TernaryLike, UnaryLike}
import com.xonai.spark.sql.parser.types.{AnyType, ArrayType, BinaryType, BooleanType, ByteType, DataType, DataTypeMap, DateType, DayTimeIntervalType, DecimalType, DoubleType, FloatType, IntegerType, LongType, NullType, ShortType, StringType, StructField, StructType, TimestampNTZType, TimestampType, TypeSet, VariantType, YearMonthIntervalType}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction]].
 */
trait AggregateFunction extends Expression {

  def aggBufferDataTypes: IndexedSeq[DataType]
}

/**
 * Used when the parser is unable to match a string to an aggregate function.
 */
case class UnknownAggregateFunction(
    functionName: String,
    children: IndexedSeq[Expression],
    dataType: DataType = AnyType
) extends AggregateFunction
    with UnknownExpression {

  override def nodeName: String = functionName

  override def aggBufferDataTypes: IndexedSeq[DataType] = {
    throw new RuntimeException(s"Buffer data types of aggregate $functionName are unknown")
  }

  override def withNewDataTypeInternal(newDataType: DataType): Expression = {
    copy(dataType = newDataType)
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate]].
 */
trait TypedImperativeAggregate extends AggregateFunction {

  override def aggBufferDataTypes: IndexedSeq[DataType] = {
    IndexedSeq(BinaryType)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression]].
 */
case class AggregateExpression(
    aggregateFunction: AggregateFunction,
    modePrefix: String,
    isDistinct: Boolean,
    filter: Option[Expression]
) extends Expression {

  override def children: IndexedSeq[Expression] = {
    IndexedSeq(aggregateFunction) ++ filter
  }

  override def dataType: DataType = aggregateFunction.dataType

  override def resolveDataType(outputType: DataType): Expression = {
    withNewChildren(
      IndexedSeq(aggregateFunction.resolveDataType(outputType)) ++
        filter.map(_.resolveDataType(BooleanType))
    )
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    val newFilter = if (filter.isDefined) Some(newChildren(1)) else None
    copy(
      aggregateFunction = newChildren(0).asInstanceOf[AggregateFunction],
      filter = newFilter
    )
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.Count]].
 */
case class Count(children: IndexedSeq[Expression]) extends AggregateFunction {

  override def dataType: DataType = LongType

  override def aggBufferDataTypes: IndexedSeq[DataType] = IndexedSeq(LongType)

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }
}

object Count {

  def apply(children: Expression*): Count = {
    new Count(children.toIndexedSeq)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.Sum]].
 */
case class Sum(child: Expression, isTry: Boolean)
    extends AggregateFunction
    with UnaryLike[Expression] {

  private lazy val dataTypeMap = DataTypeMap(
    Map(
      DecimalType -> DecimalType,
      YearMonthIntervalType -> YearMonthIntervalType,
      DayTimeIntervalType -> DayTimeIntervalType,
      ByteType -> LongType,
      ShortType -> LongType,
      IntegerType -> LongType,
      LongType -> LongType,
      FloatType -> DoubleType,
      DoubleType -> DoubleType,
      NullType -> DoubleType
    )
  )

  override lazy val dataType: DataType = {
    dataTypeMap.outputType(child.dataType)
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val inputDataType = dataTypeMap.inputType(outputType)
    val newChildDataType = child.dataType.intersect(inputDataType)
    mapChildren(_.resolveDataType(newChildDataType))
  }

  override def aggBufferDataTypes: IndexedSeq[DataType] = {
    if (shouldTrackIsEmpty) {
      IndexedSeq(dataType, BooleanType)
    } else {
      IndexedSeq(dataType)
    }
  }

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }

  private def shouldTrackIsEmpty: Boolean = {
    dataType match {
      case DecimalType =>
        true
      case LongType | DayTimeIntervalType | YearMonthIntervalType if isTry =>
        true
      case _ =>
        false
    }
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.Average]].
 */
case class Average(child: Expression, isTry: Boolean)
    extends AggregateFunction
    with UnaryLike[Expression] {

  private lazy val dataTypeMap = DataTypeMap(
    Map(
      DecimalType -> DecimalType,
      YearMonthIntervalType -> YearMonthIntervalType,
      DayTimeIntervalType -> DayTimeIntervalType,
      ByteType -> DoubleType,
      ShortType -> DoubleType,
      IntegerType -> DoubleType,
      LongType -> DoubleType,
      FloatType -> DoubleType,
      DoubleType -> DoubleType,
      NullType -> DoubleType
    )
  )

  override lazy val dataType: DataType = dataTypeMap.outputType(child.dataType)

  override def resolveDataType(outputType: DataType): Expression = {
    val inputDataType = dataTypeMap.inputType(outputType)
    val newChildDataType = child.dataType.intersect(inputDataType)
    mapChildren(_.resolveDataType(newChildDataType))
  }

  override def aggBufferDataTypes: IndexedSeq[DataType] = {
    IndexedSeq(dataType, LongType)
  }

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.Min]].
 */
case class Min(child: Expression)
    extends UnaryExpression
    with AggregateFunction
    with PropagatesDataType {

  override def dataType: DataType = child.dataType

  override def aggBufferDataTypes: IndexedSeq[DataType] = IndexedSeq(dataType)

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.Max]].
 */
case class Max(child: Expression)
    extends UnaryExpression
    with AggregateFunction
    with PropagatesDataType {

  override def dataType: DataType = child.dataType

  override def aggBufferDataTypes: IndexedSeq[DataType] = IndexedSeq(dataType)

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.First]].
 */
case class First(child: Expression, ignoreNulls: Boolean)
    extends UnaryExpression
    with AggregateFunction
    with PropagatesDataType {

  override def dataType: DataType = child.dataType

  override def aggBufferDataTypes: IndexedSeq[DataType] = {
    IndexedSeq(dataType, BooleanType)
  }

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

object First {

  def apply(child: Expression, ignoreNulls: Expression): First = {
    new First(child, ignoreNulls.asInstanceOf[Literal].booleanValue)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.Last]].
 */
case class Last(child: Expression, ignoreNulls: Boolean)
    extends UnaryExpression
    with AggregateFunction
    with PropagatesDataType {

  override def dataType: DataType = child.dataType

  override def aggBufferDataTypes: IndexedSeq[DataType] = {
    IndexedSeq(dataType, BooleanType)
  }

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

object Last {

  def apply(child: Expression, ignoreNulls: Expression): Last = {
    new Last(child, ignoreNulls.asInstanceOf[Literal].booleanValue)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.CollectList]].
 */
case class CollectList(child: Expression)
    extends TypedImperativeAggregate
    with UnaryLike[Expression] {

  override def dataType: DataType = ArrayType(child.dataType)

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = outputType.intersect(dataType)
    mapChildren(_.resolveDataType(newDataType.getArrayElementType))
  }

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.CollectSet]].
 */
case class CollectSet(child: Expression)
    extends TypedImperativeAggregate
    with UnaryLike[Expression] {

  override def dataType: DataType = ArrayType(child.dataType)

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = outputType.intersect(dataType)
    mapChildren(_.resolveDataType(newDataType.getArrayElementType))
  }

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.Mode]].
 */
case class Mode(child: Expression)
    extends TypedImperativeAggregate
    with UnaryLike[Expression]
    with PropagatesDataType {

  override def dataType: DataType = child.dataType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.Product]].
 */
case class Product(child: Expression)
    extends UnaryExpression
    with AggregateFunction
    with DataTypeIsInputType {

  override def inputType: DataType = DoubleType

  override def dataType: DataType = DoubleType

  override def aggBufferDataTypes: IndexedSeq[DataType] = IndexedSeq(dataType)

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.PearsonCorrelation]].
 */
abstract class PearsonCorrelation(x: Expression, y: Expression)
    extends AggregateFunction
    with BinaryLike[Expression]
    with DataTypeIsInputType {

  override def left: Expression = x

  override def right: Expression = y

  override def dataType: DataType = DoubleType

  override def inputType: DataType = DoubleType

  override def aggBufferDataTypes: IndexedSeq[DataType] = {
    IndexedSeq(
      DoubleType,
      DoubleType,
      DoubleType,
      DoubleType,
      DoubleType,
      DoubleType
    )
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.Corr]].
 */
case class Corr(x: Expression, y: Expression) extends PearsonCorrelation(x, y) {

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(x = newLeft, y = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.Covariance]].
 */
abstract class Covariance
    extends AggregateFunction
    with BinaryLike[Expression]
    with DataTypeIsInputType {

  override def dataType: DataType = DoubleType

  override def inputType: DataType = DoubleType

  override def aggBufferDataTypes: IndexedSeq[DataType] = {
    IndexedSeq(DoubleType, DoubleType, DoubleType, DoubleType)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.CovPopulation]].
 */
case class CovPopulation(left: Expression, right: Expression) extends Covariance {

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.CovSample]].
 */
case class CovSample(left: Expression, right: Expression) extends Covariance {

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.CentralMomentAgg]].
 */
abstract class CentralMomentAgg(child: Expression)
    extends UnaryExpression
    with AggregateFunction
    with DataTypeIsInputType {

  override def dataType: DataType = DoubleType

  override def inputType: DataType = DoubleType

  override def aggBufferDataTypes: IndexedSeq[DataType] = {
    IndexedSeq(DoubleType, DoubleType, DoubleType, DoubleType, DoubleType)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.StddevPop]].
 */
case class StddevPop(child: Expression) extends CentralMomentAgg(child) {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.StddevSamp]].
 */
case class StddevSamp(child: Expression) extends CentralMomentAgg(child) {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.VariancePop]].
 */
case class VariancePop(child: Expression) extends CentralMomentAgg(child) {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.VarianceSamp]].
 */
case class VarianceSamp(child: Expression) extends CentralMomentAgg(child) {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.RegrReplacement]].
 */
case class RegrReplacement(child: Expression) extends CentralMomentAgg(child) {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.Skewness]].
 */
case class Skewness(child: Expression) extends CentralMomentAgg(child) {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.Kurtosis]].
 */
case class Kurtosis(child: Expression) extends CentralMomentAgg(child) {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.MaxMinBy]].
 */
abstract class MaxMinBy extends AggregateFunction with BinaryLike[Expression] {

  def valueExpr: Expression

  def orderingExpr: Expression

  override def left: Expression = valueExpr

  override def right: Expression = orderingExpr

  override def dataType: DataType = valueExpr.dataType

  override def aggBufferDataTypes: IndexedSeq[DataType] = {
    IndexedSeq(valueExpr.dataType, orderingExpr.dataType)
  }

  override def resolveDataType(outputType: DataType): Expression = {
    withNewChildren(
      IndexedSeq(
        left.resolveDataType(outputType),
        right.resolveDataType(AnyType)
      )
    )
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.MaxBy]].
 */
case class MaxBy(valueExpr: Expression, orderingExpr: Expression) extends MaxMinBy {

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(valueExpr = newLeft, orderingExpr = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.MinBy]].
 */
case class MinBy(valueExpr: Expression, orderingExpr: Expression) extends MaxMinBy {

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(valueExpr = newLeft, orderingExpr = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.PercentileBase]].
 */
abstract class PercentileBase extends TypedImperativeAggregate {

  def child: Expression

  def percentageExpression: Expression

  protected lazy val resultTypeMap = DataTypeMap(
    Map(
      YearMonthIntervalType -> YearMonthIntervalType,
      DayTimeIntervalType -> DayTimeIntervalType,
      ByteType -> DoubleType,
      ShortType -> DoubleType,
      IntegerType -> DoubleType,
      LongType -> DoubleType,
      FloatType -> DoubleType,
      DoubleType -> DoubleType,
      DecimalType -> DoubleType
    )
  )

  override lazy val dataType: DataType = {
    val resultType = resultTypeMap.outputType(child.dataType)

    val percentageDataType =
      percentageExpression
        .dataType
        .intersect(TypeSet(DoubleType, ArrayType(DoubleType)))

    if (percentageDataType.isInstanceOf[ArrayType]) {
      ArrayType(resultType)
    } else if (percentageDataType == DoubleType) {
      resultType
    } else {
      resultType + ArrayType(resultType)
    }
  }

  protected def inputTypes(outputType: DataType): (DataType, DataType) = {
    val resultTypes = resultTypeMap.map.values.toSet
    val baseOutputType =
      DataType(resultTypes + ArrayType(DataType(resultTypes)))
    val knownOutputType = outputType.intersect(baseOutputType)

    val resultType =
      knownOutputType.transformDown {
        case arrayType: ArrayType =>
          arrayType.elementType
        case dataType =>
          dataType
      }
    val childDataType =
      child.dataType.intersect(resultTypeMap.inputType(resultType))

    val percentageType =
      knownOutputType.transformDown {
        case _: ArrayType =>
          ArrayType(DoubleType)
        case typeSet: TypeSet =>
          typeSet
        case _ =>
          DoubleType
      }

    (childDataType, percentageType)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.Percentile]].
 */
case class Percentile(
    child: Expression,
    percentageExpression: Expression,
    frequencyExpression: Expression,
    reverse: Boolean
) extends PercentileBase
    with TernaryExpression {

  override def first: Expression = child

  override def second: Expression = percentageExpression

  override def third: Expression = frequencyExpression

  override def resolveDataType(outputType: DataType): Expression = {
    val (newChildDataType, newPercentageType) = inputTypes(outputType)
    withNewChildren(
      IndexedSeq(
        child.resolveDataType(newChildDataType),
        percentageExpression.resolveDataType(newPercentageType),
        frequencyExpression.resolveDataType(TypeSet.Integral)
      )
    )
  }

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(child = newFirst, percentageExpression = newSecond, frequencyExpression = newThird)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.PercentileDisc]].
 */
case class PercentileDisc(
    child: Expression,
    percentageExpression: Expression,
    reverse: Boolean,
    legacyCalculation: Boolean
) extends PercentileBase
    with BinaryLike[Expression] {

  override def left: Expression = child

  override def right: Expression = percentageExpression

  override def resolveDataType(outputType: DataType): Expression = {
    val (newChildDataType, newPercentageType) = inputTypes(outputType)
    withNewChildren(
      IndexedSeq(
        child.resolveDataType(newChildDataType),
        percentageExpression.resolveDataType(newPercentageType)
      )
    )
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(child = newLeft, percentageExpression = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile]].
 */
case class ApproximatePercentile(
    child: Expression,
    percentageExpression: Expression,
    accuracyExpression: Expression
) extends TypedImperativeAggregate
    with TernaryLike[Expression] {

  override def first: Expression = child

  override def second: Expression = percentageExpression

  override def third: Expression = accuracyExpression

  override def dataType: DataType = {
    val percentageDataType =
      percentageExpression
        .dataType
        .intersect(TypeSet(DoubleType, ArrayType(DoubleType)))

    if (percentageDataType.isInstanceOf[ArrayType]) {
      ArrayType(child.dataType)
    } else if (percentageDataType == DoubleType) {
      child.dataType
    } else {
      child.dataType + ArrayType(child.dataType)
    }
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val inputType =
      TypeSet.NumericAndAnsiInterval +
        DateType +
        TimestampType +
        TimestampNTZType
    val knownOutputType = outputType.intersect(inputType + ArrayType(inputType))

    val resultType =
      knownOutputType.transformDown {
        case arrayType: ArrayType =>
          arrayType.elementType
        case dataType =>
          dataType
      }
    val newChildDataType = child.dataType.intersect(resultType)

    val newPercentageType =
      knownOutputType.transformDown {
        case _: ArrayType =>
          ArrayType(DoubleType)
        case typeSet: TypeSet =>
          typeSet
        case _ =>
          DoubleType
      }

    withNewChildren(
      IndexedSeq(
        child.resolveDataType(newChildDataType),
        percentageExpression.resolveDataType(newPercentageType),
        accuracyExpression.resolveDataType(TypeSet.Integral)
      )
    )
  }

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(
      child = newFirst,
      percentageExpression = newSecond,
      accuracyExpression = newThird
    )
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.ApproxCountDistinctForIntervals]].
 */
case class ApproxCountDistinctForIntervals(child: Expression, endpointsExpression: Expression)
    extends TypedImperativeAggregate
    with BinaryLike[Expression] {

  override def left: Expression = child

  override def right: Expression = endpointsExpression

  override def dataType: DataType = ArrayType(LongType)

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(child = newLeft, endpointsExpression = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.HyperLogLogPlusPlus]].
 */
case class HyperLogLogPlusPlus(child: Expression, relativeSD: Double)
    extends AggregateFunction
    with UnaryLike[Expression] {

  override def dataType: DataType = LongType

  override def aggBufferDataTypes: IndexedSeq[DataType] = {
    IndexedSeq.tabulate(numWords)(_ => LongType)
  }

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }

  /**
   * [[org.apache.spark.sql.catalyst.util.HyperLogLogPlusPlusHelper.numWords]].
   */
  private def numWords: Int = {
    val p =
      Math.ceil(2.0d * Math.log(1.106d / relativeSD) / Math.log(2.0d)).toInt
    val m = 1 << p
    val WORD_SIZE = java.lang.Long.SIZE
    val REGISTER_SIZE = 6
    val REGISTERS_PER_WORD = WORD_SIZE / REGISTER_SIZE
    m / REGISTERS_PER_WORD + 1
  }
}

object HyperLogLogPlusPlus {

  def apply(child: Expression, relativeSD: Expression): HyperLogLogPlusPlus = {
    new HyperLogLogPlusPlus(child, relativeSD.asInstanceOf[Literal].doubleValue)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.BitAggregate]].
 */
abstract class BitAggregate
    extends AggregateFunction
    with UnaryLike[Expression]
    with DataTypeIsInputType {

  override def inputType: DataType = TypeSet.Integral

  override def dataType: DataType = child.dataType

  override def aggBufferDataTypes: IndexedSeq[DataType] = {
    IndexedSeq(child.dataType)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.BitAndAgg]].
 */
case class BitAndAgg(child: Expression) extends BitAggregate {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.BitOrAgg]].
 */
case class BitOrAgg(child: Expression) extends BitAggregate {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.BitXorAgg]].
 */
case class BitXorAgg(child: Expression) extends BitAggregate {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate]].
 */
case class BloomFilterAggregate(
    child: Expression,
    estimatedNumItemsExpression: Expression,
    numBitsExpression: Expression
) extends TypedImperativeAggregate
    with TernaryLike[Expression] {

  override def first: Expression = child

  override def second: Expression = estimatedNumItemsExpression

  override def third: Expression = numBitsExpression

  override def dataType: DataType = BinaryType

  override def resolveDataType(outputType: DataType): Expression = {
    withNewChildren(
      IndexedSeq(
        child.resolveDataType(TypeSet.Integral + StringType),
        estimatedNumItemsExpression.resolveDataType(LongType),
        numBitsExpression.resolveDataType(LongType)
      )
    )
  }

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(
      child = newFirst,
      estimatedNumItemsExpression = newSecond,
      numBitsExpression = newThird
    )
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.CountMinSketchAgg]].
 */
case class CountMinSketchAgg(
    child: Expression,
    epsExpression: Expression,
    confidenceExpression: Expression,
    seedExpression: Expression
) extends TypedImperativeAggregate
    with QuaternaryLike[Expression] {

  override def first: Expression = child

  override def second: Expression = epsExpression

  override def third: Expression = confidenceExpression

  override def fourth: Expression = seedExpression

  override def dataType: DataType = BinaryType

  override def resolveDataType(outputType: DataType): Expression = {
    withNewChildren(
      IndexedSeq(
        child.resolveDataType(TypeSet.Integral + StringType + BinaryType),
        epsExpression.resolveDataType(DoubleType),
        confidenceExpression.resolveDataType(DoubleType),
        seedExpression.resolveDataType(IntegerType)
      )
    )
  }

  override def withNewChildrenInternal(
      first: Expression,
      second: Expression,
      third: Expression,
      fourth: Expression
  ): Expression = {
    copy(
      child = first,
      epsExpression = second,
      confidenceExpression = third,
      seedExpression = fourth
    )
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.HistogramNumeric]].
 */
case class HistogramNumeric(child: Expression, nBins: Expression)
    extends TypedImperativeAggregate
    with BinaryLike[Expression] {

  override def left: Expression = child

  override def right: Expression = nBins

  override def dataType: DataType = {
    ArrayType(
      StructType(
        Array(
          StructField("x", DoubleType + left.dataType),
          StructField("y", DoubleType)
        )
      )
    )
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val childType =
      TypeSet.Numeric +
        DateType +
        TimestampType +
        TimestampNTZType +
        YearMonthIntervalType +
        DayTimeIntervalType

    withNewChildren(
      IndexedSeq(
        child.resolveDataType(childType),
        nBins.resolveDataType(IntegerType)
      )
    )
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(child = newLeft, nBins = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.PivotFirst]].
 */
case class PivotFirst(
    pivotColumn: Expression,
    valueColumn: Expression,
    pivotColumnValues: IndexedSeq[Literal]
) extends AggregateFunction {

  override def dataType: DataType = ArrayType(valueColumn.dataType)

  override def children: IndexedSeq[Expression] = {
    pivotColumn +: valueColumn +: pivotColumnValues
  }

  override def aggBufferDataTypes: IndexedSeq[DataType] = {
    val valueDataType = valueColumn.dataType
    pivotColumnValues
      .map(_.stringValue)
      .toSet
      .toIndexedSeq
      .map((_: String) => valueDataType)
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = outputType.intersect(dataType)
    val pivotDataType = pivotColumnValues
      .map(_.dataType)
      .fold(pivotColumn.dataType)(DataType.intersection)

    withNewChildren(
      IndexedSeq(
        pivotColumn.resolveDataType(pivotDataType),
        valueColumn.resolveDataType(newDataType.getArrayElementType)
      ) ++
        pivotColumnValues.map(_.resolveDataType(pivotDataType))
    )
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(
      pivotColumn = newChildren(0),
      valueColumn = newChildren(1),
      pivotColumnValues = newChildren.tail.tail.asInstanceOf[IndexedSeq[Literal]]
    )
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.HllSketchAgg]].
 */
case class HllSketchAgg(left: Expression, right: Expression)
    extends TypedImperativeAggregate
    with BinaryLike[Expression] {

  override def dataType: DataType = BinaryType

  override def resolveDataType(outputType: DataType): Expression = {
    val leftType = TypeSet(IntegerType, LongType, StringType, BinaryType)
    val rightType = IntegerType
    withNewChildren(
      IndexedSeq(
        left.resolveDataType(leftType),
        right.resolveDataType(rightType)
      )
    )
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.HllUnionAgg]].
 */
case class HllUnionAgg(left: Expression, right: Expression)
    extends TypedImperativeAggregate
    with BinaryLike[Expression] {

  override def dataType: DataType = BinaryType

  override def resolveDataType(outputType: DataType): Expression = {
    withNewChildren(
      IndexedSeq(
        left.resolveDataType(BinaryType),
        right.resolveDataType(BooleanType)
      )
    )
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.RegrR2]].
 */
case class RegrR2(y: Expression, x: Expression) extends PearsonCorrelation(y, x) {

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(y = newLeft, x = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.RegrSXY]].
 */
case class RegrSXY(y: Expression, x: Expression) extends Covariance {

  override def left: Expression = y

  override def right: Expression = x

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(y = newLeft, x = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.RegrSlope]].
 */
case class RegrSlope(left: Expression, right: Expression)
    extends AggregateFunction
    with BinaryLike[Expression]
    with DataTypeIsInputType {

  private def covarPop = CovPopulation(right, left)

  private def varPop = VariancePop(right)

  override def inputType: DataType = DoubleType

  override def dataType: DataType = DoubleType

  override def aggBufferDataTypes: IndexedSeq[DataType] = {
    covarPop.aggBufferDataTypes ++ varPop.aggBufferDataTypes
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.RegrIntercept]].
 */
case class RegrIntercept(left: Expression, right: Expression)
    extends AggregateFunction
    with BinaryLike[Expression]
    with DataTypeIsInputType {

  private def covarPop = CovPopulation(right, left)

  private def varPop = VariancePop(right)

  override def inputType: DataType = DoubleType

  override def dataType: DataType = DoubleType

  override def aggBufferDataTypes: IndexedSeq[DataType] = {
    covarPop.aggBufferDataTypes ++ varPop.aggBufferDataTypes
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.ApproxTopK]].
 */
case class ApproxTopK(expr: Expression, k: Expression, maxItemsTracked: Expression)
    extends TypedImperativeAggregate
    with TernaryLike[Expression] {

  override def first: Expression = expr

  override def second: Expression = k

  override def third: Expression = maxItemsTracked

  override def dataType: DataType = {
    ArrayType(
      StructType(
        Array(
          StructField("item", expr.dataType),
          StructField("count", LongType)
        )
      )
    )
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = outputType.intersect(dataType)
    val exprDataType = newDataType.getArrayElementType.getStructFields(2).head.dataType
    withNewChildren(
      IndexedSeq(
        expr.resolveDataType(exprDataType),
        k.resolveDataType(IntegerType),
        maxItemsTracked.resolveDataType(IntegerType)
      )
    )
  }

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(expr = newFirst, k = newSecond, maxItemsTracked = newThird)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.ApproxTopKAccumulate]].
 */
case class ApproxTopKAccumulate(expr: Expression, maxItemsTracked: Expression)
    extends TypedImperativeAggregate
    with BinaryLike[Expression] {

  override def left: Expression = expr

  override def right: Expression = maxItemsTracked

  override def dataType: DataType = {
    StructType(
      Array(
        StructField("sketch", BinaryType),
        StructField("maxItemsTracked", IntegerType),
        StructField("itemDataType", expr.dataType),
        StructField("itemDataTypeDDL", StringType)
      )
    )
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = outputType.intersect(dataType)
    val exprDataType = newDataType.getStructFields(4).apply(2).dataType
    withNewChildren(
      IndexedSeq(
        expr.resolveDataType(exprDataType),
        maxItemsTracked.resolveDataType(IntegerType)
      )
    )
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(expr = newLeft, maxItemsTracked = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.ApproxTopKCombine]].
 */
case class ApproxTopKCombine(state: Expression, maxItemsTracked: Expression)
    extends TypedImperativeAggregate
    with BinaryLike[Expression]
    with ExpectsInputTypes {

  override def left: Expression = state

  override def right: Expression = maxItemsTracked

  private def itemDataType: DataType = {
    state.dataType.getStructFields(4).apply(2).dataType
  }

  override def dataType: DataType = {
    StructType(
      Array(
        StructField("sketch", BinaryType),
        StructField("maxItemsTracked", IntegerType),
        StructField("itemDataType", itemDataType),
        StructField("itemDataTypeDDL", StringType)
      )
    )
  }

  override def inputTypes: Seq[DataType] = {
    Seq(
      StructType(
        Array(
          StructField("sketch", BinaryType),
          StructField("maxItemsTracked", IntegerType),
          StructField("itemDataType", AnyType),
          StructField("itemDataTypeDDL", StringType)
        )
      ),
      IntegerType
    )
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = outputType.intersect(dataType)
    withNewChildren(
      IndexedSeq(
        state.resolveDataType(newDataType),
        maxItemsTracked.resolveDataType(IntegerType)
      )
    )
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(state = newLeft, maxItemsTracked = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.ListAgg]].
 */
case class ListAgg(
    child: Expression,
    delimiter: Expression,
    orderExpressions: IndexedSeq[SortOrder]
) extends TypedImperativeAggregate {

  override def dataType: DataType = child.dataType

  override def children: IndexedSeq[Expression] = {
    child +: delimiter +: orderExpressions
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = outputType.intersect(child.dataType).intersect(delimiter.dataType)
    val childDataType = newDataType.intersect(TypeSet(StringType, BinaryType))
    val delimiterType = newDataType.intersect(TypeSet(StringType, BinaryType, NullType))

    withNewChildren(
      IndexedSeq(
        child.resolveDataType(childDataType),
        delimiter.resolveDataType(delimiterType)
      ) ++ orderExpressions.map(_.resolveDataType(AnyType))
    )
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(
      child = newChildren.head,
      delimiter = newChildren(1),
      orderExpressions = newChildren.tail.tail.asInstanceOf[IndexedSeq[SortOrder]]
    )
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.ThetaSketchAgg]].
 */
case class ThetaSketchAgg(left: Expression, right: Expression)
    extends TypedImperativeAggregate
    with BinaryLike[Expression]
    with ExpectsInputTypes {

  override def dataType: DataType = BinaryType

  override def inputTypes: Seq[DataType] = {
    Seq(
      TypeSet(
        ArrayType(IntegerType),
        ArrayType(LongType),
        BinaryType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        StringType
      ),
      IntegerType
    )
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.ThetaUnionAgg]].
 */
case class ThetaUnionAgg(left: Expression, right: Expression)
    extends TypedImperativeAggregate
    with BinaryLike[Expression]
    with ExpectsInputTypes {

  override def dataType: DataType = BinaryType

  override def inputTypes: Seq[DataType] = Seq(BinaryType, IntegerType)

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.ThetaIntersectionAgg]].
 */
case class ThetaIntersectionAgg(child: Expression)
    extends TypedImperativeAggregate
    with UnaryLike[Expression]
    with ExpectsInputType {

  override def dataType: DataType = BinaryType

  override def inputType: DataType = BinaryType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.KllSketchAggBigint]].
 */
case class KllSketchAggBigint(child: Expression, kExpr: Option[Expression])
    extends TypedImperativeAggregate
    with ExpectsInputTypes {

  override def dataType: DataType = BinaryType

  override def children: IndexedSeq[Expression] = child +: kExpr.toIndexedSeq

  override def inputTypes: Seq[DataType] = Seq(TypeSet.Integral, IntegerType)

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(
      child = newChildren.head,
      kExpr = kExpr.map(_ => newChildren(1))
    )
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.KllSketchAggFloat]].
 */
case class KllSketchAggFloat(child: Expression, kExpr: Option[Expression])
    extends TypedImperativeAggregate
    with ExpectsInputTypes {

  override def dataType: DataType = BinaryType

  override def children: IndexedSeq[Expression] = child +: kExpr.toIndexedSeq

  override def inputTypes: Seq[DataType] = Seq(FloatType, IntegerType)

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(
      child = newChildren.head,
      kExpr = kExpr.map(_ => newChildren(1))
    )
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.KllSketchAggDouble]].
 */
case class KllSketchAggDouble(child: Expression, kExpr: Option[Expression])
    extends TypedImperativeAggregate
    with ExpectsInputTypes {

  override def dataType: DataType = BinaryType

  override def children: IndexedSeq[Expression] = child +: kExpr.toIndexedSeq

  override def inputTypes: Seq[DataType] = {
    Seq(TypeSet(FloatType, DoubleType), IntegerType)
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(
      child = newChildren.head,
      kExpr = kExpr.map(_ => newChildren(1))
    )
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.variant.SchemaOfVariantAgg]].
 */
case class SchemaOfVariantAgg(child: Expression)
    extends TypedImperativeAggregate
    with UnaryLike[Expression]
    with ExpectsInputType {

  override def dataType: DataType = StringType

  override def inputType: DataType = VariantType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}
