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

package com.xonai.spark.sql.support

import com.xonai.spark.sql.parser.expressions.{AttributeReference, CurrentRow, ElementAt, Explode, Expression, FrameLessOffsetWindowFunction, HyperLogLogCardinality, HyperLogLogInitSimpleAgg, Like, Literal, PromotePrecision, RowFrame, SpecifiedWindowFrame, StringTranslate, UnboundedFollowing, UnboundedPreceding, WindowExpression}
import com.xonai.spark.sql.parser.plans.{AQEShuffleReadExec, AdaptiveSparkPlanExec, BaseAggregateExec, BatchScanExec, BroadcastExchangeExec, BroadcastQueryStageExec, BuildRight, ColumnarToRowExec, CustomShuffleReaderExec, ExistenceJoin, FileSourceScanExec, FullOuter, HashJoin, HashPartitioning, HashedRelationBroadcastMode, Inner, InputAdapterExec, LeftAnti, LeftOuter, LeftSemi, RangePartitioning, ReusedExchangeExec, ReusedSubqueryExec, ShuffleExchangeExec, ShuffleQueryStageExec, SinglePartition, SortMergeJoinExec, SparkPlan, SubqueryBroadcastExec, SubqueryExec, TableCacheQueryStageExec, WholeStageCodegenExec}
import com.xonai.spark.sql.parser.types.{StringType, TypeSet}
import org.apache.spark.sql.execution.RowToColumnarExec

/**
 * Provides compatibility information with Xonai's Spark engine.
 */
class XonaiSupport extends EngineSupport {

  private val planTypeChecker =
    NodeTypeChecker.fromResource("com/xonai/spark/sql/support/xonai/plans.csv")

  private val expressionTypeChecker =
    NodeTypeChecker.fromResource("com/xonai/spark/sql/support/xonai/expressions.csv")

  private val supportedOperators =
    Seq(
      // Neutral - don't require conversion.
      classOf[WholeStageCodegenExec],
      classOf[InputAdapterExec],
      classOf[ColumnarToRowExec],
      classOf[RowToColumnarExec],
      classOf[AdaptiveSparkPlanExec],
      classOf[AQEShuffleReadExec],
      classOf[CustomShuffleReaderExec],
      classOf[ShuffleQueryStageExec],
      classOf[BroadcastQueryStageExec],
      classOf[TableCacheQueryStageExec],
      classOf[ReusedExchangeExec],
      classOf[ReusedSubqueryExec],
      classOf[SubqueryExec],
      // Manually converted.
      classOf[SubqueryBroadcastExec]
    )
      .map(_.getSimpleName)
      .toSet

  override def check(plan: SparkPlan): NodeSupport = {
    val className = plan.getClass.getSimpleName
    if (supportedOperators.contains(className)) {
      return NodeSupport.Supported
    }

    // Manual check.
    val manualSupport =
      plan match {
        case scan: BatchScanExec
            if scan.format.toLowerCase != "parquet" =>
          return NodeSupport.NotImplemented
        case exchange: BroadcastExchangeExec
            if exchange.mode.isInstanceOf[HashedRelationBroadcastMode] &&
              exchange.mode.asInstanceOf[HashedRelationBroadcastMode]
                .key
                .exists(!_.isCompatibleWith(TypeSet.Atomic)) =>
          NodeSupport(Set("UnsupportedBroadcastDataType"))
        case exchange: ShuffleExchangeExec
            if exchange.outputPartitioning != SinglePartition &&
              exchange.outputPartitioning != RangePartitioning &&
              exchange.outputPartitioning != HashPartitioning =>
          NodeSupport(Set("UnsupportedPartitioning"))
        case agg: BaseAggregateExec =>
          val filterSupport =
            if (agg.aggregateExpressions.exists(_.filter.isDefined)) {
              NodeSupport(Set("UnsupportedAggregateFilter"))
            } else {
              NodeSupport.Supported
            }
          val stringAggregateSupport =
            if (
              agg.groupingExpressions.isEmpty &&
              agg.aggregateExpressions.exists(_.dataType == StringType)
            ) {
              NodeSupport(Set("UnsupportedAggregateDataType"))
            } else {
              NodeSupport.Supported
            }
          val groupingSupport =
            if (
              agg.groupingExpressions
                .map(_.dataType)
                .filter(_.isDefined)
                .exists(!TypeSet.Atomic.types.contains(_))
            ) {
              NodeSupport(Set("UnsupportedKeyDataType"))
            } else {
              NodeSupport.Supported
            }

          filterSupport + stringAggregateSupport + groupingSupport
        case join: HashJoin
            if join.joinType != Inner &&
              join.joinType != LeftSemi &&
              join.joinType != LeftAnti &&
              !(join.joinType == LeftOuter && join.buildSide == BuildRight) &&
              !join.joinType.isInstanceOf[ExistenceJoin] =>
          NodeSupport(Set("UnsupportedJoinType"))
        case join: SortMergeJoinExec
            if join.joinType != Inner &&
              join.joinType != LeftSemi &&
              join.joinType != LeftAnti &&
              join.joinType != LeftOuter &&
              join.joinType != FullOuter &&
              !join.joinType.isInstanceOf[ExistenceJoin] =>
          NodeSupport(Set("UnsupportedJoinType"))
        case _ =>
          NodeSupport.Supported
      }

    // Check with support file.
    val variation =
      plan match {
        case scan: FileSourceScanExec =>
          Some(scan.format.toLowerCase)
        case _ =>
          None
      }
    val dataTypeSupport = planTypeChecker.checkPlan(plan, variation)

    manualSupport + dataTypeSupport
  }

  override def check(expression: Expression): NodeSupport = {
    // Handle exceptions.
    expression match {
      case e: HyperLogLogInitSimpleAgg
          if e.impl != "AgKn" =>
        return NodeSupport(Set("UnsupportedImplementation"))
      case e: HyperLogLogCardinality
          if e.impl != "AgKn" =>
        return NodeSupport(Set("UnsupportedImplementation"))
      case _: PromotePrecision =>
        return NodeSupport.Supported
      case e: ElementAt
          if e.defaultValueOutOfBound.isDefined =>
        return NodeSupport(Set("UnsupportedDefaultValue"))
      case e: Explode
          if !e.child.isInstanceOf[Literal] && !e.child.isInstanceOf[AttributeReference] =>
        return NodeSupport(Set("UnsupportedInput"))
      case e: Like
          if !e.right.isInstanceOf[Literal] =>
        return NodeSupport(Set("UnsupportedNonStatic"))
      case e: StringTranslate
          if !e.matchingExpr.isInstanceOf[Literal] || !e.replaceExpr.isInstanceOf[Literal] =>
        return NodeSupport(Set("UnsupportedNonStatic"))
      case e: WindowExpression =>
        val frame = e.windowSpec.frameSpecification.asInstanceOf[SpecifiedWindowFrame]
        if (
          (
            frame.lower != UnboundedPreceding ||
              (
                frame.upper != UnboundedFollowing &&
                  frame.upper != CurrentRow &&
                  frame.frameType != RowFrame
              )
          ) &&
          !e.windowFunction.isInstanceOf[FrameLessOffsetWindowFunction]
        ) {
          return NodeSupport(Set("UnsupportedFrameBounds"))
        }
      case _ =>
    }

    expressionTypeChecker.checkExpression(expression)
  }
}
