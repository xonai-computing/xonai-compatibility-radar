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

package com.xonai.spark.sql.parser

import com.xonai.spark.sql.parser.commands.{Command, CreateViewCommand, InsertIntoHadoopFsRelationCommand, UnknownCommand}
import com.xonai.spark.sql.parser.expressions.{AggregateExpression, Alias, Attribute, AttributeReference, NamedExpression, SortOrder, Sum, UndefinedExpression, UndefinedWindowExpression, UnknownExpression}
import com.xonai.spark.sql.parser.metric.SQLMetric
import com.xonai.spark.sql.parser.plans.{AQEShuffleReadExec, AdaptiveSparkPlanExec, AppendColumnsExec, BaseAggregateExec, BaseSubqueryExec, BatchScanExec, BroadcastExchangeExec, BroadcastHashJoinExec, BroadcastMode, BroadcastNestedLoopJoinExec, BroadcastPartitioning, BroadcastQueryStageExec, BuildLeft, BuildRight, BuildSide, CartesianProductExec, CoalesceExec, CoalescedHashPartitioning, CollectLimitExec, CollectTailExec, ColumnarToRowExec, Cross, CustomShuffleReaderExec, DataWritingCommandExec, DeserializeToObjectExec, ExecutedCommandExec, ExistenceJoin, ExpandExec, ExternalRDDScanExec, FileSourceScanExec, FilterExec, FullOuter, GenerateExec, GlobalLimitExec, HashAggregateExec, HashPartitioning, HashedRelationBroadcastMode, HiveTableScanExec, IdentityBroadcastMode, InMemoryTableScanExec, Inner, InputAdapterExec, JoinType, KeyGroupedPartitioning, LeftAnti, LeftOuter, LeftSemi, LocalLimitExec, LocalTableScanExec, MapElementsExec, MapGroupsExec, MapPartitionsExec, ObjectHashAggregateExec, Partitioning, PartitioningCollection, ProjectExec, RDDScanExec, RangeExec, RangePartitioning, ResultQueryStageExec, ReusedExchangeExec, ReusedSubqueryExec, RightOuter, RoundRobinPartitioning, RowDataSourceScanExec, RowToColumnarExec, SampleExec, SerializeFromObjectExec, ShuffleExchangeExec, ShuffleQueryStageExec, ShuffledHashJoinExec, SinglePartition, SortAggregateExec, SortExec, SortMergeJoinExec, SparkPlan, SubqueryBroadcastExec, SubqueryExec, TableCacheQueryStageExec, TakeOrderedAndProjectExec, UnaryExecNode, UnionExec, UnknownBroadcastMode, UnknownExec, UnknownPartitioning, UnknownToParserPartitioning, WholeStageCodegenExec, WindowExec, WindowGroupLimitExec, WindowGroupLimitMode, WriteFilesExec}
import com.xonai.spark.sql.parser.types.{AnyType, ArrayType, BooleanType, LongType, StructType}
import org.apache.spark.sql.execution.SparkPlanInfo

import java.util
import java.util.regex.Pattern
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

class SparkPlanParser(
    planDescription: Option[String] = None,
    metricValues: Map[Long, String] = Map.empty
) extends Parser {

  private val subqueries = mutable.Map[String, BaseSubqueryExec]()

  private val expressionParser = new ExpressionParser(subqueries.get)

  private val metricParser = new SQLMetricParser()

  private val parsedPlans = new util.HashMap[SparkPlanInfo, SparkPlan]()

  private lazy val (planDescriptionNodesByName, planDescriptionNodesById) = {
    val nodes = planDescription.map(SparkPlanParser.parsePlanDescription).getOrElse(Array.empty)
    (
      nodes.groupBy(_.name),
      nodes.groupBy(_.id).iterator.map { case (key, values) => (key, values.head) }.toMap
    )
  }

  def parse(plan: SparkPlanInfo): SparkPlan = {
    parse(plan, null)
  }

  private def parse(plan: SparkPlanInfo, parent: SparkPlanInfo): SparkPlan = {
    if (parsedPlans.containsKey(plan)) {
      return parsedPlans.get(plan)
    }

    val parsedPlan = plan.nodeName match {
      case "AdaptiveSparkPlan" =>
        AdaptiveSparkPlanExec(parseChild(plan))
      case "AQEShuffleRead" =>
        AQEShuffleReadExec(parseChild(plan))
      case "AppendColumns" =>
        parseAppendColumns(plan)
      case _
          if isBatchScan(plan) =>
        parseBatchScan(plan)
      case "BroadcastExchange" =>
        parseBroadcastExchange(plan)
      case "BroadcastHashJoin" =>
        parseBroadcastHashJoin(plan)
      case "BroadcastNestedLoopJoin" =>
        parseBroadcastNestedLoopJoin(plan)
      case "BroadcastQueryStage" =>
        BroadcastQueryStageExec(parseChild(plan))
      case "CartesianProduct" =>
        parseCartesianProduct(plan)
      case "Coalesce" =>
        parseCoalesce(plan)
      case "CollectLimit" =>
        parseCollectLimit(plan)
      case "CollectTail" =>
        parseCollectTail(plan)
      case "ColumnarToRow" =>
        ColumnarToRowExec(parseChild(plan), getMetrics(plan))
      case "CustomShuffleReader" =>
        CustomShuffleReaderExec(parseChild(plan))
      case "DeserializeToObject" =>
        parseDeserializeToObject(plan)
      case "Exchange" =>
        parseExchange(plan)
      case _
          if isExecute(plan) =>
        parseExecute(plan)
      case "Expand" =>
        parseExpand(plan)
      case _
          if isFileSourceScan(plan) =>
        parseFileSourceScan(plan)
      case "Filter" =>
        parseFilter(plan)
      case "Generate" =>
        parseGenerate(plan, parent)
      case "GlobalLimit" =>
        parseGlobalLimit(plan)
      case "HashAggregate" =>
        parseAggregate(plan, HashAggregateExec)
      case _
          if isHiveTableScan(plan) =>
        parseHiveTableScan(plan)
      case _
          if isInMemoryTableScan(plan) =>
        parseInMemoryTableScan(plan)
      case "InputAdapter" =>
        InputAdapterExec(parseChild(plan))
      case "LocalLimit" =>
        parseLocalLimit(plan)
      case "LocalTableScan" =>
        parseLocalTableScan(plan)
      case "MapElements" =>
        parseMapElements(plan)
      case "MapGroups" =>
        parseMapGroups(plan)
      case "MapPartitions" =>
        parseMapPartitions(plan)
      case "ObjectHashAggregate" =>
        parseAggregate(plan, ObjectHashAggregateExec)
      case "Project" =>
        parseProject(plan)
      case "Range" =>
        parseRange(plan)
      case _
          if isRDDScan(plan) =>
        parseRDDScan(plan)
      case "ResultQueryStage" =>
        ResultQueryStageExec(parseChild(plan))
      case "ReusedExchange" =>
        parseReusedExchange(plan)
      case "ReusedSubquery" =>
        val child = parseChild(plan)
        val reused = ReusedSubqueryExec(child)
        val cacheName =
          child match {
            case exec: SubqueryBroadcastExec =>
              exec.name
            case _ =>
              plan.simpleString
          }
        cacheSubquery(cacheName, reused)
      case "RowToColumnar" =>
        RowToColumnarExec(parseChild(plan), getMetrics(plan))
      case "Sample" =>
        SampleExec(parseChild(plan), getMetrics(plan))
      case "SerializeFromObject" =>
        parseSerializeFromObject(plan)
      case _
          if plan.nodeName.startsWith("ShuffledHashJoin") =>
        parseShuffledHashJoin(plan)
      case "ShuffleQueryStage" =>
        ShuffleQueryStageExec(parseChild(plan))
      case "Sort" =>
        parseSort(plan)
      case "SortAggregate" =>
        parseAggregate(plan, SortAggregateExec)
      case _
          if plan.nodeName.startsWith("SortMergeJoin") =>
        parseSortMergeJoin(plan)
      case "Subquery" =>
        parseSubquery(plan)
      case "SubqueryBroadcast" =>
        parseSubqueryBroadcast(plan)
      case "TableCacheQueryStage" =>
        TableCacheQueryStageExec(parseChild(plan))
      case "TakeOrderedAndProject" =>
        parseTakeOrderedAndProject(plan)
      case "Union" =>
        UnionExec(plan.children.map(parse(_, plan)).toIndexedSeq)
      case nodeName
          if nodeName.startsWith("WholeStageCodegen") =>
        WholeStageCodegenExec(parseChild(plan), getMetrics(plan))
      case "Window" =>
        parseWindow(plan)
      case "WindowGroupLimit" =>
        parseWindowGroupLimit(plan)
      case "WriteFiles" =>
        WriteFilesExec(parseChild(plan))
      case _
          if isRowDataSourceScan(plan) =>
        parseRowDataSourceScan(plan)
      case _
          if isExternalRDDScan(plan) =>
        parseExternalRDDScan(plan)
      case _ =>
        parseUnknownExec(plan)
    }

    // Propagate output if missing to child of type UnknownExec.
    val enrichedParsedPlan =
      Option(parsedPlan)
        .collect { case exec: UnaryExecNode => exec.child }
        .collect { case exec: UnknownExec if exec.output.isEmpty => exec }
        .map { unknownExec =>
          val inputStr = getNodeFieldOption(plan.nodeName, "Input")
          if (inputStr.isDefined) {
            val output = parseUnknownExecOutput(inputStr.get, unknownExec.children)
            parsedPlan.withNewChildren(IndexedSeq(unknownExec.copy(output = output)))
          } else {
            parsedPlan
          }
        }
        .getOrElse(parsedPlan)

    parsedPlans.put(plan, enrichedParsedPlan)
    enrichedParsedPlan
  }

  private def parseChild(plan: SparkPlanInfo): SparkPlan = {
    val children = plan.children.map(parse(_, plan))
    children.head
  }

  private def parseBinaryChildren(plan: SparkPlanInfo): (SparkPlan, SparkPlan) = {
    val children = plan.children.map(parse(_, plan))
    (children.head, children(1))
  }

  private def parseArguments(plan: SparkPlanInfo): IndexedSeq[String] = {
    val argumentsStr = plan.simpleString.substring(plan.nodeName.length)
    splitList(argumentsStr)
  }

  private def parseParenthesesArguments(plan: SparkPlanInfo): IndexedSeq[String] = {
    val argumentsStr = plan.simpleString.substring(plan.nodeName.length)
    splitParenthesesList(argumentsStr.trim)
  }

  /**
   * Parses metrics using [[SQLMetricParser]].
   */
  private def getMetrics(plan: SparkPlanInfo): Map[String, SQLMetric] = {
    plan
      .metrics
      .map { metricInfo =>
        val name = metricInfo.name.trim
        val value = metricValues
          .get(metricInfo.accumulatorId)
          .map(metricParser.parseValue(_, metricInfo.metricType))
          .getOrElse(0L)
        val metric = SQLMetric(metricInfo.accumulatorId, metricInfo.metricType, value)

        (name, metric)
      }
      .toMap
  }

  private def cacheSubquery(simpleString: String, subquery: BaseSubqueryExec): BaseSubqueryExec = {
    subqueries.put(simpleString, subquery)
    subquery
  }

  /**
   * Keeps as much information as possible.
   */
  private def parseUnknownExec(plan: SparkPlanInfo): SparkPlan = {
    val children = plan.children.map(parse(_, plan)).toIndexedSeq
    // Scan nodes may have more than 2 words with table name or file path.
    val nodeName = plan.nodeName.split(' ').take(2).mkString(" ")
    val output =
      getNodeFieldOption(plan.nodeName, "Output")
        .map(parseUnknownExecOutput(_, children))
        .getOrElse(Seq.empty)

    UnknownExec(nodeName, plan.simpleString, children, output, getMetrics(plan))
  }

  private def parseUnknownExecOutput(
      outputStr: String,
      children: IndexedSeq[SparkPlan]
  ): Seq[Attribute] = {
    val nonSubqueries = children
      .filterNot(_.isInstanceOf[BaseSubqueryExec])
      .filterNot(_.isInstanceOf[UnknownExec])
    val input = nonSubqueries.flatMap(_.output)

    expressionParser
      .withInput(input)(_.parseNamedList(outputStr))
      .asInstanceOf[Seq[Attribute]]
  }

  private def isFileSourceScan(plan: SparkPlanInfo): Boolean = {
    plan.simpleString.startsWith("FileScan")
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "Scan parquet ",
   *   "simpleString": "FileScan parquet [booleans#0,doubles#6]" +
   *     " Batched: true," +
   *     " DataFilters: [isnotnull(booleans#0), booleans#0]," +
   *     " Format: Parquet," +
   *     " Location: InMemoryFileIndex(1 paths)[file:/home/test/parquet]," +
   *     " PartitionFilters: []," +
   *     " PushedFilters: [IsNotNull(booleans), EqualTo(booleans,true)]," +
   *     " ReadSchema: struct<booleans:boolean,doubles:double>",
   *   "children": [],
   *   "metadata": {
   *     "Location": "InMemoryFileIndex(1 paths)[file:/home/test/parquet]",
   *     "ReadSchema": "struct<booleans:boolean,doubles:double>",
   *     "Format": "Parquet",
   *     "Batched": "true",
   *     "PartitionFilters": "[]",
   *     "PushedFilters": "[IsNotNull(booleans), EqualTo(booleans,true)]",
   *     "DataFilters": "[isnotnull(booleans#0), booleans#0]"
   *   },
   *   "metrics": ...
   * }
   * }}}
   *
   * Plan description node:
   *
   * {{{
   * (1) Scan parquet
   * Output [2]: [booleans#0, doubles#6]
   * Batched: true
   * Location: InMemoryFileIndex [file:/home/test/parquet]
   * PushedFilters: [IsNotNull(booleans), EqualTo(booleans,true)]
   * ReadSchema: struct<booleans:boolean,doubles:double>
   * }}}
   */
  private def parseFileSourceScan(plan: SparkPlanInfo): SparkPlan = {
    plan.children.foreach(parse(_, plan))

    val format = plan.metadata.getOrElse("Format", "unknown")
    val schemaStr = plan.metadata("ReadSchema")
    val schema = DataTypeParser.parseCatalogString(schemaStr).asInstanceOf[StructType]

    // Parse output from SparkPlanInfo.simpleString.
    val outputStart = plan.simpleString.indexOf("[")
    val outputEnd = plan.simpleString.indexOf("] ")
    val outputStr = plan.simpleString.substring(outputStart, outputEnd + 1)
    val (outputPrefix, outputUnknown) = getStringFields(outputStr)

    // If incomplete get the output from the plan description.
    val untypedOutput =
      if (outputUnknown == 0) {
        expressionParser.parseNamedList(outputStr)
      } else {
        val node = getNodeWithUnspacedOutput(plan, outputPrefix)
        expressionParser.parseNamedList(node.fields("Output"))
      }

    // Add types to output using schema.
    val output = untypedOutput.zipWithIndex.map { case (attribute: Attribute, i) =>
      if (i < schema.fields.length) {
        attribute.resolveDataType(schema.fields(i).dataType).asInstanceOf[Attribute]
      } else {
        attribute
      }
    }

    // Parse PartitionFilters.
    val partitionFiltersStr = plan.metadata.getOrElse("PartitionFilters", "[]")
    val partitionFilters = expressionParser
      .withInput(output)(_.parseSquareList(partitionFiltersStr))
      .map(_.resolveDataType(BooleanType))

    FileSourceScanExec(format, partitionFilters, output, getMetrics(plan))
  }

  private def isBatchScan(plan: SparkPlanInfo): Boolean = {
    plan.simpleString.startsWith("BatchScan")
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "BatchScan parquet file:/tmp/spark6521440843925085432",
   *   "simpleString": "BatchScan parquet file:/tmp/spark6521440843925085432" +
   *     "[booleans#0,doubles#6]" +
   *     " ParquetScan" +
   *     " DataFilters: []," +
   *     " Format: parquet," +
   *     " Location: InMemoryFileIndex(1 paths)[file:/tmp/spark6521440843925085432]," +
   *     " PartitionFilters: []," +
   *     " PushedAggregation: []," +
   *     " PushedFilters: []," +
   *     " PushedGroupBy: []," +
   *     " ReadSchema: struct<booleans:boolean,doubles:double>" +
   *     " RuntimeFilters: []",
   *   "children": [],
   *   "metadata": {},
   *   "metrics": ...
   * }
   * }}}
   *
   * Plan description node:
   *
   * {{{
   * (1) BatchScan parquet file:/tmp/spark6521440843925085432
   * Output [2]: [booleans#0, doubles#6]
   * Format: parquet
   * Location: InMemoryFileIndex(1 paths)[file:/tmp/spark6521440843925085432]
   * ReadSchema: struct<booleans:boolean,doubles:double>
   * }}}
   */
  private def parseBatchScan(plan: SparkPlanInfo): SparkPlan = {
    plan.children.foreach(parse(_, plan))

    val arguments = parseArguments(plan)

    // Parse format.
    val format = arguments
      .find(_.startsWith("Format:"))
      .flatMap(_.split(' ').tail.headOption)
      .getOrElse("unknown")

    // Parse output from `simpleString`.
    val outputStart = plan.simpleString.indexOf("[")
    val outputEnd = plan.simpleString.indexOf("] ")
    val outputStr = plan.simpleString.substring(outputStart, outputEnd + 1)
    val (outputPrefix, _) = getStringFields(outputStr)

    // Parse schema and output from description node.
    val descriptionNode = getNodeWithOutput(plan, outputPrefix)

    val schemaStr = descriptionNode.fields("ReadSchema")
    val schema = DataTypeParser.parseCatalogString(schemaStr).asInstanceOf[StructType]

    val outputInNodeStr = descriptionNode.fields("Output")
    val outputIds = expressionParser.parseNamedList(outputInNodeStr).map(_.exprId)
    val output = schema.fields.indices.map { i =>
      val field = schema.fields(i)
      val exprId = outputIds(i)
      AttributeReference(field.name, field.dataType, exprId)
    }

    BatchScanExec(format, output, getMetrics(plan))
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "LocalTableScan",
   *   "simpleString": "LocalTableScan [a#0]",
   *   ...
   * }
   * }}}
   *
   * Plan description node:
   *
   * {{{
   * (1) LocalTableScan
   * Output [1]: [a#0]
   * Arguments: [a#0]
   * }}}
   */
  private def parseLocalTableScan(plan: SparkPlanInfo): SparkPlan = {
    val arguments = parseArguments(plan)

    // Parse isEmpty.
    val isEmpty = arguments.headOption.contains("<empty>")

    // Parse output.
    val outputArgument = arguments.lastOption.filter(_.startsWith("[")).getOrElse("")
    val (outputPrefix, outputUnknown) = getStringFields(outputArgument)
    val outputStr =
      if (outputUnknown == 0) {
        outputPrefix
      } else {
        getNodeWithOutput(plan, outputPrefix).fields("Output")
      }
    val output = expressionParser.parseNamedList(outputStr).asInstanceOf[Seq[Attribute]]

    LocalTableScanExec(isEmpty, output, getMetrics(plan))
  }

  private def isHiveTableScan(plan: SparkPlanInfo): Boolean = {
    plan.nodeName.startsWith("Scan hive")
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "Scan hive spark_catalog.default.data",
   *   "simpleString": "Scan hive spark_catalog.default.data [id#22L, id2#24L]," +
   *     " HiveTableRelation [" +
   *     "`spark_catalog`.`default`.`data`," +
   *     " org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe," +
   *     " Data Cols: [id#22L, id1#23L, Id2#24L]," +
   *     " Partition Cols: []" +
   *     "]",
   *   ...
   * }
   * }}}
   *
   * Plan description node:
   *
   * {{{
   * (1) Scan hive spark_catalog.default.data
   * Output [2]: [id#22L, Id2#24L]
   * Arguments: [id#22L, id2#24L], HiveTableRelation [`spark_catalog`.`default`.`data`,
   *   org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe,
   *   Data Cols: [id#22L, id1#23L, Id2#24L],
   *   Partition Cols: []]
   * }}}
   */
  private def parseHiveTableScan(plan: SparkPlanInfo): SparkPlan = {
    plan.children.foreach(parse(_, plan))

    val arguments = parseArguments(plan)

    // Must use description node to keep attribute names casing.
    val attributesStr = arguments.head.substring(arguments.head.indexOf("["))
    val nodes =
      planDescriptionNodesByName
        .get(plan.nodeName)
        .map(_.filter(_.fields("Arguments").startsWith(attributesStr)))
        .getOrElse(Array.empty)
    val node = getNode(plan, nodes)

    // Parse output.
    val outputStr = node.fields("Output")
    val output = expressionParser.parseNamedList(outputStr).asInstanceOf[Seq[Attribute]]

    HiveTableScanExec(output, getMetrics(plan))
  }

  private def isRDDScan(plan: SparkPlanInfo): Boolean = {
    plan.nodeName == "Scan OneRowRelation" ||
    plan.nodeName.startsWith("Scan ExistingRDD")
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "Scan ExistingRDD",
   *   "simpleString": "Scan ExistingRDD[a#2,b#3]",
   *   ...
   * }
   * {
   *   "nodeName": "Scan OneRowRelation",
   *   "simpleString": "Scan OneRowRelation[]",
   *   ...
   * }
   * }}}
   *
   * Plan description node:
   *
   * {{{
   * (1) Scan ExistingRDD [codegen id : 1]
   * Output [2]: [a#2, b#3]
   * Arguments: [a#2, b#3], MapPartitionsRDD[1] at createDataFrame at File.scala:50, ExistingRDD,
   *   UnknownPartitioning(0)
   *
   * (1) Scan OneRowRelation [codegen id : 1]
   * Output: []
   * Arguments: ParallelCollectionRDD[0] at collect at File.scala:50, OneRowRelation,
   *   UnknownPartitioning(0)
   * }}}
   */
  private def parseRDDScan(plan: SparkPlanInfo): SparkPlan = {
    val (name, output) =
      if (plan.nodeName == "Scan OneRowRelation") {
        ("OneRowRelation", Seq.empty[Attribute])
      } else {
        val arguments = parseArguments(plan)
        val (outputPrefix, outputUnknown) = getStringFields(arguments.head)
        val outputStr =
          if (outputUnknown == 0) {
            outputPrefix
          } else {
            getNodeWithOutput(plan, outputPrefix).fields("Output")
          }
        val output = expressionParser.parseNamedList(outputStr).asInstanceOf[Seq[Attribute]]

        ("ExistingRDD", output)
      }

    RDDScanExec(name, output, getMetrics(plan))
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "Range",
   *   "simpleString": "Range (1, 10, step=2, splits=3)",
   *   ...
   * }
   * }}}
   *
   * Plan description node:
   *
   * {{{
   * (1) Range [codegen id : 1]
   * Output [1]: [id#0L]
   * Arguments: Range (1, 10, step=2, splits=Some(3))
   * }}}
   */
  private def parseRange(plan: SparkPlanInfo): SparkPlan = {
    val arguments = parseParenthesesArguments(plan)
    val start = arguments.head.toLong
    val end = arguments.apply(1).toLong
    val step = arguments.apply(2).substring("step=".length).toLong
    val numSlices = arguments.apply(3).substring("splits=".length).toInt

    val expected = s"Range ($start, $end, step=$step, splits=Some($numSlices))"
    val nodes =
      planDescriptionNodesByName
        .get("Range")
        .map(_.filter(_.fields.get("Arguments").exists(_ == expected)))
        .getOrElse(Array.empty)
    val exprId =
      if (nodes.length == 1) {
        expressionParser.parseNamedList(nodes.head.fields("Output")).head.exprId
      } else {
        -1L
      }
    val output = Seq(AttributeReference("id", LongType, exprId))

    RangeExec(start, end, step, numSlices, output, getMetrics(plan))
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "Filter",
   *   "simpleString": "Filter (isnotnull(booleans#0) AND booleans#0)",
   *   ...
   * }
   * }}}
   */
  private def parseFilter(plan: SparkPlanInfo): SparkPlan = {
    val child = parseChild(plan)
    val conditionStr = plan.simpleString.substring("Filter ".length)
    val condition = expressionParser
      .withInput(child.output)(_.parse(conditionStr))
      .resolveDataType(BooleanType)

    FilterExec(condition, child, getMetrics(plan))
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "Project",
   *   "simpleString": "Project [doubles#6, ints#7]",
   *   ...
   * }
   * }}}
   */
  private def parseProject(plan: SparkPlanInfo): SparkPlan = {
    val child = parseChild(plan)
    val arguments = parseArguments(plan)

    // Parse project list.
    val (projectListPrefix, projectListUnknown) =
      getStringFields(arguments.headOption.getOrElse(""))
    val projectListStr =
      if (projectListUnknown == 0) {
        projectListPrefix
      } else {
        getNodeWithOutput(plan, projectListPrefix).fields("Output")
      }

    val projectList =
      expressionParser
        .withInput(child.output)(_.parseNamedList(projectListStr))
        .map(_.resolveDataType(AnyType).asInstanceOf[NamedExpression])

    ProjectExec(projectList, child, getMetrics(plan))
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "Exchange",
   *   "simpleString": "Exchange RoundRobinPartitioning(5), REPARTITION_BY_NUM, [plan_id=12]",
   *   ...
   * }
   * }}}
   */
  private def parseExchange(plan: SparkPlanInfo): SparkPlan = {
    val child = parseChild(plan)
    val arguments = parseArguments(plan)
    require(arguments.length > 1)

    ShuffleExchangeExec(
      SparkPlanParser.parsePartitioning(arguments(0)),
      child,
      stripExchangeId(arguments(1)),
      getMetrics(plan)
    )
  }

  /**
   * In early versions of Spark Exchange nodes referenced a plan id as `[id=#X]` instead of
   * `[plan_id=X]`.
   */
  private def stripExchangeId(str: String): String = {
    val idIndex = str.indexOf(", [id=#")
    if (idIndex == -1) {
      str
    } else {
      str.substring(0, idIndex)
    }
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "ReusedExchange",
   *   "simpleString": "ReusedExchange [id#3L], Exchange hashpartitioning(id#0L, 200), " +
   *     "REPARTITION_BY_COL, [plan_id=37]",
   *   ...
   * }
   * }}}
   */
  private def parseReusedExchange(plan: SparkPlanInfo): SparkPlan = {
    val child = parseChild(plan)
    val input = child.output
    val arguments = parseArguments(plan)
    val (outputPrefix, outputUnknown) = getStringFields(arguments.head)
    val outputStr =
      if (outputUnknown == 0) {
        outputPrefix
      } else {
        getNodeWithOutput(plan, outputPrefix).fields("Output")
      }
    val rawOutput = expressionParser.parseNamedList(outputStr).asInstanceOf[Seq[Attribute]]
    val output =
      if (rawOutput.length == input.length) {
        rawOutput.zip(input).map { case (attribute, input) =>
          attribute.resolveDataType(input.dataType).asInstanceOf[Attribute]
        }
      } else {
        rawOutput
      }

    ReusedExchangeExec(output, child)
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "BroadcastExchange",
   *   "simpleString": "BroadcastExchange " +
   *     "HashedRelationBroadcastMode(List(input[0, bigint, false]),false), [plan_id=28]",
   *   ...
   * }
   * }}}
   */
  private def parseBroadcastExchange(plan: SparkPlanInfo): SparkPlan = {
    val child = parseChild(plan)
    val arguments = parseArguments(plan)
    require(arguments.nonEmpty)

    BroadcastExchangeExec(
      parseBroadcastMode(stripExchangeId(arguments(0))),
      child,
      getMetrics(plan)
    )
  }

  private def parseBroadcastMode(str: String): BroadcastMode = {
    str match {
      case "IdentityBroadcastMode" =>
        IdentityBroadcastMode
      case _ if str.startsWith("HashedRelationBroadcastMode") =>
        val parts = splitParenthesesList(str.substring(27))
        val keys = splitParenthesesList(parts.head.substring("List".length))
          .map(expressionParser.parse)
        HashedRelationBroadcastMode(keys)
      case _ =>
        UnknownBroadcastMode(str)
    }
  }

  private def isInMemoryTableScan(plan: SparkPlanInfo): Boolean = {
    plan.nodeName == "InMemoryTableScan" ||
    plan.nodeName.startsWith("Scan In-memory table")
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "InMemoryTableScan",
   *   "simpleString": "InMemoryTableScan [id#0L], [isnotnull(id#0L), (id#0L < 10)]",
   *   ...
   * }
   * {
   *   "nodeName": "Scan In-memory table cached",
   *   "simpleString": "Scan In-memory table cached [id#0L]",
   *   ...
   * }
   * }}}
   *
   * Plan description node:
   *
   * {{{
   * (1) InMemoryTableScan
   * Output [2]: [id#0L]
   * Arguments: [id#0L], [isnotnull(id#0L), (id#0L < 10)]
   *
   * (1) Scan In-memory table cached
   * Output [1]: [id#0L]
   * Arguments: [id#0L]
   * }}}
   */
  private def parseInMemoryTableScan(plan: SparkPlanInfo): SparkPlan = {
    val child = parseChild(plan)
    val input = child.output
    val arguments = parseArguments(plan)

    // Parse table name.
    val tablePrefix = "Scan In-memory table "
    val tableName =
      if (plan.nodeName.startsWith(tablePrefix)) {
        Some(plan.nodeName.substring(tablePrefix.length))
      } else {
        None
      }

    // Parse output.
    val (outputPrefix, outputUnknown) = getStringFields(arguments.headOption.getOrElse(""))
    val outputStr =
      if (outputUnknown == 0) {
        outputPrefix
      } else {
        getNodeWithOutput(plan, outputPrefix).fields("Output")
      }
    val output = expressionParser
      .withInput(input)(_.parseNamedList(outputStr))
      .asInstanceOf[Seq[Attribute]]

    InMemoryTableScanExec(output, child, tableName, getMetrics(plan))
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "HashAggregate",
   *   "simpleString": "HashAggregate(keys=[id#0L], functions=[count(distinct id#0L)])",
   *   ...
   * }
   * }}}
   *
   * Plan description nodes:
   *
   * {{{
   * (6) HashAggregate [codegen id : 2]
   * Input [2]: [id#0L, id#0L]
   * Keys [1]: [id#0L]
   * Functions [1]: [partial_count(distinct id#0L)]
   * Aggregate Attributes [1]: [count(id#0L)#3L]
   * Results [2]: [id#0L, count#10L]
   * }}}
   *
   * {{{
   * (7) HashAggregate [codegen id : 2]
   * Input [2]: [id#0L, count#10L]
   * Keys [1]: [id#0L]
   * Functions [1]: [count(distinct id#0L)]
   * Aggregate Attributes [1]: [count(id#0L)#3L]
   * Results [2]: [id#0L, count(id#0L)#3L AS count(DISTINCT id)#4L]
   * }}}
   */
  private def parseAggregate(
      plan: SparkPlanInfo,
      build: (
          Seq[NamedExpression],
          Seq[AggregateExpression],
          Seq[NamedExpression],
          SparkPlan,
          Map[String, SQLMetric]
      ) => BaseAggregateExec
  ): SparkPlan = {
    val child = parseChild(plan)
    val input = child.output
    val node = getAggregateExecNode(plan, child, input)

    // Parse keys.
    val keys =
      expressionParser
        .withInput(input)(_.parseNamedList(node.fields("Keys")))
        .map(_.resolveDataType(AnyType).asInstanceOf[NamedExpression])

    // Parse aggregate functions.
    val unresolvedFunctions = expressionParser.parseAggregateList(node.fields("Functions"))
    val functionsInput =
      if (!unresolvedFunctions.forall(_.modePrefix == "partial")) {
        // If one of the functions is not partial then it may reference attributes which last
        // existed in the input of the corresponding partial aggregate.
        val isPartialAgg =
          (plan: SparkPlan) =>
            plan match {
              case aggregateExec: BaseAggregateExec =>
                aggregateExec.aggregateExpressions.forall(_.modePrefix == "partial")
              case _ =>
                false
            }

        child
          .find(isPartialAgg)
          .orElse(
            child
              .find(_.isInstanceOf[ReusedExchangeExec])
              .flatMap(_.asInstanceOf[ReusedExchangeExec].child.find(isPartialAgg))
          )
          .map(_.children.head.output ++ input)
          .getOrElse(input)
      } else {
        input
      }
    val functions = unresolvedFunctions.map(DataTypeResolver.resolve(_, functionsInput, AnyType))

    // Parse results.
    val isFinal = unresolvedFunctions.forall(_.modePrefix.isEmpty)
    val resultExpressions =
      if (isFinal) {
        val attributesTypes = functions.map(_.dataType)
        val aggregateAttributes = expressionParser
          .parseNamedList(node.fields("Aggregate Attributes"))
          .zipWithIndex
          .map { case (expr, i) => AttributeReference(expr.name, attributesTypes(i), expr.exprId) }
        val resultInput = keys.map(_.toAttribute) ++ aggregateAttributes
        expressionParser
          .withInput(resultInput)(_.parseNamedList(node.fields("Results")))
          .map(_.resolveDataType(AnyType).asInstanceOf[NamedExpression])
      } else {
        val rawResultExpressions = expressionParser.parseNamedList(node.fields("Results"))

        // We cannot reliably determine the number of attribute data types of unknow expressions and
        // of Sum expressions which may have a `isEmpty` attribute.
        val invalidFunctionIndex =
          functions.indexWhere { expr =>
            expr.aggregateFunction.isInstanceOf[UnknownExpression] ||
            (expr.aggregateFunction.isInstanceOf[Sum] && !expr.dataType.isDefined)
          }
        val functionLimit =
          if (invalidFunctionIndex == -1) {
            functions.length
          } else {
            invalidFunctionIndex
          }

        val knownAttributesTypes = (0 until functionLimit).flatMap { i =>
          functions(i).aggregateFunction.aggBufferDataTypes
        }

        // If number of unknow types is the same number of invalid functions and these functions are
        // all Sum then these do not use the `isEmpty` attribute.
        val unknownTypesCount =
          rawResultExpressions.length - knownAttributesTypes.length - keys.length
        val inferredAttributeTypes =
          if (
            unknownTypesCount > 0 &&
            unknownTypesCount == (functions.length - functionLimit) &&
            (0 until unknownTypesCount)
              .map(i => functions(functionLimit + i))
              .forall(_.aggregateFunction.isInstanceOf[Sum])
          ) {
            (0 until unknownTypesCount).flatMap { i =>
              functions(functionLimit + i).aggregateFunction.aggBufferDataTypes
            }
          } else {
            Seq.empty
          }
        val resultDataTypes = keys.map(_.dataType) ++ knownAttributesTypes ++ inferredAttributeTypes

        rawResultExpressions
          .zipWithIndex
          .map { case (expr, i) =>
            val dataType =
              if (i < resultDataTypes.length) {
                resultDataTypes(i)
              } else {
                AnyType
              }
            AttributeReference(expr.name, dataType, expr.exprId)
          }
      }

    build(
      keys,
      functions,
      resultExpressions,
      child,
      getMetrics(plan)
    )
  }

  private def getAggregateExecNode(
      plan: SparkPlanInfo,
      child: SparkPlan,
      input: Seq[NamedExpression]
  ): SparkPlanDescriptionNode = {
    val arguments = parseParenthesesArguments(plan)
    val keysStart = arguments(0).indexOf("[")
    val (keysPrefix, _) = getStringFields(arguments(0).substring(keysStart))
    val (functionsPrefix, _) = getStringFields(arguments(1).substring(10))

    // Nodes that match keys and functions.
    val nodes =
      planDescriptionNodesByName
        .get(plan.nodeName)
        .map { nodes =>
          nodes.filter { node =>
            node.fields.get("Keys").exists(_.startsWith(keysPrefix)) &&
            node.fields.get("Functions").exists(_.startsWith(functionsPrefix)) &&
            node.childrenIds.nonEmpty
          }
        }
        .getOrElse(Array.empty)

    if (nodes.isEmpty) {
      throw ParseException(plan.simpleString, "No match in plan description")
    }

    if (nodes.length == 1) {
      return nodes.head
    }

    // Nodes are all the same anyway, return first.
    if (nodes.map(_.fields).toSet.size == 1) {
      return nodes.head
    }

    // Try to match the child by name.
    val childNodes = nodes.map(node => planDescriptionNodesById.get(node.childrenIds.head))
    if (childNodes.forall(_.isDefined)) {
      val childInfo = plan.children.head
      val expectedChildInfo =
        if (childInfo.nodeName == "InputAdapter") {
          childInfo.children.head
        } else {
          childInfo
        }
      val nodesWithExpectedChildName =
        nodes.filter { node =>
          val childNode = planDescriptionNodesById(node.childrenIds.head)
          childNode.name == expectedChildInfo.nodeName.trim
        }

      if (nodesWithExpectedChildName.length == 1) {
        return nodesWithExpectedChildName.head
      }
    }

    // Try to match the child by its input.
    if (childNodes.forall(_.isDefined) && childNodes.forall(_.get.fields.contains("Input"))) {
      val expectedChildInputIds = child.children.flatMap(_.output).map(_.exprId)
      val nodesWithExpectedChildInput =
        nodes.filter { node =>
          val inputStr = planDescriptionNodesById(node.childrenIds.head).fields("Input")
          val childInputIds = expressionParser.parseNamedList(inputStr).map(_.exprId)
          childInputIds == expectedChildInputIds
        }

      if (nodesWithExpectedChildInput.length == 1) {
        return nodesWithExpectedChildInput.head
      }
    }

    // Try to match the input by ids.
    val inputIds = input.map(_.exprId)
    val nodesWithExpectedInput = nodes.filter { node =>
      node
        .fields
        .get("Input")
        .exists(expressionParser.parseNamedList(_).map(_.exprId) == inputIds)
    }
    if (nodesWithExpectedInput.length == 1) {
      return nodesWithExpectedInput.head
    }

    findEquivalentByNamedExpressionsField("Input", nodes).foreach(return _)
    findEquivalentByNamedExpressionsField("Results", nodes).foreach(return _)

    throw ParseException(plan.simpleString, "Multiple nodes match plan")
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "Subquery",
   *   "simpleString": "Subquery subquery#2, [id=#30]",
   *   ...
   * }
   * }}}
   */
  private def parseSubquery(plan: SparkPlanInfo): SparkPlan = {
    val child = parseChild(plan)
    val arguments = parseArguments(plan)
    val name = arguments.head

    cacheSubquery(
      plan.simpleString,
      SubqueryExec(name, child, getMetrics(plan))
    )
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "SubqueryBroadcast",
   *   "simpleString": "SubqueryBroadcast dynamicpruning#18, 0, [part#17], [id=#102]",
   *   ...
   * }
   * }}}
   */
  private def parseSubqueryBroadcast(plan: SparkPlanInfo): SparkPlan = {
    val child = parseChild(plan)
    val arguments = parseArguments(plan)
    val name = arguments.head
    val legacySingleValue = !arguments(1).startsWith("[")
    val indices =
      if (legacySingleValue) {
        Seq(arguments(1).toInt)
      } else {
        splitSquareBracketsList(arguments(1)).map(_.toInt)
      }
    val buildKeysStr = arguments(2).substring(0, arguments(2).lastIndexOf(", [id=#"))
    val buildKeys = expressionParser
      .withInput(child.output)(_.parseSquareList(buildKeysStr))
      .map(_.resolveDataType(AnyType))

    cacheSubquery(
      name,
      SubqueryBroadcastExec(name, indices, buildKeys, child, getMetrics(plan), legacySingleValue)
    )
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "Sort",
   *   "simpleString": "Sort [(id#0 % 5) ASC NULLS FIRST, id#0 DESC NULLS LAST], true, 0",
   *   ...
   * }
   * }}}
   */
  private def parseSort(plan: SparkPlanInfo): SparkPlan = {
    val child = parseChild(plan)
    val arguments = parseArguments(plan)
    val global = arguments(1).toBoolean

    // Parse sort order.
    val sortOrder =
      expressionParser
        .withInput(child.output)(_.parseSortOrderList(arguments.head))
        .map(_.resolveDataType(AnyType).asInstanceOf[SortOrder])

    SortExec(sortOrder, global, child, getMetrics(plan))
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "TakeOrderedAndProject",
   *   "simpleString": "TakeOrderedAndProject(" +
   *     "limit=3, " +
   *     "orderBy=[id#0L DESC NULLS LAST], " +
   *     "output=[id#0L,(id + 1)#2L]" +
   *     ")",
   *   ...
   * }
   * }}}
   */
  private def parseTakeOrderedAndProject(plan: SparkPlanInfo): SparkPlan = {
    val child = parseChild(plan)
    val input = child.output
    val arguments = parseParenthesesArguments(plan)
    require(arguments.length > 2)

    val argumentsOffset = if (arguments(1).startsWith("offset")) 1 else 0

    // Parse sort order.
    val sortOrderStr = arguments(argumentsOffset + 1).substring(8)
    val sortOrder =
      expressionParser
        .withInput(input)(_.parseSortOrderList(sortOrderStr))
        .map(_.resolveDataType(AnyType).asInstanceOf[SortOrder])

    // Parse project list.
    val projectListArgument = arguments(argumentsOffset + 2).substring(7)
    val (projectListStr, projectListUnknown) = getStringFields(projectListArgument)
    val projectList =
      expressionParser
        .withInput(input)(_.parseNamedList(projectListStr))
        .map(_.resolveDataType(AnyType).asInstanceOf[NamedExpression]) ++
        (0 until projectListUnknown).map { i =>
          Alias(UndefinedExpression(AnyType), s"undefined_$i", -1)
        }

    // Parse limit.
    val limitStr = arguments.head.substring(6)
    val limit = limitStr.toInt

    // Parse offset.
    val offsets =
      planDescriptionNodesByName
        .get(plan.nodeName)
        .map(
          _.flatMap { node =>
            val argumentsStr = node.fields("Arguments")
            val arguments = splitList(argumentsStr)
            // `simpleString` does include spaces in expression lists.
            val matches =
              arguments.length > 2 &&
                arguments.head == limitStr &&
                arguments(1).filter(_ != ' ') == sortOrderStr.filter(_ != ' ') &&
                arguments(2).filter(_ != ' ') == projectListStr.filter(_ != ' ')

            if (matches && arguments.length > 3) {
              Some(arguments(3).toInt)
            } else {
              None
            }
          }
        )
        .getOrElse(Array.empty)
    val offset =
      if (offsets.length == 1) {
        offsets.head
      } else {
        0
      }

    TakeOrderedAndProjectExec(
      limit,
      sortOrder,
      projectList,
      child,
      offset,
      getMetrics(plan)
    )
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "SortMergeJoin",
   *   "simpleString": "SortMergeJoin [m2#2L], [m3#7L], Inner, (id#0L < m3#7L)",
   *   ...
   * }
   * }}}
   */
  private def parseSortMergeJoin(plan: SparkPlanInfo): SparkPlan = {
    val (left, right) = parseBinaryChildren(plan)
    val arguments = parseArguments(plan)
    require(arguments.length > 2)

    // Parse keys.
    val leftKeys =
      expressionParser
        .withInput(left.output)(_.parseSquareList(arguments.head))
        .map(_.resolveDataType(AnyType))

    val rightKeys =
      expressionParser
        .withInput(right.output)(_.parseSquareList(arguments(1)))
        .map(_.resolveDataType(AnyType))

    // Parse joinType.
    val joinType = SparkPlanParser.parseJoinType(arguments(2))

    // Parse condition.
    val condition =
      if (arguments.size > 3) {
        Some(
          expressionParser
            .withInput(left.output ++ right.output)(_.parse(arguments(3)))
            .resolveDataType(BooleanType)
        )
      } else {
        None
      }

    // Parse isSkew.
    val isSkewJoin = plan.nodeName.endsWith(")")

    SortMergeJoinExec(
      leftKeys,
      rightKeys,
      joinType,
      condition,
      left,
      right,
      isSkewJoin,
      getMetrics(plan)
    )
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "ShuffledHashJoin",
   *   "simpleString": "ShuffledHashJoin [m2#2L], [m3#7L], Inner, BuildLeft, (id#0L < m3#7L)",
   *   ...
   * }
   * }}}
   */
  private def parseShuffledHashJoin(plan: SparkPlanInfo): SparkPlan = {
    val (left, right) = parseBinaryChildren(plan)
    val arguments = parseArguments(plan)
    require(arguments.length > 3)

    // Parse keys.
    val leftKeys =
      expressionParser
        .withInput(left.output)(_.parseSquareList(arguments.head))
        .map(_.resolveDataType(AnyType))

    val rightKeys =
      expressionParser
        .withInput(right.output)(_.parseSquareList(arguments(1)))
        .map(_.resolveDataType(AnyType))

    // Parse joinType.
    val joinType = SparkPlanParser.parseJoinType(arguments(2))

    // Parse buildSide.
    val buildSide = SparkPlanParser.parseBuildSide(arguments(3))

    // Parse condition.
    val condition =
      if (arguments.size > 4) {
        Some(
          expressionParser
            .withInput(left.output ++ right.output)(_.parse(arguments(4)))
            .resolveDataType(BooleanType)
        )
      } else {
        None
      }

    // Parse isSkew.
    val isSkewJoin = plan.nodeName.endsWith(")")

    ShuffledHashJoinExec(
      leftKeys,
      rightKeys,
      joinType,
      buildSide,
      condition,
      left,
      right,
      isSkewJoin,
      getMetrics(plan)
    )
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "BroadcastHashJoin",
   *   "simpleString": "BroadcastHashJoin [m2#2], [m3#7], Inner, BuildLeft, (id#0 < m3#7), false",
   *   ...
   * }
   * }}}
   */
  private def parseBroadcastHashJoin(plan: SparkPlanInfo): SparkPlan = {
    val (left, right) = parseBinaryChildren(plan)
    val arguments = parseArguments(plan)
    require(arguments.length > 4)

    // Parse keys.
    val leftKeys =
      expressionParser
        .withInput(left.output)(_.parseSquareList(arguments.head))
        .map(_.resolveDataType(AnyType))

    val rightKeys =
      expressionParser
        .withInput(right.output)(_.parseSquareList(arguments(1)))
        .map(_.resolveDataType(AnyType))

    // Parse joinType.
    val joinType = SparkPlanParser.parseJoinType(arguments(2))

    // Parse buildSide.
    val buildSide = SparkPlanParser.parseBuildSide(arguments(3))

    // Parse condition.
    val condition =
      if (arguments.size > 5) {
        Some(
          expressionParser
            .withInput(left.output ++ right.output)(_.parse(arguments(4)))
            .resolveDataType(BooleanType)
        )
      } else {
        None
      }

    BroadcastHashJoinExec(
      leftKeys,
      rightKeys,
      joinType,
      buildSide,
      condition,
      left,
      right,
      getMetrics(plan)
    )
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "CartesianProduct",
   *   "simpleString": "CartesianProduct ((m2#2L = m3#7L) AND (id#0L < m3#7L))",
   *   ...
   * }
   * }}}
   */
  private def parseCartesianProduct(plan: SparkPlanInfo): SparkPlan = {
    val (left, right) = parseBinaryChildren(plan)
    val arguments = parseArguments(plan)

    // Parse condition.
    val condition =
      if (arguments.isEmpty) {
        None
      } else {
        Some(
          expressionParser
            .withInput(left.output ++ right.output)(_.parse(arguments.head))
            .resolveDataType(BooleanType)
        )
      }

    CartesianProductExec(left, right, condition, getMetrics(plan))
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "BroadcastNestedLoopJoin",
   *   "simpleString": "BroadcastNestedLoopJoin BuildRight, FullOuter, (id#0L < m3#7L)",
   *   ...
   * }
   * }}}
   */
  private def parseBroadcastNestedLoopJoin(plan: SparkPlanInfo): SparkPlan = {
    val (left, right) = parseBinaryChildren(plan)
    val arguments = parseArguments(plan)
    require(arguments.length > 1)

    // Parse buildSide.
    val buildSide = SparkPlanParser.parseBuildSide(arguments.head)

    // Parse joinType.
    val joinType = SparkPlanParser.parseJoinType(arguments(1))

    // Parse condition.
    val condition =
      if (arguments.size > 2) {
        Some(
          expressionParser
            .withInput(left.output ++ right.output)(_.parse(arguments(2)))
            .resolveDataType(BooleanType)
        )
      } else {
        None
      }

    BroadcastNestedLoopJoinExec(left, right, buildSide, joinType, condition, getMetrics(plan))
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "Coalesce",
   *   "simpleString": "Coalesce 1",
   *   ...
   * }
   * }}}
   */
  private def parseCoalesce(plan: SparkPlanInfo): SparkPlan = {
    val child = parseChild(plan)
    val numPartitions = plan.simpleString.substring(9).toInt
    CoalesceExec(numPartitions, child)
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "CollectLimit",
   *   "simpleString": "CollectLimit 3, 1",
   *   ...
   * }
   * }}}
   */
  private def parseCollectLimit(plan: SparkPlanInfo): SparkPlan = {
    val child = parseChild(plan)
    val arguments = parseArguments(plan)
    require(arguments.nonEmpty)

    val limit = arguments.head.toInt
    val offset = arguments.tail.headOption.map(_.toInt).getOrElse(0)

    CollectLimitExec(limit, child, offset)
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "CollectTail",
   *   "simpleString": "CollectTail 2",
   *   ...
   * }
   * }}}
   */
  private def parseCollectTail(plan: SparkPlanInfo): SparkPlan = {
    val child = parseChild(plan)
    val limit = plan.simpleString.substring(12).toInt
    CollectTailExec(limit, child)
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "LocalLimit",
   *   "simpleString": "LocalLimit 2",
   *   ...
   * }
   * }}}
   */
  private def parseLocalLimit(plan: SparkPlanInfo): SparkPlan = {
    val child = parseChild(plan)
    val limit = plan.simpleString.substring(11).toInt
    LocalLimitExec(limit, child)
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "GlobalLimit",
   *   "simpleString": "GlobalLimit 3, 1",
   *   ...
   * }
   * }}}
   */
  private def parseGlobalLimit(plan: SparkPlanInfo): SparkPlan = {
    val child = parseChild(plan)
    val arguments = parseArguments(plan)
    require(arguments.nonEmpty)

    val limit = arguments.head.toInt
    val offset = arguments.tail.headOption.map(_.toInt).getOrElse(0)

    GlobalLimitExec(limit, child, offset)
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "Generate",
   *   "simpleString": "Generate explode(array(id#0, (2 * id#0))), [id1#2], false, [col#5]",
   *   ...
   * }
   * }}}
   *
   * Plan description nodes:
   *
   * {{{
   * (2) Generate [codegen id : 1]
   * Input [1]: [id#0L]
   * Arguments: explode(array(id#0L, (2 * id#0))), [id1#2], false, [col#5L]
   * }}}
   */
  private def parseGenerate(plan: SparkPlanInfo, parent: SparkPlanInfo): SparkPlan = {
    val child = parseChild(plan)
    val input = child.output
    val arguments = parseArguments(plan)
    require(arguments.length > 2)

    // Parse generator.
    val generator = expressionParser.withInput(input)(
      _.parse(arguments.head)
        .resolveDataType(ArrayType(AnyType))
    )

    // Parse outer.
    val outerIndex = if (arguments(1).startsWith("[")) 2 else 1
    val outer = arguments(outerIndex).toBoolean

    // Parse generatorOutput.
    val rawGeneratorOutput = expressionParser.parseNamedList(arguments(outerIndex + 1))
    val generatorDataTypes =
      generator.dataType.getArrayElementType.collect {
        case structType: StructType
            if structType.fields.length == rawGeneratorOutput.length =>
          structType.fields.map(_.dataType)
      }
    val generatorOutput =
      if (generatorDataTypes.length == 1) {
        rawGeneratorOutput
          .zip(generatorDataTypes.head)
          .map { case (expr, dataType) => expr.resolveDataType(dataType).asInstanceOf[Attribute] }
      } else {
        rawGeneratorOutput.map(_.resolveDataType(AnyType).asInstanceOf[Attribute])
      }

    // Parse requiredChildOutput.
    val childOutputArgument = Some(arguments(1)).filter(_.startsWith("[")).getOrElse("")
    val (childOutputPrefix, childOutputUnknown) = getStringFields(childOutputArgument)
    val childOutputStr =
      if (childOutputUnknown == 0) {
        Some(childOutputPrefix)
      } else {
        getGenerateParentInput(plan, parent).map(
          splitSquareBracketsList(_)
            .dropRight(rawGeneratorOutput.length)
            .mkString("[", ",", "]")
        )
      }
    val childOutput =
      if (childOutputStr.isEmpty) {
        input
      } else {
        expressionParser
          .withInput(input)(_.parseNamedList(childOutputStr.get))
          .map(_.resolveDataType(AnyType).asInstanceOf[Attribute])
      }

    GenerateExec(
      generator,
      childOutput,
      outer,
      generatorOutput,
      child,
      getMetrics(plan)
    )
  }

  private def getGenerateParentInput(plan: SparkPlanInfo, parent: SparkPlanInfo): Option[String] = {
    if (parent == null) {
      return None
    }

    planDescriptionNodesByName
      .get(parent.nodeName.trim)
      .flatMap { nodes =>
        val filtered = nodes.filter(_.fields.contains("Input"))

        if (filtered.isEmpty) {
          None
        } else if (filtered.length == 1) {
          Some(filtered.head)
        } else {
          val expectedArguments = plan.simpleString.substring(9)
          planDescriptionNodesByName
            .get(plan.nodeName)
            .map(_.filter(_.fields("Arguments") == expectedArguments))
            .filter(_.length == 1)
            .map(nodes => filtered.filter(_.childrenIds.contains(nodes.head.id)))
            .filter(_.length == 1)
            .map(_.head)
        }
      }
      .map(_.fields("Input"))
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "Expand",
   *   "simpleString": "Expand [[byte#1, integer#3, 0], [byte#1, null, 1]], " +
   *     "[byte#1, integer#94, spark_grouping_id#93L]",
   *   ...
   * }
   * }}}
   */
  private def parseExpand(plan: SparkPlanInfo): SparkPlan = {
    val child = parseChild(plan)
    val input = child.output
    val arguments = parseArguments(plan)

    // Parse projections.
    val projectionsStr = splitSquareBracketsList(arguments.head)
    val projections = projectionsStr.map { projectionStr =>
      expressionParser
        .withInput(input) { parser =>
          if (projectionStr.startsWith("ArrayBuffer")) {
            splitParenthesesList(projectionStr.substring(11)).map(parser.parse)
          } else {
            parser.parseSquareList(projectionStr)
          }
        }
        .map(_.resolveDataType(AnyType))
    }

    // Parse output.
    val output =
      expressionParser
        .withInput(input)(_.parseNamedList(arguments(1)))
        .map(_.resolveDataType(AnyType).asInstanceOf[Attribute])

    ExpandExec(projections, output, child, getMetrics(plan))
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "Window",
   *   "simpleString": "Window [" +
   *       "avg(integer#3) " +
   *       "windowspecdefinition(" +
   *         "byte#1, " +
   *         "long#4L ASC NULLS FIRST, " +
   *         "specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())" +
   *       ") AS window#61" +
   *     "], " +
   *     "[byte#1], " +
   *     "[long#4L ASC NULLS FIRST]",
   *   ...
   * }
   * }}}
   */
  private def parseWindow(plan: SparkPlanInfo): SparkPlan = {
    val child = parseChild(plan)
    val input = child.output
    val arguments = parseArguments(plan)

    // Parse window expressions.
    val (windowExpressionsPrefix, windowExpressionsUnknown) = getStringFields(arguments.head)
    val windowExpressions =
      expressionParser
        .withInput(input)(_.parseWindowList(windowExpressionsPrefix))
        .map(_.resolveDataType(AnyType).asInstanceOf[NamedExpression]) ++
        (0 until windowExpressionsUnknown).map { i =>
          Alias(UndefinedWindowExpression(AnyType), s"undefined_window_$i", -1)
        }

    // Parse partition spec.
    lazy val partitionParsed =
      expressionParser
        .withInput(input)(_.parseSquareList(arguments(1)))
        .map(_.resolveDataType(AnyType))

    // Parse order spec.
    lazy val orderParsed = Try(
      expressionParser
        .withInput(input)(_.parseSortOrderList(arguments.last))
        .map(_.resolveDataType(AnyType).asInstanceOf[SortOrder])
    )

    val (partitionSpec, orderSpec) =
      if (arguments.length == 1) {
        (Seq.empty, Seq.empty)
      } else if (arguments.length == 2) {
        if (orderParsed.isSuccess) {
          (Seq.empty, orderParsed.get)
        } else {
          (partitionParsed, Seq.empty)
        }
      } else {
        (partitionParsed, orderParsed.get)
      }

    WindowExec(
      windowExpressions,
      partitionSpec,
      orderSpec,
      child,
      getMetrics(plan)
    )
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "WindowGroupLimit",
   *   "simpleString": "WindowGroupLimit [id#0L ASC NULLS FIRST], rank(id#0L), 19, Final",
   *   ...
   * }
   * }}}
   */
  private def parseWindowGroupLimit(plan: SparkPlanInfo): SparkPlan = {
    val child = parseChild(plan)
    val input = child.output
    val arguments = parseArguments(plan)
    require(arguments.length > 3)

    // Parse partition spec.
    val partitionSpec =
      if (arguments.length == 5) {
        expressionParser
          .withInput(input)(_.parseSquareList(arguments.head))
          .map(_.resolveDataType(AnyType))
      } else {
        Seq.empty
      }

    // Parse order spec.
    val orderSpec =
      expressionParser
        .withInput(input)(_.parseSortOrderList(arguments(arguments.length - 4)))
        .map(_.resolveDataType(AnyType).asInstanceOf[SortOrder])

    // Parse rank like.
    val rankLike = expressionParser.withInput(input)(
      _.parse(arguments(arguments.length - 3))
        .resolveDataType(AnyType)
    )

    // Parse limit.
    val limit = arguments(arguments.length - 2).toInt

    // Parse mode.
    val mode =
      arguments(arguments.length - 1) match {
        case "Partial" =>
          WindowGroupLimitMode.Partial
        case "Final" =>
          WindowGroupLimitMode.Final
        case str =>
          throw ParseException(str, "Unknown window group limit mode")
      }

    WindowGroupLimitExec(
      partitionSpec,
      orderSpec,
      rankLike,
      limit,
      mode,
      child,
      getMetrics(plan)
    )
  }

  private def isRowDataSourceScan(plan: SparkPlanInfo): Boolean = {
    plan.simpleString.startsWith("Scan ") &&
    plan.simpleString.contains("ReadSchema: ")
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "Scan JDBCRelation(data) [numPartitions=1] ",
   *   "simpleString": "Scan JDBCRelation(data) [numPartitions=1] " +
   *     "[int#0,str#1] " +
   *     "PushedFilters: [], " +
   *     "ReadSchema: struct<int:int,str:string>",
   *   ...
   * }
   * }}}
   *
   * Plan description node:
   *
   * {{{
   * (1) Scan JDBCRelation(data) [numPartitions=1]  [codegen id : 1]
   * Output [2]: [int#0, str#1]
   * ReadSchema: struct<int:int,str:string>
   * }}}
   */
  private def parseRowDataSourceScan(plan: SparkPlanInfo): SparkPlan = {
    plan.children.foreach(parse(_, plan))

    // Parse output from `simpleString`.
    val argumentsStr = plan.simpleString.substring(plan.nodeName.length)
    val outputStart = argumentsStr.indexOf("[")
    val outputEnd = argumentsStr.indexOf("] ")
    val outputStr = argumentsStr.substring(outputStart, outputEnd + 1)
    val (outputPrefix, _) = getStringFields(outputStr)

    // Match with plan description.
    val descriptionNode = getNodeWithUnspacedOutput(plan, outputPrefix)

    // Parse the schema.
    val schemaStr = descriptionNode.fields("ReadSchema")
    val schema = DataTypeParser.parseCatalogString(schemaStr).asInstanceOf[StructType]

    // Parse output and match with schema.
    val output = expressionParser
      .parseNamedList(descriptionNode.fields("Output"))
      .zipWithIndex
      .map { case (attribute, i) =>
        attribute.resolveDataType(schema.fields(i).dataType).asInstanceOf[Attribute]
      }

    RowDataSourceScanExec(output, getMetrics(plan))
  }

  private def isExternalRDDScan(plan: SparkPlanInfo): Boolean = {
    // Last scan to be checked.
    plan.simpleString.startsWith("Scan")
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "Scan some rdd",
   *   "simpleString": "Scan some rdd[obj#1]",
   *   ...
   * }
   * }}}
   *
   * Plan description node:
   *
   * {{{
   * (1) Scan some rdd
   * Output [1]: [obj#1]
   * Arguments: obj#1: int, some rdd ParallelCollectionRDD[0] at parallelize at SparkPlanParserSuite.scala:402
   * }}}
   */
  private def parseExternalRDDScan(plan: SparkPlanInfo): SparkPlan = {
    plan.children.foreach(parse(_, plan))

    val outputStr = plan.simpleString.substring(plan.nodeName.length)
    val descriptionNode = getNodeWithOutput(plan, outputStr)
    val argumentsStr = descriptionNode.fields("Arguments")
    val attributeStr = splitSpacedList(argumentsStr).head
    val attribute = parseAttributeSimpleString(attributeStr)

    ExternalRDDScanExec(attribute, getMetrics(plan))
  }

  private def isExecute(plan: SparkPlanInfo): Boolean = {
    plan.nodeName.startsWith("Execute ")
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "Execute CreateViewCommand",
   *   "simpleString": "Execute CreateViewCommand",
   *   ...
   * }
   * }}}
   */
  private def parseExecute(plan: SparkPlanInfo): SparkPlan = {
    val children = plan.children.map(parse(_, plan))
    val commandStr = plan.nodeName.substring("Execute ".length)
    val command = parseCommand(commandStr)
    if (children.isEmpty) {
      ExecutedCommandExec(command)
    } else {
      DataWritingCommandExec(command, children.head)
    }
  }

  def parseCommand(str: String): Command = {
    str match {
      case "CreateViewCommand" =>
        CreateViewCommand
      case "InsertIntoHadoopFsRelationCommand" =>
        InsertIntoHadoopFsRelationCommand
      case _ =>
        // TODO: remaining commands.
        UnknownCommand(str)
    }
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "SerializeFromObject",
   *   "simpleString": "SerializeFromObject [" +
   *     "knownnotnull(assertnotnull(input[0, com.xonai.spark.test.TestPoint, true])).x AS x#5, " +
   *     "knownnotnull(assertnotnull(input[0, com.xonai.spark.test.TestPoint, true])).y AS y#6L" +
   *     "]",
   *   ...
   * }
   * }}}
   */
  private def parseSerializeFromObject(plan: SparkPlanInfo): SparkPlan = {
    val child = parseChild(plan)
    val input = child.output
    val arguments = parseArguments(plan)
    val serializer = expressionParser
      .withInput(input)(_.parseNamedList(arguments.head))
      .map(_.resolveDataType(AnyType).asInstanceOf[NamedExpression])

    SerializeFromObjectExec(serializer, child)
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "DeserializeToObject",
   *   "simpleString": "DeserializeToObject " +
   *     "createexternalrow(staticinvoke(...), StructField(id,LongType,false)), " +
   *     "obj#5: org.apache.spark.sql.Row",
   *   ...
   * }
   * }}}
   */
  private def parseDeserializeToObject(plan: SparkPlanInfo): SparkPlan = {
    val child = parseChild(plan)
    val input = child.output
    val arguments = parseArguments(plan)
    val deserializer = expressionParser
      .withInput(input)(_.parse(arguments.head))
      .resolveDataType(AnyType)
    val attribute = parseAttributeSimpleString(arguments(1))

    DeserializeToObjectExec(deserializer, attribute, child)
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "MapElements",
   *   "simpleString": "MapElements " +
   *     "com.xonai.spark.sql.parser.SparkPlanParserSuite$$Lambda$1378/1408538096@eb91027, " +
   *     "obj#4: bigint",
   *   ...
   * }
   * }}}
   */
  private def parseMapElements(plan: SparkPlanInfo): SparkPlan = {
    val child = parseChild(plan)
    val arguments = parseArguments(plan)
    val attribute = parseAttributeSimpleString(arguments(1))

    MapElementsExec(attribute, child)
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "MapPartitions",
   *   "simpleString": "MapPartitions " +
   *     "com.xonai.spark.sql.parser.SparkPlanParserSuite$$Lambda$1379/1593727781@3f45dfec, " +
   *     "obj#4: bigint",
   *   ...
   * }
   * }}}
   */
  private def parseMapPartitions(plan: SparkPlanInfo): SparkPlan = {
    val child = parseChild(plan)
    val arguments = parseArguments(plan)
    val attribute = parseAttributeSimpleString(arguments(1))

    MapPartitionsExec(attribute, child)
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "MapGroups",
   *   "simpleString": "MapGroups " +
   *     "org.apache.spark.sql.KeyValueGroupedDataset$$Lambda$1393/1804328134@6a543e09, " +
   *     "staticinvoke(...), " +
   *     "staticinvoke(...), " +
   *     "[value#3L], " +
   *     "[id#0L], " +
   *     "obj#5: bigint",
   *   ...
   * }
   * }}}
   */
  private def parseMapGroups(plan: SparkPlanInfo): SparkPlan = {
    val child = parseChild(plan)
    val arguments = parseArguments(plan)
    val attribute = parseAttributeSimpleString(arguments.last)

    MapGroupsExec(attribute, child)
  }

  /**
   * SparkPlanInfo:
   *
   * {{{
   * {
   *   "nodeName": "AppendColumns",
   *   "simpleString": "AppendColumns " +
   *     "com.xonai.spark.sql.parser.SparkPlanParserSuite$$Lambda$1380/1165000566@11295cb1, " +
   *     "staticinvoke(...), " +
   *     "[input[0, java.lang.Long, true].longValue AS value#3L]",
   *   ...
   * }
   * }}}
   */
  private def parseAppendColumns(plan: SparkPlanInfo): SparkPlan = {
    val child = parseChild(plan)
    val input = child.output
    val arguments = parseArguments(plan)
    val deserializer = expressionParser
      .withInput(input)(_.parse(arguments(1)))
      .resolveDataType(AnyType)
    val serializer = expressionParser
      .withInput(input)(_.parseNamedList(arguments(2)))
      .map(_.resolveDataType(AnyType).asInstanceOf[NamedExpression])

    AppendColumnsExec(deserializer, serializer, child)
  }

  /**
   * [[org.apache.spark.sql.catalyst.util.SparkStringUtils.truncatedString]].
   *
   * Returns square bracket list string with know fields and number of unknown fields.
   */
  private def getStringFields(str: String): (String, Int) = {
    if (str.isEmpty) {
      ("[]", 0)
    } else if (str.endsWith(" more fields]")) {
      val fieldsEnd = str.lastIndexOf(",")
      val fields = str.substring(0, fieldsEnd)

      val countIndex = str.indexOf("... ", fieldsEnd) + 4
      val countStr = str.substring(countIndex, str.length - 13)
      val count = countStr.toInt

      (fields, count)
    } else {
      (str, 0)
    }
  }

  private def getNodeFieldOption(nodeName: String, fieldName: String): Option[String] = {
    val descriptionNodes = planDescriptionNodesByName.getOrElse(nodeName.trim, Array.empty)
    if (descriptionNodes.length == 1 && descriptionNodes.head.fields.contains(fieldName)) {
      Some(descriptionNodes.head.fields(fieldName))
    } else {
      None
    }
  }

  private def getNode(
      plan: SparkPlanInfo,
      nodes: Array[SparkPlanDescriptionNode]
  ): SparkPlanDescriptionNode = {
    if (nodes.isEmpty) {
      throw ParseException(plan.simpleString, "No match in plan description")
    }

    if (nodes.map(_.fields).toSet.size > 1) {
      throw ParseException(plan.simpleString, "Multiple nodes match plan")
    }

    nodes.head
  }

  private def getNodeWithOutput(
      plan: SparkPlanInfo,
      outputPrefix: String
  ): SparkPlanDescriptionNode = {
    val descriptionNodes =
      planDescriptionNodesByName
        .get(plan.nodeName.trim)
        .map { nodes =>
          nodes.filter { node =>
            node.fields.get("Output").exists(_.startsWith(outputPrefix))
          }
        }
        .getOrElse(Array.empty)

    getNode(plan, descriptionNodes)
  }

  private def getNodeWithUnspacedOutput(
      plan: SparkPlanInfo,
      outputPrefix: String
  ): SparkPlanDescriptionNode = {
    val descriptionNodes =
      planDescriptionNodesByName
        .get(plan.nodeName.trim)
        .map { nodes =>
          nodes.filter { node =>
            node.fields.get("Output").exists(
              expressionParser
                .splitSquareBracketsList(_)
                .mkString("[", ",", "]")
                .startsWith(outputPrefix)
            )
          }
        }
        .getOrElse(Array.empty)

    getNode(plan, descriptionNodes)
  }

  /**
   * Given a list of expressions with different fields, checks if the given field is what determines
   * that difference. In that case, checks if the expressions in that field are convergent and
   * returns the node with wider scope.
   */
  private def findEquivalentByNamedExpressionsField(
      fieldName: String,
      nodes: Array[SparkPlanDescriptionNode]
  ): Option[SparkPlanDescriptionNode] = {
    val differenceIsInField = nodes.map(_.fields - fieldName).toSet.size == 1
    if (differenceIsInField) {
      val expressions = nodes.map(node => expressionParser.parseNamedList(node.fields(fieldName)))

      // Check node with more expressions.
      val index = expressions.indices.maxBy(expressions.apply(_).length)
      val expectedExpressions = expressions(index).toSet

      if (expressions.forall(_.forall(expectedExpressions.contains))) {
        return Some(nodes.apply(index))
      }
    }

    None
  }

  /**
   * [[org.apache.spark.sql.catalyst.expressions.AttributeReference.simpleString]].
   */
  private def parseAttributeSimpleString(str: String): Attribute = {
    val separatorIndex = str.indexOf(": ")
    val exprStr = str.substring(0, separatorIndex)
    val dataTypeStr = str.substring(separatorIndex + 2)
    val dataType = DataTypeParser.parseSimpleString(dataTypeStr)
    expressionParser
      .parse(exprStr)
      .resolveDataType(dataType)
      .asInstanceOf[Attribute]
  }
}

object SparkPlanParser {

  /**
   * [[org.apache.spark.sql.catalyst.plans.JoinType]].
   */
  def parseJoinType(str: String): JoinType = {
    str match {
      case "Inner" => Inner
      case "Cross" => Cross
      case "LeftOuter" => LeftOuter
      case "RightOuter" => RightOuter
      case "FullOuter" => FullOuter
      case "LeftSemi" => LeftSemi
      case "LeftAnti" => LeftAnti
      case _ if str.startsWith("ExistenceJoin(") && str.endsWith(")") =>
        val existsStr = str.substring(14, str.length - 1)
        val exists = ExpressionParser
          .default
          .parse(existsStr)
          .resolveDataType(BooleanType)
          .asInstanceOf[Attribute]
        ExistenceJoin(exists)
      case _ =>
        throw ParseException(str, "Unknown join type")
    }
  }

  /**
   * [[org.apache.spark.sql.catalyst.optimizer.BuildSide]].
   */
  def parseBuildSide(str: String): BuildSide = {
    str match {
      case "BuildLeft" => BuildLeft
      case "BuildRight" => BuildRight
      case _ =>
        throw ParseException(str, "Unknown build side")
    }
  }

  /**
   * [[org.apache.spark.sql.catalyst.plans.physical.Partitioning]].
   */
  def parsePartitioning(str: String): Partitioning = {
    val parametersIndex = str.indexOf("(")
    if (parametersIndex == -1) {
      str match {
        case "SinglePartition" =>
          return SinglePartition
        case _ =>
          return UnknownToParserPartitioning(str)
      }
    }

    if (parametersIndex == 0) {
      val partitioningsStr = str.substring(1, str.length - 1)
      val partitionings = partitioningsStr.split(" or ").map(parsePartitioning)
      return PartitioningCollection(partitionings.toSeq)
    }

    val name = str.substring(0, parametersIndex)
    name match {
      case "UnknownPartitioning" =>
        UnknownPartitioning
      case "RoundRobinPartitioning" =>
        RoundRobinPartitioning
      case "hashpartitioning" =>
        HashPartitioning
      case "coalescedhashpartitioning" =>
        CoalescedHashPartitioning
      case "KeyGroupedPartitioning" =>
        KeyGroupedPartitioning
      case "rangepartitioning" =>
        RangePartitioning
      case "BroadcastPartitioning" =>
        BroadcastPartitioning
      case _ =>
        UnknownToParserPartitioning(str)
    }
  }

  /**
   * Attempts to parse a string produced using
   * [[org.apache.spark.sql.execution.QueryExecution.explainString]]. It returns the contents
   * produced by [[org.apache.spark.sql.catalyst.plans.QueryPlan.verboseStringWithOperatorId]] when
   * the [[org.apache.spark.sql.internal.SQLConf.UI_EXPLAIN_MODE]] is `formatted`. It does not
   * include nodes from adaptive initial plans.
   *
   * Example:
   *
   * {{{
   * == Physical Plan ==
   * AdaptiveSparkPlan (3)
   * +- == Final Plan ==
   *    * Project (2)
   *    +- * Range (1)
   * +- == Initial Plan ==
   *    Project (2)
   *    +- Range (1)
   *
   * (1) Range [codegen id : 1]
   * Output [1]: [id#0L]
   * Arguments: Range (0, 10, step=1, splits=Some(8))
   *
   * ...
   *
   * ===== Subqueries =====
   *
   * Subquery:1 Hosting operator id = 2 Hosting Expression = Subquery subquery#3, [id=#20]
   * AdaptiveSparkPlan (11)
   * +- == Final Plan ==
   * ...
   *
   * (4) Range [codegen id : 1]
   * Output [1]: [id#6L]
   * Arguments: Range (0, 10, step=1, splits=Some(8))
   *
   * ...
   * }}}
   */
  def parsePlanDescription(str: String): Array[SparkPlanDescriptionNode] = {
    val lines = str.split("\n")

    // Find beginning of physical plan.
    val physicalPlanLine = lines.indexOf("== Physical Plan ==")
    if (physicalPlanLine == -1) {
      // Unexpected.
      return Array.empty
    }

    var i = physicalPlanLine + 1
    // Skip physical plan tree
    while (i < lines.length && lines(i).nonEmpty) { i += 1 }

    var (childrenMap, idsToExclude) = extractPlanIds(lines.slice(physicalPlanLine + 1, i))

    // Parse groups of consecutive non-empty lines.
    val nodes = new ArrayBuffer[SparkPlanDescriptionNode]
    while (i < lines.length) {
      // Selects next group.
      while (i < lines.length && lines(i).isEmpty) { i += 1 }
      val start = i
      while (i < lines.length && lines(i).nonEmpty) { i += 1 }

      if (start < i && lines(start).startsWith("Subquery:")) {
        // Update state with subquery plan.
        val subqueryIds = extractPlanIds(lines.slice(start + 1, i))
        childrenMap = childrenMap ++ subqueryIds._1
        idsToExclude = idsToExclude ++ subqueryIds._2
      } else if (
        start < i &&
        lines(start) != "===== Subqueries =====" &&
        lines(start) != "===== Adaptively Optimized Out Exchanges =====" &&
        !lines(start).startsWith("Subplan:")
      ) {
        // If group is not garbage then it must be an operator node.
        val firstLine = lines(start)
        val idStr = firstLine.substring(1, firstLine.indexOf(" ") - 1)
        val id = idStr.toLong

        // Check that node should be included.
        if (!idsToExclude.contains(id)) {
          val annotationIndex = firstLine.lastIndexOf(" [")
          val (nameEnd, annotation) =
            if (annotationIndex == -1) {
              (firstLine.length, None)
            } else {
              val annotation = firstLine.substring(annotationIndex + 2, firstLine.length - 1)
              (annotationIndex, Some(annotation))
            }
          val name = firstLine.substring(idStr.length + 3, nameEnd).trim
          val fields = parsePlanDescriptionNodeFields(lines.slice(start + 1, i))
          val childrenIds = childrenMap.getOrElse(id, Seq.empty)

          nodes += SparkPlanDescriptionNode(id, name, annotation, fields, childrenIds)
        }
      }
    }

    nodes.toArray
  }

  /**
   * Parses line produced by [[org.apache.spark.sql.execution.ExplainUtils.generateFieldString]].
   *
   * Examples:
   *
   * {{{
   * Output [1]: [id#6L]
   * Arguments: Range (0, 10, step=1, splits=Some(8))
   * }}}
   */
  private def parsePlanDescriptionNodeFields(lines: Array[String]): Map[String, String] = {
    val fields = ArrayBuffer[(String, String)]()
    var i = 0
    while (i < lines.length) {
      var line = lines(i)
      i += 1

      // CachedRDDBuilder uses multiple lines to display plan. In that case concat them together.
      if (i < lines.length && lines(i).indexOf(":") == -1) {
        while (i < lines.length && !lines(i).startsWith(",")) {
          line += "\n" + lines(i)
          i += 1
        }
        if (lines(i).startsWith(",")) {
          line += "\n" + lines(i)
          i += 1
        }
      }

      // Parse the field by excluding list length from field name.
      val delimiterIndex = line.indexOf(":")
      val prefix = line.substring(0, delimiterIndex).trim
      val bracketIndex = prefix.lastIndexOf("[")
      val key =
        if (bracketIndex == -1) {
          prefix
        } else {
          line.substring(0, bracketIndex - 1)
        }
      val value = line.substring(delimiterIndex + 2)

      fields += (key -> value)
    }
    fields.toMap
  }

  /**
   * Extracts ids from a plan tree. It returns:
   *
   * 1. Map from node id to children node ids in the final plan.
   * 2. Set of the node ids which are not included in the final plan.
   *
   * Example:
   *
   * {{{
   * == Physical Plan ==
   * AdaptiveSparkPlan (42)
   * +- == Final Plan ==
   *    Union (25)
   *    :- * HashAggregate (13)
   *    :  +- AQEShuffleRead (12)
   *    :     +- ShuffleQueryStage (11), Statistics(sizeInBytes=96.0 B, rowCount=3)
   *    :        +- Exchange (10)
   *    :           +- * HashAggregate (9)
   *    :              +- * Project (8)
   *    :                 +- * BroadcastHashJoin Inner BuildRight (7)
   *    :                    :- * ColumnarToRow (2)
   *    :                    :  +- Scan parquet  (1)
   *    :                    +- BroadcastQueryStage (6), Statistics(sizeInBytes=1 KiB, rowCount=3)
   *    :                       +- BroadcastExchange (5)
   *    :                          +- * ColumnarToRow (4)
   *    :                             +- Scan parquet  (3)
   *    +- * HashAggregate (24)
   *       +- AQEShuffleRead (23)
   *          +- ShuffleQueryStage (22), Statistics(sizeInBytes=96.0 B, rowCount=3)
   *             +- Exchange (21)
   *                +- * HashAggregate (20)
   *                   +- * Project (19)
   *                      +- * BroadcastHashJoin Inner BuildRight (18)
   *                         :- * ColumnarToRow (15)
   *                         :  +- Scan parquet  (14)
   *                         +- BroadcastQueryStage (17), Statistics(sizeInBytes=1 KiB, rowCount=3)
   *                            +- ReusedExchange (16)
   * +- == Initial Plan ==
   *    Union (41)
   *    :- HashAggregate (32)
   *    :  +- Exchange (31)
   *    :     +- HashAggregate (30)
   *    :        +- Project (29)
   *    :           +- BroadcastHashJoin Inner BuildRight (28)
   *    :              :- Scan parquet  (26)
   *    :              +- BroadcastExchange (27)
   *    :                 +- Scan parquet  (3)
   *    +- HashAggregate (40)
   *       +- Exchange (39)
   *          +- HashAggregate (38)
   *             +- Project (37)
   *                +- BroadcastHashJoin Inner BuildRight (36)
   *                   :- Scan parquet  (33)
   *                   +- BroadcastExchange (35)
   *                      +- Scan parquet  (34)
   * }}}
   */
  private def extractPlanIds(tree: Array[String]): (Map[Long, Seq[Long]], Set[Long]) = {
    val pattern = Pattern.compile("\\([0-9]+\\)")
    def extractId(line: String): Option[Long] = {
      val matcher = pattern.matcher(line)
      if (matcher.find()) {
        val idStr = line.substring(matcher.start() + 1, matcher.end() - 1)
        Some(idStr.toLong)
      } else {
        None
      }
    }

    var i = 0
    val finalIds = ArrayBuffer[Long]()
    val initialIds = ArrayBuffer[Long]()

    var indentation = 0
    var parent: java.lang.Long = null
    val parentStack = mutable.Stack.empty[java.lang.Long]
    val children = mutable.ArrayBuffer[(Long, Long)]()

    while (i < tree.length) {
      // Iterates final plan.
      while (i < tree.length && !tree(i).endsWith("== Initial Plan ==")) {
        val line = tree(i)
        val thisIndentation = getIndentation(line)

        // Find new parent since indentation reduced.
        if (thisIndentation < indentation) {
          parent = parentStack.headOption.orNull

          if (line.charAt(thisIndentation) == '+') {
            parentStack.pop()
          }
        }

        // If this is the first child of multi child node, save the parent node.
        if (
          thisIndentation > 0 &&
          line.charAt(thisIndentation - 1) == ':' &&
          !parentStack.headOption.contains(parent)
        ) {
          parentStack.push(parent)
        }

        // Extract id, save mapping, and update parent.
        extractId(line).foreach { id =>
          if (parent != null) {
            children += (parent.toLong -> id)
          }

          parent = id
          finalIds += id
        }

        indentation = thisIndentation
        i += 1
      }

      // If there are lines remaining then it is an initial plan.
      if (i < tree.length) {
        i += 1
        val initialIndentation = getIndentation(tree(i))
        while (i < tree.length && getIndentation(tree(i)) >= initialIndentation) {
          extractId(tree(i)).foreach(initialIds += _)
          i += 1
        }
      }
    }

    (
      children.groupBy(_._1).map { case (node, children) => (node, children.map(_._2).toSeq) },
      initialIds.toSet.diff(finalIds.toSet)
    )
  }

  /**
   * Return indentation count in plan tree line.
   */
  private def getIndentation(str: String): Int = {
    var count = 0
    while (count < str.length && (str(count) == ' ' || str(count) == ':')) { count += 1 }
    count
  }
}

/**
 * The object that represents the following node description
 *
 * {{{
 * (3) ColumnarToRow [codegen id : 1]
 * Input [2]: [id#1, name#2]
 * }}}
 *
 * is
 *
 * {{{
 * SparkPlanDescriptionNode(
 *   id = 3L,
 *   name = "ColumnarToRow",
 *   annotation = "codegen id : 1",
 *   fields = Map("Input" -> "[id#1, name#2]")
 * )
 * }}}
 */
case class SparkPlanDescriptionNode(
    id: Long,
    name: String,
    annotation: Option[String],
    fields: Map[String, String],
    childrenIds: Seq[Long]
)
