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

import com.xonai.spark.sql.parser.commands.{CreateViewCommand, InsertIntoHadoopFsRelationCommand}
import com.xonai.spark.sql.parser.expressions.{Alias, Ascending, Attribute, AttributeReference, Average, CreateExternalRow, Descending, DynamicPruningExpression, EqualTo, Explode, GreaterThan, InSubqueryExec, Invoke, IsNotNull, LessThan, Multiply, NullsFirst, NullsLast, Rank, StaticInvoke, UnknownExpression, WindowExpression}
import com.xonai.spark.sql.parser.metric.SQLMetric
import com.xonai.spark.sql.parser.plans.{AdaptiveSparkPlanExec, AppendColumnsExec, BatchScanExec, BroadcastExchangeExec, BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, BroadcastPartitioning, BroadcastQueryStageExec, BuildLeft, BuildRight, CartesianProductExec, CoalesceExec, CoalescedHashPartitioning, CollectLimitExec, CollectTailExec, ColumnarToRowExec, Cross, CustomShuffleReaderExec, DataWritingCommandExec, DeserializeToObjectExec, ExecutedCommandExec, ExistenceJoin, ExpandExec, ExternalRDDScanExec, FileSourceScanExec, FilterExec, FullOuter, GenerateExec, GlobalLimitExec, HashAggregateExec, HashPartitioning, HashedRelationBroadcastMode, HiveTableScanExec, IdentityBroadcastMode, InMemoryTableScanExec, Inner, InputAdapterExec, KeyGroupedPartitioning, LeftAnti, LeftOuter, LeftSemi, LocalLimitExec, LocalTableScanExec, MapElementsExec, MapGroupsExec, MapPartitionsExec, PartitioningCollection, ProjectExec, RDDScanExec, RangeExec, RangePartitioning, ReusedExchangeExec, ReusedSubqueryExec, RightOuter, RoundRobinPartitioning, RowDataSourceScanExec, SampleExec, SerializeFromObjectExec, ShuffleExchangeExec, ShuffleQueryStageExec, ShuffledHashJoinExec, SinglePartition, SortExec, SortMergeJoinExec, SparkPlan, SubqueryBroadcastExec, TableCacheQueryStageExec, TakeOrderedAndProjectExec, UnionExec, UnknownExec, UnknownPartitioning, WholeStageCodegenExec, WindowExec, WindowGroupLimitExec, WindowGroupLimitMode, WriteFilesExec}
import com.xonai.spark.sql.parser.trees.UnknownNode
import com.xonai.spark.sql.parser.types.{AnyType, BooleanType, DataTypeUtils, DoubleType, LongType, StructField, StructType}
import com.xonai.spark.sql.status.ExtendedSQLExecution
import com.xonai.spark.test.{DataTypeAssertions, TestData, TestUtils}
import org.apache.spark
import org.apache.spark.TestTrampoline
import org.apache.spark.sql.catalyst.expressions.{ExprId, SpecificInternalRow}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession, catalyst, execution}
import org.apache.spark.sql.execution.{SparkPlanInfo, aggregate}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions.{JDBC_DRIVER_CLASS, JDBC_TABLE_NAME, JDBC_URL}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcOptionsInWrite, JdbcUtils}
import org.apache.spark.sql.execution.ui.SQLAppStatusStore
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, collect_list, countDistinct, cume_dist, expr, first, rank, row_number}
import org.apache.spark.sql.internal.SQLConf
import org.scalactic.source.Position
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.util
import scala.beans.BeanProperty
import scala.util.Try

class SparkPlanParserSuite
    extends AnyFunSuite
    with TestUtils
    with TestData
    with DataTypeAssertions
    with BeforeAndAfterAll {

  private val warehouseDirectory = createTempDir()
  warehouseDirectory.delete()

  private val sparkSessionBuilder =
    SparkSession
      .builder()
      .master("local[*]")
      .config("spark.master.rest.enabled", "false")
      .config("spark.ui.enabled", "false")
      .config("spark.ui.showConsoleProgress", "false")
      .config("spark.sql.warehouse.dir", warehouseDirectory.getAbsolutePath)
      .enableHiveSupport()

  private val sparkSession = sparkSessionBuilder.getOrCreate()

  override def afterAll(): Unit = {
    stopSparkSession()
  }

  test("UnknownExec") {
    var planInfo = new SparkPlanInfo(
      nodeName = "NewOperator",
      simpleString = "NewOperator [id#1]",
      children = Seq(
        new SparkPlanInfo(
          nodeName = "Range",
          simpleString = "Range (1, 10, step=2, splits=3)",
          children = Seq(),
          metadata = Map[String, String](),
          metrics = Seq()
        )
      ),
      metadata = Map[String, String](),
      metrics = Seq()
    )

    var plan = new SparkPlanParser().parse(planInfo)
    assert(plan.isInstanceOf[UnknownExec])

    val unknownExec = plan.asInstanceOf[UnknownExec]
    assert(unknownExec.nodeName == planInfo.nodeName)
    assert(unknownExec.simpleString == planInfo.simpleString)
    assert(unknownExec.children.head.isInstanceOf[RangeExec])
    assert(unknownExec.output.isEmpty)
    assert(unknownExec.metrics.isEmpty)

    // Output is in the plan description node.
    var planDescription =
      """== Physical Plan ==
        |...
        |
        |(1) Range [codegen id : 1]
        |Output [1]: [id#4L]
        |Arguments: Range (1, 10, step=2, splits=Some(3))
        |
        |(2) NewOperator
        |Output [1]: [id#4L]
        |Arguments: this and that
        |""".stripMargin
    plan = new SparkPlanParser(Some(planDescription)).parse(planInfo)

    var output = plan.asInstanceOf[UnknownExec].output
    assert(output.length == 1)
    assert(output.head.name == "id")
    assert(output.head.exprId == 4L)
    assert(output.head.dataType == LongType)

    // Output is in the parent plan description node.
    planInfo = new SparkPlanInfo(
      nodeName = "Filter",
      simpleString = "Filter (id#4 > 0)",
      children = Seq(planInfo),
      metadata = Map[String, String](),
      metrics = Seq()
    )

    planDescription =
      """== Physical Plan ==
        |...
        |
        |(1) Range [codegen id : 1]
        |Output [1]: [id#4L]
        |Arguments: Range (1, 10, step=2, splits=Some(3))
        |
        |(2) Filter
        |Input [1]: [id#4]
        |Condition : (id#4 > 0)
        |""".stripMargin
    plan = new SparkPlanParser(Some(planDescription)).parse(planInfo)

    output = plan.children.head.asInstanceOf[UnknownExec].output
    assert(output.length == 1)
    assert(output.head.name == "id")
    assert(output.head.exprId == 4L)
    assert(output.head.dataType == LongType)
  }

  test("ColumnarToRow, WholeStageCodegen and InputAdapter") {
    executeAndParsePlan(sparkSession) { parser =>
      val executedPlan = collect(simpleDataFrame)

      val plan = parser.apply(_.last)
      assert(plan.isInstanceOf[WholeStageCodegenExec])
      assertSameMetrics(plan.metrics, executedPlan.metrics)

      val child1 = plan.children.head
      assert(child1.isInstanceOf[ColumnarToRowExec])

      val child2 = child1.children.head
      assert(child2.isInstanceOf[InputAdapterExec])

      assertSameOutput(plan.output, executedPlan.output)
    }
  }

  test("Parquet data source") {
    withTempPath { file =>
      randomDataset(exampleSchema, size = 1)(sparkSession)
        .write
        .parquet(file.getCanonicalPath)

      executeAndParsePlan(sparkSession) { parser =>
        val executedPlan = collect(
          sparkSession.read.parquet(file.getCanonicalPath)
        )

        val plan = parser.apply(_.last)
        assert(plan.isInstanceOf[FileSourceScanExec])

        val scan = plan.asInstanceOf[FileSourceScanExec]
        assert(scan.format == "Parquet")
        assertSameOutput(scan.output, executedPlan.output)
        assertSameMetrics(scan.metrics, executedPlan.metrics)
      }

      // Datasource V2.
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        executeAndParsePlan(sparkSession) { parser =>
          val executedPlan = collect(
            sparkSession.read.parquet(file.getCanonicalPath)
          )

          val plan = parser.apply(_.last)
          assert(plan.children.head.isInstanceOf[BatchScanExec])

          val scan = plan.children.head.asInstanceOf[BatchScanExec]
          assert(scan.format == "parquet")
          assertSameOutput(scan.output, executedPlan.output)
          assertSameMetrics(scan.metrics, executedPlan.children.head.metrics)
        }
      }
    }

    // Partitioned data.
    withTempPath { file =>
      sparkSession
        .range(10)
        .selectExpr("id", "id % 5 as part", "id * 2 as id2")
        .write
        .partitionBy("part")
        .parquet(file.getAbsolutePath)

      executeAndParsePlan(sparkSession) { parser =>
        val executedPlan = collect(
          sparkSession.read.parquet(file.getAbsolutePath).filter("part > 2")
        )
        val executedScan = executedPlan.find(_.isInstanceOf[execution.FileSourceScanExec]).get

        val plan = parser.apply(_.last)
        val scan = plan.collect { case p: FileSourceScanExec => p }.head
        assert(scan.format == "Parquet")
        assert(scan.partitionFilters.length == 2)
        assert(scan.partitionFilters.head.isInstanceOf[IsNotNull])
        assert(scan.partitionFilters(1).isInstanceOf[GreaterThan])
        assert(scan.output.length == 3)
        assert(scan.output.head.dataType == LongType)
        assert(scan.output(1).dataType == LongType)
        assert(scan.output(2).dataType == AnyType)
        assertSameMetrics(scan.metrics, executedScan.metrics)
      }
    }
  }

  test("LocalTableScan") {
    executeAndParsePlan(sparkSession) { parser =>
      val executedPlan = collect(
        sparkSession.createDataFrame(
          util.Arrays.asList(Row(1, "str")),
          spark.sql.types.StructType(
            Array(
              spark.sql.types.StructField("a", spark.sql.types.IntegerType),
              spark.sql.types.StructField("b", spark.sql.types.StringType)
            )
          )
        )
      )

      val plan = parser.apply(_.last)
      assert(plan.isInstanceOf[LocalTableScanExec])
      assert(!plan.asInstanceOf[LocalTableScanExec].isEmpty)
      assert(plan.output.map(_.name) == executedPlan.output.map(_.name))
      assert(plan.output.map(_.exprId) == executedPlan.output.map(_.exprId.id))
      assert(plan.output.forall(_.dataType == AnyType))
      assertSameMetrics(plan.metrics, executedPlan.metrics)
    }

    // No columns.
    executeAndParsePlan(sparkSession) { parser =>
      val executedPlan = collect(
        sparkSession.createDataFrame(
          util.Arrays.asList(Row()),
          spark.sql.types.StructType(Array.empty[spark.sql.types.StructField])
        )
      )

      val plan = parser.apply(_.last)
      assert(plan.output.isEmpty)
      assert(!plan.asInstanceOf[LocalTableScanExec].isEmpty)
      assertSameMetrics(plan.metrics, executedPlan.metrics)
    }

    // No rows.
    executeAndParsePlan(sparkSession) { parser =>
      collect(
        sparkSession.createDataFrame(
          util.Arrays.asList[Row](),
          spark.sql.types.StructType(
            Array(
              spark.sql.types.StructField("a", spark.sql.types.IntegerType)
            )
          )
        )
      )

      val plan = parser.apply(_.last)
      assert(plan.asInstanceOf[LocalTableScanExec].isEmpty)
    }

    // No columns, no rows.
    executeAndParsePlan(sparkSession) { parser =>
      collect(
        sparkSession.createDataFrame(
          util.Arrays.asList[Row](),
          spark.sql.types.StructType(Array.empty[spark.sql.types.StructField])
        )
      )

      val plan = parser.apply(_.last)
      assert(plan.asInstanceOf[LocalTableScanExec].isEmpty)
      assert(plan.output.isEmpty)
    }
  }

  test("HiveScan") {
    withTable(sparkSession, "data") {
      sparkSession
        .range(1)
        .selectExpr("id", "id + 1 as id1", "id + 2 as Id2")
        .write
        .format("hive")
        .mode("overwrite")
        .saveAsTable("data")

      executeAndParsePlan(sparkSession) { parser =>
        val executedPlan = collect(
          sparkSession.sql("""SELECT id, id2 FROM data""")
        )

        val plan = parser.apply(_.last)
        assert(plan.isInstanceOf[HiveTableScanExec])
        assert(plan.output.map(_.name) == executedPlan.output.map(_.name))
        assert(plan.output.map(_.exprId) == executedPlan.output.map(_.exprId.id))
        assert(plan.output.forall(_.dataType == AnyType))
        assertSameMetrics(plan.metrics, executedPlan.metrics)
      }
    }
  }

  test("RDDScan") {
    // ExistingRDD.
    executeAndParsePlan(sparkSession) { parser =>
      val executedPlan = collect(
        sparkSession.createDataFrame(
          sparkSession.sparkContext.parallelize(Seq(1, 2, 10).map(v => Row(v, v.toString))),
          spark.sql.types.StructType(
            Array(
              spark.sql.types.StructField("a", spark.sql.types.IntegerType),
              spark.sql.types.StructField("b", spark.sql.types.StringType)
            )
          )
        )
      )
      val executedRddScans = findSparkPlan(executedPlan, _.isInstanceOf[execution.RDDScanExec])

      val plan = parser.apply(_.last)
      val rddScans = plan.collect { case p: RDDScanExec => p }
      assert(rddScans.length == 1)
      assert(rddScans.head.name == "ExistingRDD")
      assert(plan.output.map(_.name) == executedPlan.output.map(_.name))
      assert(plan.output.map(_.exprId) == executedPlan.output.map(_.exprId.id))
      assert(plan.output.forall(_.dataType == AnyType))
      assertSameMetrics(rddScans.head.metrics, executedRddScans.head.metrics)
    }

    // ExistingRDD - no columns.
    executeAndParsePlan(sparkSession) { parser =>
      collect(
        sparkSession.createDataFrame(
          sparkSession.sparkContext.parallelize((0 until 10).map(_ => Row())),
          spark.sql.types.StructType(Array.empty[spark.sql.types.StructField])
        )
      )

      val plan = parser.apply(_.last)
      assert(plan.collect { case p: RDDScanExec => p }.head.name == "ExistingRDD")
      assert(plan.output.isEmpty)
    }

    // OneRowRelation.
    executeAndParsePlan(sparkSession) { parser =>
      val executedPlan = collect(
        sparkSession.sql("SELECT 1, 'str'")
      )
      val executedRddScans = findSparkPlan(executedPlan, _.nodeName == "Scan OneRowRelation")

      val plan = parser.apply(_.last)
      val rddScans = plan.collect { case p: RDDScanExec => p }
      assert(rddScans.length == 1)
      assert(rddScans.head.name == "OneRowRelation")
      assert(plan.output.map(_.name) == executedPlan.output.map(_.name))
      assert(plan.output.map(_.exprId) == executedPlan.output.map(_.exprId.id))
      assert(plan.output.forall(_.dataType != AnyType))
      assertSameMetrics(rddScans.head.metrics, executedRddScans.head.metrics)
    }
  }

  test("ExternalRDDScan") {
    executeAndParsePlan(sparkSession) { parser =>
      import sparkSession.implicits._
      val executedPlan = collect(
        sparkSession.createDataset(
          sparkSession.sparkContext.parallelize(Seq(1, 2, 10)).setName("some rdd")
        )
      )
      val executedScan = executedPlan.find(_.isInstanceOf[execution.ExternalRDDScanExec[_]]).get

      val plan = parser.apply(_.last)
      val scan = plan.collect { case p: ExternalRDDScanExec => p }.head
      assertSameOutput(scan.output, executedScan.output)
      assertSameMetrics(scan.metrics, executedScan.metrics)
    }
  }

  test("RowDataSourceScan") {
    val parameters = Map(
      JDBC_URL -> "jdbc:h2:mem:testdb",
      JDBC_TABLE_NAME -> "data",
      JDBC_DRIVER_CLASS -> "org.h2.Driver"
    )

    JdbcUtils.withConnection(new JDBCOptions(parameters)) { connection =>
      JdbcUtils.createTable(
        connection,
        tableName = "data",
        schema = spark.sql.types.StructType(
          Array(
            spark.sql.types.StructField("int", spark.sql.types.IntegerType),
            spark.sql.types.StructField("str", spark.sql.types.StringType)
          )
        ),
        caseSensitive = false,
        options = new JdbcOptionsInWrite(parameters)
      )

      executeAndParsePlan(sparkSession) { parser =>
        val executedPlan = collect(
          sparkSession
            .read
            .format("jdbc")
            .options(parameters)
            .load()
        )
        val executedScan = executedPlan.find(_.isInstanceOf[execution.RowDataSourceScanExec]).get

        val plan = parser.apply(_.last)
        val scan = plan.collect { case p: RowDataSourceScanExec => p }.head
        assertSameOutput(scan.output, executedScan.output)
        assertSameMetrics(scan.metrics, executedScan.metrics)
      }
    }
  }

  test("Range") {
    executeAndParsePlan(sparkSession) { parser =>
      val executedPlan = collect(
        sparkSession.range(1, 10, 2, 3).selectExpr("id")
      )

      val plan = parser.apply(_.last)
      assert(plan.children.head.isInstanceOf[RangeExec])

      val range = plan.children.head.asInstanceOf[RangeExec]
      assert(range.start == 1)
      assert(range.end == 10)
      assert(range.step == 2)
      assert(range.numSlices == 3)
      assertSameOutput(range.output, executedPlan.output)
      assertSameMetrics(range.metrics, executedPlan.children.head.metrics)
    }
  }

  test("Filter") {
    executeAndParsePlan(sparkSession) { parser =>
      val executedPlan = collect(
        simpleDataFrame.filter("id IS NOT NULL")
      )

      val plan = parser.apply(_.last)
      assert(plan.children.head.isInstanceOf[FilterExec])

      val filter = plan.children.head.asInstanceOf[FilterExec]
      assert(filter.condition.isInstanceOf[IsNotNull])
      assertSameOutput(filter.output, executedPlan.output)
      assertSameMetrics(filter.metrics, executedPlan.children.head.metrics)
    }
  }

  test("Project") {
    executeAndParsePlan(sparkSession) { parser =>
      val executedPlan = collect(
        simpleDataFrame.selectExpr("id", "-id", "id AS other", "id IS NULL", "id + 1")
      )

      val plan = parser.apply(_.last)
      assert(plan.children.head.isInstanceOf[ProjectExec])

      val project = plan.children.head.asInstanceOf[ProjectExec]
      assert(project.projectList.length == 5)
      assert(project.projectList.head.isInstanceOf[AttributeReference])
      project.projectList.tail.foreach(e => assert(e.isInstanceOf[Alias]))
      assertSameOutput(project.output, executedPlan.output)
      assertSameMetrics(project.metrics, executedPlan.children.head.metrics)
    }

    // Empty project list.
    executeAndParsePlan(sparkSession) { parser =>
      collect(
        simpleDataFrame.filter("id < 10").agg("*" -> "count")
      )

      val plan = parser.apply(_.last)

      val project = plan.collect { case p: ProjectExec => p }.head
      assert(project.projectList.isEmpty)
    }

    // Large project list.
    executeAndParsePlan(sparkSession) { parser =>
      val schema = manyColumnsDataFrame.schema
      val executedPlan = collect(
        manyColumnsDataFrame.selectExpr(schema.map(f => s"`${f.name}` IS NULL"): _*)
      )

      val plan = parser.apply(_.last)

      assertSameOutput(plan.output, executedPlan.output)
    }
  }

  test("Sample") {
    executeAndParsePlan(sparkSession) { parser =>
      val executedPlan = collect(
        simpleDataFrame.sample(0.5)
      )

      val plan = parser.apply(_.last)
      assert(plan.children.head.isInstanceOf[SampleExec])

      val sample = plan.children.head.asInstanceOf[SampleExec]
      assertSameOutput(sample.output, executedPlan.output)
      assertSameMetrics(sample.metrics, executedPlan.children.head.metrics)
    }
  }

  test("Exchange") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      executeAndParsePlan(sparkSession) { parser =>
        val executedPlan = collect(
          simpleDataFrame.repartition(5)
        )
        val executedExchange = executedPlan.asInstanceOf[execution.exchange.ShuffleExchangeExec]

        val plan = parser.apply(_.last)
        assert(plan.isInstanceOf[ShuffleExchangeExec])

        val exchange = plan.asInstanceOf[ShuffleExchangeExec]
        assert(exchange.outputPartitioning == RoundRobinPartitioning)
        assert(exchange.shuffleOrigin == executedExchange.shuffleOrigin.toString)
        assertSameOutput(exchange.output, executedPlan.output)
        assertSameMetrics(exchange.metrics, executedPlan.metrics)
      }
    }
  }

  test("Partitioning") {
    assert(SparkPlanParser.parsePartitioning("UnknownPartitioning(10)") == UnknownPartitioning)
    assert(SparkPlanParser.parsePartitioning("RoundRobinPartitioning(10)") ==
      RoundRobinPartitioning)
    assert(SparkPlanParser.parsePartitioning("SinglePartition") == SinglePartition)
    assert(SparkPlanParser.parsePartitioning("hashpartitioning(10)") == HashPartitioning)
    assert(
      SparkPlanParser.parsePartitioning("coalescedhashpartitioning(hashpartitioning(10))") ==
        CoalescedHashPartitioning
    )
    assert(
      SparkPlanParser.parsePartitioning("KeyGroupedPartitioning(List(),10,List())") ==
        KeyGroupedPartitioning
    )
    assert(SparkPlanParser.parsePartitioning("rangepartitioning(10)") == RangePartitioning)
    assert(
      SparkPlanParser.parsePartitioning("(rangepartitioning(10) or UnknownPartitioning(10))") ==
        PartitioningCollection(Seq(RangePartitioning, UnknownPartitioning))
    )
    assert(
      SparkPlanParser.parsePartitioning("BroadcastPartitioning(IdentityBroadcastMode)") ==
        BroadcastPartitioning
    )
  }

  test("Union, ReusedExchange") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      executeAndParsePlan(sparkSession) { parser =>
        val df1 = simpleDataFrame.repartition(col("id"))
        val df2 = simpleDataFrame.repartition(col("id"))
        val executedPlan = collect(
          df1.union(df2).union(df2)
        )

        val plan = parser.apply(_.last)
        assert(plan.isInstanceOf[UnionExec])

        val union = plan.asInstanceOf[UnionExec]
        assert(union.children.head.isInstanceOf[ShuffleExchangeExec])

        assert(union.children(1).isInstanceOf[ReusedExchangeExec])
        assert(union.children(1).asInstanceOf[ReusedExchangeExec].child eq union.children(0))
        assert(union.children(1).output != union.children(0).output)
        assert(union.children(1).output.forall(_.dataType.isDefined))

        assert(union.children(2).isInstanceOf[ReusedExchangeExec])
        assert(union.children(2).asInstanceOf[ReusedExchangeExec].child eq union.children(0))
        assert(union.children(2).output != union.children(0).output)
        assert(union.children(2).output.forall(_.dataType.isDefined))

        assertSameOutput(plan.output, executedPlan.output)
      }

      executeAndParsePlan(sparkSession) { parser =>
        val df1 = manyColumnsDataFrame.repartition(col("integer"))
        val df2 = manyColumnsDataFrame.repartition(col("integer"))
        val executedPlan = collect(
          df1.union(df2).union(df2)
        )

        val plan = parser.apply(_.last)

        assertSameOutput(plan.output, executedPlan.output)
      }
    }
  }

  test("AdaptiveSparkPlan, ShuffleQueryStage, ResultQueryStageExec") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      executeAndParsePlan(sparkSession) { parser =>
        val executedPlan = collect(
          simpleDataFrame.repartition(5)
        )

        val plan = parser.apply(_.last)
        assert(plan.isInstanceOf[AdaptiveSparkPlanExec])

        val childPlan = plan.children.head
        val childIsResultQueryStage = childPlan.nodeName == "ResultQueryStageExec"
        val shuffleQueryStageExec =
          if (childIsResultQueryStage) {
            childPlan.children.head
          } else {
            childPlan
          }
        assert(shuffleQueryStageExec.isInstanceOf[ShuffleQueryStageExec])
        assertSameOutput(plan.output, executedPlan.output)
      }
    }
  }

  test("CustomShuffleReader") {
    val eventLogsPath = getResource("event-logs/spark_3_1_2_custom_shuffle_read_exec").getPath

    withStatusStore(eventLogsPath) { statusStore =>
      val sqlStatusStore = new SQLAppStatusStore(statusStore.store)
      val plan = parsePlan(sqlStatusStore, statusStore.executionsList().head)

      val reads = plan.collect { case p: CustomShuffleReaderExec => p }
      assert(reads.length == 1)
    }
  }

  test("BroadcastExchange, BroadcastQueryStage") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      executeAndParsePlan(sparkSession) { parser =>
        val executedPlan = collect(
          simpleDataFrame.join(sparkSession.range(10), "id")
        )
        val executedBroadcastExchange =
          findSparkPlan(executedPlan, _.isInstanceOf[execution.exchange.BroadcastExchangeExec]).head

        val plan = parser.apply(_.last)

        val broadcastStages = plan.collect { case p: BroadcastQueryStageExec => p }
        assert(broadcastStages.length == 1)
        assert(broadcastStages.head.child.isInstanceOf[BroadcastExchangeExec])
        assertSameOutput(broadcastStages.head.output, executedBroadcastExchange.output)

        val broadcastExchange = broadcastStages.head.child.asInstanceOf[BroadcastExchangeExec]
        assert(broadcastExchange.mode.isInstanceOf[HashedRelationBroadcastMode])
        assert(broadcastExchange.mode.asInstanceOf[HashedRelationBroadcastMode].key.length == 1)
        assertSameOutput(broadcastExchange.output, executedBroadcastExchange.output)
        assertSameMetrics(broadcastExchange.metrics, executedBroadcastExchange.metrics)
      }
    }
  }

  test("InMemoryTableScan, TableCacheQueryStage") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      "spark.sql.adaptive.forceApply" -> "true"
    ) {
      withTempPath { file =>
        randomDataset(exampleSchema, size = 1)(sparkSession)
          .write
          .parquet(file.getCanonicalPath)

        executeAndParsePlan(sparkSession) { parser =>
          val executedPlan = collect(
            sparkSession.read.parquet(file.getCanonicalPath).cache()
          )
          val executedInMemoryTableScans =
            findSparkPlan(executedPlan, _.isInstanceOf[execution.columnar.InMemoryTableScanExec])

          val plan = parser.apply(_.last)
          assert(plan.isInstanceOf[AdaptiveSparkPlanExec])

          val childPlan = plan.children.head
          val childIsResultQueryStage = childPlan.nodeName == "ResultQueryStageExec"
          val tableCacheQueryStageExec =
            if (childIsResultQueryStage) {
              childPlan.children.head
            } else {
              childPlan
            }
          assert(tableCacheQueryStageExec.isInstanceOf[TableCacheQueryStageExec])
          assert(tableCacheQueryStageExec.children.head.isInstanceOf[InMemoryTableScanExec])
          assertSameOutput(plan.output, executedPlan.output)

          val inMemoryTableScan =
            tableCacheQueryStageExec.children.head.asInstanceOf[InMemoryTableScanExec]
          assert(inMemoryTableScan.tableName.isEmpty)
          assertSameMetrics(inMemoryTableScan.metrics, executedInMemoryTableScans.head.metrics)
        }
      }

      // With predicates and variable output.
      executeAndParsePlan(sparkSession) { parser =>
        val executedPlan = collect(
          simpleDataFrame.cache().filter("id < 10").selectExpr("id", "id")
        )

        val plan = parser.apply(_.last)
        assertSameOutput(plan.output, executedPlan.output)
      }

      // Empty output.
      executeAndParsePlan(sparkSession) { parser =>
        collect(
          simpleDataFrame.cache().agg("*" -> "count")
        )

        val plan = parser.apply(_.last)
        val inMemoryTableScan = plan.collect { case p: InMemoryTableScanExec => p }.head
        assert(inMemoryTableScan.tableName.isEmpty)
        assert(inMemoryTableScan.output.isEmpty)
      }

      // Large output.
      executeAndParsePlan(sparkSession) { parser =>
        val executedPlan = collect(
          manyColumnsDataFrame.cache()
        )

        val plan = parser.apply(_.last)
        assertSameOutput(plan.output, executedPlan.output)
      }
    }

    // Cached table.
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withTable(sparkSession, "data", "cached") {
        sparkSession.range(1).createOrReplaceTempView("data")
        sparkSession.sql("CACHE TABLE cached AS SELECT * FROM data").collect()

        executeAndParsePlan(sparkSession) { parser =>
          val executedPlan = collect(
            sparkSession.sql("SELECT * FROM cached")
          )

          val plan = parser.apply(_.last)
          assert(plan.isInstanceOf[InMemoryTableScanExec])

          val inMemoryTableScan = plan.asInstanceOf[InMemoryTableScanExec]
          assert(inMemoryTableScan.tableName.contains("cached"))
          assertSameOutput(plan.output, executedPlan.output)
        }
      }
    }

    sparkSession.sharedState.cacheManager.clearCache()
  }

  test("HashAggregate") {
    withTable(sparkSession, "data") {
      simpleDataFrame.createOrReplaceTempView("data")

      executeAndParsePlan(sparkSession) { parser =>
        val executedPlan = collect(
          sparkSession.sql(
            """SELECT id % 2, AVG(id) FILTER (WHERE id > 10) * 5
              |FROM data
              |GROUP BY id % 2""".stripMargin
          )
        )
        val executedAggregates =
          findSparkPlan(executedPlan, _.isInstanceOf[aggregate.HashAggregateExec])
        assert(executedAggregates.length == 2)

        val plan = parser.apply(_.last)

        val aggregates = plan.collect { case p: HashAggregateExec => p }
        assert(aggregates.length == 2)

        // Final aggregate.
        assert(aggregates.head.groupingExpressions.length == 1)
        assert(aggregates.head.groupingExpressions.head.dataType == LongType)

        assert(aggregates.head.aggregateExpressions.length == 1)
        val finalExpression = aggregates.head.aggregateExpressions.head
        assert(finalExpression.modePrefix.isEmpty)
        assert(!finalExpression.isDistinct)
        assert(finalExpression.filter.isEmpty)
        assert(finalExpression.aggregateFunction.isInstanceOf[Average])
        assert(finalExpression.aggregateFunction.dataType == DoubleType)
        assert(finalExpression.aggregateFunction.children.head.dataType == LongType)

        assert(aggregates.head.resultExpressions.length == 2)
        assert(aggregates.head.resultExpressions.head.children.head.dataType == LongType)
        assert(aggregates.head.resultExpressions(1).children.head.isInstanceOf[Multiply])
        assert(aggregates.head.resultExpressions(1).children.head.dataType == DoubleType)

        assert(aggregates.head.output.size == 2)
        assert(aggregates.head.output.head.dataType == LongType)
        assert(aggregates.head.output(1).dataType == DoubleType)

        assertSameMetrics(aggregates.head.metrics, executedAggregates.head.metrics)

        // Partial aggregate.
        assert(aggregates(1).groupingExpressions.length == 1)
        assert(aggregates(1).groupingExpressions.head.dataType == LongType)

        assert(aggregates(1).aggregateExpressions.length == 1)
        val partialExpression = aggregates(1).aggregateExpressions.head
        assert(partialExpression.modePrefix == "partial")
        assert(!partialExpression.isDistinct)
        assert(partialExpression.filter.nonEmpty)
        assert(partialExpression.filter.exists(_.isInstanceOf[GreaterThan]))
        assert(partialExpression.aggregateFunction.isInstanceOf[Average])
        assert(partialExpression.aggregateFunction.dataType == DoubleType)
        assert(partialExpression.aggregateFunction.children.head.dataType == LongType)

        assert(aggregates(1).resultExpressions.length == 3)
        assert(aggregates(1).resultExpressions.head.dataType == LongType)
        assert(aggregates(1).resultExpressions(1).dataType == DoubleType)
        assert(aggregates(1).resultExpressions(2).dataType == LongType)

        assert(aggregates(1).output == aggregates(1).resultExpressions)

        assertSameMetrics(aggregates(1).metrics, executedAggregates(1).metrics)
      }
    }
  }

  test("HashAggregate - without keys") {
    executeAndParsePlan(sparkSession) { parser =>
      val executedPlan = collect(
        sparkSession.range(10).agg("id" -> "sum")
      )
      val plan = parser.apply(_.last)

      assertSameOutput(plan.output, executedPlan.output)
    }
  }

  test("HashAggregate - without aggregates") {
    executeAndParsePlan(sparkSession) { parser =>
      val executedPlan = collect(
        sparkSession.range(10).selectExpr("id").groupBy("id").agg(col("id"))
      )
      val plan = parser.apply(_.last)

      assertSameOutput(plan.output, executedPlan.output)
    }

    executeAndParsePlan(sparkSession) { parser =>
      val executedPlan = collect(
        sparkSession.range(10).selectExpr("id", "id + 1 AS v").groupBy("v").agg(col("v"))
      )
      val plan = parser.apply(_.last)

      assertSameOutput(plan.output, executedPlan.output)
    }
  }

  test("HashAggregate - with PartialMerge") {
    executeAndParsePlan(sparkSession) { parser =>
      val executedPlan = collect(
        sparkSession
          .range(10)
          .selectExpr("id", "id + 1 AS v")
          .agg(countDistinct("v"), avg("id"))
      )
      val plan = parser.apply(_.last)

      assertSameOutput(plan.output, executedPlan.output)
    }
  }

  test("ObjectHashAggregate") {
    executeAndParsePlan(sparkSession) { parser =>
      val executedPlan = collect(
        sparkSession.range(10).agg(collect_list(col("id")))
      )
      val plan = parser.apply(_.last)

      assertSameOutput(plan.output, executedPlan.output)
    }
  }

  test("SortAggregate") {
    executeAndParsePlan(sparkSession) { parser =>
      val executedPlan = collect(
        sparkSession.range(10).agg(first(expr("cast(id as string)")))
      )
      val plan = parser.apply(_.last)

      assertSameOutput(plan.output, executedPlan.output)
    }
  }

  test("Subquery") {
    withTable(sparkSession, "data") {
      simpleDataFrame.createOrReplaceTempView("data")

      executeAndParsePlan(sparkSession) { parser =>
        val executedPlan = collect(
          sparkSession.sql(
            """SELECT id
              |FROM data
              |WHERE
              |  id > (SELECT AVG(id) FROM data) AND
              |  id + 1 < (SELECT AVG(id) FROM data) AND
              |  id < (SELECT MAX(id) FROM data WHERE id < 100) AND
              |  id > element_at((SELECT array(MIN(id)) FROM data WHERE id < 100), 0)""".stripMargin
          )
        )
        val executedFilter = findSparkPlan(executedPlan, _.isInstanceOf[execution.FilterExec]).head

        val plan = parser.apply(_.last)

        val filters = plan.collect { case p: FilterExec => p }
        assert(filters.length == 1)
        filters.head.condition.foreach { expression =>
          assert(!expression.isInstanceOf[UnknownExpression])
          ()
        }
        assertSameOutput(plan.output, executedPlan.output)
        assertSameMetrics(filters.head.metrics, executedFilter.metrics)
      }
    }

    // Dynamic partition pruning.
    withTempPath { file =>
      sparkSession
        .range(10)
        .selectExpr("id", "id % 5 as part")
        .write
        .partitionBy("part")
        .parquet(file.getAbsolutePath)

      withTable(sparkSession, "data") {
        sparkSession.read.parquet(file.getAbsolutePath).createOrReplaceTempView("data")

        executeAndParsePlan(sparkSession) { parser =>
          val executedPlan = collect(
            sparkSession.sql(
              """WITH group as (
                |  SELECT
                |    d1.id as id1,
                |    d2.id as id2,
                |    d1.part as part
                |  FROM
                |    data d1,
                |    data d2
                |  WHERE
                |    d1.part = d2.part
                |    AND d1.part > 2
                |)
                |  SELECT part, COUNT(id1), COUNT(id2)
                |  FROM group
                |  GROUP BY part
                |UNION ALL
                |  SELECT part, SUM(id1), SUM(id2)
                |  FROM group
                |  GROUP BY part""".stripMargin
            )
          )
          val executedInSubqueries = findSparkPlan(
            executedPlan,
            plan =>
              plan.isInstanceOf[execution.FileSourceScanExec] &&
                plan
                  .asInstanceOf[execution.FileSourceScanExec]
                  .partitionFilters
                  .exists(_.isInstanceOf[catalyst.expressions.DynamicPruningExpression])
          ).flatMap { case scan: execution.FileSourceScanExec =>
            scan.partitionFilters.collect {
              case expr: catalyst.expressions.DynamicPruningExpression =>
                expr.child
            }
          }
          assert(executedInSubqueries.length == 2)

          val plan = parser.apply(_.last)
          val inSubqueries = plan
            .collect { case scan: FileSourceScanExec => scan.partitionFilters }
            .flatten
            .collect { case expr: DynamicPruningExpression => expr.child }

          assert(inSubqueries.length == 2)
          assert(inSubqueries.forall(_.isInstanceOf[InSubqueryExec]))

          val subqueryBroadcasts = inSubqueries.map(_.asInstanceOf[InSubqueryExec].plan)
          assert(subqueryBroadcasts.head.isInstanceOf[SubqueryBroadcastExec])
          assert(subqueryBroadcasts.last.isInstanceOf[ReusedSubqueryExec])
          assert(
            subqueryBroadcasts.head eq
              subqueryBroadcasts.last.asInstanceOf[ReusedSubqueryExec].child
          )

          val subqueryBroadcast = subqueryBroadcasts.head.asInstanceOf[SubqueryBroadcastExec]
          assert(subqueryBroadcast.name.startsWith("dynamicpruning#"))
          assert(subqueryBroadcast.indices == Seq(0))
          assert(subqueryBroadcast.buildKeys.size == 1)
          assertSameMetrics(
            subqueryBroadcast.metrics,
            executedInSubqueries.head.asInstanceOf[spark.sql.execution.InSubqueryExec].plan.metrics
          )
        }
      }
    }
  }

  test("Sort") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      executeAndParsePlan(sparkSession) { parser =>
        val executedPlan = collect(
          sparkSession
            .range(10)
            .sort(expr("id % 5"), col("id").desc_nulls_last)
        )

        val plan = parser.apply(_.last)
        assert(plan.children.head.isInstanceOf[SortExec])

        val sort = plan.children.head.asInstanceOf[SortExec]
        assert(sort.sortOrder.length == 2)
        assert(sort.sortOrder.head.dataType == LongType)
        assert(sort.sortOrder.head.direction == Ascending)
        assert(sort.sortOrder.head.nullOrdering == NullsFirst)
        assert(sort.sortOrder(1).dataType == LongType)
        assert(sort.sortOrder(1).direction == Descending)
        assert(sort.sortOrder(1).nullOrdering == NullsLast)
        assertSameOutput(plan.output, executedPlan.output)
        assertSameMetrics(sort.metrics, executedPlan.children.head.metrics)
      }
    }
  }

  test("TakeOrderedAndProject") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      executeAndParsePlan(sparkSession) { parser =>
        val executedPlan = collect(
          sparkSession
            .range(10)
            .selectExpr("id", "id + 1")
            .sort(col("id").desc_nulls_last)
            .offset(1)
            .limit(2)
        )

        val plan = parser.apply(_.last)
        assert(plan.isInstanceOf[TakeOrderedAndProjectExec])

        val takeOrdered = plan.asInstanceOf[TakeOrderedAndProjectExec]
        assert(takeOrdered.limit == 3)
        assert(takeOrdered.offset == 1)

        assert(takeOrdered.sortOrder.length == 1)
        assert(takeOrdered.sortOrder.head.dataType == LongType)
        assert(takeOrdered.sortOrder.head.direction == Descending)
        assert(takeOrdered.sortOrder.head.nullOrdering == NullsLast)

        assert(takeOrdered.projectList.length == 2)
        assert(takeOrdered.projectList.head.dataType == LongType)
        assert(takeOrdered.projectList(1).dataType == LongType)

        assertSameOutput(plan.output, executedPlan.output)
        assertSameMetrics(plan.metrics, executedPlan.metrics)
      }
    }
  }

  test("SortMergeJoin") {
    withSQLConf(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "0",
      SQLConf.PREFER_SORTMERGEJOIN.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false"
    ) {
      val left = sparkSession.range(10).selectExpr("id", "id * 2 as m2")
      val right = sparkSession.range(10).selectExpr("id * 3 as m3")

      executeAndParsePlan(sparkSession) { parser =>
        val executedPlan = collect(
          left.join(
            right,
            left.col("m2") === right.col("m3") &&
              left.col("id") < right.col("m3"),
            "inner"
          )
        )

        val plan = parser.apply(_.last)
        assert(plan.children.head.isInstanceOf[SortMergeJoinExec])

        val join = plan.children.head.asInstanceOf[SortMergeJoinExec]
        assert(join.joinType == Inner)

        assert(join.leftKeys.length == 1)
        assert(join.leftKeys.head.dataType == LongType)

        assert(join.rightKeys.length == 1)
        assert(join.rightKeys.head.dataType == LongType)

        assert(join.condition.exists(_.isInstanceOf[LessThan]))
        assert(join.condition.get.children(0).dataType == LongType)
        assert(join.condition.get.children(1).dataType == LongType)

        assert(!join.isSkewJoin)

        assertSameOutput(plan.output, executedPlan.output)
        assertSameMetrics(join.metrics, executedPlan.children.head.metrics)
      }

      // No condition.
      executeAndParsePlan(sparkSession) { parser =>
        collect(
          left.join(
            right,
            left.col("m2") === right.col("m3"),
            "inner"
          )
        )

        val plan = parser.apply(_.last)

        assert(plan.children.head.asInstanceOf[SortMergeJoinExec].condition.isEmpty)
      }
    }

    // Skew join.
    withSQLConf(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "0",
      SQLConf.PREFER_SORTMERGEJOIN.key -> "true",
      SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD.key -> "10kb",
      SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "1kb"
    ) {
      executeAndParsePlan(sparkSession) { parser =>
        val left = sparkSession.range(10000).selectExpr("id", "id % 2 div 2 as m2")
        val right = sparkSession.range(10).selectExpr("id * 3 as m3")
        val executedPlan = collect(
          left.join(
            right,
            left.col("m2") === right.col("m3"),
            "inner"
          )
        )

        val plan = parser.apply(_.last)
        assert(plan.collect { case p: SortMergeJoinExec => p }.head.isSkewJoin)
        assertSameOutput(plan.output, executedPlan.output)
      }
    }
  }

  test("Join types") {
    Seq(
      (catalyst.plans.Inner, Inner),
      (catalyst.plans.Cross, Cross),
      (catalyst.plans.LeftOuter, LeftOuter),
      (catalyst.plans.RightOuter, RightOuter),
      (catalyst.plans.FullOuter, FullOuter),
      (catalyst.plans.LeftSemi, LeftSemi),
      (catalyst.plans.LeftAnti, LeftAnti),
      (
        catalyst.plans.ExistenceJoin(
          catalyst.expressions.AttributeReference("exists", spark.sql.types.BooleanType)(
            ExprId(1)
          )
        ),
        ExistenceJoin(AttributeReference("exists", BooleanType, 1))
      )
    ).foreach { case (sparkJoin, expected) =>
      val actual = SparkPlanParser.parseJoinType(sparkJoin.toString)
      assert(actual == expected)
    }
  }

  test("ShuffledHashJoin") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withTable(sparkSession, "left", "right") {
        sparkSession.range(10).selectExpr("id", "id * 2 as m2").createOrReplaceTempView("left")
        sparkSession.range(10).selectExpr("id * 3 as m3").createOrReplaceTempView("right")

        executeAndParsePlan(sparkSession) { parser =>
          val executedPlan = collect(
            sparkSession.sqlContext.sql(
              """SELECT /*+ SHUFFLE_HASH(left) */ *
                |FROM
                |  left JOIN right
                |ON
                |  left.m2 = right.m3 AND left.id < right.m3""".stripMargin
            )
          )

          val plan = parser.apply(_.last)
          assert(plan.children.head.isInstanceOf[ShuffledHashJoinExec])

          val join = plan.children.head.asInstanceOf[ShuffledHashJoinExec]
          assert(join.joinType == Inner)

          assert(join.leftKeys.length == 1)
          assert(join.leftKeys.head.dataType == LongType)

          assert(join.rightKeys.length == 1)
          assert(join.rightKeys.head.dataType == LongType)

          assert(join.condition.exists(_.isInstanceOf[LessThan]))
          assert(join.condition.get.children(0).dataType == LongType)
          assert(join.condition.get.children(1).dataType == LongType)

          assert(join.buildSide == BuildLeft)

          assert(!join.isSkewJoin)

          assertSameOutput(plan.output, executedPlan.output)
          assertSameMetrics(join.metrics, executedPlan.children.head.metrics)
        }

        // No condition.
        executeAndParsePlan(sparkSession) { parser =>
          val executedPlan = collect(
            sparkSession.sqlContext.sql(
              """SELECT /*+ SHUFFLE_HASH(right) */ *
                |FROM
                |  left JOIN right
                |ON
                |  left.m2 = right.m3""".stripMargin
            )
          )

          val plan = parser.apply(_.last)
          assert(plan.children.head.isInstanceOf[ShuffledHashJoinExec])

          val join = plan.children.head.asInstanceOf[ShuffledHashJoinExec]
          assert(join.condition.isEmpty)
          assert(join.buildSide == BuildRight)
          assertSameOutput(plan.output, executedPlan.output)
          assertSameMetrics(join.metrics, executedPlan.children.head.metrics)
        }
      }
    }

    // Skew join.
    withSQLConf(
      SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD.key -> "10kb",
      SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "1kb"
    ) {
      withTable(sparkSession, "left", "right") {
        sparkSession
          .range(10000)
          .selectExpr("id", "id % 2 div 2 as m2")
          .createOrReplaceTempView("left")

        sparkSession
          .range(10)
          .selectExpr("id * 3 as m3")
          .createOrReplaceTempView("right")

        executeAndParsePlan(sparkSession) { parser =>
          val executedPlan = collect(
            sparkSession.sqlContext.sql(
              """SELECT /*+ SHUFFLE_HASH(right) */ *
                |FROM
                |  left JOIN right
                |ON
                |  left.m2 = right.m3""".stripMargin
            )
          )

          val plan = parser.apply(_.last)
          assert(plan.collect { case p: ShuffledHashJoinExec => p }.head.isSkewJoin)
          assertSameOutput(plan.output, executedPlan.output)
        }
      }
    }
  }

  test("BroadcastHashJoin") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withTable(sparkSession, "left", "right") {
        sparkSession.range(10).selectExpr("id", "id * 2 as m2").createOrReplaceTempView("left")
        sparkSession.range(10).selectExpr("id * 3 as m3").createOrReplaceTempView("right")

        executeAndParsePlan(sparkSession) { parser =>
          val executedPlan = collect(
            sparkSession.sqlContext.sql(
              """SELECT /*+ BROADCAST(left) */ *
                |FROM
                |  left JOIN right
                |ON
                |  left.m2 = right.m3 AND left.id < right.m3""".stripMargin
            )
          )

          val plan = parser.apply(_.last)
          assert(plan.children.head.isInstanceOf[BroadcastHashJoinExec])

          val join = plan.children.head.asInstanceOf[BroadcastHashJoinExec]
          assert(join.joinType == Inner)

          assert(join.leftKeys.length == 1)
          assert(join.leftKeys.head.dataType == LongType)

          assert(join.rightKeys.length == 1)
          assert(join.rightKeys.head.dataType == LongType)

          assert(join.condition.exists(_.isInstanceOf[LessThan]))
          assert(join.condition.get.children(0).dataType == LongType)
          assert(join.condition.get.children(1).dataType == LongType)

          assert(join.buildSide == BuildLeft)

          assertSameOutput(plan.output, executedPlan.output)
          assertSameMetrics(join.metrics, executedPlan.children.head.metrics)
        }

        // No condition.
        executeAndParsePlan(sparkSession) { parser =>
          val executedPlan = collect(
            sparkSession.sqlContext.sql(
              """SELECT /*+ BROADCAST(right) */ *
                |FROM
                |  left JOIN right
                |ON
                |  left.m2 = right.m3""".stripMargin
            )
          )

          val plan = parser.apply(_.last)
          assert(plan.children.head.isInstanceOf[BroadcastHashJoinExec])

          val join = plan.children.head.asInstanceOf[BroadcastHashJoinExec]
          assert(join.condition.isEmpty)
          assert(join.buildSide == BuildRight)
          assertSameOutput(plan.output, executedPlan.output)
          assertSameMetrics(join.metrics, executedPlan.children.head.metrics)
        }
      }
    }
  }

  test("CartesianProduct") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withTable(sparkSession, "left", "right") {
        sparkSession.range(10).selectExpr("id", "id * 2 as m2").createOrReplaceTempView("left")
        sparkSession.range(10).selectExpr("id * 3 as m3").createOrReplaceTempView("right")

        executeAndParsePlan(sparkSession) { parser =>
          val executedPlan = collect(
            sparkSession.sqlContext.sql(
              """SELECT /*+ SHUFFLE_REPLICATE_NL(left) */ *
                |FROM
                |  left JOIN right
                |ON
                |  left.m2 = right.m3""".stripMargin
            )
          )

          val plan = parser.apply(_.last)
          assert(plan.isInstanceOf[CartesianProductExec])

          val join = plan.asInstanceOf[CartesianProductExec]
          assert(join.condition.exists(_.isInstanceOf[EqualTo]))
          assert(join.condition.get.children(0).dataType == LongType)
          assert(join.condition.get.children(1).dataType == LongType)

          assertSameOutput(plan.output, executedPlan.output)
          assertSameMetrics(join.metrics, executedPlan.metrics)
        }

        // No condition
        executeAndParsePlan(sparkSession) { parser =>
          val executedPlan = collect(
            sparkSession.sqlContext.sql(
              """SELECT /*+ SHUFFLE_REPLICATE_NL(left) */ *
                |FROM left JOIN right""".stripMargin
            )
          )

          val plan = parser.apply(_.last)
          assert(plan.isInstanceOf[CartesianProductExec])

          val join = plan.asInstanceOf[CartesianProductExec]
          assert(join.condition.isEmpty)
          assertSameOutput(plan.output, executedPlan.output)
          assertSameMetrics(join.metrics, executedPlan.metrics)
        }
      }
    }
  }

  test("BroadcastNestedLoopJoin") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withTable(sparkSession, "left", "right") {
        sparkSession.range(10).selectExpr("id", "id * 2 as m2").createOrReplaceTempView("left")
        sparkSession.range(10).selectExpr("id * 3 as m3").createOrReplaceTempView("right")

        executeAndParsePlan(sparkSession) { parser =>
          val executedPlan = collect(
            sparkSession.sqlContext.sql(
              """SELECT /*+ SHUFFLE_REPLICATE_NL(left) */ *
                |FROM
                |  left FULL OUTER JOIN right
                |ON
                |  left.id < right.m3""".stripMargin
            )
          )

          val plan = parser.apply(_.last)
          assert(plan.isInstanceOf[BroadcastNestedLoopJoinExec])

          val join = plan.asInstanceOf[BroadcastNestedLoopJoinExec]
          assert(join.buildSide == BuildRight)

          assert(join.joinType == FullOuter)

          assert(join.condition.exists(_.isInstanceOf[LessThan]))
          assert(join.condition.get.children(0).dataType == LongType)
          assert(join.condition.get.children(1).dataType == LongType)

          assert(join.right.isInstanceOf[BroadcastExchangeExec])
          assert(join.right.asInstanceOf[BroadcastExchangeExec].mode == IdentityBroadcastMode)

          assertSameOutput(plan.output, executedPlan.output)
          assertSameMetrics(join.metrics, executedPlan.metrics)
        }

        // No condition.
        executeAndParsePlan(sparkSession) { parser =>
          val executedPlan = collect(
            sparkSession.sqlContext.sql(
              """SELECT /*+ SHUFFLE_REPLICATE_NL(left) */ *
                |FROM
                |  left FULL OUTER JOIN right""".stripMargin
            )
          )

          val plan = parser.apply(_.last)
          assert(plan.asInstanceOf[BroadcastNestedLoopJoinExec].condition.isEmpty)
          assertSameOutput(plan.output, executedPlan.output)
        }
      }
    }
  }

  test("Coalesce") {
    executeAndParsePlan(sparkSession) { parser =>
      val executedPlan = collect(
        simpleDataFrame.coalesce(1)
      )

      val plan = parser.apply(_.last)
      assert(plan.isInstanceOf[CoalesceExec])
      assert(plan.asInstanceOf[CoalesceExec].numPartitions == 1)
      assertSameOutput(plan.output, executedPlan.output)
    }
  }

  test("CollectLimit") {
    executeAndParsePlan(sparkSession) { parser =>
      val executedPlan = collect(
        simpleDataFrame.offset(1).limit(2)
      )

      val plan = parser.apply(_.last)
      assert(plan.isInstanceOf[CollectLimitExec])
      assert(plan.asInstanceOf[CollectLimitExec].limit == 3)
      assert(plan.asInstanceOf[CollectLimitExec].offset == 1)
      assertSameOutput(plan.output, executedPlan.output)
    }
  }

  test("CollectTail") {
    executeAndParsePlan(sparkSession) { parser =>
      val df = simpleDataFrame
      df.tail(2)

      val plan = parser.apply(_.last)
      assert(plan.isInstanceOf[CollectTailExec])
      assert(plan.asInstanceOf[CollectTailExec].limit == 2)
      assertSameOutput(plan.output, df.queryExecution.executedPlan.output)
    }
  }

  test("LocalLimit") {
    executeAndParsePlan(sparkSession) { parser =>
      val executedPlan = collect(
        simpleDataFrame.union(simpleDataFrame).limit(3)
      )

      val plan = parser.apply(_.last)

      val limits = plan.collect { case p: LocalLimitExec => p }
      assert(limits.length == 2)
      assert(limits.forall(_.limit == 3))
      assertSameOutput(plan.output, executedPlan.output)
    }
  }

  test("GlobalLimit") {
    executeAndParsePlan(sparkSession) { parser =>
      val df = simpleDataFrame
      collect(
        df.offset(1).limit(2).agg("id" -> "sum")
      )

      val plan = parser.apply(_.last)

      val limits = plan.collect { case p: GlobalLimitExec => p }
      assert(limits.length == 1)
      assert(limits.head.offset == 1)
      assert(limits.head.limit == 3)
      assertSameOutput(limits.head.output, df.queryExecution.executedPlan.output)
    }

    // No offset.
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      executeAndParsePlan(sparkSession) { parser =>
        collect(
          simpleDataFrame.limit(2).groupBy("id").agg("id" -> "sum")
        )

        val plan = parser.apply(_.last)
        val limit = plan.collect { case p: GlobalLimitExec => p }.head
        assert(limit.offset == 0)
        assert(limit.limit == 2)
      }
    }
  }

  test("Generate") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      val df = sparkSession.range(1).selectExpr("id", "id + 1 as id1")

      executeAndParsePlan(sparkSession) { parser =>
        val executedPlan = collect(
          df.selectExpr("explode(array(id, 2 * id, 3 * id))")
        )
        val executedGenerate = executedPlan.children.head.asInstanceOf[execution.GenerateExec]

        val plan = parser.apply(_.last)
        assert(plan.children.head.isInstanceOf[GenerateExec])

        val generate = plan.children.head.asInstanceOf[GenerateExec]
        assert(generate.generator.isInstanceOf[Explode])
        assert(!generate.outer)
        assertSameOutput(generate.requiredChildOutput, executedGenerate.requiredChildOutput)
        assertSameOutput(generate.generatorOutput, executedGenerate.generatorOutput)
        assertSameOutput(plan.output, executedPlan.output)
        assertSameMetrics(generate.metrics, executedGenerate.metrics)
      }

      // Outer.
      executeAndParsePlan(sparkSession) { parser =>
        val executedPlan = collect(
          df.selectExpr("explode_outer(array(id, 2 * id, 3 * id))")
        )

        val plan = parser.apply(_.last)
        assert(plan.children.head.asInstanceOf[GenerateExec].outer)
        assertSameOutput(plan.output, executedPlan.output)
      }

      // With child output.
      executeAndParsePlan(sparkSession) { parser =>
        val executedPlan = collect(
          df.selectExpr("id1", "explode(array(id, 2 * id, 3 * id))")
        )
        val executedGenerate = executedPlan.children.head.asInstanceOf[execution.GenerateExec]

        val plan = parser.apply(_.last)
        val generate = plan.children.head.asInstanceOf[GenerateExec]
        assertSameOutput(generate.requiredChildOutput, executedGenerate.requiredChildOutput)
        assertSameOutput(plan.output, executedPlan.output)
      }

      // Large output with known output.
      executeAndParsePlan(sparkSession) { parser =>
        val executedPlan = collect(
          manyColumnsDataFrame
            .selectExpr("*", "integer * 2")
            .selectExpr("explode(array(integer))", "*")
        )
        val executedGenerate = executedPlan.collect { case p: execution.GenerateExec => p }.head

        val plan = parser.apply(_.last)
        val generate = plan.collect { case p: GenerateExec => p }.head
        assertSameOutput(generate.requiredChildOutput, executedGenerate.requiredChildOutput)
        assertSameOutput(plan.output, executedPlan.output)
      }

      // Large output with unknown output.
      executeAndParsePlan(sparkSession) { parser =>
        val executedPlan = collect(
          manyColumnsDataFrame
            .selectExpr("*", "integer * 2")
            .selectExpr("*", "explode(array(integer))")
        )
        val executedGenerate = executedPlan.children.head.asInstanceOf[execution.GenerateExec]

        val plan = parser.apply(_.last)
        val generate = plan.children.head.asInstanceOf[GenerateExec]
        assertSameOutput(generate.requiredChildOutput, executedGenerate.requiredChildOutput)
      }
    }
  }

  test("Expand") {
    executeAndParsePlan(sparkSession) { parser =>
      val executedPlan = collect(
        manyColumnsDataFrame
          .cube("integer")
          .agg(avg("byte"))
      )
      val executedExpand = findSparkPlan(executedPlan, _.isInstanceOf[execution.ExpandExec])
        .head
        .asInstanceOf[execution.ExpandExec]

      val plan = parser.apply(_.last)
      val expand = plan.collect { case p: ExpandExec => p }.head
      assert(expand.projections.length == executedExpand.projections.length)
      expand.projections.zip(executedExpand.projections).foreach { case (actual, expected) =>
        assert(actual.length == expected.length)
      }
      assertSameOutput(plan.output, executedPlan.output)
      assertSameMetrics(expand.metrics, executedExpand.metrics)
    }
  }

  test("Window") {
    executeAndParsePlan(sparkSession) { parser =>
      val windowSpec = Window.partitionBy("short", "float").orderBy("long")
      val executedPlan = collect(
        manyColumnsDataFrame.select(
          avg(col("integer")).over(windowSpec).as("avg"),
          cume_dist().over(windowSpec).as("cume")
        )
      )
      val executedWindow =
        findSparkPlan(executedPlan, _.isInstanceOf[execution.window.WindowExec])
          .head
          .asInstanceOf[execution.window.WindowExec]

      val plan = parser.apply(_.last)
      val window = plan.collect { case p: WindowExec => p }.head
      assert(window.windowExpression.length == 2)
      assert(window.windowExpression.forall(_.children.head.isInstanceOf[WindowExpression]))
      assert(window.partitionSpec.length == 2)
      assert(window.orderSpec.length == 1)
      assertSameOutput(plan.output, executedPlan.output)
      assertSameMetrics(window.metrics, executedWindow.metrics)
    }

    // No partition spec.
    executeAndParsePlan(sparkSession) { parser =>
      val executedPlan = collect(
        simpleDataFrame.select(
          row_number().over(Window.orderBy("id")).as("num")
        )
      )

      val plan = parser.apply(_.last)
      val window = plan.collect { case p: WindowExec => p }.head
      assert(window.partitionSpec.isEmpty)
      assert(window.orderSpec.length == 1)
      assertSameOutput(plan.output, executedPlan.output)
    }

    // No order spec.
    executeAndParsePlan(sparkSession) { parser =>
      val executedPlan = collect(
        simpleDataFrame.select(
          avg(col("id")).over(Window.partitionBy("id")).as("num")
        )
      )

      val plan = parser.apply(_.last)
      val window = plan.collect { case p: WindowExec => p }.head
      assert(window.partitionSpec.length == 1)
      assert(window.orderSpec.isEmpty)
      assertSameOutput(plan.output, executedPlan.output)
    }

    // No partition and order spec.
    executeAndParsePlan(sparkSession) { parser =>
      val executedPlan = collect(
        simpleDataFrame.select(
          avg(col("id"))
            .over(Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
            .as("num")
        )
      )

      val plan = parser.apply(_.last)
      val window = plan.collect { case p: WindowExec => p }.head
      assert(window.partitionSpec.isEmpty)
      assert(window.orderSpec.isEmpty)
      assertSameOutput(plan.output, executedPlan.output)
    }
  }

  test("WindowGroupLimit") {
    executeAndParsePlan(sparkSession) { parser =>
      val window = Window
        .partitionBy("id")
        .orderBy("id")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

      val executedPlan = collect(
        simpleDataFrame
          .select(rank().over(window).as("rank"))
          .filter("rank <= 19")
      )
      val executedWindowLimits =
        findSparkPlan(executedPlan, _.isInstanceOf[execution.window.WindowGroupLimitExec])
          .map(_.asInstanceOf[execution.window.WindowGroupLimitExec])

      val plan = parser.apply(_.last)
      val windowLimits = plan.collect { case p: WindowGroupLimitExec => p }
      assert(windowLimits.length == 2)

      windowLimits.zip(executedWindowLimits).foreach { case (actual, expected) =>
        assert(actual.partitionSpec.length == 1)
        assert(actual.orderSpec.length == 1)
        assert(actual.rankLikeFunction.isInstanceOf[Rank])
        assert(actual.limit == 19)
        assertSameMetrics(actual.metrics, expected.metrics)
      }

      assert(windowLimits.head.mode == WindowGroupLimitMode.Final)
      assert(windowLimits(1).mode == WindowGroupLimitMode.Partial)
      assertSameOutput(plan.output, executedPlan.output)
    }

    // No partition spec.
    executeAndParsePlan(sparkSession) { parser =>
      val window = Window
        .orderBy("id")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

      collect(
        simpleDataFrame
          .select(rank().over(window).as("rank"))
          .filter("rank <= 19")
      )

      val plan = parser.apply(_.last)
      val windowLimits = plan.collect { case p: WindowGroupLimitExec => p }
      assert(windowLimits.length == 2)
      assert(windowLimits.forall(_.partitionSpec.isEmpty))
      assert(windowLimits.forall(_.orderSpec.length == 1))
    }
  }

  test("Execute") {
    executeAndParsePlan(sparkSession) { parser =>
      withTable(sparkSession, "data") {
        sparkSession.range(1).createOrReplaceTempView("data")

        val plan = parser.apply(_.last)
        assert(plan.isInstanceOf[ExecutedCommandExec])
        assert(plan.asInstanceOf[ExecutedCommandExec].cmd == CreateViewCommand)
        assert(plan.output.isEmpty)
      }
    }

    executeAndParsePlan(sparkSession) { parser =>
      withTempPath { file =>
        sparkSession.range(1).write.parquet(file.getAbsolutePath)

        val plan = parser.apply(_.last)
        assert(plan.isInstanceOf[DataWritingCommandExec])
        assert(plan.asInstanceOf[DataWritingCommandExec].cmd == InsertIntoHadoopFsRelationCommand)
        assert(plan.output.isEmpty)
      }
    }
  }

  test("WriteFiles") {
    executeAndParsePlan(sparkSession) { parser =>
      withTempPath { file =>
        sparkSession.range(1).write.parquet(file.getAbsolutePath)

        val plan = parser.apply(_.last)
        assert(plan.children.head.isInstanceOf[WriteFilesExec])
      }
    }
  }

  test("SerializeFromObject") {
    executeAndParsePlan(sparkSession) { parser =>
      val executedPlan = collect(
        sparkSession.createDataFrame(
          sparkSession.sparkContext.parallelize(0 until 10).map { i =>
            (
              SimpleCaseClass(i, i + 10),
              SimpleUDTClass(i, i + 10),
              Some(i),
              List(i, i + 1),
              Map((i, i + 1))
            )
          }
        )
      )

      val plan = parser.apply(_.last)
      val serializeFromObject = plan.collect { case p: SerializeFromObjectExec => p }.head
      assert(serializeFromObject.serializer.length == 5)

      val output = plan.output
      // The data types of UDT cannot be fully reconstructed.
      assert(
        output.head.dataType ==
          StructType(
            Array(
              StructField("x", AnyType),
              StructField("y", AnyType)
            )
          )
      )
      assert(output(1).dataType == AnyType)
      assertSameOutput(output.tail.tail, executedPlan.output.tail.tail)
    }
  }

  test("DeserializeToObject") {
    // CreateExternalRow.
    executeAndParsePlan(sparkSession) { parser =>
      import sparkSession.implicits._
      val executedPlan = collect(
        sparkSession
          .range(10)
          .toDF("id")
          .map(_.getLong(0))
      )
      val executedDeserializeToObject =
        findSparkPlan(executedPlan, _.isInstanceOf[execution.DeserializeToObjectExec]).head

      val plan = parser.apply(_.last)
      val deserializeToObject = plan.collect { case p: DeserializeToObjectExec => p }.head

      assert(deserializeToObject.deserializer.isInstanceOf[CreateExternalRow])
      assertSameOutput(deserializeToObject.output, executedDeserializeToObject.output)
    }

    // WrapOption.
    executeAndParsePlan(sparkSession) { parser =>
      import sparkSession.implicits._
      collect(
        sparkSession
          .range(10)
          .as[Option[Long]]
          .map(i => i)
      )

      assert(parser.apply(_.last).find(_.isInstanceOf[DeserializeToObjectExec]).isDefined)
    }

    // InitializeJavaBean.
    executeAndParsePlan(sparkSession) { parser =>
      val encoder = Encoders.bean(classOf[SimpleBeanClass])
      collect(
        sparkSession
          .range(10)
          .as[SimpleBeanClass](encoder)
          .map(i => i)(encoder)
      )

      assert(parser.apply(_.last).find(_.isInstanceOf[DeserializeToObjectExec]).isDefined)
    }

    // CatalystToExternalMap and ExternalMapToCatalyst.
    executeAndParsePlan(sparkSession) { parser =>
      import sparkSession.implicits._
      collect(
        sparkSession
          .range(10)
          .selectExpr("map(id, id)")
          .as[Map[Long, Long]]
          .map(i => i)
      )

      assert(parser.apply(_.last).find(_.isInstanceOf[DeserializeToObjectExec]).isDefined)
    }

    // EncodeUsingSerializer and DecodeUsingSerializer.
    executeAndParsePlan(sparkSession) { parser =>
      val encoder = Encoders.javaSerialization[SimpleCaseClass]
      collect(
        sparkSession
          .createDataFrame(
            sparkSession.sparkContext.parallelize(0 until 10).map { i =>
              val byteStream = new ByteArrayOutputStream()
              val objectStream = new ObjectOutputStream(byteStream)
              objectStream.writeObject(SimpleCaseClass(i, i))
              val bytes = byteStream.toByteArray
              Row(bytes)
            },
            spark.sql.types.StructType(
              Array(
                spark.sql.types.StructField("id", spark.sql.types.BinaryType)
              )
            )
          )
          .as(encoder)
          .map(i => i)(encoder)
      )

      assert(parser.apply(_.last).find(_.isInstanceOf[DeserializeToObjectExec]).isDefined)
    }

    // GetExternalRowField and ValidateExternalType.
    executeAndParsePlan(sparkSession) { parser =>
      val schema =
        spark.sql.types.StructType(
          Array(
            spark.sql.types.StructField("id", spark.sql.types.IntegerType)
          )
        )
      val encoder = Encoders.row(schema)
      collect(
        sparkSession
          .createDataFrame(
            sparkSession.sparkContext.parallelize(0 until 10).map(i => Row(i)),
            schema
          )
          .as(encoder)
          .map(i => i)(encoder)
      )

      assert(parser.apply(_.last).find(_.isInstanceOf[DeserializeToObjectExec]).isDefined)
    }
  }

  test("MapElements") {
    executeAndParsePlan(sparkSession) { parser =>
      import sparkSession.implicits._
      val executedPlan = collect(
        sparkSession.range(10).map(_ + 5)
      )
      val executedMapElements =
        findSparkPlan(executedPlan, _.isInstanceOf[execution.MapElementsExec]).head

      val plan = parser.apply(_.last)
      val mapElements = plan.collect { case p: MapElementsExec => p }.head

      assertSameOutput(mapElements.output, executedMapElements.output)
    }
  }

  test("MapPartitions") {
    executeAndParsePlan(sparkSession) { parser =>
      import sparkSession.implicits._
      val executedPlan = collect(
        sparkSession.range(10).mapPartitions(_.map(_ + 5))
      )
      val executedMapPartitions =
        findSparkPlan(executedPlan, _.isInstanceOf[execution.MapPartitionsExec]).head

      val plan = parser.apply(_.last)
      val mapPartitions = plan.collect { case p: MapPartitionsExec => p }.head

      assertSameOutput(mapPartitions.output, executedMapPartitions.output)
    }
  }

  test("MapGroupsExec, AppendColumns") {
    executeAndParsePlan(sparkSession) { parser =>
      import sparkSession.implicits._
      val executedPlan = collect(
        sparkSession
          .range(10)
          .groupByKey(identity)
          .mapGroups((_, values) => values.foldLeft(0L)(_ + _))(Encoders.scalaLong)
      )
      val executedMapGroups =
        findSparkPlan(executedPlan, _.isInstanceOf[execution.MapGroupsExec]).head

      val plan = parser.apply(_.last)

      val mapGroups = plan.collect { case p: MapGroupsExec => p }.head
      assertSameOutput(mapGroups.output, executedMapGroups.output)

      val appendColumns = plan.collect { case p: AppendColumnsExec => p }.head
      assert(appendColumns.serializer.length == 1)
      assert(appendColumns.serializer.head.children.head.isInstanceOf[Invoke])
    }
  }

  /**
   * Based on [[org.apache.spark.sql.ExpressionsSchemaSuite]].
   * Can be used to find missing expressions in the parser.
   */
  ignore("Expression coverage") {
    stopSparkSession()

    val sparkSession = sparkSessionBuilder
      .config(
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key,
        "org.apache.spark.sql.catalyst.optimizer.ConstantFolding"
      )
      .getOrCreate()
    val exampleRegex = """^(.+);\n(?s)(.+)$""".r

    withStatusStore(sparkSession) { statusStore =>
      sparkSession
        .sessionState
        .functionRegistry
        .listFunction()
        .map(funcId => sparkSession.sessionState.catalog.lookupFunctionInfo(funcId))
        .groupBy(_.getClassName)
        .toSeq
        .foreach { case (_, infos) =>
          infos.foreach { info =>
            val example = info.getExamples
            example.split("  > ").tail.filterNot(_.trim.startsWith("SET")).take(1).foreach {
              case exampleRegex(sql, _) =>
                Try(sparkSession.sql(sql).collect())
              case _ =>
            }
          }
        }

      val sqlStatusStore = sparkSession.sharedState.statusStore
      statusStore.executionsList().foreach { extendedExecution =>
        val plan = Try(parsePlan(sqlStatusStore, extendedExecution))
        lazy val execution = sqlStatusStore.execution(extendedExecution.executionId).get
        if (plan.isFailure) {
          println(execution.physicalPlanDescription)
          plan.failed.foreach(_.printStackTrace())
        } else {
          plan.get.allPlans.foreach { plan =>
            plan.expressions.foreach { expression =>
              expression.foreach {
                case expression @ (_: UnknownNode | _: StaticInvoke) =>
                  println(execution.physicalPlanDescription)
                  println(expression)
                case _ =>
              }
            }
          }
        }
      }
    }
  }

  private def simpleDataFrame: DataFrame = {
    sparkSession.read.parquet(getResource("simple.parquet").getPath)
  }

  private def manyColumnsDataFrame: DataFrame = {
    sparkSession.read.parquet(getResource("many_columns.parquet").getPath)
  }

  private def executeAndParsePlan[T](
      sparkSession: SparkSession
  )(execute: ((Seq[ExtendedSQLExecution] => ExtendedSQLExecution) => SparkPlan) => T): T = {
    withStatusStore(sparkSession) { statusStore =>
      execute { executionSelector =>
        // Wait for propagation of events, if necessary.
        var attempts = 1
        var executions: Seq[ExtendedSQLExecution] = statusStore.executionsList()
        while (executions.isEmpty) {
          if (attempts == 5) {
            fail(s"Failed to retrieve executions after $attempts attempts")
          }
          Thread.sleep(100)
          executions = statusStore.executionsList()
          attempts += 1
        }

        parsePlan(
          sparkSession.sharedState.statusStore,
          executionSelector(executions)
        )
      }
    }
  }

  private def parsePlan(
      sqlStatusStore: SQLAppStatusStore,
      extendedExecution: ExtendedSQLExecution
  ): SparkPlan = {
    val executionId = extendedExecution.executionId
    var execution = sqlStatusStore.execution(executionId)
    var attempts = 1
    while (execution.isEmpty) {
      if (attempts == 5) {
        fail(s"Failed to retrieve execution after $attempts attempts")
      }
      Thread.sleep(100)
      execution = sqlStatusStore.execution(executionId)
      attempts += 1
    }

    val parser = new SparkPlanParser(
      execution.map(_.physicalPlanDescription),
      sqlStatusStore.executionMetrics(executionId)
    )
    parser.parse(extendedExecution.sparkPlanInfo)
  }

  private def collect(dataset: Dataset[_]): execution.SparkPlan = {
    dataset.collect()
    dataset.queryExecution.executedPlan
  }

  private def assertSameOutput(
      actual: Seq[Attribute],
      expected: Seq[catalyst.expressions.Attribute]
  )(implicit pos: Position): Unit = {
    assert(actual.length == expected.length)
    expected.zip(actual).foreach { case (expected, actual) =>
      val expectedDataType = DataTypeUtils.fromSparkDataType(expected.dataType)
      assert(actual.name == expected.name)
      assert(actual.exprId == expected.exprId.id, expected.name)
      assert(actual.dataType == expectedDataType, expected.name)
    }
  }

  private def assertSameMetrics(
      actual: Map[String, SQLMetric],
      expected: Map[String, execution.metric.SQLMetric]
  )(implicit pos: Position): Unit = {
    assert(actual.size == expected.size)
    expected.foreach { case (_, metric) =>
      val metricName = metric.name.get
      assert(actual.contains(metricName), metricName)

      val actualMetric = actual(metricName)
      assert(actualMetric.metricType == metric.metricType, metricName)

      // Compare string values since precision may have been lost.
      val actualValue =
        TestTrampoline.metricsStringValue(metric.metricType, Array(actualMetric.value), Array.empty)
      val expectedValue =
        TestTrampoline.metricsStringValue(metric.metricType, Array(metric.value), Array.empty)
      assert(actualValue == expectedValue, metricName)
    }
  }

  private def stopSparkSession(): Unit = {
    try {
      try {
        sparkSession.sessionState.catalog.reset()
      } finally {
        sparkSession.stop()
      }
    } finally {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }
}

/**
 * Auxiliary object test classes.
 */

class SimpleBeanClass {
  @BeanProperty var id: Long = _
}

case class SimpleCaseClass(x: Int, y: Long) extends Serializable

@spark.sql.types.SQLUserDefinedType(udt = classOf[SimpleUDT])
case class SimpleUDTClass(x: Int, y: Long)

class SimpleUDT extends spark.sql.types.UserDefinedType[SimpleUDTClass] {

  private val schema = {
    spark.sql.types.StructType(
      Array(
        spark.sql.types.StructField("x", spark.sql.types.IntegerType),
        spark.sql.types.StructField("y", spark.sql.types.LongType)
      )
    )
  }

  override def sqlType: spark.sql.types.DataType = schema

  override def serialize(obj: SimpleUDTClass): Any = {
    val row = new SpecificInternalRow(schema.map(_.dataType))
    row.setInt(0, obj.x)
    row.setLong(1, obj.y)
    row
  }

  override def deserialize(datum: Any): SimpleUDTClass = {
    datum match {
      case values: spark.sql.catalyst.InternalRow =>
        SimpleUDTClass(values.getInt(0), values.getLong(1))
    }
  }

  override def userClass: Class[SimpleUDTClass] = classOf[SimpleUDTClass]
}
