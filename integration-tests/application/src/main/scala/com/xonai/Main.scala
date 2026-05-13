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

package com.xonai

import org.apache.spark.{SPARK_VERSION, Trampoline}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions.{avg, col, collect_list, countDistinct, cume_dist, expr, first, rank, row_number}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BinaryType, DataType, IntegerType, LongType, SQLUserDefinedType, StringType, StructField, StructType, UserDefinedType}

import java.io.{ByteArrayOutputStream, File, ObjectOutputStream}
import java.nio.file.Files
import java.util
import scala.beans.BeanProperty
import scala.math.Ordered.orderingToOrdered

/**
 * Application with multiple operators scenarios taken from SparkPlanParserSuite.
 */
object Main {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    try {
      run(spark)
    } finally {
      spark.close()
    }
  }

  def run(spark: SparkSession): Unit = {
    val parquetDirectory = createTempDir()
    parquetDirectory.delete()

    // Execute.
    spark.range(1).write.parquet(parquetDirectory.getAbsolutePath)

    withTable(spark, "data") {
      spark.range(1).createOrReplaceTempView("data")
    }

    // Parquet data source.
    val simpleDataFrame = spark.read.parquet(parquetDirectory.getAbsolutePath)
    simpleDataFrame.collect()

    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
      simpleDataFrame.collect()
    }

    withTempPath { file =>
      spark
        .range(10)
        .selectExpr("id", "id % 5 as part", "id * 2 as id2")
        .write
        .partitionBy("part")
        .parquet(file.getAbsolutePath)

      spark.read.parquet(file.getAbsolutePath).filter("part > 2").collect()
    }

    // LocalTableScan.
    spark
      .createDataFrame(
        util.Arrays.asList(Row(1, "str")),
        StructType(
          Array(
            StructField("a", IntegerType),
            StructField("b", StringType)
          )
        )
      )
      .collect()

    spark
      .createDataFrame(
        util.Arrays.asList(Row()),
        StructType(Array.empty[StructField])
      )
      .collect()

    spark
      .createDataFrame(
        util.Arrays.asList[Row](),
        StructType(
          Array(
            StructField("a", IntegerType)
          )
        )
      )
      .collect()

    spark
      .createDataFrame(
        util.Arrays.asList[Row](),
        StructType(Array.empty[StructField])
      )
      .collect()

    // HiveScan.
    withTable(spark, "data") {
      spark
        .range(1)
        .selectExpr("id", "id + 1 as id1", "id + 2 as Id2")
        .write
        .format("hive")
        .mode("overwrite")
        .saveAsTable("data")

      spark.sql("""SELECT id, id2 FROM data""").collect()
    }

    // RDDScan.
    spark
      .createDataFrame(
        spark.sparkContext.parallelize(Seq(1, 2, 10).map(v => Row(v, v.toString))),
        StructType(
          Array(
            StructField("a", IntegerType),
            StructField("b", StringType)
          )
        )
      )
      .collect()

    spark
      .createDataFrame(
        spark.sparkContext.parallelize((0 until 10).map(_ => Row())),
        StructType(Array.empty[StructField])
      )
      .collect()

    spark.sql("SELECT 1, 'str'").collect()

    // ExternalRDDScan.
    import spark.implicits._
    spark
      .createDataset(
        spark.sparkContext.parallelize(Seq(1, 2, 10)).setName("some rdd")
      )
      .collect()

    // TODO: RowDataSourceScan.

    // Range.
    spark.range(1, 10, 2, 3).selectExpr("id").collect()

    // Project.
    simpleDataFrame
      .selectExpr("id", "-id", "id AS other", "id IS NULL", "id + 1")
      .collect()

    simpleDataFrame.filter("id < 10").agg("*" -> "count").collect()

    // Sample.
    simpleDataFrame.sample(0.5).collect()

    // Exchange.
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      simpleDataFrame.repartition(5).collect()
    }

    // Union, ReusedExchange.
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df1 = simpleDataFrame.repartition(col("id"))
      val df2 = simpleDataFrame.repartition(col("id"))
      df1.union(df2).union(df2).collect()
    }

    // AdaptiveSparkPlan, ShuffleQueryStage, ResultQueryStageExec.
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      simpleDataFrame.repartition(5).collect()
    }

    // BroadcastExchange, BroadcastQueryStage.
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      simpleDataFrame.join(spark.range(10), "id").collect()
    }

    // InMemoryTableScan, TableCacheQueryStage.
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      "spark.sql.adaptive.forceApply" -> "true"
    ) {
      simpleDataFrame.cache().collect()

      simpleDataFrame.cache().filter("id < 10").selectExpr("id", "id").collect()

      simpleDataFrame.cache().agg("*" -> "count").collect()
    }

    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withTable(spark, "data", "cached") {
        spark.range(1).createOrReplaceTempView("data")
        spark.sql("CACHE TABLE cached AS SELECT * FROM data").collect()
        spark.sql("SELECT * FROM cached").collect()
      }
    }

    // HashAggregate.
    withTable(spark, "data") {
      simpleDataFrame.createOrReplaceTempView("data")

      spark
        .sql(
          """SELECT id % 2, AVG(id) FILTER (WHERE id > 10) * 5
            |FROM data
            |GROUP BY id % 2""".stripMargin
        )
        .collect()
    }

    spark.range(10).agg("id" -> "sum").collect()
    spark.range(10).selectExpr("id").groupBy("id").agg(col("id")).collect()
    spark.range(10).selectExpr("id", "id + 1 AS v").groupBy("v").agg(col("v")).collect()
    spark
      .range(10)
      .selectExpr("id", "id + 1 AS v")
      .agg(countDistinct("v"), avg("id"))
      .collect()
    spark.range(10).agg(collect_list(col("id"))).collect()
    spark.range(10).agg(first(expr("cast(id as string)"))).collect()

    // Subquery.
    withTable(spark, "data") {
      simpleDataFrame.createOrReplaceTempView("data")

      spark
        .sql(
          """SELECT id
            |FROM data
            |WHERE
            |  id > (SELECT AVG(id) FROM data) AND
            |  id + 1 < (SELECT AVG(id) FROM data) AND
            |  id < (SELECT MAX(id) FROM data WHERE id < 100) AND
            |  id > element_at((SELECT array(MIN(id)) FROM data WHERE id < 100), 0)""".stripMargin
        )
        .collect()
    }

    withTempPath { file =>
      spark
        .range(10)
        .selectExpr("id", "id % 5 as part")
        .write
        .partitionBy("part")
        .parquet(file.getAbsolutePath)

      withTable(spark, "data") {
        spark.read.parquet(file.getAbsolutePath).createOrReplaceTempView("data")
        spark
          .sql(
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
          .collect()
      }
    }

    // Sort.
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      spark
        .range(10)
        .sort(expr("id % 5"), col("id").desc_nulls_last)
        .collect()
    }

    // TakeOrderedAndProject.
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      offset(
        spark.range(10).selectExpr("id", "id + 1").sort(col("id").desc_nulls_last),
        n = 1
      )
        .limit(2)
        .collect()
    }

    // SortMergeJoin.
    withSQLConf(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "0",
      SQLConf.PREFER_SORTMERGEJOIN.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false"
    ) {
      val left = spark.range(10).selectExpr("id", "id * 2 as m2")
      val right = spark.range(10).selectExpr("id * 3 as m3")

      left
        .join(
          right,
          left.col("m2") === right.col("m3") && left.col("id") < right.col("m3"),
          "inner"
        )
        .collect()

      left
        .join(
          right,
          left.col("m2") === right.col("m3"),
          "inner"
        )
        .collect()
    }

    withSQLConf(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "0",
      SQLConf.PREFER_SORTMERGEJOIN.key -> "true",
      SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD.key -> "10kb",
      SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "1kb"
    ) {
      val left = spark.range(10000).selectExpr("id", "id % 2 div 2 as m2")
      val right = spark.range(10).selectExpr("id * 3 as m3")
      left
        .join(
          right,
          left.col("m2") === right.col("m3"),
          "inner"
        )
        .collect()
    }

    // ShuffledHashJoin.
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withTable(spark, "left", "right") {
        spark.range(10).selectExpr("id", "id * 2 as m2").createOrReplaceTempView("left")
        spark.range(10).selectExpr("id * 3 as m3").createOrReplaceTempView("right")

        spark
          .sqlContext
          .sql(
            """SELECT /*+ SHUFFLE_HASH(left) */ *
              |FROM
              |  left JOIN right
              |ON
              |  left.m2 = right.m3 AND left.id < right.m3""".stripMargin
          )
          .collect()

        spark
          .sqlContext
          .sql(
            """SELECT /*+ SHUFFLE_HASH(right) */ *
              |FROM
              |  left JOIN right
              |ON
              |  left.m2 = right.m3""".stripMargin
          )
          .collect()
      }
    }

    withSQLConf(
      SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD.key -> "10kb",
      SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "1kb"
    ) {
      withTable(spark, "left", "right") {
        spark
          .range(10000)
          .selectExpr("id", "id % 2 div 2 as m2")
          .createOrReplaceTempView("left")

        spark
          .range(10)
          .selectExpr("id * 3 as m3")
          .createOrReplaceTempView("right")

        spark
          .sqlContext
          .sql(
            """SELECT /*+ SHUFFLE_HASH(right) */ *
              |FROM
              |  left JOIN right
              |ON
              |  left.m2 = right.m3""".stripMargin
          )
          .collect()
      }
    }

    // BroadcastHashJoin.
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withTable(spark, "left", "right") {
        spark.range(10).selectExpr("id", "id * 2 as m2").createOrReplaceTempView("left")
        spark.range(10).selectExpr("id * 3 as m3").createOrReplaceTempView("right")

        spark
          .sqlContext
          .sql(
            """SELECT /*+ BROADCAST(left) */ *
              |FROM
              |  left JOIN right
              |ON
              |  left.m2 = right.m3 AND left.id < right.m3""".stripMargin
          )
          .collect()

        spark
          .sqlContext
          .sql(
            """SELECT /*+ BROADCAST(right) */ *
              |FROM
              |  left JOIN right
              |ON
              |  left.m2 = right.m3""".stripMargin
          )
          .collect()
      }
    }

    // CartesianProduct.
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withTable(spark, "left", "right") {
        spark.range(10).selectExpr("id", "id * 2 as m2").createOrReplaceTempView("left")
        spark.range(10).selectExpr("id * 3 as m3").createOrReplaceTempView("right")

        spark
          .sqlContext
          .sql(
            """SELECT /*+ SHUFFLE_REPLICATE_NL(left) */ *
              |FROM
              |  left JOIN right
              |ON
              |  left.m2 = right.m3""".stripMargin
          )
          .collect()

        spark
          .sqlContext
          .sql(
            """SELECT /*+ SHUFFLE_REPLICATE_NL(left) */ *
              |FROM left JOIN right""".stripMargin
          )
          .collect()
      }
    }

    // BroadcastNestedLoopJoin.
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withTable(spark, "left", "right") {
        spark.range(10).selectExpr("id", "id * 2 as m2").createOrReplaceTempView("left")
        spark.range(10).selectExpr("id * 3 as m3").createOrReplaceTempView("right")

        spark
          .sqlContext
          .sql(
            """SELECT /*+ SHUFFLE_REPLICATE_NL(left) */ *
              |FROM
              |  left FULL OUTER JOIN right
              |ON
              |  left.id < right.m3""".stripMargin
          )
          .collect()

        spark
          .sqlContext
          .sql(
            """SELECT /*+ SHUFFLE_REPLICATE_NL(left) */ *
              |FROM
              |  left FULL OUTER JOIN right""".stripMargin
          )
          .collect()
      }
    }

    // Coalesce.
    simpleDataFrame.coalesce(1).collect()

    // CollectLimit.
    offset(simpleDataFrame, 1).limit(2).collect()

    // CollectTail.
    simpleDataFrame.tail(2)

    // LocalLimit.
    simpleDataFrame.union(simpleDataFrame).limit(3).collect()

    // GlobalLimit.
    offset(simpleDataFrame, 1).limit(2).agg("id" -> "sum").collect()

    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      simpleDataFrame.limit(2).groupBy("id").agg("id" -> "sum").collect()
    }

    // Generate.
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      val df = spark.range(1).selectExpr("id", "id + 1 as id1")

      df.selectExpr("explode(array(id, 2 * id, 3 * id))").collect()
      df.selectExpr("explode_outer(array(id, 2 * id, 3 * id))").collect()
      df.selectExpr("id1", "explode(array(id, 2 * id, 3 * id))").collect()
    }

    // Expand.
    spark
      .range(10)
      .selectExpr("id", "id + 10 as id10", "id * 2 as id2")
      .cube("id")
      .agg(avg("id2"))
      .collect()

    // Window.
    {
      val windowSpec = Window.partitionBy("id10", "id2").orderBy("id3")
      spark
        .range(10)
        .selectExpr("id", "id + 10 as id10", "id * 2 as id2", "id - 3 as id3")
        .select(
          avg(col("id")).over(windowSpec).as("avg"),
          cume_dist().over(windowSpec).as("cume")
        )
        .collect()
    }

    simpleDataFrame
      .select(row_number().over(Window.orderBy("id")).as("num"))
      .collect()

    simpleDataFrame
      .select(avg(col("id")).over(Window.partitionBy("id")).as("num"))
      .collect()

    simpleDataFrame
      .select(
        avg(col("id"))
          .over(Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
          .as("num")
      )
      .collect()

    // WindowGroupLimit.
    {
      val window = Window
        .partitionBy("id")
        .orderBy("id")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

      simpleDataFrame
        .select(rank().over(window).as("rank"))
        .filter("rank <= 19")
        .collect()
    }
    {
      val window = Window
        .orderBy("id")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

      simpleDataFrame
        .select(rank().over(window).as("rank"))
        .filter("rank <= 19")
        .collect()
    }

    // SerializeFromObject.
    spark
      .createDataFrame(
        spark.sparkContext.parallelize(0 until 10).map { i =>
          (
            SimpleCaseClass(i, i + 10),
            SimpleUDTClass(i, i + 10),
            Some(i),
            List(i, i + 1),
            Map((i, i + 1))
          )
        }
      )
      .collect()

    // DeserializeToObject.
    spark.range(10).toDF("id").map(_.getLong(0)).collect()

    spark.range(10).as[Option[Long]].map(i => i).collect()

    {
      val encoder = Encoders.bean(classOf[SimpleBeanClass])
      spark.range(10).as[SimpleBeanClass](encoder).map(i => i)(encoder).collect()
    }

    spark.range(10).selectExpr("map(id, id)").as[Map[Long, Long]].map(i => i).collect()

    {
      val encoder = Encoders.javaSerialization[SimpleCaseClass]
      spark
        .createDataFrame(
          spark.sparkContext.parallelize(0 until 10).map { i =>
            val byteStream = new ByteArrayOutputStream()
            val objectStream = new ObjectOutputStream(byteStream)
            objectStream.writeObject(SimpleCaseClass(i, i))
            val bytes = byteStream.toByteArray
            Row(bytes)
          },
          StructType(
            Array(
              StructField("id", BinaryType)
            )
          )
        )
        .as(encoder)
        .map(i => i)(encoder)
        .collect()
    }

    if (!isVersionLessThan("3.5")) {
      val schema = StructType(Array(StructField("id", IntegerType)))
      val encoder = Encoders.row(schema)
      spark
        .createDataFrame(
          spark.sparkContext.parallelize(0 until 10).map(i => Row(i)),
          schema
        )
        .as(encoder)
        .map(i => i)(encoder)
        .collect()
    }

    // MapElements.
    spark.range(10).map(_ + 5).collect()

    // MapPartitions.
    spark.range(10).mapPartitions(_.map(_ + 5))

    // MapGroupsExec, AppendColumns.
    spark
      .range(10)
      .groupByKey(identity)
      .mapGroups((_, values) => values.foldLeft(0L)(_ + _))(Encoders.scalaLong)

    Trampoline.utilsDeleteRecursively(parquetDirectory)
  }

  def offset(df: DataFrame, n: Int): DataFrame = {
    val function =
      if (isVersionLessThan("3.4")) {
        (df: DataFrame, n: Int) => df
      } else {
        (df: DataFrame, n: Int) => df.offset(n)
      }
    function(df, n)
  }

  private def createTempDir(): File = {
    val path = Files.createTempDirectory("spark")
    new File(path.toString)
  }

  private def withTempPath(f: File => Unit): Unit = {
    val path = createTempDir()
    path.delete()
    try {
      f(path)
    } finally {
      Trampoline.utilsDeleteRecursively(path)
    }
  }

  /**
   * [[org.apache.spark.sql.catalyst.plans.SQLHelper]].
   */
  private def withSQLConf(pairs: (String, String)*)(f: => Unit): Unit = {
    val conf = SQLConf.get
    val (keys, values) = pairs.unzip
    val currentValues = keys.map { key =>
      if (conf.contains(key)) {
        Some(conf.getConfString(key))
      } else {
        None
      }
    }

    keys.zip(values).foreach { case (k, v) =>
      conf.setConfString(k, v)
    }

    try {
      f
    } finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) =>
          conf.setConfString(key, value)
        case (key, None) =>
          conf.unsetConf(key)
      }
    }
  }

  private def withTable(spark: SparkSession, tableNames: String*)(f: => Unit): Unit = {
    try {
      f
    } finally {
      tableNames.foreach { name =>
        spark.sql(s"DROP TABLE IF EXISTS $name")
      }
    }
  }

  private def isVersionLessThan(sparkVersion: String): Boolean = {
    val (actualMajor, actualMinor) = Trampoline.versionUtilsMajorMinorVersion(SPARK_VERSION)
    val (major, minor) = Trampoline.versionUtilsMajorMinorVersion(sparkVersion)

    (actualMajor, actualMinor) < (major, minor)
  }
}

/**
 * Auxiliary classes.
 */

class SimpleBeanClass {
  @BeanProperty var id: Long = _
}

case class SimpleCaseClass(x: Int, y: Long) extends Serializable

@SQLUserDefinedType(udt = classOf[SimpleUDT])
case class SimpleUDTClass(x: Int, y: Long)

class SimpleUDT extends UserDefinedType[SimpleUDTClass] {

  private val schema = {
    StructType(
      Array(
        StructField("x", IntegerType),
        StructField("y", LongType)
      )
    )
  }

  override def sqlType: DataType = schema

  override def serialize(obj: SimpleUDTClass): Any = {
    val row = new SpecificInternalRow(schema.map(_.dataType))
    row.setInt(0, obj.x)
    row.setLong(1, obj.y)
    row
  }

  override def deserialize(datum: Any): SimpleUDTClass = {
    datum match {
      case values: InternalRow =>
        SimpleUDTClass(values.getInt(0), values.getLong(1))
    }
  }

  override def userClass: Class[SimpleUDTClass] = classOf[SimpleUDTClass]
}
