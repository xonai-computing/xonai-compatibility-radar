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

package com.xonai.spark.test

import com.xonai.spark.sql.status.{ExtendedSQLAppStatusListener, ExtendedSQLAppStatusStore}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.TestTrampoline
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.{FileSourceScanExec, InSubqueryExec, ReusedSubqueryExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.util.kvstore.InMemoryStore
import org.apache.spark.xonai.HistoryProvider

import java.io.File
import java.net.URL
import java.nio.file.Files

trait TestUtils {

  def createTempDir(): File = {
    val path = Files.createTempDirectory("spark")
    new File(path.toString)
  }

  def withTempPath(f: File => Unit): Unit = {
    val path = createTempDir()
    path.delete()
    try {
      f(path)
    } finally {
      TestTrampoline.utilsDeleteRecursively(path)
    }
  }

  def withStatusStore[T](session: SparkSession)(f: ExtendedSQLAppStatusStore => T): T = {
    val store = new InMemoryStore()
    val statusStore = new ExtendedSQLAppStatusStore(store)
    val listener = new ExtendedSQLAppStatusListener(store)
    session.sparkContext.addSparkListener(listener)
    try {
      f(statusStore)
    } finally {
      session.sparkContext.removeSparkListener(listener)
    }
  }

  def withStatusStore[T](eventLogPath: String)(f: ExtendedSQLAppStatusStore => T): Option[T] = {
    val path = new Path(eventLogPath)
    val fileSystem = path.getFileSystem(new Configuration())

    val historyProvider = new HistoryProvider(fileSystem, path)
    val extendedListener = new ExtendedSQLAppStatusListener(historyProvider.store)
    historyProvider.addListener(extendedListener)
    historyProvider.rebuild { store =>
      val statusStore = new ExtendedSQLAppStatusStore(store)
      f(statusStore)
    }
  }

  def getResource(path: String): URL = {
    val resource = this.getClass.getClassLoader.getResource(path)
    if (resource == null) {
      throw new IllegalStateException(s"Could not find resource with path $path")
    }
    resource
  }

  /**
   * [[org.apache.spark.sql.catalyst.plans.SQLHelper]].
   */
  def withSQLConf(pairs: (String, String)*)(f: => Unit): Unit = {
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

  protected def withTable(spark: SparkSession, tableNames: String*)(f: => Unit): Unit = {
    try {
      f
    } finally {
      tableNames.foreach { name =>
        spark.sql(s"DROP TABLE IF EXISTS $name")
      }
    }
  }

  def findSparkPlan(plan: SparkPlan, f: SparkPlan => Boolean): List[SparkPlan] = {
    val childrenResults = plan match {
      case aqe: AdaptiveSparkPlanExec =>
        findSparkPlan(aqe.executedPlan, f)
      case qs: QueryStageExec =>
        findSparkPlan(qs.plan, f)
      case ims: InMemoryTableScanExec =>
        findSparkPlan(ims.relation.cachedPlan, f)
      case scan: FileSourceScanExec =>
        scan.expressions.foldLeft(List.empty[SparkPlan]) { (acc, child) =>
          acc ++
            child
              .collect {
                case in: InSubqueryExec
                    if !in.plan.isInstanceOf[ReusedSubqueryExec] =>
                  findSparkPlan(in.plan, f)
              }
              .flatten
        }
      case _ =>
        plan.children.foldLeft(List.empty[SparkPlan]) { (acc, child) =>
          acc ++ findSparkPlan(child, f)
        }
    }

    if (f(plan)) {
      plan :: childrenResults
    } else {
      childrenResults
    }
  }
}
