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

package org.apache.spark

import org.apache.spark.util.Utils

import java.io.File
import scala.util.Try

object TestTrampoline {

  def utilsDeleteRecursively(file: File): Unit = {
    Utils.deleteRecursively(file)
  }

  private lazy val metricsStringValueMethod =
    Try(Class.forName("org.apache.spark.util.MetricUtils"))
      .getOrElse(Class.forName("org.apache.spark.sql.execution.metric.SQLMetrics"))
      .getMethod("stringValue", classOf[String], classOf[Array[Long]], classOf[Array[Long]])

  def metricsStringValue(
      metricsType: String,
      values: Array[Long],
      maxMetrics: Array[Long]
  ): String = {
    metricsStringValueMethod
      .invoke(null, metricsType, values, maxMetrics)
      .asInstanceOf[String]
  }
}
