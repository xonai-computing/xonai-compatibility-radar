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

import java.util.regex.Pattern
import scala.concurrent.duration.Duration

class SQLMetricParser {

  private lazy val sizePattern = Pattern.compile("([0-9]+\\.[0-9]+)([a-z]+)?")

  /**
   * Parses a SQLMetric value from a string produced using
   * [[org.apache.spark.sql.execution.metric.SQLMetrics.stringValue]].
   */
  def parseValue(str: String, metricType: String): Long = {
    val trimmed = str.trim
    val parts = trimmed.split('\n').last.split('(')
    // Removes thousands separators.
    val total = parts.head.filter(_ != ',')
    val value =
      metricType match {
        case _ if total.isEmpty =>
          0L
        case "sum" =>
          total.toLong
        case "size" =>
          parseSize(total)
        case "timing" =>
          parseDuration(total).toMillis
        case "nsTiming" =>
          parseDuration(total).toNanos
        case "average" =>
          (total.toDouble * 10).toLong
        case _ =>
          0L
      }

    value
  }

  private def parseDuration(str: String): Duration = {
    // Workaround for old Scala versions.
    val canonical = str.replace(" m ", " minutes ")
    Duration(canonical)
  }

  private def parseSize(str: String): Long = {
    val canonicalized = str.toLowerCase.replace(" ", "")
    val matcher = sizePattern.matcher(canonicalized)
    if (!matcher.matches()) {
      return 0L
    }

    val unit = matcher.group(2)
    val factor = unit match {
      case "b" => 1
      case "k" => 1000
      case "kb" => 1000
      case "kib" => 1024
      case "m" => 1000 * 1000
      case "mb" => 1000 * 1000
      case "mib" => 1024 * 1024
      case "g" => 1000 * 1000 * 1000
      case "gb" => 1000 * 1000 * 1000
      case "gib" => 1024 * 1024 * 1024
      case "t" => 1000 * 1000 * 1000 * 1000
      case "tb" => 1000 * 1000 * 1000 * 1000
      case "tib" => 1024 * 1024 * 1024 * 1024
      case "p" => 1000 * 1000 * 1000 * 1000 * 1000
      case "pb" => 1000 * 1000 * 1000 * 1000 * 1000
      case "pib" => 1024 * 1024 * 1024 * 1024 * 1024
    }
    (BigDecimal(matcher.group(1)) * BigDecimal(factor)).toLong
  }
}
