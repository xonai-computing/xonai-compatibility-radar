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

package com.xonai.utils

import org.apache.spark.xonai.Trampoline

import java.io.{BufferedReader, InputStreamReader}
import scala.collection.mutable.ArrayBuffer

object Utils {

  def getSimpleName(cls: Class[_]): String = {
    Trampoline.utilsStripDollars(Trampoline.utilsGetSimpleName(cls))
  }

  def readResourceLines(resource: String): Seq[String] = {
    var reader: BufferedReader = null
    val result = ArrayBuffer[String]()
    try {
      val stream = getClass.getClassLoader.getResourceAsStream(resource)
      reader = new BufferedReader(new InputStreamReader(stream))

      var line: String = reader.readLine()
      while (line != null) {
        result += line
        line = reader.readLine()
      }
    } finally {
      if (reader != null) {
        reader.close()
      }
    }

    result.toSeq
  }

  /**
   * Based on [[org.apache.spark.sql.Dataset.showString]].
   */
  def tableString(
      rows: Seq[Seq[String]],
      maxColumnWidth: Int,
      leftPad: Set[Int] = Set.empty
  ): String = {
    val stringBuilder = new StringBuilder
    val minimumColWidth = 3
    val maxColumnWidthAdjusted = maxColumnWidth - 2
    val widths = Array.fill(rows.head.length)(minimumColWidth)

    val truncatedRows =
      rows.map { row =>
        row.map { cell =>
          val truncated =
            if (cell.length > maxColumnWidthAdjusted) {
              if (maxColumnWidthAdjusted < 4) {
                cell.substring(0, maxColumnWidthAdjusted)
              } else {
                cell.substring(0, maxColumnWidthAdjusted - 3) + "..."
              }
            } else {
              cell
            }
          " " + truncated + " "
        }
      }

    truncatedRows.foreach { row =>
      row.zipWithIndex.foreach { case (cell, i) =>
        widths(i) = math.max(widths(i), cell.length)
      }
    }

    val paddedRows =
      truncatedRows.map { row =>
        row.zipWithIndex.map { case (cell, i) =>
          if (leftPad.contains(i)) {
            stringLeftPad(cell, widths(i), ' ')
          } else {
            cell.padTo(widths(i), ' ')
          }
        }
      }

    // Line Separator.
    val separator = widths.map("-" * _).addString(stringBuilder, "+", "+", "+\n").toString()

    // Header
    paddedRows.head.addString(stringBuilder, "|", "|", "|\n")
    stringBuilder.append(separator)

    // Data.
    paddedRows.tail.foreach(_.addString(stringBuilder, "|", "|", "|\n"))
    stringBuilder.append(separator)

    stringBuilder.toString()
  }

  /**
   * Based on [[org.apache.spark.ui.UIUtils.formatDuration]].
   */
  def formatDuration(milliseconds: Long): String = {
    if (milliseconds < 1000) {
      return "%d ms".format(milliseconds)
    }
    val seconds = milliseconds.toDouble / 1000
    if (seconds < 60) {
      return "%.0f s".format(seconds)
    }
    val minutes = seconds / 60
    if (minutes < 60) {
      val remaining = seconds % 60
      return "%.0f min %02.0f s".format(minutes, remaining)
    }
    val hours = minutes / 60
    val remainingMinutes = minutes % 60
    val remainingSeconds = seconds % 60
    "%.0f h %02.0f min %02.0f s".format(hours, remainingMinutes, remainingSeconds)
  }

  def stringLeftPad(str: String, size: Int, padChar: Char): String = {
    if (str == null) {
      return null
    }
    val padSize = size - str.length()
    if (padSize <= 0) {
      return str
    }
    String.valueOf(Array.fill(padSize)(padChar)).concat(str)
  }
}
