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

import scala.collection.mutable.ArrayBuffer

/**
 * Common parsing functionality.
 */
trait Parser {

  def splitList(str: String): IndexedSeq[String] = {
    splitList(str, s => Some(s.trim))
  }

  def splitList[T](str: String, extract: String => Option[T]): IndexedSeq[T] = {
    val values = new ArrayBuffer[T]

    var i = 0
    var startIndex = 0
    var opened = 0
    while (i < str.length) {
      str.charAt(i) match {
        case '(' | '[' =>
          opened += 1
        case ')' | ']' =>
          opened -= 1
        case ',' =>
          // Checks for corner case when subquery uses a comma at root level.
          if (opened == 0 && !str.substring(i + 1).startsWith(" [id=#")) {
            val valueStr = str.substring(startIndex, i)
            val value = extract(valueStr)
            if (value.isDefined) {
              values += value.get
              startIndex = i + 1
            }
          }
        case _ =>
          ()
      }
      i += 1
    }

    if (
      (startIndex == 0 && i > 0) ||
      (startIndex > 0 && i >= startIndex)
    ) {
      val valueStr = str.substring(startIndex, i)
      values += extract(valueStr).getOrElse(throw ParseException(str, "Value cannot be extracted"))
    }

    values.toIndexedSeq
  }

  def splitSpacedList(str: String): IndexedSeq[String] = {
    val parts = splitList(str, Some(_))
    if (parts.isEmpty) {
      return IndexedSeq.empty
    }

    val result = new ArrayBuffer[String]()
    var i = 1
    var current = parts.head
    while (i < parts.length) {
      val part = parts(i)
      if (part.startsWith(" ")) {
        result += current
        current = part.tail
      } else {
        current = current + "," + part
      }
      i += 1
    }
    result += current
    result.toIndexedSeq
  }

  def splitParenthesesList(str: String): IndexedSeq[String] = {
    require(str.head == '(')
    require(str.last == ')')
    val listStr = str.substring(1, str.length - 1)
    splitList(listStr)
  }

  def splitSquareBracketsList(str: String): IndexedSeq[String] = {
    require(str.head == '[')
    require(str.last == ']')
    val listStr = str.substring(1, str.length - 1)
    splitList(listStr)
  }

  def getOption(str: String): Option[String] = {
    str match {
      case "None" =>
        None
      case str =>
        val valueStr = str.substring(5, str.length - 1)
        Some(valueStr)
    }
  }

  def getMap(str: String): Map[String, String] = {
    assert(str.startsWith("Map("))
    assert(str.endsWith(")"))
    splitSpacedList(str.substring(4, str.length - 1))
      .filter(_.nonEmpty)
      .map { pair =>
        val separatorIndex = pair.indexOf(" -> ")
        (
          pair.substring(0, separatorIndex),
          pair.substring(separatorIndex + 4)
        )
      }
      .toMap
  }

  def getClassName(str: String): String = {
    str.trim match {
      case s if s.startsWith("class ") =>
        s.substring(6)
      case s if s.startsWith("interface ") =>
        s.substring(10)
      case s =>
        s
    }
  }

  def splitTuple2(str: String): (String, String) = {
    require(str.head == '(')
    require(str.last == ')')
    val delimiter = str.indexOf(",")
    val key = str.substring(1, delimiter)
    val value = str.substring(delimiter + 1, str.length - 1)
    key -> value
  }

  /**
   * Finds the last scope at root level in a string and returns the start index.
   */
  def lastIndexOfScope(str: String, open: Char, close: Char): Int = {
    var i = str.length - 1
    var opened = 0
    while (i >= 0) {
      str.charAt(i) match {
        case `open` =>
          opened -= 1
          if (opened == 0) {
            return i
          }
        case `close` =>
          opened += 1
        case _ =>
          ()
      }
      i -= 1
    }

    -1
  }

  /**
   * Finds the first parentheses scope at root level in a string and returns the end index.
   */
  def indexOfParentheses(str: String, index: Int): Int = {
    var i = index
    var opened = 0
    while (i < str.length) {
      str.charAt(i) match {
        case '(' =>
          opened += 1
        case ')' =>
          opened -= 1
          if (opened == 0) {
            return i
          }
        case _ =>
          ()
      }
      i += 1
    }

    -1
  }

  /**
   * Find the last index of a character at root level starting from an index.
   */
  def rootLevelLastIndexOf(str: String, subString: String, index: Int): Int = {
    var i = index
    var opened = 0
    while (i >= 0) {
      if (opened == 0 && str.startsWith(subString, i)) {
        return i
      }

      str.charAt(i) match {
        case '(' | '[' =>
          opened -= 1
        case ')' | ']' =>
          opened += 1
        case _ =>
          ()
      }
      i -= 1
    }

    -1
  }
}
