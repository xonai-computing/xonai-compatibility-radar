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

import com.xonai.spark.sql.parser.types.{ArrayType, DataType, IntegerType, StringType}

/**
 * [[org.apache.spark.sql.catalyst.expressions.StringRegexExpression]].
 */
abstract class StringRegexExpression extends BinaryExpression with Predicate with ExpectsInputType {

  override def inputType: DataType = StringType
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Like]].
 */
case class Like(left: Expression, right: Expression, escapeChar: Char)
    extends StringRegexExpression {

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.RLike]].
 */
case class RLike(left: Expression, right: Expression) extends StringRegexExpression {

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.MultiLikeBase]].
 */
abstract class MultiLikeBase extends UnaryExpression with Predicate with ExpectsInputType {

  override def inputType: DataType = StringType
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.LikeAll]].
 */
case class LikeAll(child: Expression, patterns: Seq[String]) extends MultiLikeBase {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.NotLikeAll]].
 */
case class NotLikeAll(child: Expression, patterns: Seq[String]) extends MultiLikeBase {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.LikeAny]].
 */
case class LikeAny(child: Expression, patterns: Seq[String]) extends MultiLikeBase {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.NotLikeAny]].
 */
case class NotLikeAny(child: Expression, patterns: Seq[String]) extends MultiLikeBase {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.StringSplit]].
 */
case class StringSplit(str: Expression, regex: Expression, limit: Expression)
    extends TernaryExpression
    with ExpectsInputTypes {

  override def first: Expression = str

  override def second: Expression = regex

  override def third: Expression = limit

  override def dataType: DataType = ArrayType(StringType)

  override def inputTypes: Seq[DataType] = Seq(StringType, StringType, IntegerType)

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(str = newFirst, regex = newSecond, limit = newThird)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.RegExpReplace]].
 */
case class RegExpReplace(subject: Expression, regexp: Expression, rep: Expression, pos: Expression)
    extends QuaternaryExpression
    with ExpectsInputTypes {

  override def first: Expression = subject

  override def second: Expression = regexp

  override def third: Expression = rep

  override def fourth: Expression = pos

  override def dataType: DataType = StringType

  override def inputTypes: Seq[DataType] = {
    Seq(StringType, StringType, StringType, IntegerType)
  }

  override def withNewChildrenInternal(
      first: Expression,
      second: Expression,
      third: Expression,
      fourth: Expression
  ): Expression = {
    copy(subject = first, regexp = second, rep = third, pos = fourth)
  }
}

object RegExpReplace {

  def apply(children: IndexedSeq[Expression]): RegExpReplace = {
    new RegExpReplace(
      children.head,
      children.apply(1),
      children.apply(2),
      children.apply(3)
    )
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.RegExpExtractBase]].
 */
abstract class RegExpExtractBase extends TernaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[DataType] = {
    Seq(StringType, StringType, IntegerType)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.RegExpExtract]].
 */
case class RegExpExtract(subject: Expression, regexp: Expression, idx: Expression)
    extends RegExpExtractBase {

  override def first: Expression = subject

  override def second: Expression = regexp

  override def third: Expression = idx

  override def dataType: DataType = StringType

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(subject = newFirst, regexp = newSecond, idx = newThird)
  }
}

object RegExpExtract {

  def apply(children: IndexedSeq[Expression]): RegExpExtract = {
    new RegExpExtract(
      children.head,
      children.apply(1),
      children.apply(2)
    )
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.RegExpExtractAll]].
 */
case class RegExpExtractAll(subject: Expression, regexp: Expression, idx: Expression)
    extends RegExpExtractBase {

  override def first: Expression = subject

  override def second: Expression = regexp

  override def third: Expression = idx

  override def dataType: DataType = ArrayType(StringType)

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(subject = newFirst, regexp = newSecond, idx = newThird)
  }
}

object RegExpExtractAll {

  def apply(children: IndexedSeq[Expression]): RegExpExtractAll = {
    new RegExpExtractAll(
      children.head,
      children.apply(1),
      children.apply(2)
    )
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.RegExpInStr]].
 */
case class RegExpInStr(subject: Expression, regexp: Expression, idx: Expression)
    extends RegExpExtractBase {

  override def first: Expression = subject

  override def second: Expression = regexp

  override def third: Expression = idx

  override def dataType: DataType = IntegerType

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(subject = newFirst, regexp = newSecond, idx = newThird)
  }
}

object RegExpInStr {

  def apply(children: IndexedSeq[Expression]): RegExpInStr = {
    new RegExpInStr(
      children.head,
      children.apply(1),
      children.apply(2)
    )
  }
}
