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

import com.xonai.spark.sql.parser.types.{ArrayType, BinaryType, BooleanType, DataType, IntegerType, LongType, StringType, TypeSet}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ConcatWs]].
 */
case class ConcatWs(children: IndexedSeq[Expression]) extends Expression with ExpectsInputTypes {

  override def dataType: DataType = StringType

  override def inputTypes: Seq[DataType] = {
    StringType +: children.tail.map(_ => TypeSet(ArrayType(StringType), StringType))
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Elt]].
 */
case class Elt(children: IndexedSeq[Expression]) extends Expression {

  override def dataType: DataType = {
    children.tail.headOption.map(_.dataType).getOrElse(StringType)
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = outputType.intersect(TypeSet(StringType, BinaryType))
    val inputDataType =
      children
        .tail
        .map(_.dataType)
        .foldLeft(newDataType)(DataType.intersection)

    withNewChildren(
      children.head.resolveDataType(IntegerType) +:
        children.tail.map(_.resolveDataType(inputDataType))
    )
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.String2StringExpression]].
 */
trait String2StringExpression extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = StringType

  override def inputType: DataType = StringType
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Upper]].
 */
case class Upper(child: Expression) extends String2StringExpression {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Lower]].
 */
case class Lower(child: Expression) extends String2StringExpression {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.StringPredicate]].
 */
abstract class StringPredicate extends BinaryExpression with Predicate

/**
 * [[org.apache.spark.sql.catalyst.expressions.Contains]].
 */
case class Contains(left: Expression, right: Expression) extends StringPredicate {

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.StartsWith]].
 */
case class StartsWith(left: Expression, right: Expression) extends StringPredicate {

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.EndsWith]].
 */
case class EndsWith(left: Expression, right: Expression) extends StringPredicate {

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.StringReplace]].
 */
case class StringReplace(srcExpr: Expression, searchExpr: Expression, replaceExpr: Expression)
    extends TernaryExpression
    with ExpectsInputType {

  override def first: Expression = srcExpr

  override def second: Expression = searchExpr

  override def third: Expression = replaceExpr

  override def dataType: DataType = StringType

  override def inputType: DataType = StringType

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(srcExpr = newFirst, searchExpr = newSecond, replaceExpr = newThird)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Overlay]].
 */
case class Overlay(input: Expression, replace: Expression, pos: Expression, len: Expression)
    extends QuaternaryExpression {

  override def first: Expression = input

  override def second: Expression = replace

  override def third: Expression = pos

  override def fourth: Expression = len

  override def dataType: DataType = input.dataType

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = outputType
      .intersect(input.dataType)
      .intersect(replace.dataType)
      .intersect(TypeSet(StringType, BinaryType))

    withNewChildren(
      IndexedSeq(
        input.resolveDataType(newDataType),
        replace.resolveDataType(newDataType),
        pos.resolveDataType(IntegerType),
        len.resolveDataType(IntegerType)
      )
    )
  }

  override def withNewChildrenInternal(
      first: Expression,
      second: Expression,
      third: Expression,
      fourth: Expression
  ): Expression = {
    copy(input = first, replace = second, pos = third, len = fourth)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.StringTranslate]].
 */
case class StringTranslate(srcExpr: Expression, matchingExpr: Expression, replaceExpr: Expression)
    extends TernaryExpression
    with ExpectsInputType {

  override def first: Expression = srcExpr

  override def second: Expression = matchingExpr

  override def third: Expression = replaceExpr

  override def dataType: DataType = StringType

  override def inputType: DataType = StringType

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(srcExpr = newFirst, matchingExpr = newSecond, replaceExpr = newThird)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.FindInSet]].
 */
case class FindInSet(left: Expression, right: Expression)
    extends BinaryExpression
    with ExpectsInputType {

  override def dataType: DataType = IntegerType

  override def inputType: DataType = StringType

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.String2TrimExpression]].
 */
trait String2TrimExpression extends Expression with ExpectsInputType {

  protected def srcStr: Expression

  protected def trimStr: Option[Expression]

  override def children: IndexedSeq[Expression] = srcStr +: trimStr.toIndexedSeq

  override def dataType: DataType = StringType

  override def inputType: DataType = StringType
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.StringTrim]].
 */
case class StringTrim(srcStr: Expression, trimStr: Option[Expression])
    extends String2TrimExpression {

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(
      srcStr = newChildren.head,
      trimStr = if (trimStr.isDefined) Some(newChildren(1)) else None
    )
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.StringTrimLeft]].
 */
case class StringTrimLeft(srcStr: Expression, trimStr: Option[Expression])
    extends String2TrimExpression {

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(
      srcStr = newChildren.head,
      trimStr = if (trimStr.isDefined) Some(newChildren(1)) else None
    )
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.StringTrimRight]].
 */
case class StringTrimRight(srcStr: Expression, trimStr: Option[Expression])
    extends String2TrimExpression {

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(
      srcStr = newChildren.head,
      trimStr = if (trimStr.isDefined) Some(newChildren(1)) else None
    )
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.StringInstr]].
 */
case class StringInstr(str: Expression, substr: Expression)
    extends BinaryExpression
    with ExpectsInputType {

  override def left: Expression = str

  override def right: Expression = substr

  override def dataType: DataType = IntegerType

  override def inputType: DataType = StringType

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(str = newLeft, substr = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.SubstringIndex]].
 */
case class SubstringIndex(strExpr: Expression, delimExpr: Expression, countExpr: Expression)
    extends TernaryExpression
    with ExpectsInputTypes {

  override def first: Expression = strExpr

  override def second: Expression = delimExpr

  override def third: Expression = countExpr

  override def dataType: DataType = StringType

  override def inputTypes: Seq[DataType] = Seq(StringType, StringType, IntegerType)

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(strExpr = newFirst, delimExpr = newSecond, countExpr = newThird)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.StringLocate]].
 */
case class StringLocate(substr: Expression, str: Expression, start: Expression)
    extends TernaryExpression
    with ExpectsInputTypes {

  override def first: Expression = substr

  override def second: Expression = str

  override def third: Expression = start

  override def dataType: DataType = IntegerType

  override def inputTypes: Seq[DataType] = Seq(StringType, StringType, IntegerType)

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(substr = newFirst, str = newSecond, start = newThird)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.StringLPad]].
 */
case class StringLPad(str: Expression, len: Expression, pad: Expression)
    extends TernaryExpression
    with ExpectsInputTypes {

  override def first: Expression = str

  override def second: Expression = len

  override def third: Expression = pad

  override def dataType: DataType = str.dataType

  override def inputTypes: Seq[DataType] = Seq(StringType, IntegerType, StringType)

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(str = newFirst, len = newSecond, pad = newThird)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.StringRPad]].
 */
case class StringRPad(str: Expression, len: Expression, pad: Expression)
    extends TernaryExpression
    with ExpectsInputTypes {

  override def first: Expression = str

  override def second: Expression = len

  override def third: Expression = pad

  override def dataType: DataType = str.dataType

  override def inputTypes: Seq[DataType] = Seq(StringType, IntegerType, StringType)

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(str = newFirst, len = newSecond, pad = newThird)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.BinaryPad]].
 */
case class BinaryPad(functionName: String, str: Expression, len: Expression, pad: Expression)
    extends TernaryExpression
    with ExpectsInputTypes {

  override def dataType: DataType = BinaryType

  override def first: Expression = str

  override def second: Expression = len

  override def third: Expression = pad

  override def inputTypes: Seq[DataType] = Seq(BinaryType, IntegerType, BinaryType)

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(str = newFirst, len = newSecond, pad = newThird)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.FormatString]].
 */
case class FormatString(children: IndexedSeq[Expression]) extends Expression {

  override def dataType: DataType = StringType

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.InitCap]].
 */
case class InitCap(child: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = StringType

  override def inputType: DataType = StringType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.StringRepeat]].
 */
case class StringRepeat(str: Expression, times: Expression)
    extends BinaryExpression
    with ExpectsInputTypes {

  override def left: Expression = str

  override def right: Expression = times

  override def dataType: DataType = StringType

  override def inputTypes: Seq[DataType] = Seq(StringType, IntegerType)

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(str = newLeft, times = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.StringSpace]].
 */
case class StringSpace(child: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = StringType

  override def inputType: DataType = IntegerType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Substring]].
 */
case class Substring(str: Expression, pos: Expression, len: Expression) extends TernaryExpression {

  override def first: Expression = str

  override def second: Expression = pos

  override def third: Expression = len

  override def dataType: DataType = str.dataType

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = outputType
      .intersect(dataType)
      .intersect(TypeSet(BinaryType, StringType))

    withNewChildren(
      IndexedSeq(
        str.resolveDataType(newDataType),
        pos.resolveDataType(IntegerType),
        len.resolveDataType(IntegerType)
      )
    )
  }

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(str = newFirst, pos = newSecond, len = newThird)
  }
}

/**
 * [[com.databricks.sql.optimizer.EphemeralSubstring]].
 */
case class EphemeralSubstring(str: Expression, pos: Expression, len: Expression)
    extends TernaryExpression {

  override def first: Expression = str

  override def second: Expression = pos

  override def third: Expression = len

  override def dataType: DataType = str.dataType

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(str = newFirst, pos = newSecond, len = newThird)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Length]].
 */
case class Length(child: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = IntegerType

  override def inputType: DataType = TypeSet(BinaryType, StringType)

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.BitLength]].
 */
case class BitLength(child: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = IntegerType

  override def inputType: DataType = TypeSet(BinaryType, StringType)

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.OctetLength]].
 */
case class OctetLength(child: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = IntegerType

  override def inputType: DataType = TypeSet(BinaryType, StringType)

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Levenshtein]].
 */
case class Levenshtein(left: Expression, right: Expression, threshold: Option[Expression])
    extends Expression
    with ExpectsInputTypes {

  override def dataType: DataType = IntegerType

  override def inputTypes: Seq[DataType] = {
    Seq(StringType, StringType) ++ threshold.map(_ => IntegerType)
  }

  override def children: IndexedSeq[Expression] = {
    IndexedSeq(left, right) ++ threshold.toIndexedSeq
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(
      left = newChildren(0),
      right = newChildren(1),
      threshold = if (threshold.isDefined) Some(newChildren(2)) else None
    )
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.SoundEx]].
 */
case class SoundEx(child: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = StringType

  override def inputType: DataType = StringType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Ascii]].
 */
case class Ascii(child: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = IntegerType

  override def inputType: DataType = StringType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Chr]].
 */
case class Chr(child: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = StringType

  override def inputType: DataType = LongType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Base64]].
 */
case class Base64(child: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = StringType

  override def inputType: DataType = BinaryType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.UnBase64]].
 */
case class UnBase64(child: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = BinaryType

  override def inputType: DataType = StringType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.StringDecode]].
 */
case class StringDecode(bin: Expression, charset: Expression)
    extends BinaryExpression
    with ExpectsInputTypes {

  override def left: Expression = bin

  override def right: Expression = charset

  override def dataType: DataType = StringType

  override def inputTypes: Seq[DataType] = Seq(BinaryType, StringType)

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(bin = newLeft, charset = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Encode]].
 */
case class Encode(value: Expression, charset: Expression)
    extends BinaryExpression
    with ExpectsInputType {

  override def left: Expression = value

  override def right: Expression = charset

  override def dataType: DataType = BinaryType

  override def inputType: DataType = StringType

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(value = newLeft, charset = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.FormatNumber]].
 */
case class FormatNumber(x: Expression, d: Expression)
    extends BinaryExpression
    with ExpectsInputTypes {

  override def left: Expression = x

  override def right: Expression = d

  override def dataType: DataType = StringType

  override def inputTypes: Seq[DataType] = {
    Seq(TypeSet.Numeric, TypeSet(IntegerType, StringType))
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(x = newLeft, d = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Sentences]].
 */
case class Sentences(str: Expression, language: Expression, country: Expression)
    extends TernaryExpression
    with ExpectsInputType {

  override def first: Expression = str

  override def second: Expression = language

  override def third: Expression = country

  override def dataType: DataType = ArrayType(ArrayType(StringType))

  override def inputType: DataType = StringType

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(str = newFirst, language = newSecond, country = newThird)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.StringSplitSQL]].
 */
case class StringSplitSQL(str: Expression, delimiter: Expression)
    extends BinaryExpression
    with ExpectsInputType {

  override def left: Expression = str

  override def right: Expression = delimiter

  override def dataType: DataType = ArrayType(StringType)

  override def inputType: DataType = StringType

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(str = newLeft, delimiter = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Luhncheck]].
 */
case class Luhncheck(child: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = BooleanType

  override def inputType: DataType = StringType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.IsValidUTF8]].
 */
case class IsValidUTF8(input: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = BooleanType

  override def child: Expression = input

  override def inputType: DataType = StringType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(input = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.MakeValidUTF8]].
 */
case class MakeValidUTF8(input: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = input.dataType

  override def child: Expression = input

  override def inputType: DataType = StringType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(input = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ValidateUTF8]].
 */
case class ValidateUTF8(input: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = input.dataType

  override def child: Expression = input

  override def inputType: DataType = StringType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(input = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.TryValidateUTF8]].
 */
case class TryValidateUTF8(input: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = input.dataType

  override def child: Expression = input

  override def inputType: DataType = StringType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(input = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Quote]].
 */
case class Quote(input: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = input.dataType

  override def child: Expression = input

  override def inputType: DataType = StringType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(input = newChild)
  }
}
