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

import com.xonai.spark.sql.parser.types.{ArrayType, DataType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType}

/**
 * [[org.apache.spark.sql.catalyst.expressions.xml.XPathExtract]].
 */
abstract class XPathExtract extends BinaryExpression with ExpectsInputType {

  def xml: Expression

  def path: Expression

  override def left: Expression = xml

  override def right: Expression = path

  override def inputType: DataType = StringType
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.xml.XPathBoolean]].
 */
case class XPathBoolean(xml: Expression, path: Expression) extends XPathExtract with Predicate {

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(xml = newLeft, path = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.xml.XPathShort]].
 */
case class XPathShort(xml: Expression, path: Expression) extends XPathExtract {

  override def dataType: DataType = ShortType

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(xml = newLeft, path = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.xml.XPathInt]].
 */
case class XPathInt(xml: Expression, path: Expression) extends XPathExtract {

  override def dataType: DataType = IntegerType

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(xml = newLeft, path = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.xml.XPathLong]].
 */
case class XPathLong(xml: Expression, path: Expression) extends XPathExtract {

  override def dataType: DataType = LongType

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(xml = newLeft, path = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.xml.XPathFloat]].
 */
case class XPathFloat(xml: Expression, path: Expression) extends XPathExtract {

  override def dataType: DataType = FloatType

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(xml = newLeft, path = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.xml.XPathDouble]].
 */
case class XPathDouble(xml: Expression, path: Expression) extends XPathExtract {

  override def dataType: DataType = DoubleType

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(xml = newLeft, path = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.xml.XPathString]].
 */
case class XPathString(xml: Expression, path: Expression) extends XPathExtract {

  override def dataType: DataType = StringType

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(xml = newLeft, path = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.xml.XPathList]].
 */
case class XPathList(xml: Expression, path: Expression) extends XPathExtract {

  override def dataType: DataType = ArrayType(StringType)

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(xml = newLeft, path = newRight)
  }
}
