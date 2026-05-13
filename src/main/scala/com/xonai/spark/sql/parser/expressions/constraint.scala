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

import com.xonai.spark.sql.parser.types.{AnyType, ArrayType, DataType}

/**
 * [[org.apache.spark.sql.catalyst.expressions.TaggingExpression]].
 */
trait TaggingExpression extends UnaryExpression {

  override def dataType: DataType = child.dataType
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.KnownNullable]].
 */
case class KnownNullable(child: Expression) extends TaggingExpression with PropagatesDataType {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.KnownNotNull]].
 */
case class KnownNotNull(child: Expression) extends TaggingExpression with PropagatesDataType {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.KnownFloatingPointNormalized]].
 */
case class KnownFloatingPointNormalized(child: Expression)
    extends TaggingExpression
    with PropagatesDataType {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.KnownNotContainsNull]].
 */
case class KnownNotContainsNull(child: Expression)
    extends TaggingExpression
    with DataTypeIsInputType {

  override def inputType: DataType = ArrayType(AnyType)

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}
