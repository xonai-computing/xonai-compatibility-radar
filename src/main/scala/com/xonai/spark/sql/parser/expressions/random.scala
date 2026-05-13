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

import com.xonai.spark.sql.parser.types.{DataType, DoubleType, IntegerType, LongType, StringType, TypeSet}

/**
 * [[org.apache.spark.sql.catalyst.expressions.RDG]].
 */
abstract class RDG extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = DoubleType

  override def inputType: DataType = TypeSet(IntegerType, LongType)
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Rand]].
 */
case class Rand(child: Expression) extends RDG {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Randn]].
 */
case class Randn(child: Expression) extends RDG {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.RandStr]].
 */
case class RandStr(length: Expression, seedExpression: Expression)
    extends BinaryExpression
    with ExpectsInputTypes {

  override def dataType: DataType = StringType

  override def left: Expression = length

  override def right: Expression = seedExpression

  override def inputTypes: Seq[DataType] = Seq(IntegerType, TypeSet(IntegerType, LongType))

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(length = newLeft, seedExpression = newRight)
  }
}
