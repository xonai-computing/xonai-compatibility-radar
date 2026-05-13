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

import com.xonai.spark.sql.parser.types.{AnyType, DataType, StringType}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Collate]].
 */
case class Collate(child: Expression, collation: Expression)
    extends BinaryExpression
    with ExpectsInputTypes {

  override def left: Expression = child

  override def right: Expression = collation

  override def dataType: DataType = collation.dataType

  override def inputTypes: Seq[DataType] = Seq(StringType, AnyType)

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(child = newLeft, collation = newRight)
  }
}
