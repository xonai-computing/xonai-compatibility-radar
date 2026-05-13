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

import com.xonai.spark.sql.parser.types.{DataType, StringType}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Mask]].
 */
case class Mask(
    input: Expression,
    upperChar: Expression,
    lowerChar: Expression,
    digitChar: Expression,
    otherChar: Expression
) extends Expression
    with ExpectsInputType {

  override def dataType: DataType = StringType

  override def inputType: DataType = StringType

  override def children: IndexedSeq[Expression] = {
    IndexedSeq(input, upperChar, lowerChar, digitChar, otherChar)
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(
      input = newChildren(0),
      upperChar = newChildren(1),
      lowerChar = newChildren(2),
      digitChar = newChildren(3),
      otherChar = newChildren(4)
    )
  }
}
