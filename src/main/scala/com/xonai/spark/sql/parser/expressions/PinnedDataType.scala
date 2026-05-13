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

import com.xonai.spark.sql.parser.types.{AnyType, DataType}

trait PinnedDataType extends Expression {

  def withNewDataTypeInternal(newDataType: DataType): Expression

  def withNewDataType(newDataType: DataType): Expression = {
    if (dataType == newDataType) {
      this
    } else {
      val newExpr = withNewDataTypeInternal(newDataType)
      newExpr.copyTagsFrom(this)
      newExpr
    }
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = outputType.intersect(dataType)
    withNewDataType(newDataType).mapChildren(_.resolveDataType(AnyType))
  }
}
