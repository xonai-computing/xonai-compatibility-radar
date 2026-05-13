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

package com.xonai.spark.sql.parser.types

case class DataTypeMap(map: Map[DataType, DataType]) {

  def outputType(dataType: DataType): DataType = {
    dataType match {
      case AnyType =>
        DataType(map.values.toSet)
      case typeSet: TypeSet =>
        typeSet.flatMap(map.get)
      case _ =>
        map.getOrElse(dataType, TypeSet.Empty)
    }
  }

  def inputType(outputType: DataType): DataType = {
    lazy val inputTypes =
      map
        .groupBy { case (_, to) => to }
        .map { case (outputType, inputs) => (outputType, inputs.keySet) }

    outputType match {
      case AnyType =>
        DataType(map.keySet)
      case typeSet: TypeSet =>
        typeSet.flatMap(inputTypes.getOrElse(_, Set.empty))
      case _ =>
        DataType(inputTypes.getOrElse(outputType, Set.empty))
    }
  }
}
