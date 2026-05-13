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

import com.xonai.spark.sql.parser.types.{BinaryType, DataType, GeographyType, GeometryType, IntegerType, TypeSet}

/**
 * [[org.apache.spark.sql.catalyst.expressions.st.ST_AsBinary]].
 */
case class ST_AsBinary(geo: Expression) extends UnaryExpression with ExpectsInputType {

  override def child: Expression = geo

  override def dataType: DataType = BinaryType

  override def inputType: DataType = TypeSet(GeographyType, GeometryType)

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(geo = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.st.ST_GeogFromWKB]].
 */
case class ST_GeogFromWKB(wkb: Expression) extends UnaryExpression with ExpectsInputType {

  override def child: Expression = wkb

  override def dataType: DataType = GeographyType

  override def inputType: DataType = BinaryType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(wkb = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.st.ST_GeomFromWKB]].
 */
case class ST_GeomFromWKB(wkb: Expression) extends UnaryExpression with ExpectsInputType {

  override def child: Expression = wkb

  override def dataType: DataType = GeometryType

  override def inputType: DataType = BinaryType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(wkb = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.st.ST_Srid]].
 */
case class ST_Srid(geo: Expression) extends UnaryExpression with ExpectsInputType {

  override def child: Expression = geo

  override def dataType: DataType = IntegerType

  override def inputType: DataType = TypeSet(GeographyType, GeometryType)

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(geo = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.st.ST_SetSrid]].
 */
case class ST_SetSrid(geo: Expression, srid: Expression)
    extends BinaryExpression
    with ExpectsInputTypes {

  override def left: Expression = geo

  override def right: Expression = srid

  override def dataType: DataType = geo.dataType

  override def inputTypes: Seq[DataType] = Seq(TypeSet(GeographyType, GeometryType), IntegerType)

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(geo = newLeft, srid = newRight)
  }
}
