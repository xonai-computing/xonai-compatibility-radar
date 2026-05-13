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

/**
 * [[org.apache.spark.sql.types.DataType]].
 */
trait DataType {

  def isDefined: Boolean = {
    this match {
      case AnyType | _: TypeSet =>
        false
      case structType: StructType =>
        structType.fields.forall(_.dataType.isDefined)
      case arrayType: ArrayType =>
        arrayType.elementType.isDefined
      case mapType: MapType =>
        mapType.keyType.isDefined && mapType.valueType.isDefined
      case udt: UserDefinedType =>
        udt.sqlType.isDefined
      case _ =>
        true
    }
  }

  def +(dataType: DataType): DataType = {
    DataType(Set(this, dataType))
  }

  def ++(dataTypes: Set[DataType]): DataType = {
    DataType(dataTypes + this)
  }

  def intersect(dataType: DataType): DataType = {
    DataType.intersection(this, dataType)
  }

  def getArrayElementType: DataType = {
    this match {
      case arrayType: ArrayType =>
        arrayType.elementType
      case AnyType =>
        AnyType
      case typeSet: TypeSet =>
        typeSet
          .types
          .collectFirst { case arrayType: ArrayType =>
            arrayType.elementType
          }
          .getOrElse(TypeSet.Empty)
      case _ =>
        TypeSet.Empty
    }
  }

  def getMapKeyType: DataType = {
    this match {
      case mapType: MapType =>
        mapType.keyType
      case AnyType =>
        AnyType
      case typeSet: TypeSet =>
        typeSet
          .types
          .collectFirst { case mapType: MapType =>
            mapType.keyType
          }
          .getOrElse(TypeSet.Empty)
      case _ =>
        TypeSet.Empty
    }
  }

  def getMapValueType: DataType = {
    this match {
      case mapType: MapType =>
        mapType.valueType
      case AnyType =>
        AnyType
      case typeSet: TypeSet =>
        typeSet
          .types
          .collectFirst { case mapType: MapType =>
            mapType.valueType
          }
          .getOrElse(TypeSet.Empty)
      case _ =>
        TypeSet.Empty
    }
  }

  def getStructFields(size: Int): Seq[StructField] = {
    lazy val emptyResult = (0 until size).map(_ => StructField("", TypeSet.Empty))
    this match {
      case structType: StructType =>
        structType.fields.toSeq
      case AnyType =>
        (0 until size).map(_ => StructField("", AnyType))
      case typeSet: TypeSet =>
        typeSet
          .types
          .collectFirst { case structType: StructType =>
            structType.fields.toSeq
          }
          .getOrElse(emptyResult)
      case _ =>
        emptyResult
    }
  }

  def transformDown(f: DataType => DataType): DataType = {
    f(this) match {
      case structType: StructType =>
        StructType(
          structType.fields.map { field =>
            StructField(field.name, field.dataType.transformDown(f))
          }
        )
      case arrayType: ArrayType =>
        ArrayType(
          arrayType.elementType.transformDown(f)
        )
      case mapType: MapType =>
        MapType(
          mapType.keyType.transformDown(f),
          mapType.valueType.transformDown(f)
        )
      case typeSet: TypeSet =>
        DataType(
          typeSet.types.map(_.transformDown(f))
        )
      case dataType =>
        dataType
    }
  }

  def foreach(f: DataType => Unit): Unit = {
    f(this)
    this match {
      case typeSet: TypeSet =>
        typeSet.types.foreach(f)
      case _ =>
    }
  }

  def collect[T](pf: PartialFunction[DataType, T]): Seq[T] = {
    val result = new collection.mutable.ArrayBuffer[T]()
    val lifted = pf.lift
    foreach(node => lifted(node).foreach(result += _))
    result.toSeq
  }

  def collectFirst[T](pf: PartialFunction[DataType, T]): Option[T] = {
    foreach { dataType =>
      if (pf.isDefinedAt(dataType)) {
        return Some(pf.apply(dataType))
      }
    }
    None
  }
}

object DataType {

  def apply(types: Set[DataType]): DataType = {
    if (types.isEmpty) {
      TypeSet.Empty
    } else if (types.size == 1) {
      types.head
    } else if (types.contains(AnyType)) {
      AnyType
    } else if (types.exists(_.isInstanceOf[TypeSet])) {
      TypeSet(
        types.flatMap {
          case typeSet: TypeSet =>
            typeSet.types
          case dataType =>
            Some(dataType)
        }
      )
    } else {
      TypeSet(types)
    }
  }

  /**
   * Returns the intersection of two data types. It allows narrowing data types that use [[AnyType]]
   * and [[TypeSet]].
   */
  def intersection(left: DataType, right: DataType): DataType = {
    if (left == right || right == AnyType) {
      return left
    } else if (left == AnyType) {
      return right
    }

    (left, right) match {
      case (left: TypeSet, right: TypeSet) =>
        val result =
          for {
            left <- left.types
            right <- right.types
            result = intersection(left, right)
            if !result.isInstanceOf[TypeSet]
          } yield {
            result
          }

        DataType(result)

      case (left: TypeSet, right) =>
        intersectTypeSet(left, right)

      case (left, right: TypeSet) =>
        intersectTypeSet(right, left)

      case (left: ArrayType, right: ArrayType) =>
        val elementType = intersection(left.elementType, right.elementType)
        if (elementType == TypeSet.Empty) {
          TypeSet.Empty
        } else {
          ArrayType(elementType)
        }

      case (left: MapType, right: MapType) =>
        val keyType = intersection(left.keyType, right.keyType)
        val valueType = intersection(left.valueType, right.valueType)
        if (keyType == TypeSet.Empty || valueType == TypeSet.Empty) {
          TypeSet.Empty
        } else {
          MapType(keyType, valueType)
        }

      case (left: StructType, right: StructType)
          if left.fields.length == right.fields.length =>
        val fields =
          left.fields.zip(right.fields).map { case (leftField, rightField) =>
            val name = if (leftField.name.nonEmpty) leftField.name else rightField.name
            val dataType = intersection(leftField.dataType, rightField.dataType)
            StructField(name, dataType)
          }
        if (fields.exists(_.dataType == TypeSet.Empty)) {
          TypeSet.Empty
        } else {
          StructType(fields)
        }

      case _ =>
        TypeSet.Empty
    }
  }

  private def intersectTypeSet(typeSet: TypeSet, dataType: DataType): DataType = {
    if (typeSet.types.contains(dataType)) {
      return dataType
    }

    typeSet
      .types
      .map(intersection(_, dataType))
      .find(!_.isInstanceOf[TypeSet])
      .getOrElse(TypeSet.Empty)
  }
}

case object AnyType extends DataType
case object NullType extends DataType
case object BooleanType extends DataType
case object ByteType extends DataType
case object ShortType extends DataType
case object IntegerType extends DataType
case object LongType extends DataType
case object FloatType extends DataType
case object DoubleType extends DataType
case object DecimalType extends DataType
case object StringType extends DataType
case object BinaryType extends DataType
case object DateType extends DataType
case object TimestampType extends DataType
case object TimestampNTZType extends DataType
case object CalendarIntervalType extends DataType
case object DayTimeIntervalType extends DataType
case object YearMonthIntervalType extends DataType
case object TimeType extends DataType
case object VariantType extends DataType
case object GeographyType extends DataType
case object GeometryType extends DataType
case object ArrayType extends DataType
case object StructType extends DataType
case object MapType extends DataType

case class ArrayType(elementType: DataType) extends DataType

case class StructField(name: String, dataType: DataType)

case class StructType(fields: Array[StructField]) extends DataType {

  override def equals(other: Any): Boolean = {
    other.isInstanceOf[StructType] && {
      val otherFields = other.asInstanceOf[StructType].fields
      fields.length == otherFields.length &&
      fields.indices.forall(i => fields(i) == otherFields(i))
    }
  }

  def indexOf(name: String): Int = {
    val lowerCaseName = name.toLowerCase
    fields.indexWhere(_.name.toLowerCase == lowerCaseName)
  }
}

case class MapType(keyType: DataType, valueType: DataType) extends DataType

case class ObjectType(className: String) extends DataType

case class UserDefinedType(sqlType: DataType) extends DataType

case class UnknownType(description: String) extends DataType

case class TypeSet(types: Set[DataType]) extends DataType {
  require(types.forall(!_.isInstanceOf[TypeSet]))

  override def +(dataType: DataType): DataType = {
    dataType ++ types
  }

  override def ++(dataTypes: Set[DataType]): DataType = {
    DataType(types ++ dataTypes)
  }

  def map(f: DataType => DataType): DataType = {
    DataType(types.map(f))
  }

  def flatMap(f: DataType => Iterable[DataType]): DataType = {
    DataType(types.flatMap(f))
  }
}

object TypeSet {

  val Empty =
    TypeSet()

  val Integral =
    TypeSet(ByteType, ShortType, IntegerType, LongType)

  val Numeric =
    TypeSet(ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, DecimalType)

  val NumericAndAnsiInterval =
    TypeSet(Numeric.types + DayTimeIntervalType + YearMonthIntervalType)

  lazy val NumericAndInterval =
    TypeSet(NumericAndAnsiInterval.types + CalendarIntervalType)

  lazy val AnyTimestamp =
    TypeSet(TimestampType, TimestampNTZType)

  lazy val Atomic =
    TypeSet(
      NumericAndAnsiInterval.types ++
        AnyTimestamp.types +
        BooleanType +
        DateType +
        StringType +
        BinaryType
    )

  def apply(types: DataType*): TypeSet = {
    new TypeSet(types.toSet)
  }
}
