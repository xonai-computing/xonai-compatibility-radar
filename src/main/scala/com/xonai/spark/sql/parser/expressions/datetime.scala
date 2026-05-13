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

import com.xonai.spark.sql.parser.types.{BooleanType, ByteType, CalendarIntervalType, DataType, DateType, DayTimeIntervalType, DecimalType, DoubleType, IntegerType, LongType, ShortType, StringType, TimestampNTZType, TimestampType, TypeSet, YearMonthIntervalType}

/**
 * [[org.apache.spark.sql.catalyst.expressions.CurrentTimeZone]].
 */
case class CurrentTimeZone() extends LeafExpression {

  override def dataType: DataType = StringType
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.CurrentDate]].
 */
case class CurrentDate() extends LeafExpression {

  override def dataType: DataType = DateType
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.CurrentTimestamp]].
 */
case class CurrentTimestamp() extends LeafExpression {

  override def dataType: DataType = TimestampType
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Now]].
 */
case class Now() extends LeafExpression {

  override def dataType: DataType = TimestampType
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.LocalTimestamp]].
 */
case class LocalTimestamp() extends LeafExpression {

  override def dataType: DataType = TimestampNTZType
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.GetDateField]].
 */
trait GetDateField extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = IntegerType

  override def inputType: DataType = DateType
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Year]].
 */
case class Year(child: Expression) extends GetDateField {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.YearOfWeek]].
 */
case class YearOfWeek(child: Expression) extends GetDateField {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Quarter]].
 */
case class Quarter(child: Expression) extends GetDateField {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Month]].
 */
case class Month(child: Expression) extends GetDateField {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.DayOfMonth]].
 */
case class DayOfMonth(child: Expression) extends GetDateField {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.DayOfYear]].
 */
case class DayOfYear(child: Expression) extends GetDateField {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.DayOfWeek]].
 */
case class DayOfWeek(child: Expression) extends GetDateField {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.WeekDay]].
 */
case class WeekDay(child: Expression) extends GetDateField {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.WeekOfYear]].
 */
case class WeekOfYear(child: Expression) extends GetDateField {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.GetTimeField]].
 */
trait GetTimeField extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = IntegerType

  override def inputType: DataType = TypeSet.AnyTimestamp
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Hour]].
 */
case class Hour(child: Expression, timeZoneId: Option[String]) extends GetTimeField {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Minute]].
 */
case class Minute(child: Expression, timeZoneId: Option[String]) extends GetTimeField {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.Second]].
 */
case class Second(child: Expression, timeZoneId: Option[String]) extends GetTimeField {

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.SecondWithFraction]].
 */
case class SecondWithFraction(child: Expression, timeZoneId: Option[String]) extends GetTimeField {

  override def dataType: DataType = DecimalType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.UnixDate]].
 */
case class UnixDate(child: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = IntegerType

  override def inputType: DataType = DateType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.DateFromUnixDate]].
 */
case class DateFromUnixDate(child: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = DateType

  override def inputType: DataType = IntegerType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.LastDay]].
 */
case class LastDay(child: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = DateType

  override def inputType: DataType = DateType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.SecondsToTimestamp]].
 */
case class SecondsToTimestamp(child: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = TimestampType

  override def inputType: DataType = TypeSet.Numeric

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.MillisToTimestamp]].
 */
case class MillisToTimestamp(child: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = TimestampType

  override def inputType: DataType = TypeSet.Integral

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.MicrosToTimestamp]].
 */
case class MicrosToTimestamp(child: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = TimestampType

  override def inputType: DataType = TypeSet.Integral

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.UnixSeconds]].
 */
case class UnixSeconds(child: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = LongType

  override def inputType: DataType = TimestampType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.CastTimestampNTZToLong]].
 */
case class CastTimestampNTZToLong(child: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = LongType

  override def inputType: DataType = TimestampNTZType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.UnixMillis]].
 */
case class UnixMillis(child: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = LongType

  override def inputType: DataType = TimestampType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.UnixMicros]].
 */
case class UnixMicros(child: Expression) extends UnaryExpression with ExpectsInputType {

  override def dataType: DataType = LongType

  override def inputType: DataType = TimestampType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.DateAdd]].
 */
case class DateAdd(startDate: Expression, days: Expression)
    extends BinaryExpression
    with ExpectsInputTypes {

  override def left: Expression = startDate

  override def right: Expression = days

  override def dataType: DataType = DateType

  override def inputTypes: Seq[DataType] = {
    Seq(DateType, TypeSet(IntegerType, ShortType, ByteType))
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(startDate = newLeft, days = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.DateSub]].
 */
case class DateSub(startDate: Expression, days: Expression)
    extends BinaryExpression
    with ExpectsInputTypes {

  override def left: Expression = startDate

  override def right: Expression = days

  override def dataType: DataType = DateType

  override def inputTypes: Seq[DataType] = {
    Seq(DateType, TypeSet(IntegerType, ShortType, ByteType))
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(startDate = newLeft, days = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.NextDay]].
 */
case class NextDay(startDate: Expression, dayOfWeek: Expression)
    extends BinaryExpression
    with ExpectsInputTypes {

  override def left: Expression = startDate

  override def right: Expression = dayOfWeek

  override def dataType: DataType = DateType

  override def inputTypes: Seq[DataType] = {
    Seq(DateType, StringType)
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(startDate = newLeft, dayOfWeek = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.AddMonths]].
 */
case class AddMonths(startDate: Expression, numMonths: Expression)
    extends BinaryExpression
    with ExpectsInputTypes {

  override def left: Expression = startDate

  override def right: Expression = numMonths

  override def dataType: DataType = DateType

  override def inputTypes: Seq[DataType] = {
    Seq(DateType, IntegerType)
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(startDate = newLeft, numMonths = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.DateAddYMInterval]].
 */
case class DateAddYMInterval(date: Expression, interval: Expression)
    extends BinaryExpression
    with ExpectsInputTypes {

  override def left: Expression = date

  override def right: Expression = interval

  override def dataType: DataType = DateType

  override def inputTypes: Seq[DataType] = {
    Seq(DateType, YearMonthIntervalType)
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(date = newLeft, interval = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.DateAddInterval]].
 */
case class DateAddInterval(start: Expression, interval: Expression)
    extends BinaryExpression
    with ExpectsInputTypes {

  override def left: Expression = start

  override def right: Expression = interval

  override def dataType: DataType = DateType

  override def inputTypes: Seq[DataType] = {
    Seq(DateType, CalendarIntervalType)
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(start = newLeft, interval = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.TruncDate]].
 */
case class TruncDate(date: Expression, format: Expression)
    extends BinaryExpression
    with ExpectsInputTypes {

  override def left: Expression = date

  override def right: Expression = format

  override def dataType: DataType = DateType

  override def inputTypes: Seq[DataType] = {
    Seq(DateType, StringType)
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(date = newLeft, format = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.DateFormatClass]].
 */
case class DateFormatClass(left: Expression, right: Expression, timeZoneId: Option[String])
    extends BinaryExpression
    with ExpectsInputTypes {

  override def dataType: DataType = StringType

  override def inputTypes: Seq[DataType] = {
    Seq(TimestampType, StringType)
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ToTimestamp]].
 */
abstract class ToTimestamp extends BinaryExpression with ExpectsInputTypes {

  override def dataType: DataType = LongType

  override def inputTypes: Seq[DataType] = {
    Seq(TypeSet(StringType, DateType, TimestampType, TimestampNTZType), StringType)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ToUnixTimestamp]].
 */
case class ToUnixTimestamp(timeExp: Expression, format: Expression, timeZoneId: Option[String])
    extends ToTimestamp {

  override def left: Expression = timeExp

  override def right: Expression = format

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(timeExp = newLeft, format = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.UnixTimestamp]].
 */
case class UnixTimestamp(timeExp: Expression, format: Expression, timeZoneId: Option[String])
    extends ToTimestamp {

  override def left: Expression = timeExp

  override def right: Expression = format

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(timeExp = newLeft, format = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.FromUnixTime]].
 */
case class FromUnixTime(sec: Expression, format: Expression, timeZoneId: Option[String])
    extends BinaryExpression
    with ExpectsInputTypes {

  override def left: Expression = sec

  override def right: Expression = format

  override def dataType: DataType = StringType

  override def inputTypes: Seq[DataType] = {
    Seq(LongType, StringType)
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(sec = newLeft, format = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.TimeAdd]].
 */
case class TimeAdd(start: Expression, interval: Expression) extends BinaryExpression {

  override def left: Expression = start

  override def right: Expression = interval

  override def dataType: DataType = start.dataType

  override def resolveDataType(outputType: DataType): Expression = {
    withNewChildren(
      IndexedSeq(
        start.resolveDataType(outputType.intersect(TypeSet.AnyTimestamp)),
        interval.resolveDataType(TypeSet(CalendarIntervalType, DayTimeIntervalType))
      )
    )
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(start = newLeft, interval = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.TimestampAddYMInterval]].
 */
case class TimestampAddYMInterval(timestamp: Expression, interval: Expression)
    extends BinaryExpression {

  override def left: Expression = timestamp

  override def right: Expression = interval

  override def dataType: DataType = timestamp.dataType

  override def resolveDataType(outputType: DataType): Expression = {
    withNewChildren(
      IndexedSeq(
        timestamp.resolveDataType(outputType.intersect(TypeSet.AnyTimestamp)),
        interval.resolveDataType(YearMonthIntervalType)
      )
    )
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(timestamp = newLeft, interval = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.DateDiff]].
 */
case class DateDiff(endDate: Expression, startDate: Expression)
    extends BinaryExpression
    with ExpectsInputType {

  override def left: Expression = endDate

  override def right: Expression = startDate

  override def dataType: DataType = IntegerType

  override def inputType: DataType = DateType

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(endDate = newLeft, startDate = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.UTCTimestamp]].
 */
trait UTCTimestamp extends BinaryExpression with ExpectsInputTypes {

  override def dataType: DataType = TimestampType

  override def inputTypes: Seq[DataType] = {
    Seq(TimestampType, StringType)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.FromUTCTimestamp]].
 */
case class FromUTCTimestamp(left: Expression, right: Expression) extends UTCTimestamp {

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ToUTCTimestamp]].
 */
case class ToUTCTimestamp(left: Expression, right: Expression) extends UTCTimestamp {

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.TruncTimestamp]].
 */
case class TruncTimestamp(format: Expression, timestamp: Expression, timeZoneId: Option[String])
    extends BinaryExpression
    with ExpectsInputTypes {

  override def left: Expression = format

  override def right: Expression = timestamp

  override def dataType: DataType = TimestampType

  override def inputTypes: Seq[DataType] = {
    Seq(StringType, TimestampType)
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(format = newLeft, timestamp = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.MonthsBetween]].
 */
case class MonthsBetween(
    date1: Expression,
    date2: Expression,
    roundOff: Expression,
    timeZoneId: Option[String]
) extends TernaryExpression
    with ExpectsInputTypes {

  override def first: Expression = date1

  override def second: Expression = date2

  override def third: Expression = roundOff

  override def dataType: DataType = DoubleType

  override def inputTypes: Seq[DataType] = {
    Seq(TimestampType, TimestampType, BooleanType)
  }

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(date1 = newFirst, date2 = newSecond, roundOff = newThird)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.MakeDate]].
 */
case class MakeDate(year: Expression, month: Expression, day: Expression)
    extends TernaryExpression
    with ExpectsInputType {

  override def first: Expression = year

  override def second: Expression = month

  override def third: Expression = day

  override def dataType: DataType = DateType

  override def inputType: DataType = IntegerType

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(year = newFirst, month = newSecond, day = newThird)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.MakeTimestamp]].
 */
case class MakeTimestamp(
    year: Expression,
    month: Expression,
    day: Expression,
    hour: Expression,
    min: Expression,
    sec: Expression,
    timezone: Option[Expression],
    timeZoneId: Option[String],
    failOnError: Boolean,
    dataType: DataType
) extends Expression
    with ExpectsInputTypes {

  override def children: IndexedSeq[Expression] = {
    IndexedSeq(year, month, day, hour, min, sec) ++ timezone.toIndexedSeq
  }

  override def inputTypes: Seq[DataType] = {
    Seq(IntegerType, IntegerType, IntegerType, IntegerType, IntegerType, DecimalType, StringType)
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    val newTimezone = if (timezone.isDefined) Some(newChildren(6)) else None
    copy(
      year = newChildren(0),
      month = newChildren(1),
      day = newChildren(2),
      hour = newChildren(3),
      min = newChildren(4),
      sec = newChildren(5),
      timezone = newTimezone
    )
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.GetTimestamp]].
 */
case class GetTimestamp(
    left: Expression,
    right: Expression,
    override val dataType: DataType,
    timeZoneId: Option[String]
) extends ToTimestamp {

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.CurrentBatchTimestamp]].
 */
case class CurrentBatchTimestamp(timestampMs: Long, override val dataType: DataType)
    extends LeafExpression

/**
 * [[org.apache.spark.sql.catalyst.expressions.SubtractTimestamps]].
 */
case class SubtractTimestamps(left: Expression, right: Expression, dataType: DataType)
    extends BinaryExpression
    with PinnedDataType {

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = outputType.intersect(dataType)
    withNewDataType(newDataType).mapChildren(_.resolveDataType(TypeSet.AnyTimestamp))
  }

  override def withNewDataTypeInternal(newDataType: DataType): Expression = {
    copy(dataType = newDataType)
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

object SubtractTimestamps {

  def apply(left: Expression, right: Expression): SubtractTimestamps = {
    new SubtractTimestamps(left, right, TypeSet(CalendarIntervalType, DayTimeIntervalType))
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.SubtractDates]].
 */
case class SubtractDates(left: Expression, right: Expression, dataType: DataType)
    extends BinaryExpression
    with PinnedDataType {

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = outputType.intersect(dataType)
    withNewDataType(newDataType).mapChildren(_.resolveDataType(DateType))
  }

  override def withNewDataTypeInternal(newDataType: DataType): Expression = {
    copy(dataType = newDataType)
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

object SubtractDates {

  def apply(left: Expression, right: Expression): SubtractDates = {
    new SubtractDates(left, right, TypeSet(CalendarIntervalType, DayTimeIntervalType))
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.ConvertTimezone]].
 */
case class ConvertTimezone(sourceTz: Expression, targetTz: Expression, sourceTs: Expression)
    extends TernaryExpression
    with ExpectsInputTypes {

  override def first: Expression = sourceTz

  override def second: Expression = targetTz

  override def third: Expression = sourceTs

  override def dataType: DataType = TimestampNTZType

  override def inputTypes: Seq[DataType] = {
    Seq(StringType, StringType, TimestampNTZType)
  }

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(sourceTz = newFirst, targetTz = newSecond, sourceTs = newThird)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.TimestampAdd]].
 */
case class TimestampAdd(
    unit: String,
    quantity: Expression,
    timestamp: Expression,
    timeZoneId: Option[String]
) extends BinaryExpression {

  override def left: Expression = quantity

  override def right: Expression = timestamp

  override def dataType: DataType = timestamp.dataType

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = outputType.intersect(TypeSet.AnyTimestamp)
    withNewChildren(
      IndexedSeq(
        quantity.resolveDataType(IntegerType),
        timestamp.resolveDataType(newDataType)
      )
    )
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(quantity = newLeft, timestamp = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.TimestampDiff]].
 */
case class TimestampDiff(
    unit: String,
    startTimestamp: Expression,
    endTimestamp: Expression,
    timeZoneId: Option[String]
) extends BinaryExpression
    with ExpectsInputType {

  override def left: Expression = startTimestamp

  override def right: Expression = endTimestamp

  override def dataType: DataType = LongType

  override def inputType: DataType = TimestampType

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(startTimestamp = newLeft, endTimestamp = newRight)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.DayName]].
 */
case class DayName(child: Expression) extends GetDateField {

  override def dataType: DataType = StringType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.MonthName]].
 */
case class MonthName(child: Expression) extends GetDateField {

  override def dataType: DataType = StringType

  override def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}
