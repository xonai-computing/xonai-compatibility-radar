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

package com.xonai.spark.sql.parser

import com.xonai.spark.sql.parser.dsl.expressions._
import com.xonai.spark.sql.parser.expressions.{Abs, Acos, Acosh, Add, AddMonths, AesDecrypt, AesEncrypt, AggregateExpression, Alias, AmbiguousExpression, And, ApproxTopK, ApproxTopKAccumulate, ApproxTopKCombine, ApproxTopKEstimate, ApproximatePercentile, ArrayAggregate, ArrayAppend, ArrayContains, ArrayDistinct, ArrayExcept, ArrayExists, ArrayFilter, ArrayForAll, ArrayInsert, ArrayIntersect, ArrayJoin, ArrayMax, ArrayMin, ArrayPosition, ArrayRemove, ArrayRepeat, ArraySort, ArrayTransform, ArrayUnion, ArraysOverlap, ArraysZip, Ascending, Ascii, Asin, Asinh, AssertNotNull, AtLeastNNonNulls, Atan, Atan2, Atanh, Attribute, AttributeReference, Average, AvroDataToCatalyst, BRound, Base64, Bin, BinaryPad, BitAndAgg, BitLength, BitOrAgg, BitXorAgg, BitmapAndAgg, BitmapBitPosition, BitmapBucketNumber, BitmapConstructAgg, BitmapCount, BitmapOrAgg, BitwiseAnd, BitwiseCount, BitwiseGet, BitwiseNot, BitwiseOr, BitwiseReverse, BitwiseXor, BloomFilterAggregate, BloomFilterMightContain, BoundReference, CallMethodViaReflection, CaseWhen, Cast, CastTimestampNTZToLong, CatalystDataToAvro, CatalystDataToProtobuf, CatalystToExternalMap, Cbrt, Ceil, CheckOverflow, Chr, Coalesce, Collate, CollectList, CollectSet, Concat, ConcatWs, Conv, ConvertTimezone, Corr, Cos, Cosh, Cot, Count, CountMinSketchAgg, CovPopulation, CovSample, Crc32, CreateArray, CreateExternalRow, CreateMap, CreateNamedStruct, Csc, CsvToStructs, CurrentRow, DateAdd, DateAddInterval, DateAddYMInterval, DateDiff, DateFormatClass, DateFromUnixDate, DateSub, DayName, DayOfMonth, DayOfWeek, DayOfYear, DenseRank, Descending, Divide, DivideDTInterval, DivideInterval, DivideYMInterval, EWM, ElementAt, Elt, Encode, EqualNullSafe, EqualTo, Exp, Explode, Expm1, Expression, ExtractANSIIntervalDays, ExtractANSIIntervalHours, ExtractANSIIntervalMinutes, ExtractANSIIntervalMonths, ExtractANSIIntervalSeconds, ExtractANSIIntervalYears, ExtractIntervalDays, ExtractIntervalHours, ExtractIntervalMinutes, ExtractIntervalMonths, ExtractIntervalSeconds, ExtractIntervalYears, Factorial, FindInSet, First, Flatten, Floor, FormatNumber, FormatString, FromUTCTimestamp, FromUnixTime, GetArrayItem, GetArrayStructFields, GetJsonObject, GetMapValue, GetStructField, GetTimestamp, GreaterThan, GreaterThanOrEqual, Greatest, Hex, HistogramNumeric, HllSketchAgg, HllSketchEstimate, HllUnion, HllUnionAgg, Hour, HyperLogLogCardinality, HyperLogLogInitSimpleAgg, HyperLogLogPlusPlus, Hypot, If, In, InSet, Inline, IntegralDivide, Invoke, IsNaN, IsNotNull, IsNull, IsValidUTF8, IsVariantNull, JsonObjectKeys, JsonToStructs, JsonTuple, KllSketchAggBigint, KllSketchAggDouble, KllSketchAggFloat, KllSketchGetNBigint, KllSketchGetNDouble, KllSketchGetNFloat, KllSketchGetQuantileBigint, KllSketchGetQuantileDouble, KllSketchGetQuantileFloat, KllSketchGetRankBigint, KllSketchGetRankDouble, KllSketchGetRankFloat, KllSketchMergeBigint, KllSketchMergeDouble, KllSketchMergeFloat, KllSketchToStringBigint, KllSketchToStringDouble, KllSketchToStringFloat, KnownFloatingPointNormalized, KnownNotContainsNull, KnownNotNull, KnownNullable, Kurtosis, Lag, LambdaFunction, LambdaVariable, Last, LastDay, LastNonNull, Lead, Least, Length, LengthOfJsonArray, LessThan, LessThanOrEqual, Levenshtein, Like, LikeAll, LikeAny, ListAgg, Literal, Log, Log10, Log1p, Log2, Logarithm, Lower, Luhncheck, MakeDTInterval, MakeDate, MakeDecimal, MakeInterval, MakeTime, MakeTimestamp, MakeValidUTF8, MakeYMInterval, MapConcat, MapEntries, MapFilter, MapFromArrays, MapFromEntries, MapKeys, MapValues, MapZipWith, Mask, Max, MaxBy, Md5, MicrosToTimestamp, MillisToTimestamp, Min, MinBy, Minute, Mode, Month, MonthName, MonthsBetween, Multiply, MultiplyDTInterval, MultiplyInterval, MultiplyYMInterval, Murmur3Hash, NTile, NaNvl, NamedLambdaVariable, NewInstance, NextDay, NormalizeNaNAndZero, Not, NotLikeAll, NotLikeAny, NthValue, NullIndex, NullsFirst, NullsLast, OctetLength, Or, Overlay, ParseJson, ParseUrl, PercentRank, Percentile, PercentileDisc, PivotFirst, Pmod, PosExplode, Pow, PreciseTimestampConversion, Product, PromotePrecision, ProtobufDataToCatalyst, Quarter, Quote, RLike, RaiseError, Rand, RandStr, Randn, RangeFrame, Rank, RegExpExtract, RegExpExtractAll, RegExpInStr, RegExpReplace, RegrIntercept, RegrR2, RegrReplacement, RegrSXY, RegrSlope, Remainder, ReplicateRows, Reverse, Rint, Round, RoundCeil, RoundFloor, ST_AsBinary, ST_GeogFromWKB, ST_GeomFromWKB, ST_SetSrid, ST_Srid, SchemaOfCsv, SchemaOfJson, SchemaOfVariant, SchemaOfVariantAgg, SchemaOfXml, Sec, Second, SecondWithFraction, SecondsToTimestamp, Sentences, Sequence, Sha1, Sha2, ShiftLeft, ShiftRight, ShiftRightUnsigned, Shuffle, Signum, Sin, Sinh, Size, Skewness, Slice, SortArray, SortOrder, SoundEx, SpecifiedWindowFrame, Sqrt, Stack, StaticInvoke, StddevPop, StddevSamp, StringDecode, StringInstr, StringLPad, StringLocate, StringRPad, StringRepeat, StringReplace, StringSpace, StringSplit, StringSplitSQL, StringToMap, StringTranslate, StringTrim, StringTrimLeft, StringTrimRight, StructsToCsv, StructsToJson, StructsToXml, Substring, SubstringIndex, Subtract, SubtractDates, SubtractTimestamps, Sum, Tan, Tanh, ThetaDifference, ThetaIntersection, ThetaIntersectionAgg, ThetaSketchAgg, ThetaSketchEstimate, ThetaUnion, ThetaUnionAgg, TimeAdd, TimeDiff, TimeTrunc, TimestampAdd, TimestampAddYMInterval, TimestampDiff, ToCharacter, ToDegrees, ToNumber, ToRadians, ToTime, ToUTCTimestamp, ToUnixTimestamp, ToVariantObject, TransformKeys, TransformValues, TruncDate, TruncTimestamp, TryEval, TryToNumber, TryValidateUTF8, UnBase64, UnaryMinus, UnaryPositive, UnboundedPreceding, Unhex, UnixDate, UnixMicros, UnixMillis, UnixSeconds, UnixTimestamp, UnknownAggregateFunction, UnknownSQLExpression, UnknownSQLFunction, UnscaledValue, UnwrapOption, Upper, UrlDecode, UrlEncode, ValidateUTF8, VariancePop, VarianceSamp, VariantExplode, VariantGet, WeekDay, WeekOfYear, WidthBucket, WindowExpression, WindowSpecDefinition, WrapOption, XPathBoolean, XPathDouble, XPathFloat, XPathInt, XPathList, XPathLong, XPathShort, XPathString, XmlToStructs, XxHash64, Year, YearOfWeek, ZipWith}
import com.xonai.spark.sql.parser.types.{AnyType, ArrayType, BinaryType, BooleanType, ByteType, CalendarIntervalType, DataType, DateType, DayTimeIntervalType, DecimalType, DoubleType, FloatType, GeographyType, GeometryType, IntegerType, LongType, MapType, NullType, ObjectType, ShortType, StringType, StructField, StructType, TimeType, TimestampNTZType, TimestampType, TypeSet, VariantType, YearMonthIntervalType}
import org.scalactic.source.Position
import org.scalatest.funsuite.AnyFunSuite

class DataTypeResolverSuite extends AnyFunSuite {

  implicit class SuiteDslString(sql: String) {

    def resolveLiteralIs(expected: DataType)(implicit pos: Position): Unit = {
      assert(DataTypeResolver.resolveLiteral(sql) == expected)
    }
  }

  implicit class SuiteDslExpression(expression: Expression) {

    def child: Expression = {
      expression.children.head
    }

    def child(index: Int): Expression = {
      expression.children(index)
    }
  }

  test("resolveReferences - without references") {
    val expression = Add("1", "2")
    val input = Seq.empty[Attribute]
    val resolved = DataTypeResolver.resolveReferences(expression, input)
    assert(resolved eq expression)
  }

  test("resolveReferences - single reference") {
    val expression = Add(AttributeReference("id", 0), "2")
    val input = Seq(AttributeReference("id", LongType, 0))
    val resolved = DataTypeResolver.resolveReferences(expression, input)

    assert(expression.child.dataType == AnyType)
    assert(resolved.child.dataType == LongType)
  }

  test("resolveReferences - multiple references") {
    val expression =
      Add(
        Multiply(
          AttributeReference("id", 0),
          AttributeReference("factor", 1)
        ),
        Subtract(
          AttributeReference("id", 2),
          "2"
        )
      )
    // Types are inconsistent, but we only want to test the type resolution.
    val input = Seq(
      AttributeReference("id", LongType, 0),
      AttributeReference("factor", ByteType, 1),
      AttributeReference("id", DoubleType, 2)
    )
    val resolved = DataTypeResolver.resolveReferences(expression, input)

    assert(resolved.children(0).children(0) == input.head)
    assert(resolved.children(0).children(1) == input(1))
    assert(resolved.children(1).children(0) == input(2))
  }

  test("resolveLiteral") {
    "" resolveLiteralIs StringType
    "null" resolveLiteralIs AnyType

    val booleanTypeSet = TypeSet(BooleanType, StringType)
    "false" resolveLiteralIs booleanTypeSet
    "true" resolveLiteralIs booleanTypeSet

    val byteTypeSet = TypeSet(ByteType, ShortType, IntegerType, LongType, DecimalType, StringType)
    "0" resolveLiteralIs byteTypeSet
    "1" resolveLiteralIs byteTypeSet
    "-1" resolveLiteralIs byteTypeSet
    "127" resolveLiteralIs byteTypeSet
    "-128" resolveLiteralIs byteTypeSet

    val shortTypeSet = TypeSet(ShortType, IntegerType, LongType, DecimalType, StringType)
    "128" resolveLiteralIs shortTypeSet
    "-129" resolveLiteralIs shortTypeSet
    "32767" resolveLiteralIs shortTypeSet
    "-32768" resolveLiteralIs shortTypeSet

    val integerTypeSet = TypeSet(IntegerType, LongType, DecimalType, StringType)
    "32768" resolveLiteralIs integerTypeSet
    "-32769" resolveLiteralIs integerTypeSet
    "2147483647" resolveLiteralIs integerTypeSet
    "-2147483648" resolveLiteralIs integerTypeSet

    val longTypeSet = TypeSet(LongType, DecimalType, StringType)
    "2147483648" resolveLiteralIs longTypeSet
    "-2147483649" resolveLiteralIs longTypeSet
    "9223372036854775807" resolveLiteralIs longTypeSet
    "-9223372036854775808" resolveLiteralIs longTypeSet

    val decimalTypeSet = TypeSet(DecimalType, StringType)
    "9223372036854775808" resolveLiteralIs decimalTypeSet
    "-9223372036854775809" resolveLiteralIs decimalTypeSet
    "99999999999999999999999999999999999999" resolveLiteralIs decimalTypeSet
    "-99999999999999999999999999999999999999" resolveLiteralIs decimalTypeSet

    val numericStringTypeSet = StringType
    "100000000000000000000000000000000000000" resolveLiteralIs numericStringTypeSet
    "-100000000000000000000000000000000000000" resolveLiteralIs numericStringTypeSet

    val floatingPointTypeSet = TypeSet(FloatType, DoubleType, DecimalType, StringType)
    "1.11" resolveLiteralIs floatingPointTypeSet
    "0.0" resolveLiteralIs floatingPointTypeSet
    "-0.0" resolveLiteralIs floatingPointTypeSet
    "1.7E-20" resolveLiteralIs floatingPointTypeSet
    "1.4E-45" resolveLiteralIs floatingPointTypeSet
    "4.9E-324" resolveLiteralIs floatingPointTypeSet
    "-1.7976931348623157E308" resolveLiteralIs floatingPointTypeSet
    "1.7976931348623157E308" resolveLiteralIs floatingPointTypeSet

    // String.
    "ABCD" resolveLiteralIs StringType
    "abcd" resolveLiteralIs StringType
    "123abcd" resolveLiteralIs StringType
    "abcd123" resolveLiteralIs StringType
    "with spaces" resolveLiteralIs StringType
    "- with - dash" resolveLiteralIs StringType

    val binaryTypeSet = TypeSet(BinaryType, StringType)
    "0x" resolveLiteralIs binaryTypeSet
    "0x05" resolveLiteralIs binaryTypeSet
    "0x0123456789abcdef" resolveLiteralIs binaryTypeSet

    // Invalid binary.
    "0xg" resolveLiteralIs StringType

    val dateTypeSet = TypeSet(DateType, StringType)
    "0000-01-01" resolveLiteralIs dateTypeSet
    "1987-12-30" resolveLiteralIs dateTypeSet

    // Invalid dates.
    "000-01-01" resolveLiteralIs StringType
    "0000-1-01" resolveLiteralIs StringType
    "0000-01-1" resolveLiteralIs StringType

    val timestampTypeSet = TypeSet(TimestampType, TimestampNTZType, StringType)
    "0000-01-01 00:00:00" resolveLiteralIs timestampTypeSet
    "0000-01-01 00:00:00.000001" resolveLiteralIs timestampTypeSet
    "1987-01-31 09:26:56.123" resolveLiteralIs timestampTypeSet

    // Invalid timestamps.
    "000-01-01 00:00:00" resolveLiteralIs StringType
    "0000-1-01 00:00:00" resolveLiteralIs StringType
    "0000-01-1 00:00:00" resolveLiteralIs StringType
    "0000-01-01 0:00:00" resolveLiteralIs StringType
    "0000-01-01 00:0:00" resolveLiteralIs StringType
    "0000-01-01 00:00:0" resolveLiteralIs StringType
    "0000-01-01 00:00:00.0000000" resolveLiteralIs StringType

    val timeTypeSet = TypeSet(TimeType, StringType)
    "00:00:00" resolveLiteralIs timeTypeSet
    "00:00:00.000001" resolveLiteralIs timeTypeSet
    "09:26:56.123" resolveLiteralIs timeTypeSet

    // Invalid times.
    "0:00:00" resolveLiteralIs StringType
    "00:0:00" resolveLiteralIs StringType
    "00:00:0" resolveLiteralIs StringType
    "00:00:00.0000000" resolveLiteralIs StringType

    val calendarTypeSet = TypeSet(CalendarIntervalType, StringType)
    "0 seconds" resolveLiteralIs calendarTypeSet
    "1 seconds" resolveLiteralIs calendarTypeSet
    "1 minutes" resolveLiteralIs calendarTypeSet
    "1 minutes 1 seconds" resolveLiteralIs calendarTypeSet
    "1 hours" resolveLiteralIs calendarTypeSet
    "1 hours 1 seconds" resolveLiteralIs calendarTypeSet
    "1 hours 1 minutes" resolveLiteralIs calendarTypeSet
    "1 hours 1 minutes 1 seconds" resolveLiteralIs calendarTypeSet
    "24 hours" resolveLiteralIs calendarTypeSet
    "1 days" resolveLiteralIs calendarTypeSet
    "1 days 1 hours" resolveLiteralIs calendarTypeSet
    "1 days 1 hours 1 minutes" resolveLiteralIs calendarTypeSet
    "1 days 1 hours 1 minutes 1 seconds" resolveLiteralIs calendarTypeSet
    "56 days" resolveLiteralIs calendarTypeSet
    "1 months" resolveLiteralIs calendarTypeSet
    "1 months 1 days" resolveLiteralIs calendarTypeSet
    "1 months 1 days 1 hours" resolveLiteralIs calendarTypeSet
    "1 months 1 days 1 hours 1 minutes" resolveLiteralIs calendarTypeSet
    "1 months 1 days 1 hours 1 minutes 1.000001 seconds" resolveLiteralIs calendarTypeSet
    "1 years" resolveLiteralIs calendarTypeSet
    "1 years 1 months" resolveLiteralIs calendarTypeSet
    "1 years 1 months 1 days" resolveLiteralIs calendarTypeSet
    "1 years 1 months 1 days 1 hours" resolveLiteralIs calendarTypeSet
    "1 years 1 months 1 days 1 hours 1 minutes" resolveLiteralIs calendarTypeSet
    "1 years 1 months 1 days 1 hours 1 minutes 1.000001 seconds" resolveLiteralIs calendarTypeSet

    // Invalid calendar intervals.
    " seconds" resolveLiteralIs StringType
    " minutes" resolveLiteralIs StringType
    " hours" resolveLiteralIs StringType
    " days" resolveLiteralIs StringType
    " months" resolveLiteralIs StringType
    " years" resolveLiteralIs StringType

    val dayTimeTypeSet = TypeSet(DayTimeIntervalType, StringType)
    "INTERVAL '100' DAY" resolveLiteralIs dayTimeTypeSet
    "INTERVAL '100' HOUR" resolveLiteralIs dayTimeTypeSet
    "INTERVAL '100' MINUTE" resolveLiteralIs dayTimeTypeSet
    "INTERVAL '100' SECOND" resolveLiteralIs dayTimeTypeSet
    "INTERVAL '100 05' DAY TO HOUR" resolveLiteralIs dayTimeTypeSet
    "INTERVAL '100 05:05' DAY TO MINUTE" resolveLiteralIs dayTimeTypeSet
    "INTERVAL '100 05:05:05' DAY TO SECOND" resolveLiteralIs dayTimeTypeSet
    "INTERVAL '100 05:05:05.050505' DAY TO SECOND" resolveLiteralIs dayTimeTypeSet
    "INTERVAL '100:05' HOUR TO MINUTE" resolveLiteralIs dayTimeTypeSet
    "INTERVAL '100:05:05' HOUR TO SECOND" resolveLiteralIs dayTimeTypeSet
    "INTERVAL '100:05:05.050505' HOUR TO SECOND" resolveLiteralIs dayTimeTypeSet
    "INTERVAL '100:05' MINUTE TO SECOND" resolveLiteralIs dayTimeTypeSet
    "INTERVAL '100:05.050505' MINUTE TO SECOND" resolveLiteralIs dayTimeTypeSet

    val yearMonthTypeSet = TypeSet(YearMonthIntervalType, StringType)
    "INTERVAL '20' YEAR" resolveLiteralIs yearMonthTypeSet
    "INTERVAL '20' MONTH" resolveLiteralIs yearMonthTypeSet
    "INTERVAL '20-3' YEAR TO MONTH" resolveLiteralIs yearMonthTypeSet

    // Invalid intervals.
    "INTERVAL '" resolveLiteralIs StringType
    " DAY" resolveLiteralIs StringType
    " HOUR" resolveLiteralIs StringType
    " MINUTE" resolveLiteralIs StringType
    " SECOND" resolveLiteralIs StringType
    " YEAR" resolveLiteralIs StringType
    " MONTH" resolveLiteralIs StringType

    // Array or Struct.
    val nestedByteTypeSet =
      TypeSet.Integral + TypeSet(
        DecimalType,
        StringType,
        DateType,
        TimestampType,
        TimestampNTZType,
        DayTimeIntervalType,
        YearMonthIntervalType,
        TimeType
      )
    val nestedLongTypeSet =
      TypeSet(
        LongType,
        DecimalType,
        StringType,
        TimestampType,
        TimestampNTZType,
        DayTimeIntervalType,
        TimeType
      )
    val arrayOrStructTypeSet =
      TypeSet(
        ArrayType(nestedByteTypeSet),
        StructType(
          Array(
            StructField("", nestedByteTypeSet)
          )
        ),
        StringType
      )

    "[]" resolveLiteralIs
      TypeSet(
        ArrayType(AnyType),
        StructType(
          Array(
            StructField("", StringType)
          )
        ),
        StringType
      )
    "[21]" resolveLiteralIs arrayOrStructTypeSet
    "[[21],[22]]" resolveLiteralIs
      TypeSet(
        ArrayType(arrayOrStructTypeSet),
        StructType(
          Array(
            StructField("", arrayOrStructTypeSet),
            StructField("", arrayOrStructTypeSet)
          )
        ),
        StringType
      )
    "[21,[22]]" resolveLiteralIs
      TypeSet(
        ArrayType(StringType),
        StructType(
          Array(
            StructField("", nestedByteTypeSet),
            StructField("", arrayOrStructTypeSet)
          )
        ),
        StringType
      )
    "[21,2147483648,1.11]" resolveLiteralIs
      TypeSet(
        ArrayType(TypeSet(DecimalType, StringType)),
        StructType(
          Array(
            StructField("", nestedByteTypeSet),
            StructField("", nestedLongTypeSet),
            StructField("", floatingPointTypeSet)
          )
        ),
        StringType
      )

    // Map.
    "keys: [a,b,c], values: [1,5,6]" resolveLiteralIs
      MapType(StringType, nestedByteTypeSet)
    "keys: [a,b,c], values: [[1],[5,6],[6]]" resolveLiteralIs
      MapType(StringType, TypeSet(StringType, ArrayType(nestedByteTypeSet)))

    // Struct with map.
    "[keys: [1,2], values: [a,b],23]" resolveLiteralIs
      TypeSet(
        StructType(
          Array(
            StructField("", MapType(nestedByteTypeSet, StringType)),
            StructField("", nestedByteTypeSet)
          )
        ),
        StringType
      )

    // Nested map.
    val mapType = MapType(nestedByteTypeSet, MapType(StringType, nestedByteTypeSet))
    "[keys: [1,2], values: [keys: [a], values: [10],keys: [b], values: [20]]]" resolveLiteralIs
      TypeSet(
        ArrayType(mapType),
        StructType(Array(StructField("", mapType))),
        StringType
      )

    // Possibly conflicting.
    "-" resolveLiteralIs StringType
    "map" resolveLiteralIs StringType
    "(" resolveLiteralIs StringType
    ")" resolveLiteralIs StringType
    "[" resolveLiteralIs StringType
    "]" resolveLiteralIs StringType
    "," resolveLiteralIs StringType
    "'" resolveLiteralIs StringType
    ":" resolveLiteralIs StringType
  }

  test("resolveDataType - Literal") {
    // Contradictory data types.
    assert(Literal("1", IntegerType).resolveDataType(DoubleType).dataType == TypeSet.Empty)

    // Output type defined.
    assert("false".resolveDataType(BooleanType).dataType == BooleanType)

    // Output type undefined.
    assert("false".resolveDataType(AnyType).dataType == TypeSet(BooleanType, StringType))
    assert("false".resolveDataType(TypeSet(ShortType, StringType)).dataType == StringType)
  }

  test("resolveDataType - Attribute") {
    var expr = AttributeReference("id", ShortType, 0)
    assert(expr.resolveDataType(DoubleType).dataType == TypeSet.Empty)

    expr = AttributeReference("id", 0)
    assert(expr.resolveDataType(DoubleType).dataType == DoubleType)
    assert(expr.resolveDataType(AnyType).dataType == AnyType)
    assert(expr.resolveDataType(TypeSet.Numeric).dataType == TypeSet.Numeric)
  }

  test("resolveDataType - Alias") {
    val expr = Alias("false", "id", 0)
    assert(expr.resolveDataType(AnyType).dataType == TypeSet(BooleanType, StringType))
    assert(expr.resolveDataType(BooleanType).dataType == BooleanType)
  }

  test("resolveDataType - BoundReference") {
    val expr = BoundReference(1, AnyType)
    assert(expr.resolveDataType(AnyType).dataType == AnyType)
    assert(expr.resolveDataType(BooleanType).dataType == BooleanType)

    // Contradictory data type.
    assert(BoundReference(1, BooleanType).resolveDataType(IntegerType).dataType == TypeSet.Empty)
  }

  test("resolveDataType - Unknown") {
    var expr: Expression = UnknownSQLExpression("(booleans#0 ? 1 : id#0)")
    var resolved = expr.resolveDataType(IntegerType)
    assert(resolved.dataType == IntegerType)

    expr = UnknownSQLFunction("unknown", IndexedSeq(Add(AttributeReference("id", 0), "2")))
    resolved = expr.resolveDataType(IntegerType)
    assert(resolved.dataType == IntegerType)
    assert(resolved.child.child(1).dataType == TypeSet.Integral + DecimalType)

    expr = UnknownAggregateFunction("unknown", IndexedSeq(Add(AttributeReference("id", 0), "2")))
    resolved = expr.resolveDataType(IntegerType)
    assert(resolved.dataType == IntegerType)
    assert(resolved.child.child(1).dataType == TypeSet.Integral + DecimalType)
  }

  test("resolveDataType - UnaryMinus") {
    val expr = UnaryMinus("1")
    assert(expr.resolveDataType(AnyType).dataType == TypeSet.Integral + DecimalType)
    assert(expr.resolveDataType(IntegerType).dataType == IntegerType)
  }

  test("resolveDataType - UnaryPositive") {
    val expr = UnaryPositive("1")
    assert(expr.resolveDataType(AnyType).dataType == TypeSet.Integral + DecimalType)
    assert(expr.resolveDataType(IntegerType).dataType == IntegerType)
  }

  test("resolveDataType - Abs") {
    val expr = Abs("1")
    assert(expr.resolveDataType(AnyType).dataType == TypeSet.Integral + DecimalType)
    assert(expr.resolveDataType(IntegerType).dataType == IntegerType)
  }

  test("resolveDataType - BitwiseNot") {
    val expr = BitwiseNot("1")
    assert(expr.resolveDataType(AnyType).child.dataType == TypeSet.Integral)
    assert(expr.resolveDataType(IntegerType).child.dataType == IntegerType)
  }

  test("resolveDataType - Add") {
    var expr = Add("1", "2")
    assert(expr.resolveDataType(AnyType).child.dataType == TypeSet.Integral + DecimalType)
    assert(expr.resolveDataType(IntegerType).child.dataType == IntegerType)

    expr = Add("1", AttributeReference("id", IntegerType))
    assert(expr.resolveDataType(AnyType).child.dataType == IntegerType)
  }

  test("resolveDataType - Subtract") {
    var expr = Subtract("1", "2")
    assert(expr.resolveDataType(AnyType).child.dataType == TypeSet.Integral + DecimalType)
    assert(expr.resolveDataType(IntegerType).child.dataType == IntegerType)

    expr = Subtract("1", AttributeReference("id", IntegerType))
    assert(expr.resolveDataType(AnyType).child.dataType == IntegerType)
  }

  test("resolveDataType - Multiply") {
    var expr = Multiply("1", "2")
    assert(expr.resolveDataType(AnyType).child.dataType == TypeSet.Integral + DecimalType)
    assert(expr.resolveDataType(IntegerType).child.dataType == IntegerType)

    expr = Multiply("1", AttributeReference("id", IntegerType))
    assert(expr.resolveDataType(AnyType).child.dataType == IntegerType)
  }

  test("resolveDataType - Divide") {
    var expr = Divide("1.0", "2.0")
    assert(expr.resolveDataType(AnyType).child.dataType == TypeSet(DoubleType, DecimalType))
    assert(expr.resolveDataType(DoubleType).child.dataType == DoubleType)

    expr = Divide("1.0", AttributeReference("id", DecimalType))
    assert(expr.resolveDataType(AnyType).child.dataType == DecimalType)
  }

  test("resolveDataType - IntegralDivide") {
    var expr = IntegralDivide("1", "2")
    assert(expr.resolveDataType(AnyType).child.dataType == TypeSet(LongType, DecimalType))
    assert(expr.resolveDataType(LongType).child.dataType == LongType)

    expr = IntegralDivide("1", AttributeReference("id", DecimalType))
    assert(expr.resolveDataType(AnyType).child.dataType == DecimalType)
  }

  test("resolveDataType - Remainder") {
    var expr = Remainder("1", "2")
    assert(expr.resolveDataType(AnyType).child.dataType == TypeSet.Integral + DecimalType)
    assert(expr.resolveDataType(LongType).child.dataType == LongType)

    expr = Remainder("1", AttributeReference("id", DecimalType))
    assert(expr.resolveDataType(AnyType).child.dataType == DecimalType)
  }

  test("resolveDataType - Pmod") {
    var expr = Pmod("1", "2")
    assert(expr.resolveDataType(AnyType).child.dataType == TypeSet.Integral + DecimalType)
    assert(expr.resolveDataType(LongType).child.dataType == LongType)

    expr = Pmod("1", AttributeReference("id", DecimalType))
    assert(expr.resolveDataType(AnyType).child.dataType == DecimalType)
  }

  test("resolveDataType - Least") {
    val expr = Least(IndexedSeq(AttributeReference("id", AnyType), "2"))

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == TypeSet.Integral + StringType + DecimalType)
    assert(resolved.child(1).dataType == TypeSet.Integral + StringType + DecimalType)

    resolved = expr.resolveDataType(LongType)
    assert(resolved.child(0).dataType == LongType)
    assert(resolved.child(1).dataType == LongType)
  }

  test("resolveDataType - Greatest") {
    val expr = Greatest(IndexedSeq(AttributeReference("id", AnyType), "2"))

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == TypeSet.Integral + StringType + DecimalType)
    assert(resolved.child(1).dataType == TypeSet.Integral + StringType + DecimalType)

    resolved = expr.resolveDataType(LongType)
    assert(resolved.child(0).dataType == LongType)
    assert(resolved.child(1).dataType == LongType)
  }

  test("resolveDataType - BitwiseOr") {
    var expr = BitwiseOr("1", "2")
    assert(expr.resolveDataType(AnyType).child.dataType == TypeSet.Integral)
    assert(expr.resolveDataType(LongType).child.dataType == LongType)

    expr = BitwiseOr("1", AttributeReference("id", ShortType))
    assert(expr.resolveDataType(AnyType).child.dataType == ShortType)
  }

  test("resolveDataType - BitwiseAnd") {
    var expr = BitwiseAnd("1", "2")
    assert(expr.resolveDataType(AnyType).child.dataType == TypeSet.Integral)
    assert(expr.resolveDataType(LongType).child.dataType == LongType)

    expr = BitwiseAnd("1", AttributeReference("id", ShortType))
    assert(expr.resolveDataType(AnyType).child.dataType == ShortType)
  }

  test("resolveDataType - BitwiseXor") {
    var expr = BitwiseXor("1", "2")
    assert(expr.resolveDataType(AnyType).child.dataType == TypeSet.Integral)
    assert(expr.resolveDataType(LongType).child.dataType == LongType)

    expr = BitwiseXor("1", AttributeReference("id", ShortType))
    assert(expr.resolveDataType(AnyType).child.dataType == ShortType)
  }

  test("resolveDataType - BitwiseCount") {
    val expr = BitwiseCount(AttributeReference("id", AnyType))

    val resolved = expr.resolveDataType(IntegerType)
    assert(resolved.child.dataType == TypeSet.Integral + BooleanType)
  }

  test("resolveDataType - BitwiseGet") {
    val expr = BitwiseGet(AttributeReference("id", AnyType), "1")

    val resolved = expr.resolveDataType(ByteType)
    assert(resolved.child(0).dataType == TypeSet.Integral)
    assert(resolved.child(1).dataType == IntegerType)
  }

  test("resolveDataType - BitwiseReverse") {
    val expr = BitwiseReverse(AttributeReference("id", AnyType))

    assert(expr.resolveDataType(AnyType).child.dataType == TypeSet.Integral)
    assert(expr.resolveDataType(IntegerType).child.dataType == IntegerType)
  }

  test("resolveDataType - EqualTo") {
    var expr = EqualTo("1", "2")
    val expectedDataType = TypeSet.Integral + DecimalType + StringType
    assert(expr.resolveDataType(AnyType).child.dataType == expectedDataType)
    assert(expr.resolveDataType(BooleanType).child.dataType == expectedDataType)

    expr = EqualTo("1", AttributeReference("id", ShortType))
    assert(expr.resolveDataType(AnyType).child.dataType == ShortType)
  }

  test("resolveDataType - EqualNullSafe") {
    var expr = EqualNullSafe("1", "2")
    val expectedDataType = TypeSet.Integral + DecimalType + StringType
    assert(expr.resolveDataType(AnyType).child.dataType == expectedDataType)
    assert(expr.resolveDataType(BooleanType).child.dataType == expectedDataType)

    expr = EqualNullSafe("1", AttributeReference("id", ShortType))
    assert(expr.resolveDataType(AnyType).child.dataType == ShortType)
  }

  test("resolveDataType - LessThan") {
    var expr = LessThan("1", "2")
    val expectedDataType = TypeSet.Integral + DecimalType + StringType
    assert(expr.resolveDataType(AnyType).child.dataType == expectedDataType)
    assert(expr.resolveDataType(BooleanType).child.dataType == expectedDataType)

    expr = LessThan("1", AttributeReference("id", ShortType))
    assert(expr.resolveDataType(AnyType).child.dataType == ShortType)
  }

  test("resolveDataType - GreaterThan") {
    var expr = GreaterThan("1", "2")
    val expectedDataType = TypeSet.Integral + DecimalType + StringType
    assert(expr.resolveDataType(AnyType).child.dataType == expectedDataType)
    assert(expr.resolveDataType(BooleanType).child.dataType == expectedDataType)

    expr = GreaterThan("1", AttributeReference("id", ShortType))
    assert(expr.resolveDataType(AnyType).child.dataType == ShortType)
  }

  test("resolveDataType - LessThanOrEqual") {
    var expr = LessThanOrEqual("1", "2")
    val expectedDataType = TypeSet.Integral + DecimalType + StringType
    assert(expr.resolveDataType(AnyType).child.dataType == expectedDataType)
    assert(expr.resolveDataType(BooleanType).child.dataType == expectedDataType)

    expr = LessThanOrEqual("1", AttributeReference("id", ShortType))
    assert(expr.resolveDataType(AnyType).child.dataType == ShortType)
  }

  test("resolveDataType - GreaterThanOrEqual") {
    var expr = GreaterThanOrEqual("1", "2")
    val expectedDataType = TypeSet.Integral + DecimalType + StringType
    assert(expr.resolveDataType(AnyType).child.dataType == expectedDataType)
    assert(expr.resolveDataType(BooleanType).child.dataType == expectedDataType)

    expr = GreaterThanOrEqual("1", AttributeReference("id", ShortType))
    assert(expr.resolveDataType(AnyType).child.dataType == ShortType)
  }

  test("resolveDataType - Not") {
    var expr = Not("false")
    assert(expr.resolveDataType(AnyType).child.dataType == BooleanType)

    expr = Not(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == BooleanType)
  }

  test("resolveDataType - And") {
    var expr = And("false", "true")
    assert(expr.resolveDataType(AnyType).child.dataType == BooleanType)

    expr = And(AttributeReference("id", AnyType), "true")
    assert(expr.resolveDataType(AnyType).child.dataType == BooleanType)
  }

  test("resolveDataType - Or") {
    var expr = Or("false", "true")
    assert(expr.resolveDataType(AnyType).child.dataType == BooleanType)

    expr = Or(AttributeReference("id", AnyType), "true")
    assert(expr.resolveDataType(AnyType).child.dataType == BooleanType)
  }

  test("resolveDataType - CaseWhen") {
    var expr = CaseWhen(
      IndexedSeq(
        (AttributeReference("id", IntegerType) < "1", "100".expr),
        (AttributeReference("id", IntegerType) < "10", "1000".expr)
      ),
      Some(Long.MaxValue.toString)
    )
    var resolved = expr.resolveDataType(AnyType).asInstanceOf[CaseWhen]
    assert(resolved.branches(0)._1.child(1).dataType == IntegerType)
    assert(resolved.branches(0)._2.dataType == TypeSet(LongType, DecimalType, StringType))
    assert(resolved.branches(1)._1.child(1).dataType == IntegerType)
    assert(resolved.branches(1)._2.dataType == TypeSet(LongType, DecimalType, StringType))
    assert(resolved.elseValue.get.dataType == TypeSet(LongType, DecimalType, StringType))

    resolved = expr.resolveDataType(LongType).asInstanceOf[CaseWhen]
    assert(resolved.branches(0)._1.child(1).dataType == IntegerType)
    assert(resolved.branches(0)._2.dataType == LongType)
    assert(resolved.branches(1)._1.child(1).dataType == IntegerType)
    assert(resolved.branches(1)._2.dataType == LongType)
    assert(resolved.elseValue.get.dataType == LongType)

    expr = CaseWhen(
      IndexedSeq(
        (AttributeReference("cond1", AnyType), "false".expr),
        (AttributeReference("cond2", AnyType), "1".expr)
      ),
      None
    )
    resolved = expr.resolveDataType(AnyType).asInstanceOf[CaseWhen]
    assert(resolved.branches(0)._1.dataType == BooleanType)
    assert(resolved.branches(0)._2.dataType == StringType)
    assert(resolved.branches(1)._1.dataType == BooleanType)
    assert(resolved.branches(1)._2.dataType == StringType)
  }

  test("resolveDataType - If") {
    var expr = If(
      AttributeReference("id", IntegerType) < "1",
      "100",
      Long.MaxValue.toString
    )
    var resolved = expr.resolveDataType(AnyType).asInstanceOf[If]
    assert(resolved.predicate.child(1).dataType == IntegerType)
    assert(resolved.trueValue.dataType == TypeSet(LongType, DecimalType, StringType))
    assert(resolved.falseValue.dataType == TypeSet(LongType, DecimalType, StringType))

    resolved = expr.resolveDataType(LongType).asInstanceOf[If]
    assert(resolved.predicate.child(1).dataType == IntegerType)
    assert(resolved.trueValue.dataType == LongType)
    assert(resolved.falseValue.dataType == LongType)

    expr = If(
      AttributeReference("cond", AnyType),
      "false",
      "1"
    )
    resolved = expr.resolveDataType(AnyType).asInstanceOf[If]
    assert(resolved.predicate.dataType == BooleanType)
    assert(resolved.trueValue.dataType == StringType)
    assert(resolved.falseValue.dataType == StringType)
  }

  test("resolveDataType - In") {
    var expr = In(
      AttributeReference("id", AnyType),
      IndexedSeq[Expression]("1", "10000", Long.MaxValue.toString)
    )
    var resolved = expr.resolveDataType(AnyType).asInstanceOf[In]
    assert(resolved.value.dataType == TypeSet(LongType, DecimalType, StringType))
    assert(resolved.list.forall(_.dataType == TypeSet(LongType, DecimalType, StringType)))

    resolved = expr.resolveDataType(BooleanType).asInstanceOf[In]
    assert(resolved.value.dataType == TypeSet(LongType, DecimalType, StringType))
    assert(resolved.list.forall(_.dataType == TypeSet(LongType, DecimalType, StringType)))

    expr = In(
      AttributeReference("id", LongType),
      IndexedSeq[Expression]("1", AttributeReference("other", AnyType), Long.MaxValue.toString)
    )
    resolved = expr.resolveDataType(AnyType).asInstanceOf[In]
    assert(resolved.value.dataType == LongType)
    assert(resolved.list.forall(_.dataType == LongType))
  }

  test("resolveDataType - InSet") {
    var expr = InSet(
      AttributeReference("id", AnyType),
      IndexedSeq[Literal]("1", "10000", Long.MaxValue.toString)
    )
    var resolved = expr.resolveDataType(AnyType).asInstanceOf[InSet]
    assert(resolved.child.dataType == TypeSet(LongType, DecimalType, StringType))
    assert(resolved.set.forall(_.dataType == TypeSet(LongType, DecimalType, StringType)))

    resolved = expr.resolveDataType(BooleanType).asInstanceOf[InSet]
    assert(resolved.child.dataType == TypeSet(LongType, DecimalType, StringType))
    assert(resolved.set.forall(_.dataType == TypeSet(LongType, DecimalType, StringType)))

    expr = InSet(
      AttributeReference("id", LongType),
      IndexedSeq[Literal]("1", "10000", Long.MaxValue.toString)
    )
    resolved = expr.resolveDataType(AnyType).asInstanceOf[InSet]
    assert(resolved.child.dataType == LongType)
    assert(resolved.set.forall(_.dataType == LongType))
  }

  test("resolveDataType - Like") {
    val expr = Like(AttributeReference("id", AnyType), "_", '\\')
    assert(expr.resolveDataType(AnyType).child.dataType == StringType)
    assert(expr.resolveDataType(BooleanType).child.dataType == StringType)
  }

  test("resolveDataType - Cast") {
    val expr = Cast("false", IntegerType, isTryCast = false)
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == IntegerType)
    assert(resolved.child.dataType == TypeSet(BooleanType, StringType))
  }

  test("resolveDataType - AmbiguousExpression") {
    var expr =
      AmbiguousExpression(
        IndexedSeq(
          AttributeReference("alt1", AnyType),
          AttributeReference("alt2", LongType)
        )
      )

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == AnyType)
    assert(resolved.child(0).dataType == AnyType)
    assert(resolved.child(1).dataType == LongType)

    resolved = expr.resolveDataType(LongType)
    assert(resolved.dataType == LongType)
    assert(resolved.child(0).dataType == LongType)
    assert(resolved.child(1).dataType == LongType)

    expr = AmbiguousExpression(
      IndexedSeq(
        AttributeReference("alt1", ShortType),
        AttributeReference("alt2", LongType)
      )
    )

    resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == TypeSet(ShortType, LongType))
    assert(resolved.child(0).dataType == ShortType)
    assert(resolved.child(1).dataType == LongType)

    resolved = expr.resolveDataType(LongType)
    assert(resolved == AttributeReference("alt2", LongType))

    expr = AmbiguousExpression(
      IndexedSeq(
        Multiply(AttributeReference("alt1", DecimalType), "1"),
        MultiplyYMInterval(AttributeReference("alt1", DecimalType), "1"),
        MultiplyDTInterval(AttributeReference("alt1", DecimalType), "1")
      )
    )
    assert(expr.resolveDataType(AnyType).isInstanceOf[Multiply])
  }

  test("resolveDataType - AggregateExpression") {
    val expr = AggregateExpression(
      aggregateFunction = Count("1"),
      modePrefix = "",
      isDistinct = false,
      filter = Some(AttributeReference("id", AnyType, 0))
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == LongType)
    assert(resolved.child.dataType == LongType)
    assert(resolved.child.child.dataType == TypeSet.Integral + DecimalType + StringType)
    assert(resolved.child(1).dataType == BooleanType)
  }

  test("resolveDataType - Sum") {
    val expr = (dataType: DataType) => Sum(AttributeReference("id", dataType), isTry = false)

    var resolved = expr(AnyType).resolveDataType(AnyType)
    assert(
      resolved.dataType ==
        TypeSet(LongType, DecimalType, DoubleType, YearMonthIntervalType, DayTimeIntervalType)
    )

    resolved = expr(AnyType).resolveDataType(LongType)
    assert(resolved.dataType == LongType)
    assert(resolved.child.dataType == TypeSet.Integral)

    resolved = expr(TypeSet(StringType, LongType, DecimalType)).resolveDataType(AnyType)
    assert(resolved.dataType == TypeSet(LongType, DecimalType))
    assert(resolved.child.dataType == TypeSet(LongType, DecimalType))

    resolved = expr(TypeSet.Integral + StringType).resolveDataType(AnyType)
    assert(resolved.dataType == LongType)
    assert(resolved.child.dataType == TypeSet.Integral)

    resolved = expr(TypeSet(StringType, FloatType, DoubleType, NullType)).resolveDataType(AnyType)
    assert(resolved.dataType == DoubleType)
    assert(resolved.child.dataType == TypeSet(FloatType, DoubleType, NullType))

    resolved = expr(TypeSet(StringType, DecimalType)).resolveDataType(AnyType)
    assert(resolved.dataType == DecimalType)
    assert(resolved.child.dataType == DecimalType)

    resolved = expr(TypeSet(StringType, YearMonthIntervalType)).resolveDataType(AnyType)
    assert(resolved.dataType == YearMonthIntervalType)
    assert(resolved.child.dataType == YearMonthIntervalType)

    resolved = expr(TypeSet(StringType, DayTimeIntervalType)).resolveDataType(AnyType)
    assert(resolved.dataType == DayTimeIntervalType)
    assert(resolved.child.dataType == DayTimeIntervalType)
  }

  test("resolveDataType - Average") {
    val expr = (dataType: DataType) => Average(AttributeReference("id", dataType), isTry = false)

    var resolved = expr(AnyType).resolveDataType(AnyType)
    assert(
      resolved.dataType ==
        TypeSet(DecimalType, DoubleType, YearMonthIntervalType, DayTimeIntervalType)
    )

    resolved = expr(AnyType).resolveDataType(DoubleType)
    assert(resolved.dataType == DoubleType)
    assert(resolved.child.dataType == TypeSet.Integral + FloatType + DoubleType + NullType)

    resolved = expr(TypeSet(StringType, LongType, DecimalType)).resolveDataType(AnyType)
    assert(resolved.dataType == TypeSet(DoubleType, DecimalType))
    assert(resolved.child.dataType == TypeSet(LongType, DecimalType))

    resolved = expr(TypeSet.Integral + StringType + FloatType + DoubleType + NullType)
      .resolveDataType(AnyType)
    assert(resolved.dataType == DoubleType)
    assert(resolved.child.dataType == TypeSet.Integral + FloatType + DoubleType + NullType)

    resolved = expr(TypeSet(StringType, DecimalType)).resolveDataType(AnyType)
    assert(resolved.dataType == DecimalType)
    assert(resolved.child.dataType == DecimalType)

    resolved = expr(TypeSet(StringType, YearMonthIntervalType)).resolveDataType(AnyType)
    assert(resolved.dataType == YearMonthIntervalType)
    assert(resolved.child.dataType == YearMonthIntervalType)

    resolved = expr(TypeSet(StringType, DayTimeIntervalType)).resolveDataType(AnyType)
    assert(resolved.dataType == DayTimeIntervalType)
    assert(resolved.child.dataType == DayTimeIntervalType)
  }

  test("resolveDataType - Min") {
    val expr = Min(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).dataType == AnyType)
    assert(expr.resolveDataType(LongType).dataType == LongType)
  }

  test("resolveDataType - Max") {
    val expr = Max(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).dataType == AnyType)
    assert(expr.resolveDataType(LongType).dataType == LongType)
  }

  test("resolveDataType - First") {
    val expr = First(AttributeReference("id", AnyType), ignoreNulls = false)
    assert(expr.resolveDataType(AnyType).dataType == AnyType)
    assert(expr.resolveDataType(LongType).dataType == LongType)
  }

  test("resolveDataType - Last") {
    val expr = Last(AttributeReference("id", AnyType), ignoreNulls = false)
    assert(expr.resolveDataType(AnyType).dataType == AnyType)
    assert(expr.resolveDataType(LongType).dataType == LongType)
  }

  test("resolveDataType - CollectList") {
    val expr = CollectList(AttributeReference("id", AnyType))
    assert(expr.dataType == ArrayType(AnyType))
    assert(expr.resolveDataType(AnyType).dataType == ArrayType(AnyType))
    assert(expr.resolveDataType(ArrayType(LongType)).dataType == ArrayType(LongType))
  }

  test("resolveDataType - CollectSet") {
    val expr = CollectSet(AttributeReference("id", AnyType))
    assert(expr.dataType == ArrayType(AnyType))
    assert(expr.resolveDataType(AnyType).dataType == ArrayType(AnyType))
    assert(expr.resolveDataType(ArrayType(LongType)).dataType == ArrayType(LongType))
  }

  test("resolveDataType - Mode") {
    val expr = Mode(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).dataType == AnyType)
    assert(expr.resolveDataType(LongType).dataType == LongType)
  }

  test("resolveDataType - Product") {
    val expr = Product(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DoubleType)
    assert(expr.resolveDataType(DoubleType).child.dataType == DoubleType)
  }

  test("resolveDataType - Corr") {
    val expr = Corr(AttributeReference("id1", AnyType), AttributeReference("id2", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DoubleType)
    assert(expr.resolveDataType(AnyType).child(1).dataType == DoubleType)
  }

  test("resolveDataType - CovPopulation") {
    val expr = CovPopulation(AttributeReference("id1", AnyType), AttributeReference("id2", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DoubleType)
    assert(expr.resolveDataType(AnyType).child(1).dataType == DoubleType)
  }

  test("resolveDataType - CovSample") {
    val expr = CovSample(AttributeReference("id1", AnyType), AttributeReference("id2", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DoubleType)
    assert(expr.resolveDataType(AnyType).child(1).dataType == DoubleType)
  }

  test("resolveDataType - StddevPop") {
    val expr = StddevPop(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DoubleType)
  }

  test("resolveDataType - StddevSamp") {
    val expr = StddevSamp(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DoubleType)
  }

  test("resolveDataType - VariancePop") {
    val expr = VariancePop(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DoubleType)
  }

  test("resolveDataType - VarianceSamp") {
    val expr = VarianceSamp(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DoubleType)
  }

  test("resolveDataType - RegrReplacement") {
    val expr = RegrReplacement(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DoubleType)
  }

  test("resolveDataType - Skewness") {
    val expr = Skewness(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DoubleType)
  }

  test("resolveDataType - Kurtosis") {
    val expr = Kurtosis(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DoubleType)
  }

  test("resolveDataType - MaxBy") {
    val expr = MaxBy(AttributeReference("id1", AnyType), AttributeReference("id2", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == AnyType)
    assert(expr.resolveDataType(AnyType).child(1).dataType == AnyType)
    assert(expr.resolveDataType(LongType).child.dataType == LongType)
    assert(expr.resolveDataType(LongType).child(1).dataType == AnyType)
  }

  test("resolveDataType - MinBy") {
    val expr = MinBy(AttributeReference("id1", AnyType), AttributeReference("id2", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == AnyType)
    assert(expr.resolveDataType(AnyType).child(1).dataType == AnyType)
    assert(expr.resolveDataType(LongType).child.dataType == LongType)
    assert(expr.resolveDataType(LongType).child(1).dataType == AnyType)
  }

  test("resolveDataType - Percentile") {
    var expr = Percentile(
      AttributeReference("child", AnyType),
      AttributeReference("percentage", AnyType),
      AttributeReference("frequency", AnyType),
      reverse = false
    )
    var resolved = expr.resolveDataType(AnyType)
    val allTypes = TypeSet(DoubleType, YearMonthIntervalType, DayTimeIntervalType)
    assert(resolved.dataType == allTypes + ArrayType(allTypes))
    assert(resolved.child(0).dataType == TypeSet.NumericAndAnsiInterval)
    assert(resolved.child(1).dataType == TypeSet(DoubleType, ArrayType(DoubleType)))
    assert(resolved.child(2).dataType == TypeSet.Integral)

    resolved = expr.resolveDataType(TypeSet.Numeric)
    assert(resolved.dataType == DoubleType)
    assert(resolved.child(0).dataType == TypeSet.Numeric)
    assert(resolved.child(1).dataType == DoubleType)
    assert(resolved.child(2).dataType == TypeSet.Integral)

    resolved = expr.resolveDataType(ArrayType(DoubleType))
    assert(resolved.dataType == ArrayType(DoubleType))
    assert(resolved.child(0).dataType == TypeSet.Numeric)
    assert(resolved.child(1).dataType == ArrayType(DoubleType))
    assert(resolved.child(2).dataType == TypeSet.Integral)

    resolved = expr.resolveDataType(YearMonthIntervalType)
    assert(resolved.dataType == YearMonthIntervalType)
    assert(resolved.child(0).dataType == YearMonthIntervalType)
    assert(resolved.child(1).dataType == DoubleType)
    assert(resolved.child(2).dataType == TypeSet.Integral)

    resolved = expr.resolveDataType(DayTimeIntervalType)
    assert(resolved.dataType == DayTimeIntervalType)
    assert(resolved.child(0).dataType == DayTimeIntervalType)
    assert(resolved.child(1).dataType == DoubleType)
    assert(resolved.child(2).dataType == TypeSet.Integral)

    resolved = expr.resolveDataType(ArrayType(AnyType))
    assert(resolved.dataType == ArrayType(allTypes))
    assert(resolved.child(0).dataType == TypeSet.NumericAndAnsiInterval)
    assert(resolved.child(1).dataType == ArrayType(DoubleType))
    assert(resolved.child(2).dataType == TypeSet.Integral)

    expr = Percentile("1", "0.75", "1", reverse = false)
    resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == DoubleType)
    assert(resolved.child(0).dataType == TypeSet.Integral + DecimalType)
    assert(resolved.child(1).dataType == DoubleType)
    assert(resolved.child(2).dataType == TypeSet.Integral)

    expr = Percentile("1", "[0.75,0.8]", "1", reverse = false)
    resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == ArrayType(DoubleType))
    assert(resolved.child(0).dataType == TypeSet.Integral + DecimalType)
    assert(resolved.child(1).dataType == ArrayType(DoubleType))
    assert(resolved.child(2).dataType == TypeSet.Integral)
  }

  test("resolveDataType - PercentileDisc") {
    var expr = PercentileDisc(
      AttributeReference("child", AnyType),
      AttributeReference("percentage", AnyType),
      reverse = false,
      legacyCalculation = false
    )
    var resolved = expr.resolveDataType(AnyType)
    val allTypes = TypeSet(DoubleType, YearMonthIntervalType, DayTimeIntervalType)
    assert(resolved.dataType == allTypes + ArrayType(allTypes))
    assert(resolved.child(0).dataType == TypeSet.NumericAndAnsiInterval)
    assert(resolved.child(1).dataType == TypeSet(DoubleType, ArrayType(DoubleType)))

    resolved = expr.resolveDataType(TypeSet.Numeric)
    assert(resolved.dataType == DoubleType)
    assert(resolved.child(0).dataType == TypeSet.Numeric)
    assert(resolved.child(1).dataType == DoubleType)

    resolved = expr.resolveDataType(ArrayType(DoubleType))
    assert(resolved.dataType == ArrayType(DoubleType))
    assert(resolved.child(0).dataType == TypeSet.Numeric)
    assert(resolved.child(1).dataType == ArrayType(DoubleType))

    resolved = expr.resolveDataType(YearMonthIntervalType)
    assert(resolved.dataType == YearMonthIntervalType)
    assert(resolved.child(0).dataType == YearMonthIntervalType)
    assert(resolved.child(1).dataType == DoubleType)

    resolved = expr.resolveDataType(DayTimeIntervalType)
    assert(resolved.dataType == DayTimeIntervalType)
    assert(resolved.child(0).dataType == DayTimeIntervalType)
    assert(resolved.child(1).dataType == DoubleType)

    resolved = expr.resolveDataType(ArrayType(AnyType))
    assert(resolved.dataType == ArrayType(allTypes))
    assert(resolved.child(0).dataType == TypeSet.NumericAndAnsiInterval)
    assert(resolved.child(1).dataType == ArrayType(DoubleType))

    expr = PercentileDisc("1", "0.75", reverse = false, legacyCalculation = false)
    resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == DoubleType)
    assert(resolved.child(0).dataType == TypeSet.Integral + DecimalType)
    assert(resolved.child(1).dataType == DoubleType)

    expr = PercentileDisc("1", "[0.75,0.8]", reverse = false, legacyCalculation = false)
    resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == ArrayType(DoubleType))
    assert(resolved.child(0).dataType == TypeSet.Integral + DecimalType)
    assert(resolved.child(1).dataType == ArrayType(DoubleType))
  }

  test("resolveDataType - ApproximatePercentile") {
    var expr = ApproximatePercentile(
      AttributeReference("child", AnyType),
      AttributeReference("percentage", AnyType),
      AttributeReference("accuracy", AnyType)
    )
    var resolved = expr.resolveDataType(AnyType)
    val allTypes = TypeSet.NumericAndAnsiInterval + DateType + TimestampType + TimestampNTZType
    assert(resolved.dataType == allTypes + ArrayType(allTypes))
    assert(resolved.child(0).dataType == allTypes)
    assert(resolved.child(1).dataType == TypeSet(DoubleType, ArrayType(DoubleType)))
    assert(resolved.child(2).dataType == TypeSet.Integral)

    resolved = expr.resolveDataType(allTypes)
    assert(resolved.dataType == allTypes)
    assert(resolved.child(0).dataType == allTypes)
    assert(resolved.child(1).dataType == DoubleType)
    assert(resolved.child(2).dataType == TypeSet.Integral)

    resolved = expr.resolveDataType(DateType)
    assert(resolved.dataType == DateType)
    assert(resolved.child(0).dataType == DateType)
    assert(resolved.child(1).dataType == DoubleType)
    assert(resolved.child(2).dataType == TypeSet.Integral)

    resolved = expr.resolveDataType(ArrayType(DateType))
    assert(resolved.dataType == ArrayType(DateType))
    assert(resolved.child(0).dataType == DateType)
    assert(resolved.child(1).dataType == ArrayType(DoubleType))
    assert(resolved.child(2).dataType == TypeSet.Integral)

    resolved = expr.resolveDataType(ArrayType(AnyType))
    assert(resolved.dataType == ArrayType(allTypes))
    assert(resolved.child(0).dataType == allTypes)
    assert(resolved.child(1).dataType == ArrayType(DoubleType))
    assert(resolved.child(2).dataType == TypeSet.Integral)

    expr = ApproximatePercentile("1", "0.75", "100")
    resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == TypeSet.Integral + DecimalType)
    assert(resolved.child(0).dataType == TypeSet.Integral + DecimalType)
    assert(resolved.child(1).dataType == DoubleType)
    assert(resolved.child(2).dataType == TypeSet.Integral)

    expr = ApproximatePercentile("1", "[0.75,0.8]", "100")
    resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == ArrayType(TypeSet.Integral + DecimalType))
    assert(resolved.child(0).dataType == TypeSet.Integral + DecimalType)
    assert(resolved.child(1).dataType == ArrayType(DoubleType))
    assert(resolved.child(2).dataType == TypeSet.Integral)
  }

  test("resolveDataType - HyperLogLogPlusPlus") {
    var expr = HyperLogLogPlusPlus(AttributeReference("id", AnyType), relativeSD = 0.05)
    assert(expr.dataType == LongType)
    assert(expr.resolveDataType(LongType).child.dataType == AnyType)

    expr = HyperLogLogPlusPlus(AttributeReference("id", IntegerType), relativeSD = 0.05)
    assert(expr.resolveDataType(LongType).child.dataType == IntegerType)
  }

  test("resolveDataType - BitAndAgg") {
    val expr = BitAndAgg(AttributeReference("id", AnyType))

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == TypeSet.Integral)
    assert(resolved.child.dataType == TypeSet.Integral)

    resolved = expr.resolveDataType(LongType)
    assert(resolved.dataType == LongType)
    assert(resolved.child.dataType == LongType)
  }

  test("resolveDataType - BitOrAgg") {
    val expr = BitOrAgg(AttributeReference("id", AnyType))

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == TypeSet.Integral)
    assert(resolved.child.dataType == TypeSet.Integral)

    resolved = expr.resolveDataType(LongType)
    assert(resolved.dataType == LongType)
    assert(resolved.child.dataType == LongType)
  }

  test("resolveDataType - BitXorAgg") {
    val expr = BitXorAgg(AttributeReference("id", AnyType))

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == TypeSet.Integral)
    assert(resolved.child.dataType == TypeSet.Integral)

    resolved = expr.resolveDataType(LongType)
    assert(resolved.dataType == LongType)
    assert(resolved.child.dataType == LongType)
  }

  test("resolveDataType - BloomFilterAggregate") {
    val expr = BloomFilterAggregate(
      AttributeReference("id", AnyType),
      "1000000",
      "8388608"
    )

    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == BinaryType)
    assert(resolved.child(0).dataType == TypeSet.Integral + StringType)
    assert(resolved.child(1).dataType == LongType)
    assert(resolved.child(2).dataType == LongType)
  }

  test("resolveDataType - CountMinSketchAgg") {
    val expr =
      CountMinSketchAgg(AttributeReference("id", AnyType), "0.5", "0.6", "1")

    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == BinaryType)
    assert(resolved.child(0).dataType == TypeSet.Integral + StringType + BinaryType)
    assert(resolved.child(1).dataType == DoubleType)
    assert(resolved.child(2).dataType == DoubleType)
    assert(resolved.child(3).dataType == IntegerType)
  }

  test("resolveDataType - HistogramNumeric") {
    val expr = HistogramNumeric(AttributeReference("id", AnyType), "5")

    val childExpectedType =
      TypeSet.Numeric +
        DateType +
        TimestampType +
        TimestampNTZType +
        YearMonthIntervalType +
        DayTimeIntervalType

    val initialExpectedType = ArrayType(
      StructType(
        Array(
          StructField("x", childExpectedType),
          StructField("y", DoubleType)
        )
      )
    )

    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == initialExpectedType)
    assert(resolved.child(0).dataType == childExpectedType)
    assert(resolved.child(1).dataType == IntegerType)
  }

  test("resolveDataType - PivotFirst") {
    val expr = PivotFirst(
      AttributeReference("id", AnyType),
      AttributeReference("sum(id)", TypeSet.Numeric),
      IndexedSeq("1.0", "3.0")
    )

    val expectedInitialType =
      TypeSet(FloatType, DoubleType, DecimalType, StringType)

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == ArrayType(TypeSet.Numeric))
    assert(resolved.child(0).dataType == expectedInitialType)
    assert(resolved.child(1).dataType == TypeSet.Numeric)
    assert(resolved.child(2).dataType == expectedInitialType)
    assert(resolved.child(3).dataType == expectedInitialType)

    resolved = expr.resolveDataType(ArrayType(LongType))
    assert(resolved.dataType == ArrayType(LongType))
    assert(resolved.child(0).dataType == expectedInitialType)
    assert(resolved.child(1).dataType == LongType)
    assert(resolved.child(2).dataType == expectedInitialType)
    assert(resolved.child(3).dataType == expectedInitialType)
  }

  test("resolveDataType - HllSketchAgg") {
    val expr = HllSketchAgg(AttributeReference("id", AnyType), "5")

    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == BinaryType)
    assert(resolved.child(0).dataType == IntegerType + LongType + StringType + BinaryType)
    assert(resolved.child(1).dataType == IntegerType)
  }

  test("resolveDataType - HllUnionAgg") {
    val expr = HllUnionAgg(AttributeReference("id", AnyType), "true")

    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == BinaryType)
    assert(resolved.child(0).dataType == BinaryType)
    assert(resolved.child(1).dataType == BooleanType)
  }

  test("resolveDataType - RegrR2") {
    val expr = RegrR2(AttributeReference("id", AnyType), "1.0")

    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == DoubleType)
    assert(resolved.child(0).dataType == DoubleType)
    assert(resolved.child(1).dataType == DoubleType)
  }

  test("resolveDataType - RegrSXY") {
    val expr = RegrSXY(AttributeReference("id", AnyType), "1.0")

    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == DoubleType)
    assert(resolved.child(0).dataType == DoubleType)
    assert(resolved.child(1).dataType == DoubleType)
  }

  test("resolveDataType - RegrSlope") {
    val expr = RegrSlope(AttributeReference("id", AnyType), "1.0")

    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == DoubleType)
    assert(resolved.child(0).dataType == DoubleType)
    assert(resolved.child(1).dataType == DoubleType)
  }

  test("resolveDataType - RegrIntercept") {
    val expr = RegrIntercept(AttributeReference("id", AnyType), "1.0")

    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == DoubleType)
    assert(resolved.child(0).dataType == DoubleType)
    assert(resolved.child(1).dataType == DoubleType)
  }

  test("resolveDataType - ApproxTopK") {
    val expr = ApproxTopK(
      AttributeReference("expr", AnyType),
      AttributeReference("k", AnyType),
      AttributeReference("maxItemsTracked", AnyType)
    )
    val expectedDataType = (inputDataType: DataType) => {
      ArrayType(
        StructType(
          Array(
            StructField("item", inputDataType),
            StructField("count", LongType)
          )
        )
      )
    }

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == expectedDataType(AnyType))
    assert(resolved.child(0).dataType == AnyType)
    assert(resolved.child(1).dataType == IntegerType)
    assert(resolved.child(2).dataType == IntegerType)

    resolved = expr.resolveDataType(expectedDataType(LongType))
    assert(resolved.dataType == expectedDataType(LongType))
    assert(resolved.child(0).dataType == LongType)
    assert(resolved.child(1).dataType == IntegerType)
    assert(resolved.child(2).dataType == IntegerType)
  }

  test("resolveDataType - ApproxTopKAccumulate") {
    val expr = ApproxTopKAccumulate(
      AttributeReference("expr", AnyType),
      AttributeReference("maxItemsTracked", AnyType)
    )
    val expectedDataType = (inputDataType: DataType) => {
      StructType(
        Array(
          StructField("sketch", BinaryType),
          StructField("maxItemsTracked", IntegerType),
          StructField("itemDataType", inputDataType),
          StructField("itemDataTypeDDL", StringType)
        )
      )
    }

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == expectedDataType(AnyType))
    assert(resolved.child(0).dataType == AnyType)
    assert(resolved.child(1).dataType == IntegerType)

    resolved = expr.resolveDataType(expectedDataType(LongType))
    assert(resolved.dataType == expectedDataType(LongType))
    assert(resolved.child(0).dataType == LongType)
    assert(resolved.child(1).dataType == IntegerType)
  }

  test("resolveDataType - ApproxTopKCombine") {
    val expr = ApproxTopKCombine(
      AttributeReference("state", AnyType),
      AttributeReference("maxItemsTracked", AnyType)
    )
    val expectedDataType = (itemDataType: DataType) => {
      StructType(
        Array(
          StructField("sketch", BinaryType),
          StructField("maxItemsTracked", IntegerType),
          StructField("itemDataType", itemDataType),
          StructField("itemDataTypeDDL", StringType)
        )
      )
    }

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == expectedDataType(AnyType))
    assert(resolved.child(0).dataType == expectedDataType(AnyType))
    assert(resolved.child(1).dataType == IntegerType)

    resolved = expr.resolveDataType(expectedDataType(LongType))
    assert(resolved.dataType == expectedDataType(LongType))
    assert(resolved.child(0).dataType == expectedDataType(LongType))
    assert(resolved.child(1).dataType == IntegerType)
  }

  test("resolveDataType - ListAgg") {
    val expr = ListAgg(
      AttributeReference("child", AnyType),
      AttributeReference("delimiter", AnyType),
      IndexedSeq(
        SortOrder(AttributeReference("order", AnyType), Descending, NullsLast)
      )
    )

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == TypeSet(StringType, BinaryType))
    assert(resolved.child(1).dataType == TypeSet(StringType, BinaryType, NullType))
    assert(resolved.child(2).dataType == AnyType)

    resolved = expr.resolveDataType(StringType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == StringType)
    assert(resolved.child(2).dataType == AnyType)
  }

  test("resolveDataType - ThetaSketchAgg") {
    val expr = ThetaSketchAgg(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )

    val resolved = expr.resolveDataType(BinaryType)
    assert(
      resolved.child(0).dataType ==
        TypeSet(
          ArrayType(IntegerType),
          ArrayType(LongType),
          BinaryType,
          DoubleType,
          FloatType,
          IntegerType,
          LongType,
          StringType
        )
    )
    assert(resolved.child(1).dataType == IntegerType)
  }

  test("resolveDataType - ThetaUnionAgg") {
    val expr = ThetaUnionAgg(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )

    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == BinaryType)
    assert(resolved.child(1).dataType == IntegerType)
  }

  test("resolveDataType - ThetaIntersectionAgg") {
    val expr = ThetaIntersectionAgg(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == BinaryType)
  }

  test("resolveDataType - KllSketchAggBigint") {
    val expr = KllSketchAggBigint(
      AttributeReference("child", AnyType),
      Some(AttributeReference("kExpr", AnyType))
    )

    val resolved = expr.resolveDataType(BinaryType)
    assert(resolved.child(0).dataType == TypeSet.Integral)
    assert(resolved.child(1).dataType == IntegerType)
  }

  test("resolveDataType - KllSketchAggFloat") {
    val expr = KllSketchAggFloat(
      AttributeReference("child", AnyType),
      Some(AttributeReference("kExpr", AnyType))
    )

    val resolved = expr.resolveDataType(BinaryType)
    assert(resolved.child(0).dataType == FloatType)
    assert(resolved.child(1).dataType == IntegerType)
  }

  test("resolveDataType - KllSketchAggDouble") {
    val expr = KllSketchAggDouble(
      AttributeReference("child", AnyType),
      Some(AttributeReference("kExpr", AnyType))
    )

    val resolved = expr.resolveDataType(BinaryType)
    assert(resolved.child(0).dataType == TypeSet(FloatType, DoubleType))
    assert(resolved.child(1).dataType == IntegerType)
  }

  test("resolveDataType - SchemaOfVariantAgg") {
    val expr = SchemaOfVariantAgg(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == VariantType)
  }

  test("resolveDataType - HyperLogLogInitSimpleAgg") {
    val expr =
      HyperLogLogInitSimpleAgg(AttributeReference("id", AnyType), 0.05, "AgKn")

    val resolved = expr.resolveDataType(BinaryType)
    assert(resolved.dataType == BinaryType)
    assert(resolved.child.dataType == AnyType)
  }

  test("resolveDataType - HyperLogLogCardinality") {
    val expr = HyperLogLogCardinality(AttributeReference("id", AnyType), "AgKn")

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == LongType)
    assert(resolved.child.dataType == BinaryType)

    resolved = expr.resolveDataType(LongType)
    assert(resolved.dataType == LongType)
    assert(resolved.child.dataType == BinaryType)
  }

  test("resolveDataType - ApproxTopKEstimate") {
    val expr = ApproxTopKEstimate(
      AttributeReference("state", AnyType),
      AttributeReference("k", AnyType)
    )
    val expectedInputType = (itemDataType: DataType) => {
      StructType(
        Array(
          StructField("sketch", BinaryType),
          StructField("maxItemsTracked", IntegerType),
          StructField("itemDataType", itemDataType),
          StructField("itemDataTypeDDL", StringType)
        )
      )
    }
    val expectedDataType = (itemDataType: DataType) => {
      ArrayType(
        StructType(
          Array(
            StructField("item", itemDataType),
            StructField("count", LongType)
          )
        )
      )
    }

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == expectedDataType(AnyType))
    assert(resolved.child(0).dataType == expectedInputType(AnyType))
    assert(resolved.child(1).dataType == IntegerType)

    resolved = expr.resolveDataType(expectedDataType(StringType))
    assert(resolved.dataType == expectedDataType(StringType))
    assert(resolved.child(0).dataType == expectedInputType(StringType))
    assert(resolved.child(1).dataType == IntegerType)
  }

  test("resolveDataType - AvroDataToCatalyst") {
    val expr = AvroDataToCatalyst(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == BinaryType)
  }

  test("resolveDataType - CatalystDataToAvro") {
    val expr = CatalystDataToAvro(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(BinaryType).child.dataType == AnyType)
  }

  test("resolveDataType - BitmapBucketNumber") {
    val expr = BitmapBucketNumber(AttributeReference("id", AnyType))

    assert(expr.resolveDataType(AnyType).child.dataType == LongType)
    assert(expr.resolveDataType(LongType).child.dataType == LongType)
  }

  test("resolveDataType - BitmapBitPosition") {
    val expr = BitmapBitPosition(AttributeReference("id", AnyType))

    assert(expr.resolveDataType(AnyType).child.dataType == LongType)
    assert(expr.resolveDataType(LongType).child.dataType == LongType)
  }

  test("resolveDataType - BitmapCount") {
    val expr = BitmapCount(AttributeReference("id", AnyType))

    assert(expr.resolveDataType(AnyType).child.dataType == BinaryType)
    assert(expr.resolveDataType(LongType).child.dataType == BinaryType)
  }

  test("resolveDataType - BitmapConstructAgg") {
    val expr = BitmapConstructAgg(AttributeReference("id", AnyType))

    assert(expr.resolveDataType(AnyType).child.dataType == LongType)
    assert(expr.resolveDataType(BinaryType).child.dataType == LongType)
  }

  test("resolveDataType - BitmapOrAgg") {
    val expr = BitmapOrAgg(AttributeReference("id", AnyType))

    assert(expr.resolveDataType(AnyType).child.dataType == BinaryType)
    assert(expr.resolveDataType(BinaryType).child.dataType == BinaryType)
  }

  test("resolveDataType - BitmapAndAgg") {
    val expr = BitmapAndAgg(AttributeReference("id", AnyType))

    assert(expr.resolveDataType(AnyType).child.dataType == BinaryType)
    assert(expr.resolveDataType(BinaryType).child.dataType == BinaryType)
  }

  test("resolveDataType - BloomFilterMightContain") {
    var expr = BloomFilterMightContain(
      AttributeReference("filter", AnyType),
      AttributeReference("value", AnyType)
    )

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == TypeSet(BinaryType, NullType))
    assert(resolved.child(1).dataType == TypeSet(LongType, NullType))

    expr = BloomFilterMightContain(
      AttributeReference("filter", AnyType),
      "2"
    )
    resolved = expr.resolveDataType(BooleanType)
    assert(resolved.child(0).dataType == TypeSet(BinaryType, NullType))
    assert(resolved.child(1).dataType == LongType)
  }

  test("resolveDataType - Collate") {
    val expr = Collate(
      AttributeReference("child", AnyType),
      AttributeReference("collation", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == AnyType)
  }

  test("resolveDataType - Size") {
    val expr = Size(AttributeReference("id", AnyType))
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child.dataType == TypeSet(ArrayType(AnyType), MapType(AnyType, AnyType)))
  }

  test("resolveDataType - MapKeys") {
    val expr = MapKeys(AttributeReference("id", AnyType))

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == ArrayType(AnyType))
    assert(resolved.child.dataType == MapType(AnyType, AnyType))

    resolved = expr.resolveDataType(ArrayType(LongType))
    assert(resolved.dataType == ArrayType(LongType))
    assert(resolved.child.dataType == MapType(LongType, AnyType))
  }

  test("resolveDataType - MapValues") {
    val expr = MapValues(AttributeReference("id", AnyType))

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == ArrayType(AnyType))
    assert(resolved.child.dataType == MapType(AnyType, AnyType))

    resolved = expr.resolveDataType(ArrayType(LongType))
    assert(resolved.dataType == ArrayType(LongType))
    assert(resolved.child.dataType == MapType(AnyType, LongType))
  }

  test("resolveDataType - MapEntries") {
    val expr = MapEntries(AttributeReference("id", AnyType))
    val expectedDataType =
      (keyType: DataType, valueType: DataType) => {
        ArrayType(
          StructType(
            Array(
              StructField("key", keyType),
              StructField("value", valueType)
            )
          )
        )
      }

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == expectedDataType(AnyType, AnyType))
    assert(resolved.child.dataType == MapType(AnyType, AnyType))

    val concreteDataType = expectedDataType(LongType, StringType)
    resolved = expr.resolveDataType(concreteDataType)
    assert(resolved.dataType == concreteDataType)
    assert(resolved.child.dataType == MapType(LongType, StringType))
  }

  test("resolveDataType - MapConcat") {
    assert(MapConcat(IndexedSeq.empty).dataType == MapType(StringType, StringType))

    var expr = MapConcat(
      IndexedSeq(
        AttributeReference("map1", AnyType),
        AttributeReference("map2", AnyType)
      )
    )

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == MapType(AnyType, AnyType))
    assert(resolved.child(0).dataType == MapType(AnyType, AnyType))
    assert(resolved.child(1).dataType == MapType(AnyType, AnyType))

    resolved = expr.resolveDataType(MapType(LongType, StringType))
    assert(resolved.dataType == MapType(LongType, StringType))
    assert(resolved.child(0).dataType == MapType(LongType, StringType))
    assert(resolved.child(1).dataType == MapType(LongType, StringType))

    expr = MapConcat(
      IndexedSeq(
        AttributeReference("map1", AnyType),
        AttributeReference("map2", MapType(LongType, StringType))
      )
    )

    resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == MapType(LongType, StringType))
    assert(resolved.child(0).dataType == MapType(LongType, StringType))
    assert(resolved.child(1).dataType == MapType(LongType, StringType))
  }

  test("resolveDataType - MapFromEntries") {
    val expr = MapFromEntries(AttributeReference("id", AnyType))
    val expectedChildType =
      (keyType: DataType, valueType: DataType) => {
        ArrayType(
          StructType(
            Array(
              StructField("key", keyType),
              StructField("value", valueType)
            )
          )
        )
      }

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == MapType(AnyType, AnyType))
    assert(resolved.child.dataType == expectedChildType(AnyType, AnyType))

    resolved = expr.resolveDataType(MapType(LongType, StringType))
    assert(resolved.dataType == MapType(LongType, StringType))
    assert(resolved.child.dataType == expectedChildType(LongType, StringType))
  }

  test("resolveDataType - SortArray") {
    val expr = SortArray(AttributeReference("id", AnyType), "true")

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == ArrayType(AnyType))
    assert(resolved.child(0).dataType == ArrayType(AnyType))
    assert(resolved.child(1).dataType == BooleanType)

    resolved = expr.resolveDataType(ArrayType(LongType))
    assert(resolved.dataType == ArrayType(LongType))
    assert(resolved.child(0).dataType == ArrayType(LongType))
    assert(resolved.child(1).dataType == BooleanType)
  }

  test("resolveDataType - Shuffle") {
    val expr = Shuffle(AttributeReference("id", AnyType))

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == ArrayType(AnyType))
    assert(resolved.child.dataType == ArrayType(AnyType))

    resolved = expr.resolveDataType(ArrayType(LongType))
    assert(resolved.dataType == ArrayType(LongType))
    assert(resolved.child.dataType == ArrayType(LongType))
  }

  test("resolveDataType - Reverse") {
    val expr = Reverse(AttributeReference("id", AnyType))

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == TypeSet(StringType, ArrayType(AnyType)))
    assert(resolved.child.dataType == TypeSet(StringType, ArrayType(AnyType)))

    resolved = expr.resolveDataType(ArrayType(LongType))
    assert(resolved.dataType == ArrayType(LongType))
    assert(resolved.child.dataType == ArrayType(LongType))
  }

  test("resolveDataType - ArrayContains") {
    var expr = ArrayContains(AttributeReference("id", AnyType), "2")
    var resolved = expr.resolveDataType(BooleanType)
    assert(resolved.dataType == BooleanType)
    assert(resolved.child(0).dataType == ArrayType(TypeSet.Integral + DecimalType + StringType))
    assert(resolved.child(1).dataType == TypeSet.Integral + DecimalType + StringType)

    expr = ArrayContains(AttributeReference("id", ArrayType(LongType)), "2")
    resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == BooleanType)
    assert(resolved.child(0).dataType == ArrayType(LongType))
    assert(resolved.child(1).dataType == LongType)
  }

  test("resolveDataType - ArraysOverlap") {
    var expr = ArraysOverlap(
      AttributeReference("array1", AnyType),
      AttributeReference("array2", ArrayType(LongType))
    )
    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == BooleanType)
    assert(resolved.child(0).dataType == ArrayType(LongType))
    assert(resolved.child(1).dataType == ArrayType(LongType))

    expr = ArraysOverlap(
      AttributeReference("array1", ArrayType(LongType)),
      AttributeReference("array2", AnyType)
    )
    resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == BooleanType)
    assert(resolved.child(0).dataType == ArrayType(LongType))
    assert(resolved.child(1).dataType == ArrayType(LongType))
  }

  test("resolveDataType - Slice") {
    var expr = Slice(AttributeReference("id", AnyType), "1", "5")

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == ArrayType(AnyType))
    assert(resolved.child(0).dataType == ArrayType(AnyType))
    assert(resolved.child(1).dataType == IntegerType)
    assert(resolved.child(1).dataType == IntegerType)

    expr = Slice(AttributeReference("id", ArrayType(LongType)), "1", "5")
    resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == ArrayType(LongType))
    assert(resolved.child(0).dataType == ArrayType(LongType))
    assert(resolved.child(1).dataType == IntegerType)
    assert(resolved.child(1).dataType == IntegerType)
  }

  test("resolveDataType - ArrayJoin") {
    val expr = ArrayJoin(AttributeReference("id", AnyType), "_", Some(","))

    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == StringType)
    assert(resolved.child(0).dataType == ArrayType(StringType))
    assert(resolved.child(1).dataType == StringType)
    assert(resolved.child(1).dataType == StringType)
  }

  test("resolveDataType - ArrayMin") {
    val expr = ArrayMin(AttributeReference("id", AnyType))

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == AnyType)
    assert(resolved.child.dataType == ArrayType(AnyType))

    resolved = expr.resolveDataType(LongType)
    assert(resolved.dataType == LongType)
    assert(resolved.child.dataType == ArrayType(LongType))
  }

  test("resolveDataType - ArrayMax") {
    val expr = ArrayMax(AttributeReference("id", AnyType))

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == AnyType)
    assert(resolved.child.dataType == ArrayType(AnyType))

    resolved = expr.resolveDataType(LongType)
    assert(resolved.dataType == LongType)
    assert(resolved.child.dataType == ArrayType(LongType))
  }

  test("resolveDataType - ArrayPosition") {
    var expr = ArrayPosition(AttributeReference("id", AnyType), "3")

    var resolved = expr.resolveDataType(LongType)
    assert(resolved.dataType == LongType)
    assert(resolved.child(0).dataType == ArrayType(TypeSet.Integral + DecimalType + StringType))
    assert(resolved.child(1).dataType == TypeSet.Integral + DecimalType + StringType)

    expr = ArrayPosition(AttributeReference("id", ArrayType(LongType)), "2")
    resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == LongType)
    assert(resolved.child(0).dataType == ArrayType(LongType))
    assert(resolved.child(1).dataType == LongType)
  }

  test("resolveDataType - ElementAt") {
    var expr = ElementAt(AttributeReference("id", AnyType), "2", None, failOnError = false)

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == AnyType)
    assert(resolved.child(0).dataType == AnyType)
    assert(resolved.child(1).dataType == TypeSet.Integral + DecimalType + StringType)

    resolved = expr.resolveDataType(LongType)
    assert(resolved.dataType == AnyType)
    assert(resolved.child(0).dataType == AnyType)
    assert(resolved.child(1).dataType == TypeSet.Integral + DecimalType + StringType)

    expr = ElementAt(AttributeReference("id", ArrayType(AnyType)), "2", None, failOnError = false)

    resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == AnyType)
    assert(resolved.child(0).dataType == ArrayType(AnyType))
    assert(resolved.child(1).dataType == IntegerType)

    resolved = expr.resolveDataType(LongType)
    assert(resolved.dataType == LongType)
    assert(resolved.child(0).dataType == ArrayType(LongType))
    assert(resolved.child(1).dataType == IntegerType)

    expr = ElementAt(
      AttributeReference("id", MapType(ByteType, AnyType)),
      "2",
      None,
      failOnError = false
    )

    resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == AnyType)
    assert(resolved.child(0).dataType == MapType(ByteType, AnyType))
    assert(resolved.child(1).dataType == ByteType)

    resolved = expr.resolveDataType(LongType)
    assert(resolved.dataType == LongType)
    assert(resolved.child(0).dataType == MapType(ByteType, LongType))
    assert(resolved.child(1).dataType == ByteType)
  }

  test("resolveDataType - Concat") {
    var expr = Concat(
      IndexedSeq(
        AttributeReference("array1", AnyType),
        AttributeReference("array2", AnyType)
      )
    )

    var resolved = expr.resolveDataType(AnyType)
    val expectedDataType = TypeSet(BinaryType, StringType, ArrayType(AnyType))
    assert(resolved.dataType == expectedDataType)
    assert(resolved.child(0).dataType == expectedDataType)
    assert(resolved.child(1).dataType == expectedDataType)

    resolved = expr.resolveDataType(ArrayType(AnyType))
    assert(resolved.dataType == ArrayType(AnyType))
    assert(resolved.child(0).dataType == ArrayType(AnyType))
    assert(resolved.child(1).dataType == ArrayType(AnyType))

    expr = Concat(
      IndexedSeq(
        AttributeReference("array1", AnyType),
        AttributeReference("array2", ArrayType(LongType))
      )
    )
    resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == ArrayType(LongType))
    assert(resolved.child(0).dataType == ArrayType(LongType))
    assert(resolved.child(1).dataType == ArrayType(LongType))

    expr = Concat(
      IndexedSeq(
        AttributeReference("array1", AnyType),
        "somestring"
      )
    )
    resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == StringType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == StringType)
  }

  test("resolveDataType - Flatten") {
    val expr = Flatten(AttributeReference("id", AnyType))

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == ArrayType(AnyType))
    assert(resolved.child.dataType == ArrayType(ArrayType(AnyType)))

    resolved = expr.resolveDataType(ArrayType(LongType))
    assert(resolved.dataType == ArrayType(LongType))
    assert(resolved.child.dataType == ArrayType(ArrayType(LongType)))
  }

  test("resolveDataType - Sequence") {
    var expr = Sequence(
      AttributeReference("start", AnyType),
      AttributeReference("stop", AnyType),
      Some(AttributeReference("step", AnyType)),
      timeZoneId = None
    )

    var resolved = expr.resolveDataType(AnyType)
    var expectedChildType = TypeSet.Integral + TimestampType + TimestampNTZType + DateType
    assert(resolved.dataType == ArrayType(expectedChildType))
    assert(resolved.child(0).dataType == expectedChildType)
    assert(resolved.child(1).dataType == expectedChildType)
    assert(resolved.child(2).dataType == AnyType)

    val intervalTypes = TypeSet(CalendarIntervalType, YearMonthIntervalType, DayTimeIntervalType)

    resolved = expr.resolveDataType(ArrayType(TimestampType))
    assert(resolved.dataType == ArrayType(TimestampType))
    assert(resolved.child(0).dataType == TimestampType)
    assert(resolved.child(1).dataType == TimestampType)
    assert(resolved.child(2).dataType == intervalTypes)

    resolved = expr.resolveDataType(ArrayType(DateType))
    assert(resolved.dataType == ArrayType(DateType))
    assert(resolved.child(0).dataType == DateType)
    assert(resolved.child(1).dataType == DateType)
    assert(resolved.child(2).dataType == intervalTypes)

    resolved = expr.resolveDataType(ArrayType(LongType))
    assert(resolved.dataType == ArrayType(LongType))
    assert(resolved.child(0).dataType == LongType)
    assert(resolved.child(1).dataType == LongType)
    assert(resolved.child(2).dataType == LongType)

    expr = Sequence(
      AttributeReference("start", AnyType),
      AttributeReference("stop", AnyType),
      Some(AttributeReference("step", CalendarIntervalType)),
      timeZoneId = None
    )
    resolved = expr.resolveDataType(AnyType)
    expectedChildType = TypeSet(TimestampType, TimestampNTZType, DateType)
    assert(resolved.dataType == ArrayType(expectedChildType))
    assert(resolved.child(0).dataType == expectedChildType)
    assert(resolved.child(1).dataType == expectedChildType)
    assert(resolved.child(2).dataType == CalendarIntervalType)

    expr = Sequence(
      AttributeReference("start", AnyType),
      AttributeReference("stop", AnyType),
      Some(AttributeReference("step", ShortType)),
      timeZoneId = None
    )
    resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == ArrayType(ShortType))
    assert(resolved.child(0).dataType == ShortType)
    assert(resolved.child(1).dataType == ShortType)
    assert(resolved.child(2).dataType == ShortType)
  }

  test("resolveDataType - ArrayInsert") {
    var expr = ArrayInsert(
      AttributeReference("id", AnyType),
      pos = "2",
      item = "3",
      legacyNegativeIndex = false
    )

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == ArrayType(TypeSet.Integral + DecimalType + StringType))
    assert(resolved.child(0).dataType == ArrayType(TypeSet.Integral + DecimalType + StringType))
    assert(resolved.child(1).dataType == IntegerType)
    assert(resolved.child(2).dataType == TypeSet.Integral + DecimalType + StringType)

    resolved = expr.resolveDataType(ArrayType(ShortType))
    assert(resolved.dataType == ArrayType(ShortType))
    assert(resolved.child(0).dataType == ArrayType(ShortType))
    assert(resolved.child(1).dataType == IntegerType)
    assert(resolved.child(2).dataType == ShortType)

    expr = ArrayInsert(
      AttributeReference("id", ArrayType(ByteType)),
      pos = "2",
      item = "3",
      legacyNegativeIndex = false
    )

    resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == ArrayType(ByteType))
    assert(resolved.child(0).dataType == ArrayType(ByteType))
    assert(resolved.child(1).dataType == IntegerType)
    assert(resolved.child(2).dataType == ByteType)
  }

  test("resolveDataType - ArrayRepeat") {
    val expr = ArrayRepeat(AttributeReference("id", AnyType), "5")

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == ArrayType(AnyType))
    assert(resolved.child(0).dataType == AnyType)
    assert(resolved.child(1).dataType == IntegerType)

    resolved = expr.resolveDataType(ArrayType(LongType))
    assert(resolved.dataType == ArrayType(LongType))
    assert(resolved.child(0).dataType == LongType)
    assert(resolved.child(1).dataType == IntegerType)
  }

  test("resolveDataType - ArrayRemove") {
    val expr = ArrayRemove(AttributeReference("id", AnyType), "3")

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == ArrayType(TypeSet.Integral + DecimalType + StringType))
    assert(resolved.child(0).dataType == ArrayType(TypeSet.Integral + DecimalType + StringType))
    assert(resolved.child(1).dataType == TypeSet.Integral + DecimalType + StringType)

    resolved = expr.resolveDataType(ArrayType(LongType))
    assert(resolved.dataType == ArrayType(LongType))
    assert(resolved.child(0).dataType == ArrayType(LongType))
    assert(resolved.child(1).dataType == LongType)
  }

  test("resolveDataType - ArrayDistinct") {
    val expr = ArrayDistinct(AttributeReference("id", AnyType))

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == ArrayType(AnyType))
    assert(resolved.child.dataType == ArrayType(AnyType))

    resolved = expr.resolveDataType(ArrayType(LongType))
    assert(resolved.dataType == ArrayType(LongType))
    assert(resolved.child.dataType == ArrayType(LongType))
  }

  test("resolveDataType - ArrayUnion") {
    var expr = ArrayUnion(
      AttributeReference("array1", AnyType),
      AttributeReference("array2", ArrayType(LongType))
    )
    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == ArrayType(LongType))
    assert(resolved.child(0).dataType == ArrayType(LongType))
    assert(resolved.child(1).dataType == ArrayType(LongType))

    expr = ArrayUnion(
      AttributeReference("array1", ArrayType(LongType)),
      AttributeReference("array2", AnyType)
    )
    resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == ArrayType(LongType))
    assert(resolved.child(0).dataType == ArrayType(LongType))
    assert(resolved.child(1).dataType == ArrayType(LongType))
  }

  test("resolveDataType - ArrayIntersect") {
    var expr = ArrayIntersect(
      AttributeReference("array1", AnyType),
      AttributeReference("array2", ArrayType(LongType))
    )
    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == ArrayType(LongType))
    assert(resolved.child(0).dataType == ArrayType(LongType))
    assert(resolved.child(1).dataType == ArrayType(LongType))

    expr = ArrayIntersect(
      AttributeReference("array1", ArrayType(LongType)),
      AttributeReference("array2", AnyType)
    )
    resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == ArrayType(LongType))
    assert(resolved.child(0).dataType == ArrayType(LongType))
    assert(resolved.child(1).dataType == ArrayType(LongType))
  }

  test("resolveDataType - ArrayExcept") {
    var expr = ArrayExcept(
      AttributeReference("array1", AnyType),
      AttributeReference("array2", ArrayType(LongType))
    )
    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == ArrayType(LongType))
    assert(resolved.child(0).dataType == ArrayType(LongType))
    assert(resolved.child(1).dataType == ArrayType(LongType))

    expr = ArrayExcept(
      AttributeReference("array1", ArrayType(LongType)),
      AttributeReference("array2", AnyType)
    )
    resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == ArrayType(LongType))
    assert(resolved.child(0).dataType == ArrayType(LongType))
    assert(resolved.child(1).dataType == ArrayType(LongType))
  }

  test("resolveDataType - ArrayAppend") {
    val expr = ArrayAppend(AttributeReference("id", AnyType), "3")

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == ArrayType(TypeSet.Integral + DecimalType + StringType))
    assert(resolved.child(0).dataType == ArrayType(TypeSet.Integral + DecimalType + StringType))
    assert(resolved.child(1).dataType == TypeSet.Integral + DecimalType + StringType)

    resolved = expr.resolveDataType(ArrayType(LongType))
    assert(resolved.dataType == ArrayType(LongType))
    assert(resolved.child(0).dataType == ArrayType(LongType))
    assert(resolved.child(1).dataType == LongType)
  }

  test("resolveDataType - ArraysZip") {
    val expr =
      ArraysZip(
        IndexedSeq(
          AttributeReference("id", AnyType),
          AttributeReference("array", ArrayType(LongType)),
          "[]"
        ),
        Seq("0", "1", "2")
      )
    val expectedDataType = (first: DataType, second: DataType, third: DataType) => {
      ArrayType(
        StructType(
          Array(
            StructField("0", first),
            StructField("1", second),
            StructField("2", third)
          )
        )
      )
    }

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == expectedDataType(AnyType, LongType, AnyType))
    assert(resolved.child(0).dataType == ArrayType(AnyType))
    assert(resolved.child(1).dataType == ArrayType(LongType))
    assert(resolved.child(2).dataType == ArrayType(AnyType))

    resolved = expr.resolveDataType(expectedDataType(ByteType, AnyType, IntegerType))
    assert(resolved.dataType == expectedDataType(ByteType, LongType, IntegerType))
    assert(resolved.child(0).dataType == ArrayType(ByteType))
    assert(resolved.child(1).dataType == ArrayType(LongType))
    assert(resolved.child(2).dataType == ArrayType(IntegerType))
  }

  test("resolveDataType - CreateArray") {
    var expr = CreateArray(IndexedSeq.empty)
    assert(expr.dataType == ArrayType(TypeSet(StringType, NullType)))

    var resolved = expr.resolveDataType(ArrayType(StringType))
    assert(resolved.dataType == ArrayType(StringType))

    expr = CreateArray(
      IndexedSeq(
        AttributeReference("v1", AnyType),
        AttributeReference("v2", TypeSet.Integral)
      )
    )
    assert(expr.dataType == ArrayType(TypeSet.Integral))

    resolved = expr.resolveDataType(ArrayType(LongType))
    assert(resolved.dataType == ArrayType(LongType))
    assert(resolved.child(0).dataType == LongType)
    assert(resolved.child(1).dataType == LongType)
  }

  test("resolveDataType - CreateMap") {
    var expr = CreateMap(IndexedSeq.empty)
    assert(expr.dataType == MapType(TypeSet(StringType, NullType), TypeSet(StringType, NullType)))

    var resolved = expr.resolveDataType(MapType(StringType, StringType))
    assert(resolved.dataType == MapType(StringType, StringType))

    expr = CreateMap(
      IndexedSeq(
        AttributeReference("k1", AnyType),
        AttributeReference("v1", TypeSet.Numeric),
        AttributeReference("k2", AnyType),
        AttributeReference("v2", TypeSet.Integral)
      )
    )
    assert(expr.dataType == MapType(AnyType, TypeSet.Integral))

    resolved = expr.resolveDataType(MapType(StringType, LongType))
    assert(resolved.dataType == MapType(StringType, LongType))
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == LongType)
    assert(resolved.child(2).dataType == StringType)
    assert(resolved.child(3).dataType == LongType)
  }

  test("resolveDataType - MapFromArrays") {
    val expr = MapFromArrays(
      AttributeReference("array1", AnyType),
      AttributeReference("array2", ArrayType(LongType))
    )
    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == MapType(AnyType, LongType))
    assert(resolved.child(0).dataType == ArrayType(AnyType))
    assert(resolved.child(1).dataType == ArrayType(LongType))

    resolved = expr.resolveDataType(MapType(StringType, LongType))
    assert(resolved.dataType == MapType(StringType, LongType))
    assert(resolved.child(0).dataType == ArrayType(StringType))
    assert(resolved.child(1).dataType == ArrayType(LongType))
  }

  test("resolveDataType - CreateNamedStruct") {
    val expr = CreateNamedStruct(
      IndexedSeq(
        "1",
        AttributeReference("v1", AnyType),
        "b",
        AttributeReference("v2", LongType)
      )
    )
    val expectedDataType = (dataType1: DataType, dateType2: DataType) => {
      StructType(
        Array(
          StructField("1", dataType1),
          StructField("b", dateType2)
        )
      )
    }
    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == expectedDataType(AnyType, LongType))
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == AnyType)
    assert(resolved.child(2).dataType == StringType)
    assert(resolved.child(3).dataType == LongType)

    resolved = expr.resolveDataType(expectedDataType(IntegerType, LongType))
    assert(resolved.dataType == expectedDataType(IntegerType, LongType))
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == IntegerType)
    assert(resolved.child(2).dataType == StringType)
    assert(resolved.child(3).dataType == LongType)
  }

  test("resolveDataType - StringToMap") {
    val expr = StringToMap(
      AttributeReference("text", AnyType),
      AttributeReference("pairDelim", AnyType),
      AttributeReference("keyValueDelim", AnyType)
    )
    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == MapType(StringType, StringType))
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == StringType)
    assert(resolved.child(0).dataType == StringType)

    resolved = expr.resolveDataType(MapType(StringType, StringType))
    assert(resolved.dataType == MapType(StringType, StringType))
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == StringType)
    assert(resolved.child(0).dataType == StringType)
  }

  test("resolveDataType - GetStructField") {
    val expectedStructType = (dataType: DataType) => {
      StructType(
        Array(
          StructField("a", LongType),
          StructField("b", dataType)
        )
      )
    }

    // Parser guarantees the child data type is StructType.
    val expr = GetStructField(
      AttributeReference("id", expectedStructType(AnyType)),
      ordinal = 1,
      name = None,
      numFields = 2
    )
    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == AnyType)
    assert(resolved.child.dataType == expectedStructType(AnyType))

    resolved = expr.resolveDataType(IntegerType)
    assert(resolved.dataType == IntegerType)
    assert(resolved.child.dataType == expectedStructType(IntegerType))
  }

  test("resolveDataType - GetArrayStructFields") {
    val expectedArrayType = (dataType: DataType) => {
      ArrayType(
        StructType(
          Array(
            StructField("a", LongType),
            StructField("b", dataType)
          )
        )
      )
    }

    // Parser guarantees the child data type is StructType.
    val expr = GetArrayStructFields(
      AttributeReference("id", expectedArrayType(AnyType)),
      field = StructField("b", AnyType),
      ordinal = 1,
      numFields = 2
    )
    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == ArrayType(AnyType))
    assert(resolved.child.dataType == expectedArrayType(AnyType))

    resolved = expr.resolveDataType(ArrayType(IntegerType))
    assert(resolved.dataType == ArrayType(IntegerType))
    assert(resolved.child.dataType == expectedArrayType(IntegerType))
  }

  test("resolveDataType - GetArrayItem") {
    var expr = GetArrayItem(AttributeReference("id", MapType(AnyType, AnyType)), "0")
    assert(expr.dataType == TypeSet.Empty)

    expr = GetArrayItem(AttributeReference("id", AnyType), "0")
    assert(expr.dataType == AnyType)

    var resolved = expr.resolveDataType(AnyType).asInstanceOf[GetArrayItem]
    assert(resolved.child.dataType == ArrayType(AnyType))
    assert(resolved.ordinal.dataType == TypeSet.Integral)

    resolved = expr.resolveDataType(StringType).asInstanceOf[GetArrayItem]
    assert(resolved.child.dataType == ArrayType(StringType))
    assert(resolved.ordinal.dataType == TypeSet.Integral)

    val structOrArrayType =
      TypeSet(
        StructType(Array(StructField("", StringType))),
        ArrayType(StringType)
      )
    expr = GetArrayItem(AttributeReference("id", structOrArrayType), "0")
    assert(expr.dataType == StringType)

    resolved = expr.resolveDataType(AnyType).asInstanceOf[GetArrayItem]
    assert(resolved.child.dataType == ArrayType(StringType))
    assert(resolved.ordinal.dataType == TypeSet.Integral)
  }

  test("resolveDataType - GetMapValue") {
    var expr = GetMapValue(AttributeReference("id", ArrayType(AnyType)), "0")
    assert(expr.dataType == TypeSet.Empty)

    expr = GetMapValue(AttributeReference("id", AnyType), "0")
    assert(expr.dataType == AnyType)

    var resolved = expr.resolveDataType(AnyType).asInstanceOf[GetMapValue]
    val expectedKeyType = TypeSet.Integral + DecimalType + StringType
    assert(resolved.child.dataType == MapType(expectedKeyType, AnyType))
    assert(resolved.key.dataType == expectedKeyType)

    resolved = expr.resolveDataType(StringType).asInstanceOf[GetMapValue]
    assert(resolved.child.dataType == MapType(expectedKeyType, StringType))
    assert(resolved.key.dataType == expectedKeyType)

    expr = GetMapValue(
      AttributeReference("id", MapType(LongType, AnyType)),
      AttributeReference("key", AnyType)
    )

    resolved = expr.resolveDataType(AnyType).asInstanceOf[GetMapValue]
    assert(resolved.child.dataType == MapType(LongType, AnyType))
    assert(resolved.key.dataType == LongType)

    resolved = expr.resolveDataType(StringType).asInstanceOf[GetMapValue]
    assert(resolved.child.dataType == MapType(LongType, StringType))
    assert(resolved.key.dataType == LongType)
  }

  test("resolveDataType - KnownNullable") {
    val expr = KnownNullable(AttributeReference("id", AnyType))

    val resolved = expr.resolveDataType(LongType)
    assert(resolved.dataType == LongType)
    assert(resolved.child.dataType == LongType)
  }

  test("resolveDataType - KnownNotNull") {
    val expr = KnownNotNull(AttributeReference("id", AnyType))

    val resolved = expr.resolveDataType(LongType)
    assert(resolved.dataType == LongType)
    assert(resolved.child.dataType == LongType)
  }

  test("resolveDataType - KnownFloatingPointNormalized") {
    val expr = KnownFloatingPointNormalized(AttributeReference("id", AnyType))

    val resolved = expr.resolveDataType(LongType)
    assert(resolved.dataType == LongType)
    assert(resolved.child.dataType == LongType)
  }

  test("resolveDataType - KnownNotContainsNull") {
    val expr = KnownNotContainsNull(AttributeReference("id", AnyType))

    assert(expr.resolveDataType(AnyType).child.dataType == ArrayType(AnyType))
    assert(expr.resolveDataType(ArrayType(StringType)).child.dataType == ArrayType(StringType))
  }

  test("resolveDataType - CsvToStructs") {
    val expr = CsvToStructs(
      StructType(Array(StructField("a", IntegerType))),
      options = Map.empty,
      child = AttributeReference("id", AnyType),
      timeZoneId = None,
      requiredSchema = None
    )

    assert(expr.resolveDataType(AnyType).child.dataType == StringType)
  }

  test("resolveDataType - SchemaOfCsv") {
    val expr = SchemaOfCsv("1", Map.empty)
    assert(expr.resolveDataType(AnyType).child.dataType == StringType)
  }

  test("resolveDataType - StructsToCsv") {
    val expr = StructsToCsv(Map.empty, AttributeReference("id", AnyType), None)

    // We can't infer number of fields.
    assert(expr.resolveDataType(AnyType).child.dataType == AnyType)
  }

  test("resolveDataType - HllSketchEstimate") {
    val expr = HllSketchEstimate(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == BinaryType)
  }

  test("resolveDataType - HllUnion") {
    val expr = HllUnion(
      AttributeReference("hll1", AnyType),
      AttributeReference("hll2", AnyType),
      AttributeReference("allowDifferentLgConfigK", AnyType)
    )

    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == BinaryType)
    assert(resolved.child(1).dataType == BinaryType)
    assert(resolved.child(2).dataType == BooleanType)
  }

  test("resolveDataType - Year") {
    val expr = Year(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DateType)
  }

  test("resolveDataType - YearOfWeek") {
    val expr = YearOfWeek(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DateType)
  }

  test("resolveDataType - Quarter") {
    val expr = Quarter(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DateType)
  }

  test("resolveDataType - Month") {
    val expr = Month(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DateType)
  }

  test("resolveDataType - DayOfMonth") {
    val expr = DayOfMonth(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DateType)
  }

  test("resolveDataType - DayOfYear") {
    val expr = DayOfYear(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DateType)
  }

  test("resolveDataType - DayOfWeek") {
    val expr = DayOfWeek(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DateType)
  }

  test("resolveDataType - WeekDay") {
    val expr = WeekDay(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DateType)
  }

  test("resolveDataType - WeekOfYear") {
    val expr = WeekOfYear(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DateType)
  }

  test("resolveDataType - Hour") {
    val expr = Hour(AttributeReference("id", AnyType), None)
    assert(expr.resolveDataType(AnyType).child.dataType == TypeSet.AnyTimestamp)
  }

  test("resolveDataType - Minute") {
    val expr = Minute(AttributeReference("id", AnyType), None)
    assert(expr.resolveDataType(AnyType).child.dataType == TypeSet.AnyTimestamp)
  }

  test("resolveDataType - Second") {
    val expr = Second(AttributeReference("id", AnyType), None)
    assert(expr.resolveDataType(AnyType).child.dataType == TypeSet.AnyTimestamp)
  }

  test("resolveDataType - SecondWithFraction") {
    val expr = SecondWithFraction(AttributeReference("id", AnyType), None)
    assert(expr.resolveDataType(AnyType).child.dataType == TypeSet.AnyTimestamp)
  }

  test("resolveDataType - UnixDate") {
    val expr = UnixDate(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DateType)
  }

  test("resolveDataType - DateFromUnixDate") {
    val expr = DateFromUnixDate(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == IntegerType)
  }

  test("resolveDataType - LastDay") {
    val expr = LastDay(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DateType)
  }

  test("resolveDataType - SecondsToTimestamp") {
    val expr = SecondsToTimestamp(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == TypeSet.Numeric)
  }

  test("resolveDataType - MillisToTimestamp") {
    val expr = MillisToTimestamp(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == TypeSet.Integral)
  }

  test("resolveDataType - MicrosToTimestamp") {
    val expr = MicrosToTimestamp(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == TypeSet.Integral)
  }

  test("resolveDataType - UnixSeconds") {
    val expr = UnixSeconds(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == TimestampType)
  }

  test("resolveDataType - CastTimestampNTZToLong") {
    val expr = CastTimestampNTZToLong(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == TimestampNTZType)
  }

  test("resolveDataType - UnixMillis") {
    val expr = UnixMillis(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == TimestampType)
  }

  test("resolveDataType - UnixMicros") {
    val expr = UnixMicros(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == TimestampType)
  }

  test("resolveDataType - DateAdd") {
    val expr = DateAdd(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == DateType)
    assert(resolved.child(1).dataType == TypeSet(IntegerType, ShortType, ByteType))
  }

  test("resolveDataType - DateSub") {
    val expr = DateSub(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == DateType)
    assert(resolved.child(1).dataType == TypeSet(IntegerType, ShortType, ByteType))
  }

  test("resolveDataType - NextDay") {
    val expr = NextDay(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == DateType)
    assert(resolved.child(1).dataType == StringType)
  }

  test("resolveDataType - AddMonths") {
    val expr = AddMonths(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == DateType)
    assert(resolved.child(1).dataType == IntegerType)
  }

  test("resolveDataType - DateAddYMInterval") {
    val expr = DateAddYMInterval(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == DateType)
    assert(resolved.child(1).dataType == YearMonthIntervalType)
  }

  test("resolveDataType - DateAddInterval") {
    val expr = DateAddInterval(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == DateType)
    assert(resolved.child(1).dataType == CalendarIntervalType)
  }

  test("resolveDataType - TruncDate") {
    val expr = TruncDate(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == DateType)
    assert(resolved.child(1).dataType == StringType)
  }

  test("resolveDataType - DateFormatClass") {
    val expr = DateFormatClass(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType),
      None
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == TimestampType)
    assert(resolved.child(1).dataType == StringType)
  }

  test("resolveDataType - ToUnixTimestamp") {
    val expr = ToUnixTimestamp(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType),
      None
    )
    val expectedTimeType = TypeSet(StringType, DateType, TimestampType, TimestampNTZType)
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == expectedTimeType)
    assert(resolved.child(1).dataType == StringType)
  }

  test("resolveDataType - UnixTimestamp") {
    val expr = UnixTimestamp(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType),
      None
    )
    val expectedTimeType = TypeSet(StringType, DateType, TimestampType, TimestampNTZType)
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == expectedTimeType)
    assert(resolved.child(1).dataType == StringType)
  }

  test("resolveDataType - FromUnixTime") {
    val expr = FromUnixTime(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType),
      None
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == LongType)
    assert(resolved.child(1).dataType == StringType)
  }

  test("resolveDataType - TimeAdd") {
    val expr = TimeAdd(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == TypeSet.AnyTimestamp)
    assert(resolved.child(1).dataType == TypeSet(CalendarIntervalType, DayTimeIntervalType))

    resolved = expr.resolveDataType(TimestampType)
    assert(resolved.child(0).dataType == TimestampType)
    assert(resolved.child(1).dataType == TypeSet(CalendarIntervalType, DayTimeIntervalType))
  }

  test("resolveDataType - TimestampAddYMInterval") {
    val expr = TimestampAddYMInterval(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == TypeSet.AnyTimestamp)
    assert(resolved.child(1).dataType == YearMonthIntervalType)

    resolved = expr.resolveDataType(TimestampType)
    assert(resolved.child(0).dataType == TimestampType)
    assert(resolved.child(1).dataType == YearMonthIntervalType)
  }

  test("resolveDataType - DateDiff") {
    val expr = DateDiff(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == DateType)
    assert(resolved.child(1).dataType == DateType)
  }

  test("resolveDataType - FromUTCTimestamp") {
    val expr = FromUTCTimestamp(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == TimestampType)
    assert(resolved.child(1).dataType == StringType)
  }

  test("resolveDataType - ToUTCTimestamp") {
    val expr = ToUTCTimestamp(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == TimestampType)
    assert(resolved.child(1).dataType == StringType)
  }

  test("resolveDataType - TruncTimestamp") {
    val expr = TruncTimestamp(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType),
      None
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == TimestampType)
  }

  test("resolveDataType - MonthsBetween") {
    val expr = MonthsBetween(
      AttributeReference("date1", AnyType),
      AttributeReference("date2", AnyType),
      AttributeReference("roundOff", AnyType),
      None
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == TimestampType)
    assert(resolved.child(1).dataType == TimestampType)
    assert(resolved.child(2).dataType == BooleanType)
  }

  test("resolveDataType - MakeDate") {
    val expr = MakeDate(
      AttributeReference("year", AnyType),
      AttributeReference("month", AnyType),
      AttributeReference("day", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == IntegerType)
    assert(resolved.child(1).dataType == IntegerType)
    assert(resolved.child(2).dataType == IntegerType)
  }

  test("resolveDataType - MakeTimestamp") {
    val expr = MakeTimestamp(
      AttributeReference("year", AnyType),
      AttributeReference("month", AnyType),
      AttributeReference("day", AnyType),
      AttributeReference("hour", AnyType),
      AttributeReference("min", AnyType),
      AttributeReference("sec", AnyType),
      Some(AttributeReference("timezone", AnyType)),
      None,
      failOnError = false,
      TimestampType
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == IntegerType)
    assert(resolved.child(1).dataType == IntegerType)
    assert(resolved.child(2).dataType == IntegerType)
    assert(resolved.child(3).dataType == IntegerType)
    assert(resolved.child(4).dataType == IntegerType)
    assert(resolved.child(5).dataType == DecimalType)
    assert(resolved.child(6).dataType == StringType)

    MakeTimestamp(
      AttributeReference("year", AnyType),
      AttributeReference("month", AnyType),
      AttributeReference("day", AnyType),
      AttributeReference("hour", AnyType),
      AttributeReference("min", AnyType),
      AttributeReference("sec", AnyType),
      None,
      None,
      failOnError = false,
      TimestampType
    ).resolveDataType(AnyType)
  }

  test("resolveDataType - GetTimestamp") {
    val expr = GetTimestamp(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType),
      TimestampType,
      None
    )
    val expectedTimeType = TypeSet(StringType, DateType, TimestampType, TimestampNTZType)
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == expectedTimeType)
    assert(resolved.child(1).dataType == StringType)
  }

  test("resolveDataType - SubtractTimestamps") {
    val expr = SubtractTimestamps(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == TypeSet(CalendarIntervalType, DayTimeIntervalType))
    assert(resolved.child(0).dataType == TypeSet.AnyTimestamp)
    assert(resolved.child(1).dataType == TypeSet.AnyTimestamp)

    resolved = expr.resolveDataType(DayTimeIntervalType)
    assert(resolved.dataType == DayTimeIntervalType)
    assert(resolved.child(0).dataType == TypeSet.AnyTimestamp)
    assert(resolved.child(1).dataType == TypeSet.AnyTimestamp)
  }

  test("resolveDataType - SubtractDates") {
    val expr = SubtractDates(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == TypeSet(CalendarIntervalType, DayTimeIntervalType))
    assert(resolved.child(0).dataType == DateType)
    assert(resolved.child(1).dataType == DateType)

    resolved = expr.resolveDataType(DayTimeIntervalType)
    assert(resolved.dataType == DayTimeIntervalType)
    assert(resolved.child(0).dataType == DateType)
    assert(resolved.child(1).dataType == DateType)
  }

  test("resolveDataType - ConvertTimezone") {
    val expr = ConvertTimezone(
      AttributeReference("sourceTz", AnyType),
      AttributeReference("targetTz", AnyType),
      AttributeReference("sourceTs", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == StringType)
    assert(resolved.child(2).dataType == TimestampNTZType)
  }

  test("resolveDataType - TimestampAdd") {
    val expr = TimestampAdd(
      "HOUR",
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType),
      None
    )

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == IntegerType)
    assert(resolved.child(1).dataType == TypeSet.AnyTimestamp)

    resolved = expr.resolveDataType(TimestampType)
    assert(resolved.child(0).dataType == IntegerType)
    assert(resolved.child(1).dataType == TimestampType)
  }

  test("resolveDataType - TimestampDiff") {
    val expr = TimestampDiff(
      "HOUR",
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType),
      None
    )

    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == TimestampType)
    assert(resolved.child(1).dataType == TimestampType)
  }

  test("resolveDataType - DayName") {
    val expr = DayName(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DateType)
  }

  test("resolveDataType - MonthName") {
    val expr = MonthName(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DateType)
  }

  test("resolveDataType - UnscaledValue") {
    val expr = UnscaledValue(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DecimalType)
  }

  test("resolveDataType - MakeDecimal") {
    val expr = MakeDecimal(AttributeReference("id", AnyType), 18, 2)
    assert(expr.resolveDataType(AnyType).child.dataType == LongType)
  }

  test("resolveDataType - PromotePrecision") {
    val expr = PromotePrecision(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DecimalType)
  }

  test("resolveDataType - CheckOverflow") {
    val expr = CheckOverflow(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DecimalType)
  }

  test("resolveDataType - Explode") {
    val expr = Explode(AttributeReference("id", AnyType))

    var resolved = expr.resolveDataType(AnyType)
    val defaultChildType = TypeSet(ArrayType(AnyType), MapType(AnyType, AnyType))
    assert(resolved.dataType == ArrayType(Explode.defaultElementType))
    assert(resolved.child.dataType == defaultChildType)

    resolved = expr.resolveDataType(ArrayType(AnyType))
    assert(resolved.dataType == ArrayType(Explode.defaultElementType))
    assert(resolved.child.dataType == defaultChildType)

    // Array type child.
    resolved = expr.resolveDataType(ArrayType(StructType(Array(StructField("a", LongType)))))
    assert(resolved.dataType == ArrayType(StructType(Array(StructField("col", LongType)))))
    assert(resolved.child.dataType == ArrayType(LongType))

    // Map type child.
    resolved = expr.resolveDataType(
      ArrayType(
        StructType(
          Array(
            StructField("a", LongType),
            StructField("b", LongType)
          )
        )
      )
    )
    assert(
      resolved.dataType ==
        ArrayType(
          StructType(
            Array(
              StructField("key", LongType),
              StructField("value", LongType)
            )
          )
        )
    )
    assert(resolved.child.dataType == MapType(LongType, LongType))
  }

  test("resolveDataType - PosExplode") {
    val expr = PosExplode(AttributeReference("id", AnyType))

    var resolved = expr.resolveDataType(AnyType)
    val defaultChildType = TypeSet(ArrayType(AnyType), MapType(AnyType, AnyType))
    assert(resolved.dataType == ArrayType(PosExplode.defaultElementType))
    assert(resolved.child.dataType == defaultChildType)

    resolved = expr.resolveDataType(ArrayType(AnyType))
    assert(resolved.dataType == ArrayType(PosExplode.defaultElementType))
    assert(resolved.child.dataType == defaultChildType)

    // Array type child.
    resolved = expr.resolveDataType(
      ArrayType(
        StructType(
          Array(
            StructField("i", IntegerType),
            StructField("a", LongType)
          )
        )
      )
    )
    assert(
      resolved.dataType ==
        ArrayType(
          StructType(
            Array(
              StructField("pos", IntegerType),
              StructField("col", LongType)
            )
          )
        )
    )
    assert(resolved.child.dataType == ArrayType(LongType))

    // Map type child.
    resolved = expr.resolveDataType(
      ArrayType(
        StructType(
          Array(
            StructField("i", IntegerType),
            StructField("a", LongType),
            StructField("b", LongType)
          )
        )
      )
    )
    assert(
      resolved.dataType ==
        ArrayType(
          StructType(
            Array(
              StructField("pos", IntegerType),
              StructField("key", LongType),
              StructField("value", LongType)
            )
          )
        )
    )
    assert(resolved.child.dataType == MapType(LongType, LongType))
  }

  test("resolveDataType - Inline") {
    val expr = Inline(AttributeReference("id", AnyType))

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == ArrayType(AnyType))
    assert(resolved.child.dataType == ArrayType(AnyType))

    resolved = expr.resolveDataType(ArrayType(AnyType))
    assert(resolved.dataType == ArrayType(AnyType))
    assert(resolved.child.dataType == ArrayType(AnyType))

    val arrayOfStructType = ArrayType(StructType(Array(StructField("a", LongType))))
    resolved = expr.resolveDataType(arrayOfStructType)
    assert(resolved.dataType == arrayOfStructType)
    assert(resolved.child.dataType == arrayOfStructType)
  }

  test("resolveDataType - JsonTuple") {
    val expr = JsonTuple(
      IndexedSeq(
        AttributeReference("json", AnyType),
        AttributeReference("f1", AnyType),
        AttributeReference("f2", AnyType)
      )
    )
    val expectedDataType =
      ArrayType(
        StructType(
          Array(
            StructField("c0", StringType),
            StructField("c1", StringType)
          )
        )
      )
    assert(expr.dataType == expectedDataType)

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == expectedDataType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == StringType)
    assert(resolved.child(2).dataType == StringType)

    resolved = expr.resolveDataType(ArrayType(AnyType))
    assert(resolved.dataType == expectedDataType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == StringType)
    assert(resolved.child(2).dataType == StringType)
  }

  test("resolveDataType - Stack") {
    val expr = Stack(
      IndexedSeq("2", AttributeReference("id", AnyType), "3", "5", "a", "7.0")
    )
    val initialDataType =
      ArrayType(
        StructType(
          Array(
            StructField("col0", StringType),
            StructField("col1", DecimalType + StringType),
            StructField("col2", TypeSet.Integral + DecimalType + StringType)
          )
        )
      )
    assert(expr.dataType == initialDataType)

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == initialDataType)
    assert(resolved.child(0).dataType == IntegerType)
    assert(resolved.child(1).dataType == StringType)
    assert(resolved.child(2).dataType == DecimalType + StringType)
    assert(resolved.child(3).dataType == TypeSet.Integral + DecimalType + StringType)
    assert(resolved.child(4).dataType == StringType)
    assert(resolved.child(5).dataType == DecimalType + StringType)

    resolved = expr.resolveDataType(
      ArrayType(
        StructType(
          Array(
            StructField("a", AnyType),
            StructField("b", DecimalType),
            StructField("c", LongType)
          )
        )
      )
    )
    assert(
      resolved.dataType ==
        ArrayType(
          StructType(
            Array(
              StructField("col0", StringType),
              StructField("col1", DecimalType),
              StructField("col2", LongType)
            )
          )
        )
    )
    assert(resolved.child(0).dataType == IntegerType)
    assert(resolved.child(1).dataType == StringType)
    assert(resolved.child(2).dataType == DecimalType)
    assert(resolved.child(3).dataType == LongType)
    assert(resolved.child(4).dataType == StringType)
    assert(resolved.child(5).dataType == DecimalType)
  }

  test("resolveDataType - ReplicateRows") {
    val expr = ReplicateRows(
      IndexedSeq(
        AttributeReference("rows", AnyType),
        AttributeReference("v1", StringType),
        AttributeReference("v2", AnyType)
      )
    )
    val initialDataType =
      ArrayType(
        StructType(
          Array(
            StructField("col0", StringType),
            StructField("col1", AnyType)
          )
        )
      )
    assert(expr.dataType == initialDataType)

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == initialDataType)
    assert(resolved.child(0).dataType == LongType)
    assert(resolved.child(1).dataType == StringType)
    assert(resolved.child(2).dataType == AnyType)

    resolved = expr.resolveDataType(
      ArrayType(
        StructType(
          Array(
            StructField("a", AnyType),
            StructField("b", DecimalType)
          )
        )
      )
    )
    assert(
      resolved.dataType ==
        ArrayType(
          StructType(
            Array(
              StructField("col0", StringType),
              StructField("col1", DecimalType)
            )
          )
        )
    )
    assert(resolved.child(0).dataType == LongType)
    assert(resolved.child(1).dataType == StringType)
    assert(resolved.child(2).dataType == DecimalType)
  }

  test("resolveDataType - VariantExplode") {
    val expr = VariantExplode(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == VariantType)
  }

  test("resolveDataType - Md5") {
    val expr = Md5(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == BinaryType)
  }

  test("resolveDataType - Sha2") {
    val expr = Sha2(AttributeReference("id", AnyType), "256")
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == BinaryType)
    assert(resolved.child(1).dataType == IntegerType)
  }

  test("resolveDataType - Sha1") {
    val expr = Sha1(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == BinaryType)
  }

  test("resolveDataType - Crc32") {
    val expr = Crc32(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == BinaryType)
  }

  test("resolveDataType - Murmur3Hash") {
    val expr = Murmur3Hash(IndexedSeq(AttributeReference("id", AnyType)), 42)
    assert(expr.resolveDataType(AnyType).child.dataType == AnyType)
  }

  test("resolveDataType - XxHash64") {
    val expr = XxHash64(IndexedSeq(AttributeReference("id", AnyType)), 42)
    assert(expr.resolveDataType(AnyType).child.dataType == AnyType)
  }

  test("resolveDataType - NamedLambdaVariable") {
    var expr = NamedLambdaVariable("id", AnyType, 1)
    assert(expr.resolveDataType(AnyType).dataType == AnyType)
    assert(expr.resolveDataType(LongType).dataType == LongType)

    expr = NamedLambdaVariable("id", LongType, 1)
    assert(expr.resolveDataType(AnyType).dataType == LongType)
    assert(expr.resolveDataType(LongType).dataType == LongType)
    assert(expr.resolveDataType(IntegerType).dataType == TypeSet.Empty)
  }

  test("resolveDataType - LambdaFunction") {
    val variable = NamedLambdaVariable("id", 1)
    var expr = LambdaFunction(variable, IndexedSeq(variable))

    var resolved = expr.resolveDataType(LongType)
    assert(resolved.child(0).dataType == LongType)
    assert(resolved.child(1).dataType == LongType)

    expr = LambdaFunction(
      GreaterThan(variable, AttributeReference("other", 2)),
      IndexedSeq(variable)
    )

    resolved = expr.resolveDataType(BooleanType)
    assert(resolved.child(0).child(0).dataType == AnyType)
    assert(resolved.child(0).child(1).dataType == AnyType)
    assert(resolved.child(1).dataType == AnyType)

    resolved = expr.resolveDataType(BooleanType, Seq(LongType))
    assert(resolved.child(0).child(0).dataType == LongType)
    assert(resolved.child(0).child(1).dataType == LongType)
    assert(resolved.child(1).dataType == LongType)
  }

  test("resolveDataType - ArrayTransform") {
    val variable = NamedLambdaVariable("x", 1)
    var expr = ArrayTransform(
      AttributeReference("id", AnyType),
      LambdaFunction(variable, IndexedSeq(variable))
    )

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == ArrayType(AnyType))
    assert(resolved.child(1).child(0).dataType == AnyType)
    assert(resolved.child(1).child(1).dataType == AnyType)

    resolved = expr.resolveDataType(ArrayType(IntegerType))
    assert(resolved.child(0).dataType == ArrayType(IntegerType))
    assert(resolved.child(1).child(0).dataType == IntegerType)
    assert(resolved.child(1).child(1).dataType == IntegerType)

    expr = ArrayTransform(
      AttributeReference("id", AnyType),
      LambdaFunction(
        NamedLambdaVariable("i", 2),
        IndexedSeq(variable, NamedLambdaVariable("i", 2))
      )
    )
    resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == ArrayType(AnyType))
    assert(resolved.child(1).child(0).dataType == IntegerType)
    assert(resolved.child(1).child(1).dataType == AnyType)
    assert(resolved.child(1).child(2).dataType == IntegerType)

    expr = ArrayTransform(
      AttributeReference("id", AnyType),
      LambdaFunction(Upper(variable), IndexedSeq(variable))
    )
    resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == ArrayType(StringType))
    assert(resolved.child(1).child(1).dataType == StringType)

    expr = ArrayTransform(
      AttributeReference("id", ArrayType(LongType)),
      LambdaFunction(variable, IndexedSeq(variable))
    )
    resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == ArrayType(LongType))
    assert(resolved.child(1).child(0).dataType == LongType)
    assert(resolved.child(1).child(1).dataType == LongType)
  }

  test("resolveDataType - ArraySort") {
    var expr = ArraySort(
      AttributeReference("id", AnyType),
      LambdaFunction(
        "0",
        IndexedSeq(
          NamedLambdaVariable("left", 1),
          NamedLambdaVariable("right", 2)
        )
      )
    )

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == ArrayType(AnyType))
    assert(resolved.child(1).child(0).dataType == IntegerType)
    assert(resolved.child(1).child(1).dataType == AnyType)
    assert(resolved.child(1).child(2).dataType == AnyType)

    resolved = expr.resolveDataType(ArrayType(LongType))
    assert(resolved.child(0).dataType == ArrayType(LongType))
    assert(resolved.child(1).child(0).dataType == IntegerType)
    assert(resolved.child(1).child(1).dataType == LongType)
    assert(resolved.child(1).child(2).dataType == LongType)

    expr = ArraySort(
      AttributeReference("id", AnyType),
      LambdaFunction(
        NamedLambdaVariable("left", 1),
        IndexedSeq(
          NamedLambdaVariable("left", 1),
          NamedLambdaVariable("right", 2)
        )
      )
    )
    resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == ArrayType(IntegerType))
    assert(resolved.child(1).child(0).dataType == IntegerType)
    assert(resolved.child(1).child(1).dataType == IntegerType)
    assert(resolved.child(1).child(2).dataType == IntegerType)

    expr = ArraySort(
      AttributeReference("id", ArrayType(LongType)),
      LambdaFunction(
        "1",
        IndexedSeq(
          NamedLambdaVariable("left", 1),
          NamedLambdaVariable("right", 2)
        )
      )
    )
    resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == ArrayType(LongType))
    assert(resolved.child(1).child(0).dataType == IntegerType)
    assert(resolved.child(1).child(1).dataType == LongType)
    assert(resolved.child(1).child(2).dataType == LongType)
  }

  test("resolveDataType - MapFilter") {
    var expr = MapFilter(
      AttributeReference("id", AnyType),
      LambdaFunction(
        NamedLambdaVariable("key", 1),
        IndexedSeq(
          NamedLambdaVariable("key", 1),
          NamedLambdaVariable("value", 2)
        )
      )
    )

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == MapType(BooleanType, AnyType))
    assert(resolved.child(1).child(0).dataType == BooleanType)
    assert(resolved.child(1).child(1).dataType == BooleanType)
    assert(resolved.child(1).child(2).dataType == AnyType)

    expr = MapFilter(
      AttributeReference("id", AnyType),
      LambdaFunction(
        GreaterThan(NamedLambdaVariable("key", 1), "1"),
        IndexedSeq(
          NamedLambdaVariable("key", 1),
          NamedLambdaVariable("value", 2)
        )
      )
    )
    resolved = expr.resolveDataType(MapType(LongType, StringType))
    assert(resolved.child(0).dataType == MapType(LongType, StringType))
    assert(resolved.child(1).child(0).child(0).dataType == LongType)
    assert(resolved.child(1).child(1).dataType == LongType)
    assert(resolved.child(1).child(2).dataType == StringType)

    expr = MapFilter(
      AttributeReference("id", MapType(LongType, StringType)),
      LambdaFunction(
        GreaterThan(NamedLambdaVariable("key", 1), "1"),
        IndexedSeq(
          NamedLambdaVariable("key", 1),
          NamedLambdaVariable("value", 2)
        )
      )
    )
    resolved = expr.resolveDataType(MapType(LongType, StringType))
    assert(resolved.child(0).dataType == MapType(LongType, StringType))
    assert(resolved.child(1).child(0).child(0).dataType == LongType)
    assert(resolved.child(1).child(1).dataType == LongType)
    assert(resolved.child(1).child(2).dataType == StringType)
  }

  test("resolveDataType - ArrayFilter") {
    var expr = ArrayFilter(
      AttributeReference("id", AnyType),
      LambdaFunction(
        NamedLambdaVariable("x", 1),
        IndexedSeq(NamedLambdaVariable("x", 1))
      )
    )

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == ArrayType(BooleanType))
    assert(resolved.child(1).child(0).dataType == BooleanType)
    assert(resolved.child(1).child(1).dataType == BooleanType)

    expr = ArrayFilter(
      AttributeReference("id", AnyType),
      LambdaFunction(
        GreaterThan(NamedLambdaVariable("x", 1), "1"),
        IndexedSeq(NamedLambdaVariable("x", 1))
      )
    )
    resolved = expr.resolveDataType(ArrayType(LongType))
    assert(resolved.child(0).dataType == ArrayType(LongType))
    assert(resolved.child(1).child(0).child(0).dataType == LongType)
    assert(resolved.child(1).child(1).dataType == LongType)

    expr = ArrayFilter(
      AttributeReference("id", ArrayType(LongType)),
      LambdaFunction(
        GreaterThan(NamedLambdaVariable("x", 1), "1"),
        IndexedSeq(
          NamedLambdaVariable("x", 1),
          NamedLambdaVariable("i", 2)
        )
      )
    )
    resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == ArrayType(LongType))
    assert(resolved.child(1).child(0).child(0).dataType == LongType)
    assert(resolved.child(1).child(1).dataType == LongType)
    assert(resolved.child(1).child(2).dataType == IntegerType)
  }

  test("resolveDataType - ArrayExists") {
    var expr = ArrayExists(
      AttributeReference("id", AnyType),
      LambdaFunction(
        NamedLambdaVariable("x", 1),
        IndexedSeq(NamedLambdaVariable("x", 1))
      )
    )

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == ArrayType(BooleanType))
    assert(resolved.child(1).child(0).dataType == BooleanType)
    assert(resolved.child(1).child(1).dataType == BooleanType)

    expr = ArrayExists(
      AttributeReference("id", AnyType),
      LambdaFunction(
        GreaterThan(
          NamedLambdaVariable("x", 1),
          AttributeReference("other", 2)
        ),
        IndexedSeq(NamedLambdaVariable("x", 1))
      )
    )
    resolved = expr.resolveDataType(BooleanType)
    assert(resolved.child(0).dataType == ArrayType(AnyType))
    assert(resolved.child(1).child(0).child(0).dataType == AnyType)
    assert(resolved.child(1).child(1).dataType == AnyType)

    expr = ArrayExists(
      AttributeReference("id", ArrayType(LongType)),
      LambdaFunction(
        GreaterThan(NamedLambdaVariable("x", 1), "1"),
        IndexedSeq(NamedLambdaVariable("x", 1))
      )
    )
    resolved = expr.resolveDataType(BooleanType)
    assert(resolved.child(0).dataType == ArrayType(LongType))
    assert(resolved.child(1).child(0).child(0).dataType == LongType)
    assert(resolved.child(1).child(1).dataType == LongType)
  }

  test("resolveDataType - ArrayForAll") {
    var expr = ArrayForAll(
      AttributeReference("id", AnyType),
      LambdaFunction(
        NamedLambdaVariable("x", 1),
        IndexedSeq(NamedLambdaVariable("x", 1))
      )
    )

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == ArrayType(BooleanType))
    assert(resolved.child(1).child(0).dataType == BooleanType)
    assert(resolved.child(1).child(1).dataType == BooleanType)

    expr = ArrayForAll(
      AttributeReference("id", AnyType),
      LambdaFunction(
        GreaterThan(
          NamedLambdaVariable("x", 1),
          AttributeReference("other", 2)
        ),
        IndexedSeq(NamedLambdaVariable("x", 1))
      )
    )
    resolved = expr.resolveDataType(BooleanType)
    assert(resolved.child(0).dataType == ArrayType(AnyType))
    assert(resolved.child(1).child(0).child(0).dataType == AnyType)
    assert(resolved.child(1).child(1).dataType == AnyType)

    expr = ArrayForAll(
      AttributeReference("id", ArrayType(LongType)),
      LambdaFunction(
        GreaterThan(NamedLambdaVariable("x", 1), "1"),
        IndexedSeq(NamedLambdaVariable("x", 1))
      )
    )
    resolved = expr.resolveDataType(BooleanType)
    assert(resolved.child(0).dataType == ArrayType(LongType))
    assert(resolved.child(1).child(0).child(0).dataType == LongType)
    assert(resolved.child(1).child(1).dataType == LongType)
  }

  test("resolveDataType - ArrayAggregate") {
    val x1 = NamedLambdaVariable("x", 1)
    val acc2 = NamedLambdaVariable("acc", 2)
    val acc3 = NamedLambdaVariable("acc", 3)
    var expr = ArrayAggregate(
      AttributeReference("id", AnyType),
      zero = "a",
      merge = LambdaFunction(x1, IndexedSeq(acc2, x1)),
      finish = LambdaFunction(acc3, IndexedSeq(acc3))
    )

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == ArrayType(AnyType))
    assert(resolved.child(1).dataType == StringType)
    assert(resolved.child(2).child(0).dataType == StringType)
    assert(resolved.child(2).child(1).dataType == StringType)
    assert(resolved.child(2).child(2).dataType == StringType)
    assert(resolved.child(3).child(0).dataType == StringType)
    assert(resolved.child(3).child(1).dataType == StringType)

    expr = ArrayAggregate(
      AttributeReference("id", AnyType),
      zero = AttributeReference("zero", AnyType),
      merge = LambdaFunction(x1, IndexedSeq(acc2, x1)),
      finish = LambdaFunction(acc3, IndexedSeq(acc3))
    )
    resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == ArrayType(AnyType))
    assert(resolved.child(1).dataType == AnyType)
    assert(resolved.child(2).child(0).dataType == AnyType)
    assert(resolved.child(2).child(1).dataType == AnyType)
    assert(resolved.child(2).child(2).dataType == AnyType)
    assert(resolved.child(3).child(0).dataType == AnyType)
    assert(resolved.child(3).child(1).dataType == AnyType)

    expr = ArrayAggregate(
      AttributeReference("id", ArrayType(LongType)),
      zero = AttributeReference("zero", AnyType),
      merge = LambdaFunction(acc2, IndexedSeq(acc2, x1)),
      finish = LambdaFunction(acc3, IndexedSeq(acc3))
    )
    resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == ArrayType(LongType))
    assert(resolved.child(1).dataType == AnyType)
    assert(resolved.child(2).child(0).dataType == AnyType)
    assert(resolved.child(2).child(1).dataType == AnyType)
    assert(resolved.child(2).child(2).dataType == LongType)
    assert(resolved.child(3).child(0).dataType == AnyType)
    assert(resolved.child(3).child(1).dataType == AnyType)

    expr = ArrayAggregate(
      AttributeReference("id", ArrayType(LongType)),
      zero = AttributeReference("zero", AnyType),
      merge = LambdaFunction(x1, IndexedSeq(acc2, x1)),
      finish = LambdaFunction(acc3, IndexedSeq(acc3))
    )
    resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == ArrayType(LongType))
    assert(resolved.child(1).dataType == LongType)
    assert(resolved.child(2).child(0).dataType == LongType)
    assert(resolved.child(2).child(1).dataType == LongType)
    assert(resolved.child(2).child(2).dataType == LongType)
    assert(resolved.child(3).child(0).dataType == LongType)
    assert(resolved.child(3).child(1).dataType == LongType)
  }

  test("resolveDataType - TransformKeys") {
    val key = NamedLambdaVariable("key", 1)
    val value = NamedLambdaVariable("value", 2)
    var expr = TransformKeys(
      AttributeReference("id", AnyType),
      LambdaFunction(
        Cast(value, AnyType, isTryCast = false),
        IndexedSeq(key, value)
      )
    )

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == MapType(AnyType, AnyType))
    assert(resolved.child(1).child(0).child(0).dataType == AnyType)
    assert(resolved.child(1).child(1).dataType == AnyType)
    assert(resolved.child(1).child(2).dataType == AnyType)

    resolved = expr.resolveDataType(MapType(IntegerType, StringType))
    assert(resolved.child(0).dataType == MapType(AnyType, StringType))
    assert(resolved.child(1).child(0).child(0).dataType == StringType)
    assert(resolved.child(1).child(1).dataType == AnyType)
    assert(resolved.child(1).child(2).dataType == StringType)

    expr = TransformKeys(
      AttributeReference("id", AnyType),
      LambdaFunction(Upper(value), IndexedSeq(key, value))
    )
    resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == MapType(AnyType, StringType))
    assert(resolved.child(1).child(0).child(0).dataType == StringType)
    assert(resolved.child(1).child(1).dataType == AnyType)
    assert(resolved.child(1).child(2).dataType == StringType)

    expr = TransformKeys(
      AttributeReference("id", MapType(IntegerType, StringType)),
      LambdaFunction(key, IndexedSeq(key, value))
    )
    resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == MapType(IntegerType, StringType))
    assert(resolved.child(1).child(0).dataType == IntegerType)
    assert(resolved.child(1).child(1).dataType == IntegerType)
    assert(resolved.child(1).child(2).dataType == StringType)
  }

  test("resolveDataType - TransformValues") {
    val key = NamedLambdaVariable("key", 1)
    val value = NamedLambdaVariable("value", 2)
    var expr = TransformValues(
      AttributeReference("id", AnyType),
      LambdaFunction(
        Cast(key, AnyType, isTryCast = false),
        IndexedSeq(key, value)
      )
    )

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == MapType(AnyType, AnyType))
    assert(resolved.child(1).child(0).child(0).dataType == AnyType)
    assert(resolved.child(1).child(1).dataType == AnyType)
    assert(resolved.child(1).child(2).dataType == AnyType)

    resolved = expr.resolveDataType(MapType(IntegerType, StringType))
    assert(resolved.child(0).dataType == MapType(IntegerType, AnyType))
    assert(resolved.child(1).child(0).child(0).dataType == IntegerType)
    assert(resolved.child(1).child(1).dataType == IntegerType)
    assert(resolved.child(1).child(2).dataType == AnyType)

    expr = TransformValues(
      AttributeReference("id", AnyType),
      LambdaFunction(Upper(key), IndexedSeq(key, value))
    )
    resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == MapType(StringType, AnyType))
    assert(resolved.child(1).child(0).child(0).dataType == StringType)
    assert(resolved.child(1).child(1).dataType == StringType)
    assert(resolved.child(1).child(2).dataType == AnyType)

    expr = TransformValues(
      AttributeReference("id", MapType(IntegerType, StringType)),
      LambdaFunction(value, IndexedSeq(key, value))
    )
    resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == MapType(IntegerType, StringType))
    assert(resolved.child(1).child(0).dataType == StringType)
    assert(resolved.child(1).child(1).dataType == IntegerType)
    assert(resolved.child(1).child(2).dataType == StringType)
  }

  test("resolveDataType - MapZipWith") {
    val key = NamedLambdaVariable("key", 1)
    val value1 = NamedLambdaVariable("value1", 2)
    val value2 = NamedLambdaVariable("value2", 3)
    var expr = MapZipWith(
      AttributeReference("map1", AnyType),
      AttributeReference("map2", AnyType),
      LambdaFunction(
        value1,
        IndexedSeq(key, value1, value2)
      )
    )

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == MapType(AnyType, AnyType))
    assert(resolved.child(1).dataType == MapType(AnyType, AnyType))
    assert(resolved.child(2).child(0).dataType == AnyType)
    assert(resolved.child(2).child(1).dataType == AnyType)
    assert(resolved.child(2).child(2).dataType == AnyType)
    assert(resolved.child(2).child(3).dataType == AnyType)

    resolved = expr.resolveDataType(MapType(IntegerType, StringType))
    assert(resolved.child(0).dataType == MapType(IntegerType, StringType))
    assert(resolved.child(1).dataType == MapType(IntegerType, AnyType))
    assert(resolved.child(2).child(0).dataType == StringType)
    assert(resolved.child(2).child(1).dataType == IntegerType)
    assert(resolved.child(2).child(2).dataType == StringType)
    assert(resolved.child(2).child(3).dataType == AnyType)

    expr = MapZipWith(
      AttributeReference("map1", AnyType),
      AttributeReference("map2", AnyType),
      LambdaFunction(
        Concat(IndexedSeq(value1, value2)),
        IndexedSeq(key, value1, value2)
      )
    )
    resolved = expr.resolveDataType(MapType(IntegerType, StringType))
    assert(resolved.child(0).dataType == MapType(IntegerType, StringType))
    assert(resolved.child(1).dataType == MapType(IntegerType, StringType))
    assert(resolved.child(2).child(0).child(0).dataType == StringType)
    assert(resolved.child(2).child(0).child(1).dataType == StringType)
    assert(resolved.child(2).child(1).dataType == IntegerType)
    assert(resolved.child(2).child(2).dataType == StringType)
    assert(resolved.child(2).child(3).dataType == StringType)

    expr = MapZipWith(
      AttributeReference("map1", MapType(IntegerType, LongType)),
      AttributeReference("map2", MapType(IntegerType, StringType)),
      LambdaFunction(
        value1,
        IndexedSeq(key, value1, value2)
      )
    )
    resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == MapType(IntegerType, LongType))
    assert(resolved.child(1).dataType == MapType(IntegerType, StringType))
    assert(resolved.child(2).child(0).dataType == LongType)
    assert(resolved.child(2).child(1).dataType == IntegerType)
    assert(resolved.child(2).child(2).dataType == LongType)
    assert(resolved.child(2).child(3).dataType == StringType)
  }

  test("resolveDataType - ZipWith") {
    val left = NamedLambdaVariable("left", 1)
    val right = NamedLambdaVariable("right", 2)
    var expr = ZipWith(
      AttributeReference("array1", AnyType),
      AttributeReference("array2", AnyType),
      LambdaFunction(
        left,
        IndexedSeq(left, right)
      )
    )

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == ArrayType(AnyType))
    assert(resolved.child(1).dataType == ArrayType(AnyType))
    assert(resolved.child(2).child(0).dataType == AnyType)
    assert(resolved.child(2).child(1).dataType == AnyType)
    assert(resolved.child(2).child(2).dataType == AnyType)

    resolved = expr.resolveDataType(ArrayType(IntegerType))
    assert(resolved.child(0).dataType == ArrayType(IntegerType))
    assert(resolved.child(1).dataType == ArrayType(AnyType))
    assert(resolved.child(2).child(0).dataType == IntegerType)
    assert(resolved.child(2).child(1).dataType == IntegerType)
    assert(resolved.child(2).child(2).dataType == AnyType)

    expr = ZipWith(
      AttributeReference("array1", AnyType),
      AttributeReference("array2", AnyType),
      LambdaFunction(
        FindInSet(left, right),
        IndexedSeq(left, right)
      )
    )
    resolved = expr.resolveDataType(ArrayType(IntegerType))
    assert(resolved.child(0).dataType == ArrayType(StringType))
    assert(resolved.child(1).dataType == ArrayType(StringType))
    assert(resolved.child(2).child(0).child(0).dataType == StringType)
    assert(resolved.child(2).child(0).child(1).dataType == StringType)
    assert(resolved.child(2).child(1).dataType == StringType)
    assert(resolved.child(2).child(2).dataType == StringType)

    expr = ZipWith(
      AttributeReference("array1", ArrayType(LongType)),
      AttributeReference("array2", ArrayType(StringType)),
      LambdaFunction(
        left,
        IndexedSeq(left, right)
      )
    )
    resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == ArrayType(LongType))
    assert(resolved.child(1).dataType == ArrayType(StringType))
    assert(resolved.child(2).child(0).dataType == LongType)
    assert(resolved.child(2).child(1).dataType == LongType)
    assert(resolved.child(2).child(2).dataType == StringType)
  }

  test("resolveDataType - ExtractIntervalYears") {
    val expr = ExtractIntervalYears(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == CalendarIntervalType)
  }

  test("resolveDataType - ExtractIntervalMonths") {
    val expr = ExtractIntervalMonths(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == CalendarIntervalType)
  }

  test("resolveDataType - ExtractIntervalDays") {
    val expr = ExtractIntervalDays(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == CalendarIntervalType)
  }

  test("resolveDataType - ExtractIntervalHours") {
    val expr = ExtractIntervalHours(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == CalendarIntervalType)
  }

  test("resolveDataType - ExtractIntervalMinutes") {
    val expr = ExtractIntervalMinutes(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == CalendarIntervalType)
  }

  test("resolveDataType - ExtractIntervalSeconds") {
    val expr = ExtractIntervalSeconds(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == CalendarIntervalType)
  }

  test("resolveDataType - ExtractANSIIntervalYears") {
    val expr = ExtractANSIIntervalYears(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == YearMonthIntervalType)
  }

  test("resolveDataType - ExtractANSIIntervalMonths") {
    val expr = ExtractANSIIntervalMonths(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == YearMonthIntervalType)
  }

  test("resolveDataType - ExtractANSIIntervalDays") {
    val expr = ExtractANSIIntervalDays(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DayTimeIntervalType)
  }

  test("resolveDataType - ExtractANSIIntervalHours") {
    val expr = ExtractANSIIntervalHours(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DayTimeIntervalType)
  }

  test("resolveDataType - ExtractANSIIntervalMinutes") {
    val expr = ExtractANSIIntervalMinutes(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DayTimeIntervalType)
  }

  test("resolveDataType - ExtractANSIIntervalSeconds") {
    val expr = ExtractANSIIntervalSeconds(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DayTimeIntervalType)
  }

  test("resolveDataType - MultiplyInterval") {
    val expr = MultiplyInterval(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == CalendarIntervalType)
    assert(resolved.child(1).dataType == DoubleType)
  }

  test("resolveDataType - DivideInterval") {
    val expr = DivideInterval(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == CalendarIntervalType)
    assert(resolved.child(1).dataType == DoubleType)
  }

  test("resolveDataType - MakeInterval") {
    val expr = MakeInterval(
      AttributeReference("years", AnyType),
      AttributeReference("months", AnyType),
      AttributeReference("weeks", AnyType),
      AttributeReference("days", AnyType),
      AttributeReference("hours", AnyType),
      AttributeReference("mins", AnyType),
      AttributeReference("secs", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == IntegerType)
    assert(resolved.child(1).dataType == IntegerType)
    assert(resolved.child(2).dataType == IntegerType)
    assert(resolved.child(3).dataType == IntegerType)
    assert(resolved.child(4).dataType == IntegerType)
    assert(resolved.child(5).dataType == IntegerType)
    assert(resolved.child(6).dataType == DecimalType)
  }

  test("resolveDataType - MakeDTInterval") {
    val expr = MakeDTInterval(
      AttributeReference("days", AnyType),
      AttributeReference("hours", AnyType),
      AttributeReference("mins", AnyType),
      AttributeReference("secs", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == IntegerType)
    assert(resolved.child(1).dataType == IntegerType)
    assert(resolved.child(2).dataType == IntegerType)
    assert(resolved.child(3).dataType == DecimalType)
  }

  test("resolveDataType - MakeYMInterval") {
    val expr = MakeYMInterval(
      AttributeReference("years", AnyType),
      AttributeReference("months", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == IntegerType)
    assert(resolved.child(1).dataType == IntegerType)
  }

  test("resolveDataType - MultiplyYMInterval") {
    val expr = MultiplyYMInterval(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == YearMonthIntervalType)
    assert(resolved.child(1).dataType == TypeSet.Numeric)
  }

  test("resolveDataType - MultiplyDTInterval") {
    val expr = MultiplyDTInterval(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == DayTimeIntervalType)
    assert(resolved.child(1).dataType == TypeSet.Numeric)
  }

  test("resolveDataType - DivideYMInterval") {
    val expr = DivideYMInterval(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == YearMonthIntervalType)
    assert(resolved.child(1).dataType == TypeSet.Numeric)
  }

  test("resolveDataType - DivideDTInterval") {
    val expr = DivideDTInterval(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == DayTimeIntervalType)
    assert(resolved.child(1).dataType == TypeSet.Numeric)
  }

  test("resolveDataType - GetJsonObject") {
    val expr = GetJsonObject(
      AttributeReference("json", AnyType),
      AttributeReference("path", AnyType)
    )

    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == StringType)
  }

  test("resolveDataType - JsonToStructs") {
    val expr = JsonToStructs(
      StructType(Array(StructField("a", IntegerType))),
      options = Map.empty,
      child = AttributeReference("id", AnyType),
      timeZoneId = None
    )

    assert(expr.resolveDataType(AnyType).child.dataType == StringType)
  }

  test("resolveDataType - StructsToJson") {
    val expr = StructsToJson(Map.empty, AttributeReference("id", AnyType), None)

    // We can't infer number of fields.
    assert(expr.resolveDataType(AnyType).child.dataType == AnyType)
  }

  test("resolveDataType - SchemaOfJson") {
    val expr = SchemaOfJson(AttributeReference("id", AnyType), Map.empty)
    assert(expr.resolveDataType(AnyType).child.dataType == StringType)
  }

  test("resolveDataType - LengthOfJsonArray") {
    val expr = LengthOfJsonArray(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == StringType)
  }

  test("resolveDataType - JsonObjectKeys") {
    val expr = JsonObjectKeys(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == StringType)
  }

  test("resolveDataType - KllSketchToStringBigint") {
    val expr = KllSketchToStringBigint(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == BinaryType)
  }

  test("resolveDataType - KllSketchToStringFloat") {
    val expr = KllSketchToStringFloat(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == BinaryType)
  }

  test("resolveDataType - KllSketchToStringDouble") {
    val expr = KllSketchToStringDouble(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == BinaryType)
  }

  test("resolveDataType - KllSketchGetNBigint") {
    val expr = KllSketchGetNBigint(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == BinaryType)
  }

  test("resolveDataType - KllSketchGetNFloat") {
    val expr = KllSketchGetNFloat(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == BinaryType)
  }

  test("resolveDataType - KllSketchGetNDouble") {
    val expr = KllSketchGetNDouble(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == BinaryType)
  }

  test("resolveDataType - KllSketchMergeBigint") {
    val expr = KllSketchMergeBigint(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == BinaryType)
    assert(resolved.child(1).dataType == BinaryType)
  }

  test("resolveDataType - KllSketchMergeFloat") {
    val expr = KllSketchMergeFloat(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == BinaryType)
    assert(resolved.child(1).dataType == BinaryType)
  }

  test("resolveDataType - KllSketchMergeDouble") {
    val expr = KllSketchMergeDouble(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == BinaryType)
    assert(resolved.child(1).dataType == BinaryType)
  }

  test("resolveDataType - KllSketchGetQuantileBigint") {
    val expr = KllSketchGetQuantileBigint(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == TypeSet(LongType, ArrayType(LongType)))
    assert(resolved.child(0).dataType == BinaryType)
    assert(resolved.child(1).dataType == TypeSet(DoubleType, ArrayType(DoubleType)))

    resolved = expr.resolveDataType(LongType)
    assert(resolved.dataType == LongType)
    assert(resolved.child(0).dataType == BinaryType)
    assert(resolved.child(1).dataType == DoubleType)

    resolved = expr.resolveDataType(ArrayType(LongType))
    assert(resolved.dataType == ArrayType(LongType))
    assert(resolved.child(0).dataType == BinaryType)
    assert(resolved.child(1).dataType == ArrayType(DoubleType))
  }

  test("resolveDataType - KllSketchGetQuantileFloat") {
    val expr = KllSketchGetQuantileFloat(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == TypeSet(FloatType, ArrayType(FloatType)))
    assert(resolved.child(0).dataType == BinaryType)
    assert(resolved.child(1).dataType == TypeSet(DoubleType, ArrayType(DoubleType)))

    resolved = expr.resolveDataType(FloatType)
    assert(resolved.dataType == FloatType)
    assert(resolved.child(0).dataType == BinaryType)
    assert(resolved.child(1).dataType == DoubleType)

    resolved = expr.resolveDataType(ArrayType(FloatType))
    assert(resolved.dataType == ArrayType(FloatType))
    assert(resolved.child(0).dataType == BinaryType)
    assert(resolved.child(1).dataType == ArrayType(DoubleType))
  }

  test("resolveDataType - KllSketchGetQuantileDouble") {
    val expr = KllSketchGetQuantileDouble(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == TypeSet(DoubleType, ArrayType(DoubleType)))
    assert(resolved.child(0).dataType == BinaryType)
    assert(resolved.child(1).dataType == TypeSet(DoubleType, ArrayType(DoubleType)))

    resolved = expr.resolveDataType(DoubleType)
    assert(resolved.dataType == DoubleType)
    assert(resolved.child(0).dataType == BinaryType)
    assert(resolved.child(1).dataType == DoubleType)

    resolved = expr.resolveDataType(ArrayType(DoubleType))
    assert(resolved.dataType == ArrayType(DoubleType))
    assert(resolved.child(0).dataType == BinaryType)
    assert(resolved.child(1).dataType == ArrayType(DoubleType))
  }

  test("resolveDataType - KllSketchGetRankBigint") {
    val expr = KllSketchGetRankBigint(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == TypeSet(DoubleType, ArrayType(DoubleType)))
    assert(resolved.child(0).dataType == BinaryType)
    assert(resolved.child(1).dataType == TypeSet(LongType, ArrayType(LongType)))

    resolved = expr.resolveDataType(DoubleType)
    assert(resolved.dataType == DoubleType)
    assert(resolved.child(0).dataType == BinaryType)
    assert(resolved.child(1).dataType == LongType)

    resolved = expr.resolveDataType(ArrayType(DoubleType))
    assert(resolved.dataType == ArrayType(DoubleType))
    assert(resolved.child(0).dataType == BinaryType)
    assert(resolved.child(1).dataType == ArrayType(LongType))
  }

  test("resolveDataType - KllSketchGetRankFloat") {
    val expr = KllSketchGetRankFloat(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == TypeSet(DoubleType, ArrayType(DoubleType)))
    assert(resolved.child(0).dataType == BinaryType)
    assert(resolved.child(1).dataType == TypeSet(FloatType, ArrayType(FloatType)))

    resolved = expr.resolveDataType(DoubleType)
    assert(resolved.dataType == DoubleType)
    assert(resolved.child(0).dataType == BinaryType)
    assert(resolved.child(1).dataType == FloatType)

    resolved = expr.resolveDataType(ArrayType(DoubleType))
    assert(resolved.dataType == ArrayType(DoubleType))
    assert(resolved.child(0).dataType == BinaryType)
    assert(resolved.child(1).dataType == ArrayType(FloatType))
  }

  test("resolveDataType - KllSketchGetRankDouble") {
    val expr = KllSketchGetRankDouble(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == TypeSet(DoubleType, ArrayType(DoubleType)))
    assert(resolved.child(0).dataType == BinaryType)
    assert(resolved.child(1).dataType == TypeSet(DoubleType, ArrayType(DoubleType)))

    resolved = expr.resolveDataType(DoubleType)
    assert(resolved.dataType == DoubleType)
    assert(resolved.child(0).dataType == BinaryType)
    assert(resolved.child(1).dataType == DoubleType)

    resolved = expr.resolveDataType(ArrayType(DoubleType))
    assert(resolved.dataType == ArrayType(DoubleType))
    assert(resolved.child(0).dataType == BinaryType)
    assert(resolved.child(1).dataType == ArrayType(DoubleType))
  }

  test("resolveDataType - Mask") {
    val expr = Mask(
      AttributeReference("id", AnyType),
      AttributeReference("id", AnyType),
      AttributeReference("id", AnyType),
      AttributeReference("id", AnyType),
      AttributeReference("id", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == StringType)
    assert(resolved.child(2).dataType == StringType)
    assert(resolved.child(3).dataType == StringType)
    assert(resolved.child(4).dataType == StringType)
  }

  test("resolveDataType - Acos") {
    val expr = Acos(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DoubleType)
  }

  test("resolveDataType - Asin") {
    val expr = Asin(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DoubleType)
  }

  test("resolveDataType - Atan") {
    val expr = Atan(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DoubleType)
  }

  test("resolveDataType - Cbrt") {
    val expr = Cbrt(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DoubleType)
  }

  test("resolveDataType - Cos") {
    val expr = Cos(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DoubleType)
  }

  test("resolveDataType - Sec") {
    val expr = Sec(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DoubleType)
  }

  test("resolveDataType - Cosh") {
    val expr = Cosh(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DoubleType)
  }

  test("resolveDataType - Acosh") {
    val expr = Acosh(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DoubleType)
  }

  test("resolveDataType - Exp") {
    val expr = Exp(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DoubleType)
  }

  test("resolveDataType - Expm1") {
    val expr = Expm1(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DoubleType)
  }

  test("resolveDataType - Log") {
    val expr = Log(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DoubleType)
  }

  test("resolveDataType - Log2") {
    val expr = Log2(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DoubleType)
  }

  test("resolveDataType - Log10") {
    val expr = Log10(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DoubleType)
  }

  test("resolveDataType - Log1p") {
    val expr = Log1p(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DoubleType)
  }

  test("resolveDataType - Rint") {
    val expr = Rint(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DoubleType)
  }

  test("resolveDataType - Signum") {
    val expr = Signum(AttributeReference("id", AnyType))
    val resolved = expr.resolveDataType(DoubleType)
    assert(
      resolved.child.dataType ==
        TypeSet(DoubleType, YearMonthIntervalType, DayTimeIntervalType)
    )
  }

  test("resolveDataType - Sin") {
    val expr = Sin(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DoubleType)
  }

  test("resolveDataType - Csc") {
    val expr = Csc(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DoubleType)
  }

  test("resolveDataType - Sinh") {
    val expr = Sinh(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DoubleType)
  }

  test("resolveDataType - Asinh") {
    val expr = Asinh(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DoubleType)
  }

  test("resolveDataType - Sqrt") {
    val expr = Sqrt(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DoubleType)
  }

  test("resolveDataType - Tan") {
    val expr = Tan(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DoubleType)
  }

  test("resolveDataType - Cot") {
    val expr = Cot(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DoubleType)
  }

  test("resolveDataType - Tanh") {
    val expr = Tanh(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DoubleType)
  }

  test("resolveDataType - Atanh") {
    val expr = Atanh(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DoubleType)
  }

  test("resolveDataType - ToDegrees") {
    val expr = ToDegrees(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DoubleType)
  }

  test("resolveDataType - ToRadians") {
    val expr = ToRadians(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == DoubleType)
  }

  test("resolveDataType - Ceil") {
    var expr = Ceil(AttributeReference("id", AnyType))

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == TypeSet(DecimalType, LongType))
    assert(resolved.child.dataType == TypeSet(DoubleType, DecimalType, LongType))

    resolved = expr.resolveDataType(LongType)
    assert(resolved.dataType == LongType)
    assert(resolved.child.dataType == TypeSet(DoubleType, LongType))

    expr = Ceil(AttributeReference("id", TypeSet.Integral))

    resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == LongType)
    assert(resolved.child.dataType == LongType)
  }

  test("resolveDataType - Floor") {
    var expr = Floor(AttributeReference("id", AnyType))

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == TypeSet(DecimalType, LongType))
    assert(resolved.child.dataType == TypeSet(DoubleType, DecimalType, LongType))

    resolved = expr.resolveDataType(LongType)
    assert(resolved.dataType == LongType)
    assert(resolved.child.dataType == TypeSet(DoubleType, LongType))

    expr = Floor(AttributeReference("id", TypeSet.Integral))

    resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == LongType)
    assert(resolved.child.dataType == LongType)
  }

  test("resolveDataType - Factorial") {
    val expr = Factorial(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == IntegerType)
  }

  test("resolveDataType - Bin") {
    val expr = Bin(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == LongType)
  }

  test("resolveDataType - Hex") {
    val expr = Hex(AttributeReference("id", AnyType))
    val resolved = expr.resolveDataType(StringType)
    assert(resolved.child.dataType == TypeSet(LongType, BinaryType, StringType))
  }

  test("resolveDataType - Unhex") {
    val expr = Unhex(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(StringType).child.dataType == StringType)
  }

  test("resolveDataType - Atan2") {
    val expr = Atan2(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == DoubleType)
    assert(resolved.child(1).dataType == DoubleType)
  }

  test("resolveDataType - Pow") {
    val expr = Pow(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == DoubleType)
    assert(resolved.child(1).dataType == DoubleType)
  }

  test("resolveDataType - Hypot") {
    val expr = Hypot(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == DoubleType)
    assert(resolved.child(1).dataType == DoubleType)
  }

  test("resolveDataType - Logarithm") {
    val expr = Logarithm(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == DoubleType)
    assert(resolved.child(1).dataType == DoubleType)
  }

  test("resolveDataType - ShiftLeft") {
    val expr = ShiftLeft(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == TypeSet(IntegerType, LongType))
    assert(resolved.child(1).dataType == IntegerType)

    resolved = expr.resolveDataType(LongType)
    assert(resolved.child(0).dataType == LongType)
    assert(resolved.child(1).dataType == IntegerType)
  }

  test("resolveDataType - ShiftRight") {
    val expr = ShiftRight(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == TypeSet(IntegerType, LongType))
    assert(resolved.child(1).dataType == IntegerType)

    resolved = expr.resolveDataType(LongType)
    assert(resolved.child(0).dataType == LongType)
    assert(resolved.child(1).dataType == IntegerType)
  }

  test("resolveDataType - ShiftRightUnsigned") {
    val expr = ShiftRightUnsigned(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == TypeSet(IntegerType, LongType))
    assert(resolved.child(1).dataType == IntegerType)

    resolved = expr.resolveDataType(LongType)
    assert(resolved.child(0).dataType == LongType)
    assert(resolved.child(1).dataType == IntegerType)
  }

  test("resolveDataType - RoundCeil") {
    val expr = RoundCeil(AttributeReference("id", AnyType), "2")

    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == DecimalType)
    assert(resolved.child(0).dataType == DecimalType)
    assert(resolved.child(1).dataType == IntegerType)
  }

  test("resolveDataType - RoundFloor") {
    val expr = RoundFloor(AttributeReference("id", AnyType), "2")

    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == DecimalType)
    assert(resolved.child(0).dataType == DecimalType)
    assert(resolved.child(1).dataType == IntegerType)
  }

  test("resolveDataType - Round") {
    val expr = Round(AttributeReference("id", AnyType), "2")

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == TypeSet.Numeric)
    assert(resolved.child(0).dataType == TypeSet.Numeric)
    assert(resolved.child(1).dataType == IntegerType)

    resolved = expr.resolveDataType(FloatType)
    assert(resolved.dataType == FloatType)
    assert(resolved.child(0).dataType == FloatType)
    assert(resolved.child(1).dataType == IntegerType)
  }

  test("resolveDataType - BRound") {
    val expr = BRound(AttributeReference("id", AnyType), "2")

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == TypeSet.Numeric)
    assert(resolved.child(0).dataType == TypeSet.Numeric)
    assert(resolved.child(1).dataType == IntegerType)

    resolved = expr.resolveDataType(FloatType)
    assert(resolved.dataType == FloatType)
    assert(resolved.child(0).dataType == FloatType)
    assert(resolved.child(1).dataType == IntegerType)
  }

  test("resolveDataType - Conv") {
    val expr = Conv(
      AttributeReference("num", AnyType),
      AttributeReference("fromBase", AnyType),
      AttributeReference("toBase", AnyType)
    )

    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == IntegerType)
    assert(resolved.child(2).dataType == IntegerType)
  }

  test("resolveDataType - WidthBucket") {
    val expr = WidthBucket(
      AttributeReference("value", AnyType),
      AttributeReference("min", AnyType),
      AttributeReference("max", AnyType),
      AttributeReference("numBucket", AnyType)
    )

    val resolved = expr.resolveDataType(LongType)
    val expectedChildType = TypeSet(DoubleType, YearMonthIntervalType, DayTimeIntervalType)
    assert(resolved.child(0).dataType == expectedChildType)
    assert(resolved.child(1).dataType == expectedChildType)
    assert(resolved.child(2).dataType == expectedChildType)
    assert(resolved.child(3).dataType == LongType)
  }

  test("resolveDataType - RaiseError") {
    val expr = RaiseError(AttributeReference("id", AnyType), NullType)
    assert(expr.resolveDataType(AnyType).child.dataType == StringType)
  }

  test("resolveDataType - AesEncrypt") {
    val expr = AesEncrypt(
      AttributeReference("input", AnyType),
      AttributeReference("key", AnyType),
      AttributeReference("mode", AnyType),
      AttributeReference("padding", AnyType),
      AttributeReference("iv", AnyType),
      AttributeReference("aad", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == BinaryType)
    assert(resolved.child(1).dataType == BinaryType)
    assert(resolved.child(2).dataType == StringType)
    assert(resolved.child(3).dataType == StringType)
    assert(resolved.child(4).dataType == BinaryType)
    assert(resolved.child(5).dataType == BinaryType)
  }

  test("resolveDataType - AesDecrypt") {
    val expr = AesDecrypt(
      AttributeReference("input", AnyType),
      AttributeReference("key", AnyType),
      AttributeReference("mode", AnyType),
      AttributeReference("padding", AnyType),
      AttributeReference("aad", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == BinaryType)
    assert(resolved.child(1).dataType == BinaryType)
    assert(resolved.child(2).dataType == StringType)
    assert(resolved.child(3).dataType == StringType)
    assert(resolved.child(4).dataType == BinaryType)
  }

  test("resolveDataType - CallMethodViaReflection") {
    val expr = CallMethodViaReflection(
      IndexedSeq(
        "java.util.UUID",
        "fromString",
        AttributeReference("id", AnyType)
      )
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == StringType)
    assert(
      resolved.child(2).dataType == TypeSet(
        BooleanType,
        ByteType,
        ShortType,
        IntegerType,
        LongType,
        FloatType,
        DoubleType,
        StringType
      )
    )
  }

  test("resolveDataType - Coalesce") {
    val expr = Coalesce(
      IndexedSeq(
        AttributeReference("id", AnyType),
        AttributeReference("value", TypeSet.Numeric)
      )
    )

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == TypeSet.Numeric)
    assert(resolved.child(0).dataType == TypeSet.Numeric)
    assert(resolved.child(1).dataType == TypeSet.Numeric)

    resolved = expr.resolveDataType(FloatType)
    assert(resolved.dataType == FloatType)
    assert(resolved.child(0).dataType == FloatType)
    assert(resolved.child(1).dataType == FloatType)
  }

  test("resolveDataType - IsNaN") {
    val expr = IsNaN(AttributeReference("id", AnyType))
    val resolved = expr.resolveDataType(BooleanType)
    assert(resolved.child.dataType == TypeSet(FloatType, DoubleType))
  }

  test("resolveDataType - NaNvl") {
    val expr = NaNvl(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == TypeSet(FloatType, DoubleType))
    assert(resolved.child(1).dataType == TypeSet(FloatType, DoubleType))

    resolved = expr.resolveDataType(FloatType)
    assert(resolved.child(0).dataType == FloatType)
    assert(resolved.child(1).dataType == FloatType)
  }

  test("resolveDataType - IsNull") {
    val expr = IsNull(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(BooleanType).child.dataType == AnyType)
  }

  test("resolveDataType - IsNotNull") {
    val expr = IsNotNull(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(BooleanType).child.dataType == AnyType)
  }

  test("resolveDataType - AtLeastNNonNulls") {
    val expr = AtLeastNNonNulls(
      n = 1,
      IndexedSeq(
        AttributeReference("col1", AnyType),
        AttributeReference("col2", AnyType)
      )
    )
    val resolved = expr.resolveDataType(BooleanType)
    assert(resolved.child(0).dataType == AnyType)
    assert(resolved.child(1).dataType == AnyType)
  }

  test("resolveDataType - ToNumber") {
    val expr = ToNumber(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )

    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == StringType)
  }

  test("resolveDataType - TryToNumber") {
    val expr = TryToNumber(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )

    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == StringType)
  }

  test("resolveDataType - ToCharacter") {
    val expr = ToCharacter(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )

    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == DecimalType)
    assert(resolved.child(1).dataType == StringType)
  }

  test("resolveDataType - StaticInvoke") {
    val expr = StaticInvoke(
      "java.lang.Long",
      ObjectType("java.lang.Long"),
      "valueOf",
      IndexedSeq(AttributeReference("id", AnyType))
    )
    assert(expr.resolveDataType(LongType).dataType == TypeSet.Empty)
  }

  test("resolveDataType - Invoke") {
    val expr = Invoke(BoundReference(0, ObjectType("scala.Tuple5")), "_3")
    assert(expr.dataType == AnyType)
    assert(expr.resolveDataType(LongType).dataType == LongType)
  }

  test("resolveDataType - NewInstance") {
    val expr = NewInstance("com.xonai.spark.sql.parser.SimpleUDT")
    val objectType = ObjectType("com.xonai.spark.sql.parser.SimpleUDT")
    assert(expr.dataType == AnyType)
    assert(expr.resolveDataType(objectType).dataType == objectType)
  }

  test("resolveDataType - UnwrapOption") {
    val expr = UnwrapOption(IntegerType, AttributeReference("id", AnyType))
    assert(expr.resolveDataType(LongType).dataType == TypeSet.Empty)
  }

  test("resolveDataType - WrapOption") {
    val expr = WrapOption(AttributeReference("id", AnyType), LongType)
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child.dataType == LongType)
  }

  test("resolveDataType - LambdaVariable") {
    val expr = LambdaVariable("MapObject", ObjectType("java.lang.Object"))
    assert(expr.resolveDataType(LongType).dataType == TypeSet.Empty)
  }

  test("resolveDataType - CatalystToExternalMap") {
    val expr =
      CatalystToExternalMap(
        LambdaVariable("CatalystToExternalMap_key", LongType),
        LambdaVariable("CatalystToExternalMap_key", LongType),
        LambdaVariable("CatalystToExternalMap_value", IntegerType),
        LambdaVariable("CatalystToExternalMap_value", IntegerType),
        AttributeReference("id", AnyType),
        "scala.collection.immutable.Map"
      )

    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(4).dataType == MapType(LongType, IntegerType))
  }

  test("resolveDataType - CreateExternalRow") {
    val expr =
      CreateExternalRow(
        IndexedSeq(
          AttributeReference("value1", AnyType),
          AttributeReference("value2", AnyType)
        ),
        StructType(
          Array(
            StructField("value1", LongType),
            StructField("value2", IntegerType)
          )
        )
      )

    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == LongType)
    assert(resolved.child(1).dataType == IntegerType)
  }

  test("resolveDataType - AssertNotNull") {
    val expr = AssertNotNull(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == AnyType)
    assert(expr.resolveDataType(LongType).child.dataType == LongType)
  }

  test("resolveDataType - NormalizeNaNAndZero") {
    val expr = NormalizeNaNAndZero(AttributeReference("id", AnyType))

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.child.dataType == TypeSet(FloatType, DoubleType))

    resolved = expr.resolveDataType(FloatType)
    assert(resolved.child.dataType == FloatType)
  }

  test("resolveDataType - ProtobufDataToCatalyst") {
    val expr = ProtobufDataToCatalyst(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == BinaryType)
  }

  test("resolveDataType - CatalystDataToProtobuf") {
    val expr = CatalystDataToProtobuf(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(BinaryType).child.dataType == AnyType)
  }

  test("resolveDataType - Rand") {
    val expr = Rand("123132")
    assert(expr.resolveDataType(DoubleType).child.dataType == TypeSet(IntegerType, LongType))
  }

  test("resolveDataType - Randn") {
    val expr = Randn("123132")
    assert(expr.resolveDataType(DoubleType).child.dataType == TypeSet(IntegerType, LongType))
  }

  test("resolveDataType - RandStr") {
    val expr = RandStr("20", "-7327388")
    val resolved = expr.resolveDataType(StringType)
    assert(resolved.child(0).dataType == IntegerType)
    assert(resolved.child(1).dataType == TypeSet(IntegerType, LongType))
  }

  test("resolveDataType - RLike") {
    val expr = RLike(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == StringType)
  }

  test("resolveDataType - LikeAll") {
    val expr = LikeAll(AttributeReference("id", AnyType), Seq.empty)
    assert(expr.resolveDataType(AnyType).child.dataType == StringType)
  }

  test("resolveDataType - NotLikeAll") {
    val expr = NotLikeAll(AttributeReference("id", AnyType), Seq.empty)
    assert(expr.resolveDataType(AnyType).child.dataType == StringType)
  }

  test("resolveDataType - LikeAny") {
    val expr = LikeAny(AttributeReference("id", AnyType), Seq.empty)
    assert(expr.resolveDataType(AnyType).child.dataType == StringType)
  }

  test("resolveDataType - NotLikeAny") {
    val expr = NotLikeAny(AttributeReference("id", AnyType), Seq.empty)
    assert(expr.resolveDataType(AnyType).child.dataType == StringType)
  }

  test("resolveDataType - StringSplit") {
    val expr = StringSplit(
      AttributeReference("str", AnyType),
      AttributeReference("regex", AnyType),
      AttributeReference("limit", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == StringType)
    assert(resolved.child(2).dataType == IntegerType)
  }

  test("resolveDataType - RegExpReplace") {
    val expr = RegExpReplace(
      AttributeReference("subject", AnyType),
      AttributeReference("regex", AnyType),
      AttributeReference("rep", AnyType),
      AttributeReference("pos", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == StringType)
    assert(resolved.child(2).dataType == StringType)
    assert(resolved.child(3).dataType == IntegerType)
  }

  test("resolveDataType - RegExpExtract") {
    val expr = RegExpExtract(
      AttributeReference("subject", AnyType),
      AttributeReference("regex", AnyType),
      AttributeReference("idx", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == StringType)
    assert(resolved.child(2).dataType == IntegerType)
  }

  test("resolveDataType - RegExpExtractAll") {
    val expr = RegExpExtractAll(
      AttributeReference("subject", AnyType),
      AttributeReference("regex", AnyType),
      AttributeReference("idx", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == StringType)
    assert(resolved.child(2).dataType == IntegerType)
  }

  test("resolveDataType - RegExpInStr") {
    val expr = RegExpInStr(
      AttributeReference("subject", AnyType),
      AttributeReference("regex", AnyType),
      AttributeReference("idx", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == StringType)
    assert(resolved.child(2).dataType == IntegerType)
  }

  test("resolveDataType - ST_AsBinary") {
    val expr = ST_AsBinary(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == TypeSet(GeographyType, GeometryType))
  }

  test("resolveDataType - ST_GeogFromWKB") {
    val expr = ST_GeogFromWKB(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == BinaryType)
  }

  test("resolveDataType - ST_GeomFromWKB") {
    val expr = ST_GeomFromWKB(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == BinaryType)
  }

  test("resolveDataType - ST_Srid") {
    val expr = ST_Srid(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == TypeSet(GeographyType, GeometryType))
  }

  test("resolveDataType - ST_SetSrid") {
    val expr = ST_SetSrid(
      AttributeReference("geo", AnyType),
      AttributeReference("srid", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == TypeSet(GeographyType, GeometryType))
    assert(resolved.child(1).dataType == IntegerType)
  }

  test("resolveDataType - ConcatWs") {
    val expr = ConcatWs(
      IndexedSeq(
        AttributeReference("sep", AnyType),
        AttributeReference("id", AnyType),
        "str"
      )
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == TypeSet(ArrayType(StringType), StringType))
    assert(resolved.child(2).dataType == StringType)
  }

  test("resolveDataType - Elt") {
    var expr = Elt(
      IndexedSeq(
        AttributeReference("index", AnyType),
        AttributeReference("input1", AnyType),
        AttributeReference("input2", AnyType)
      )
    )

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == IntegerType)
    assert(resolved.child(1).dataType == TypeSet(BinaryType, StringType))
    assert(resolved.child(2).dataType == TypeSet(BinaryType, StringType))

    resolved = expr.resolveDataType(StringType)
    assert(resolved.child(0).dataType == IntegerType)
    assert(resolved.child(1).dataType == StringType)
    assert(resolved.child(2).dataType == StringType)

    expr = Elt(
      IndexedSeq(
        AttributeReference("index", AnyType),
        AttributeReference("input1", StringType),
        AttributeReference("input2", AnyType)
      )
    )
    resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == IntegerType)
    assert(resolved.child(1).dataType == StringType)
    assert(resolved.child(2).dataType == StringType)

    expr = Elt(
      IndexedSeq(
        AttributeReference("index", AnyType),
        AttributeReference("input1", AnyType),
        AttributeReference("input2", BinaryType)
      )
    )
    resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == IntegerType)
    assert(resolved.child(1).dataType == BinaryType)
    assert(resolved.child(2).dataType == BinaryType)
  }

  test("resolveDataType - Upper") {
    val expr = Upper(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == StringType)
  }

  test("resolveDataType - Lower") {
    val expr = Lower(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == StringType)
  }

  test("resolveDataType - StringReplace") {
    val expr = StringReplace(
      AttributeReference("src", AnyType),
      AttributeReference("search", AnyType),
      AttributeReference("replace", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == StringType)
    assert(resolved.child(2).dataType == StringType)
  }

  test("resolveDataType - Overlay") {
    val expr = Overlay(
      AttributeReference("input", AnyType),
      AttributeReference("replace", AnyType),
      AttributeReference("pos", AnyType),
      AttributeReference("len", AnyType)
    )

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == TypeSet(StringType, BinaryType))
    assert(resolved.child(1).dataType == TypeSet(StringType, BinaryType))
    assert(resolved.child(2).dataType == IntegerType)
    assert(resolved.child(3).dataType == IntegerType)

    resolved = expr.resolveDataType(StringType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == StringType)
    assert(resolved.child(2).dataType == IntegerType)
    assert(resolved.child(3).dataType == IntegerType)
  }

  test("resolveDataType - StringTranslate") {
    val expr = StringTranslate(
      AttributeReference("src", AnyType),
      AttributeReference("matching", AnyType),
      AttributeReference("replace", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == StringType)
    assert(resolved.child(2).dataType == StringType)
  }

  test("resolveDataType - FindInSet") {
    val expr = FindInSet(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == StringType)
  }

  test("resolveDataType - StringTrim") {
    var expr = StringTrim(AttributeReference("src", AnyType), None)
    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)

    expr = StringTrim(
      AttributeReference("src", AnyType),
      Some(AttributeReference("trim", AnyType))
    )
    resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == StringType)
  }

  test("resolveDataType - StringTrimLeft") {
    var expr = StringTrimLeft(AttributeReference("src", AnyType), None)
    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)

    expr = StringTrimLeft(
      AttributeReference("src", AnyType),
      Some(AttributeReference("trim", AnyType))
    )
    resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == StringType)
  }

  test("resolveDataType - StringTrimRight") {
    var expr = StringTrimRight(AttributeReference("src", AnyType), None)
    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)

    expr = StringTrimRight(
      AttributeReference("src", AnyType),
      Some(AttributeReference("trim", AnyType))
    )
    resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == StringType)
  }

  test("resolveDataType - StringInstr") {
    val expr = StringInstr(
      AttributeReference("str", AnyType),
      AttributeReference("substr", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == StringType)
  }

  test("resolveDataType - SubstringIndex") {
    val expr = SubstringIndex(
      AttributeReference("str", AnyType),
      AttributeReference("delim", AnyType),
      AttributeReference("count", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == StringType)
    assert(resolved.child(2).dataType == IntegerType)
  }

  test("resolveDataType - StringLocate") {
    val expr = StringLocate(
      AttributeReference("substr", AnyType),
      AttributeReference("str", AnyType),
      AttributeReference("start", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == StringType)
    assert(resolved.child(2).dataType == IntegerType)
  }

  test("resolveDataType - StringLPad") {
    val expr = StringLPad(
      AttributeReference("str", AnyType),
      AttributeReference("len", AnyType),
      AttributeReference("pad", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == IntegerType)
    assert(resolved.child(2).dataType == StringType)
  }

  test("resolveDataType - StringRPad") {
    val expr = StringRPad(
      AttributeReference("str", AnyType),
      AttributeReference("len", AnyType),
      AttributeReference("pad", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == IntegerType)
    assert(resolved.child(2).dataType == StringType)
  }

  test("resolveDataType - BinaryPad") {
    val expr = BinaryPad(
      "lpad",
      AttributeReference("str", AnyType),
      AttributeReference("len", AnyType),
      AttributeReference("pad", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == BinaryType)
    assert(resolved.child(1).dataType == IntegerType)
    assert(resolved.child(2).dataType == BinaryType)
  }

  test("resolveDataType - FormatString") {
    val expr = FormatString(
      IndexedSeq(
        "Hello %d %s",
        AttributeReference("val1", AnyType),
        AttributeReference("val2", IntegerType)
      )
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == AnyType)
    assert(resolved.child(2).dataType == IntegerType)
  }

  test("resolveDataType - InitCap") {
    val expr = Upper(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == StringType)
  }

  test("resolveDataType - StringRepeat") {
    val expr = StringRepeat(
      AttributeReference("str", AnyType),
      AttributeReference("times", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == IntegerType)
  }

  test("resolveDataType - StringSpace") {
    val expr = StringSpace(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == IntegerType)
  }

  test("resolveDataType - Substring") {
    val expr = Substring(
      AttributeReference("str", AnyType),
      AttributeReference("pos", AnyType),
      AttributeReference("len", AnyType)
    )

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == TypeSet(StringType, BinaryType))
    assert(resolved.child(1).dataType == IntegerType)
    assert(resolved.child(2).dataType == IntegerType)

    resolved = expr.resolveDataType(StringType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == IntegerType)
    assert(resolved.child(2).dataType == IntegerType)
  }

  test("resolveDataType - Length") {
    val expr = Length(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == TypeSet(BinaryType, StringType))
  }

  test("resolveDataType - BitLength") {
    val expr = BitLength(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == TypeSet(BinaryType, StringType))
  }

  test("resolveDataType - OctetLength") {
    val expr = OctetLength(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == TypeSet(BinaryType, StringType))
  }

  test("resolveDataType - Levenshtein") {
    var expr = Levenshtein(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType),
      None
    )
    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == StringType)

    expr = Levenshtein(
      AttributeReference("left", AnyType),
      AttributeReference("right", AnyType),
      Some(AttributeReference("threshold", AnyType))
    )
    resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == StringType)
    assert(resolved.child(2).dataType == IntegerType)
  }

  test("resolveDataType - SoundEx") {
    val expr = SoundEx(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == StringType)
  }

  test("resolveDataType - Ascii") {
    val expr = Ascii(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == StringType)
  }

  test("resolveDataType - Chr") {
    val expr = Chr(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == LongType)
  }

  test("resolveDataType - Base64") {
    val expr = Base64(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == BinaryType)
  }

  test("resolveDataType - UnBase64") {
    val expr = UnBase64(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == StringType)
  }

  test("resolveDataType - StringDecode") {
    val expr = StringDecode(
      AttributeReference("bin", AnyType),
      AttributeReference("charset", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == BinaryType)
    assert(resolved.child(1).dataType == StringType)
  }

  test("resolveDataType - Encode") {
    val expr = Encode(
      AttributeReference("value", AnyType),
      AttributeReference("charset", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == StringType)
  }

  test("resolveDataType - FormatNumber") {
    val expr = FormatNumber(
      AttributeReference("x", AnyType),
      AttributeReference("d", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == TypeSet.Numeric)
    assert(resolved.child(1).dataType == TypeSet(IntegerType, StringType))
  }

  test("resolveDataType - Sentences") {
    val expr = Sentences(
      AttributeReference("str", AnyType),
      AttributeReference("language", AnyType),
      AttributeReference("country", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == StringType)
    assert(resolved.child(2).dataType == StringType)
  }

  test("resolveDataType - StringSplitSQL") {
    val expr = StringSplitSQL(
      AttributeReference("str", AnyType),
      AttributeReference("delimiter", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == StringType)
  }

  test("resolveDataType - Luhncheck") {
    val expr = Luhncheck(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == StringType)
  }

  test("resolveDataType - IsValidUTF8") {
    val expr = IsValidUTF8(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == StringType)
  }

  test("resolveDataType - MakeValidUTF8") {
    val expr = MakeValidUTF8(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == StringType)
  }

  test("resolveDataType - ValidateUTF8") {
    val expr = ValidateUTF8(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == StringType)
  }

  test("resolveDataType - TryValidateUTF8") {
    val expr = TryValidateUTF8(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == StringType)
  }

  test("resolveDataType - Quote") {
    val expr = Quote(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == StringType)
  }

  test("resolveDataType - ThetaSketchEstimate") {
    val expr = ThetaSketchEstimate(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == BinaryType)
  }

  test("resolveDataType - ThetaUnion") {
    val expr = ThetaUnion(
      AttributeReference("first", AnyType),
      AttributeReference("second", AnyType),
      AttributeReference("third", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == BinaryType)
    assert(resolved.child(1).dataType == BinaryType)
    assert(resolved.child(2).dataType == IntegerType)
  }

  test("resolveDataType - ThetaDifference") {
    val expr = ThetaDifference(
      AttributeReference("first", AnyType),
      AttributeReference("second", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == BinaryType)
    assert(resolved.child(1).dataType == BinaryType)
  }

  test("resolveDataType - ThetaIntersection") {
    val expr = ThetaIntersection(
      AttributeReference("first", AnyType),
      AttributeReference("second", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == BinaryType)
    assert(resolved.child(1).dataType == BinaryType)
  }

  test("resolveDataType - ToTime") {
    val expr = ToTime(
      AttributeReference("str", AnyType),
      Some(AttributeReference("format", AnyType))
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == StringType)
  }

  test("resolveDataType - MakeTime") {
    val expr = MakeTime(
      AttributeReference("hours", AnyType),
      AttributeReference("minutes", AnyType),
      AttributeReference("secsAndMicros", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == IntegerType)
    assert(resolved.child(1).dataType == IntegerType)
    assert(resolved.child(2).dataType == DecimalType)
  }

  test("resolveDataType - TimeDiff") {
    val expr = TimeDiff(
      AttributeReference("unit", AnyType),
      AttributeReference("start", AnyType),
      AttributeReference("end", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == TimeType)
    assert(resolved.child(2).dataType == TimeType)
  }

  test("resolveDataType - TimeTrunc") {
    val expr = TimeTrunc(
      AttributeReference("unit", AnyType),
      AttributeReference("time", AnyType)
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == TimeType)
  }

  test("resolveDataType - PreciseTimestampConversion") {
    val expr =
      PreciseTimestampConversion(AttributeReference("id", AnyType), TimestampType, LongType)
    assert(expr.resolveDataType(AnyType).child.dataType == TimestampType)
  }

  test("resolveDataType - TryEval") {
    val expr = TryEval(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(LongType).child.dataType == LongType)
  }

  test("resolveDataType - UrlEncode") {
    val expr = UrlEncode(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == StringType)
  }

  test("resolveDataType - UrlDecode") {
    val expr = UrlDecode(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == StringType)
  }

  test("resolveDataType - ParseUrl") {
    var expr = ParseUrl(
      IndexedSeq(
        AttributeReference("url", AnyType),
        AttributeReference("part", AnyType)
      )
    )
    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == StringType)

    expr = ParseUrl(
      IndexedSeq(
        AttributeReference("url", AnyType),
        AttributeReference("part", AnyType),
        AttributeReference("key", AnyType)
      )
    )
    resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == StringType)
    assert(resolved.child(1).dataType == StringType)
    assert(resolved.child(2).dataType == StringType)
  }

  test("resolveDataType - ParseJson") {
    val expr = ParseJson(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == StringType)
  }

  test("resolveDataType - IsVariantNull") {
    val expr = IsVariantNull(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == VariantType)
  }

  test("resolveDataType - ToVariantObject") {
    val expr = ToVariantObject(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(VariantType).child.dataType == AnyType)
  }

  test("resolveDataType - VariantGet") {
    val expr = VariantGet(
      AttributeReference("child", AnyType),
      AttributeReference("path", AnyType),
      LongType,
      failOnError = false
    )
    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.child(0).dataType == VariantType)
    assert(resolved.child(1).dataType == StringType)
  }

  test("resolveDataType - SchemaOfVariant") {
    val expr = SchemaOfVariant(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == VariantType)
  }

  test("resolveDataType - WindowExpression") {
    val expr = WindowExpression(
      First(AttributeReference("id", AnyType), ignoreNulls = true),
      WindowSpecDefinition(
        IndexedSeq(
          AttributeReference("v", AnyType)
        ),
        IndexedSeq(
          SortOrder(AttributeReference("order", AnyType), Ascending, NullsFirst)
        ),
        SpecifiedWindowFrame(RangeFrame, UnboundedPreceding, CurrentRow)
      )
    )

    var resolved = expr.resolveDataType(AnyType).asInstanceOf[WindowExpression]
    assert(resolved.windowFunction.dataType == AnyType)
    assert(resolved.windowSpec.partitionSpec.head.dataType == AnyType)
    assert(resolved.windowSpec.orderSpec.head.dataType == AnyType)

    resolved = expr.resolveDataType(LongType).asInstanceOf[WindowExpression]
    assert(resolved.windowFunction.dataType == LongType)
    assert(resolved.windowSpec.partitionSpec.head.dataType == AnyType)
    assert(resolved.windowSpec.orderSpec.head.dataType == AnyType)
  }

  test("resolveDataType - Rank") {
    val expr = Rank(IndexedSeq(AttributeReference("id", AnyType)))

    val resolved = expr.resolveDataType(IntegerType)
    assert(resolved.dataType == IntegerType)
    assert(resolved.child.dataType == AnyType)
  }

  test("resolveDataType - DenseRank") {
    val expr = DenseRank(IndexedSeq(AttributeReference("id", AnyType)))

    val resolved = expr.resolveDataType(IntegerType)
    assert(resolved.dataType == IntegerType)
    assert(resolved.child.dataType == AnyType)
  }

  test("resolveDataType - PercentRank") {
    val expr = PercentRank(IndexedSeq(AttributeReference("id", AnyType)))

    val resolved = expr.resolveDataType(DoubleType)
    assert(resolved.dataType == DoubleType)
    assert(resolved.child.dataType == AnyType)
  }

  test("resolveDataType - NTile") {
    val expr = NTile(AttributeReference("id", AnyType))

    val resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == IntegerType)
    assert(resolved.child.dataType == IntegerType)
  }

  test("resolveDataType - NthValue") {
    val expr = NthValue(
      AttributeReference("id", AnyType),
      AttributeReference("off", AnyType),
      ignoreNulls = false
    )

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == AnyType)
    assert(resolved.child(0).dataType == AnyType)
    assert(resolved.child(1).dataType == IntegerType)

    resolved = expr.resolveDataType(LongType)
    assert(resolved.dataType == LongType)
    assert(resolved.child(0).dataType == LongType)
    assert(resolved.child(1).dataType == IntegerType)
  }

  test("resolveDataType - Lead/Lag") {
    Seq(
      Lead.apply _,
      Lag.apply _
    ).foreach { builder =>
      var expr = builder(
        AttributeReference("id", AnyType),
        AttributeReference("off", AnyType),
        "null"
      )

      var resolved = expr.resolveDataType(AnyType)
      assert(resolved.dataType == AnyType)
      assert(resolved.child(0).dataType == AnyType)
      assert(resolved.child(1).dataType == IntegerType)
      assert(resolved.child(2).dataType == AnyType)

      resolved = expr.resolveDataType(LongType)
      assert(resolved.dataType == LongType)
      assert(resolved.child(0).dataType == LongType)
      assert(resolved.child(1).dataType == IntegerType)
      assert(resolved.child(2).dataType == LongType)

      expr = builder(
        AttributeReference("id", AnyType),
        AttributeReference("off", AnyType),
        "1"
      )
      resolved = expr.resolveDataType(AnyType)
      assert(resolved.dataType == TypeSet.Integral + DecimalType + StringType)

      expr = builder(
        AttributeReference("id", LongType),
        AttributeReference("off", AnyType),
        "1"
      )
      resolved = expr.resolveDataType(AnyType)
      assert(resolved.dataType == LongType)
    }
  }

  test("resolveDataType - NullIndex") {
    val expr = NullIndex(AttributeReference("id", AnyType))

    val resolved = expr.resolveDataType(IntegerType)
    assert(resolved.dataType == IntegerType)
    assert(resolved.child.dataType == AnyType)
  }

  test("resolveDataType - EWM") {
    val expr = EWM(AttributeReference("id", AnyType), 0.5, ignoreNA = false)

    val resolved = expr.resolveDataType(DoubleType)
    assert(resolved.dataType == DoubleType)
    assert(resolved.child.dataType == AnyType)
  }

  test("resolveDataType - LastNonNull") {
    val expr = LastNonNull(AttributeReference("id", AnyType))

    var resolved = expr.resolveDataType(AnyType)
    assert(resolved.dataType == AnyType)
    assert(resolved.child.dataType == AnyType)

    resolved = expr.resolveDataType(LongType)
    assert(resolved.dataType == LongType)
    assert(resolved.child.dataType == LongType)
  }

  test("resolveDataType - XmlToStructs") {
    val expr = XmlToStructs(
      StructType(Array(StructField("a", IntegerType))),
      options = Map.empty,
      child = AttributeReference("id", AnyType),
      timeZoneId = None
    )

    assert(expr.resolveDataType(AnyType).child.dataType == StringType)
  }

  test("resolveDataType - SchemaOfXml") {
    val expr = SchemaOfXml(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(AnyType).child.dataType == StringType)
  }

  test("resolveDataType - StructsToXml") {
    val expr = StructsToXml(AttributeReference("id", AnyType))
    assert(expr.resolveDataType(StringType).child.dataType == AnyType)
  }

  Seq(
    ("XPathBoolean", XPathBoolean(_, _)),
    ("XPathShort", XPathShort(_, _)),
    ("XPathInt", XPathInt(_, _)),
    ("XPathLong", XPathLong(_, _)),
    ("XPathFloat", XPathFloat(_, _)),
    ("XPathDouble", XPathDouble(_, _)),
    ("XPathString", XPathString(_, _)),
    ("XPathList", XPathList(_, _))
  ).foreach { case (expressionName, build) =>
    test(s"resolveDataType - $expressionName") {
      val expr = build(
        AttributeReference("xml", AnyType),
        AttributeReference("path", AnyType)
      )
      val resolved = expr.resolveDataType(AnyType)
      assert(resolved.child(0).dataType == StringType)
      assert(resolved.child(1).dataType == StringType)
    }
  }
}
