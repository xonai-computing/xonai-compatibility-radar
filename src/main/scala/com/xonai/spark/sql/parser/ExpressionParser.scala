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

import com.xonai.spark.sql.parser.expressions._
import com.xonai.spark.sql.parser.plans.BaseSubqueryExec
import com.xonai.spark.sql.parser.trees.UnknownNode
import com.xonai.spark.sql.parser.types.{AnyType, ArrayType, BinaryType, BooleanType, CalendarIntervalType, DataType, DateType, DayTimeIntervalType, DecimalType, DoubleType, IntegerType, MapType, ObjectType, StringType, StructType, TimestampNTZType, TimestampType, TypeSet, UnknownType, YearMonthIntervalType}

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/**
 * Attempts to reconstruct an expression tree from a string produced using
 * [[org.apache.spark.sql.catalyst.expressions.Expression.toString]]. Given the syntax can be
 * ambiguous the output may not match the expression that produced the input.
 *
 * The main building blocks are the following:
 *
 * {{{
 * ... AS <name>#<number>
 * <name>#<number>
 * CASE WHEN ... THEN ... ELSE ... END
 * (... <op> ...)
 * <function>(...)
 * NOT ...
 * -...
 * ~...
 * if (...) ... else ...
 * ... IN (...)
 * ... IN ...
 * ... + ...
 * ... LIKE ...
 * ... INSET <literal>, ..., <literal>
 * ...[...]
 * ...<dot>...
 * Subquery <name>#<number>
 * Subquery <name>#<number>, [id=#<number>]
 * ReusedSubquery <subquery>
 * <literal>
 * }}}
 *
 * The block that is more prone to ambiguity is `if (...) ... else ...` as the `else` statement can
 * be of any data type. The other open-ended blocks require more restrict data types and are
 * unlikely to cause ambiguity.
 *
 * @param subqueries Function that provides subquery plans from the `simpleString` of a
 *                   [[org.apache.spark.sql.execution.SubqueryExec]].
 */
class ExpressionParser(subqueries: String => Option[BaseSubqueryExec]) extends Parser {

  private var inputById: Map[Long, Attribute] = Map.empty
  private var inputByName: Map[String, Attribute] = Map.empty

  private val binaryOperatorParsers =
    Map(
      ("*", parseMultiply _),
      ("/", parseDivide _),
      ("%", binaryOperatorParser(Remainder)),
      ("div", binaryOperatorParser(IntegralDivide)),
      ("+", binaryOperatorParser(Add)),
      ("-", parseSubtract _),
      ("^", binaryOperatorParser(BitwiseXor)),
      ("&", binaryOperatorParser(BitwiseAnd)),
      ("|", binaryOperatorParser(BitwiseOr)),
      ("OR", binaryOperatorParser(Or)),
      ("AND", binaryOperatorParser(And)),
      ("<=", binaryOperatorParser(LessThanOrEqual)),
      ("<", binaryOperatorParser(LessThan)),
      (">=", binaryOperatorParser(GreaterThanOrEqual)),
      (">", binaryOperatorParser(GreaterThan)),
      ("=", binaryOperatorParser(EqualTo)),
      ("<=>", binaryOperatorParser(EqualNullSafe)),
      ("<<", binaryOperatorParser(ShiftLeft)),
      (">>", binaryOperatorParser(ShiftRight)),
      (">>>", binaryOperatorParser(ShiftRightUnsigned))
    )

  private val aggregateParsers =
    Map(
      "count" -> variableLengthParser(new Count(_)),
      "sum" -> unaryParser(Sum(_, isTry = false)),
      "try_sum" -> unaryParser(Sum(_, isTry = true)),
      "avg" -> unaryParser(Average(_, isTry = false)),
      "try_avg" -> unaryParser(Average(_, isTry = true)),
      "mean" -> unaryParser(Average(_, isTry = false)),
      "min" -> unaryParser(Min),
      "max" -> unaryParser(Max),
      "first" -> binaryParser(First.apply),
      "first_value" -> binaryParser(First.apply),
      "last" -> binaryParser(Last.apply),
      "last_value" -> binaryParser(Last.apply),
      "collect_list" -> unaryParser(CollectList),
      "collect_set" -> unaryParser(CollectSet),
      "mode" -> unaryParser(Mode),
      "product" -> unaryParser(Product),
      "corr" -> binaryParser(Corr),
      "covar_pop" -> binaryParser(CovPopulation),
      "covar_samp" -> binaryParser(CovSample),
      "stddev_pop" -> unaryParser(StddevPop),
      "stddev_samp" -> unaryParser(StddevSamp),
      "stddev" -> unaryParser(StddevSamp),
      "std" -> unaryParser(StddevSamp),
      "var_pop" -> unaryParser(VariancePop),
      "var_samp" -> unaryParser(VarianceSamp),
      "variance" -> unaryParser(VarianceSamp),
      "skewness" -> unaryParser(Skewness),
      "kurtosis" -> unaryParser(Kurtosis),
      "max_by" -> binaryParser(MaxBy),
      "min_by" -> binaryParser(MinBy),
      "percentile" -> parsePercentile _,
      "percentile_disc" -> parsePercentileDisc _,
      "approx_percentile" -> parseApproximatePercentile _,
      "percentile_approx" -> parseApproximatePercentile _,
      "approx_count_distinct" -> binaryParser(HyperLogLogPlusPlus.apply),
      "every" -> parseBoolAnd _,
      "any" -> parseBoolOr _,
      "some" -> parseBoolOr _,
      "bit_and" -> unaryParser(BitAndAgg),
      "bit_or" -> unaryParser(BitOrAgg),
      "bit_xor" -> unaryParser(BitXorAgg),
      "bloom_filter_agg" -> ternaryParser(BloomFilterAggregate),
      "count_min_sketch" -> quaternaryParser(CountMinSketchAgg),
      "histogram_numeric" -> parseHistogramNumeric _,
      "pivotfirst" -> parsePivotFirst _,
      "hll_sketch_agg" -> binaryParser(HllSketchAgg),
      "hll_union_agg" -> binaryParser(HllUnionAgg),
      "regrreplacement" -> unaryParser(RegrReplacement),
      "regr_r2" -> binaryParser(RegrR2),
      "regr_sxy" -> binaryParser(RegrSXY),
      "regr_slope" -> binaryParser(RegrSlope),
      "regr_intercept" -> binaryParser(RegrIntercept),
      "hll_init_agg" -> parseHyperLogLogInitSimpleAgg _,
      "bitmap_construct_agg" -> unaryParser(BitmapConstructAgg),
      "bitmap_or_agg" -> unaryParser(BitmapOrAgg),
      "bitmap_and_agg" -> unaryParser(BitmapAndAgg),
      "approx_top_k" -> ternaryParser(ApproxTopK),
      "approx_top_k_accumulate" -> binaryParser(ApproxTopKAccumulate),
      "approx_top_k_combine" -> binaryParser(ApproxTopKCombine),
      "listagg" -> parseListAgg _,
      "string_agg" -> parseListAgg _,
      "theta_sketch_agg" -> binaryParser(ThetaSketchAgg),
      "theta_union_agg" -> binaryParser(ThetaUnionAgg),
      "theta_intersection_agg" -> unaryParser(ThetaIntersectionAgg),
      "kll_sketch_agg_bigint" -> unaryOptionParser(KllSketchAggBigint),
      "kll_sketch_agg_float" -> unaryOptionParser(KllSketchAggFloat),
      "kll_sketch_agg_double" -> unaryOptionParser(KllSketchAggDouble),
      "schema_of_variant_agg" -> unaryParser(SchemaOfVariantAgg)
    )

  private val functionParsers =
    Map(
      // Alchemy.
      "hll_cardinality" -> parseHyperLogLogCardinality _,

      // ApproxTopK.
      "approx_top_k_estimate" -> binaryParser(ApproxTopKEstimate),

      // Arithmetic.
      "positive" -> unaryParser(UnaryPositive),
      "mod" -> binaryParser(Remainder),
      "abs" -> unaryParser(Abs),
      "pmod" -> binaryParser(Pmod),
      "least" -> variableLengthParser(Least),
      "greatest" -> variableLengthParser(Greatest),

      // Avro.
      "from_avro" -> unaryParser(AvroDataToCatalyst),
      "to_avro" -> unaryParser(CatalystDataToAvro),

      // Bitwise.
      "bit_count" -> unaryParser(BitwiseCount),
      "bit_get" -> binaryParser(BitwiseGet),
      "getbit" -> binaryParser(BitwiseGet),
      "bit_reverse" -> unaryParser(BitwiseReverse),

      // Bloom filter.
      "might_contain" -> binaryParser(BloomFilterMightContain),

      // Cast.
      "cast" -> parseCast(isTryCast = false) _,
      "try_cast" -> parseCast(isTryCast = true) _,

      // Collation.
      "collate" -> binaryParser(Collate),

      // Collection.
      "size" -> unaryParser(Size),
      "cardinality" -> unaryParser(Size),
      "map_keys" -> unaryParser(MapKeys),
      "map_values" -> unaryParser(MapValues),
      "map_entries" -> unaryParser(MapEntries),
      "map_concat" -> variableLengthParser(MapConcat),
      "map_from_entries" -> unaryParser(MapFromEntries),
      "sort_array" -> binaryParser(SortArray),
      "shuffle" -> parseShuffle _,
      "reverse" -> unaryParser(Reverse),
      "array_contains" -> binaryParser(ArrayContains),
      "arrays_overlap" -> binaryParser(ArraysOverlap),
      "slice" -> ternaryParser(Slice),
      "array_join" -> parseArrayJoin _,
      "array_min" -> unaryParser(ArrayMin),
      "array_max" -> unaryParser(ArrayMax),
      "array_position" -> binaryParser(ArrayPosition),
      "element_at" -> parseElementAt _,
      "concat" -> variableLengthParser(Concat),
      "flatten" -> unaryParser(Flatten),
      "sequence" -> parseSequence _,
      "array_insert" -> parseArrayInsert _,
      "array_repeat" -> binaryParser(ArrayRepeat),
      "array_remove" -> binaryParser(ArrayRemove),
      "array_distinct" -> unaryParser(ArrayDistinct),
      "array_union" -> binaryParser(ArrayUnion),
      "array_intersect" -> binaryParser(ArrayIntersect),
      "array_except" -> binaryParser(ArrayExcept),
      "array_append" -> binaryParser(ArrayAppend),
      "arrays_zip" -> parseArraysZip _,

      // Complex.
      "array" -> variableLengthParser(CreateArray.apply),
      "map" -> parseMap _,
      "map_from_arrays" -> binaryParser(MapFromArrays),
      "struct" -> parseCreateNamedStruct _,
      "named_struct" -> parseCreateNamedStruct _,
      "str_to_map" -> ternaryParser(StringToMap),

      // Constraint.
      "knownnullable" -> unaryParser(KnownNullable),
      "knownnotnull" -> unaryParser(KnownNotNull),
      "knownfloatingpointnormalized" -> unaryParser(KnownFloatingPointNormalized),
      "knownnotcontainsnull" -> unaryParser(KnownNotContainsNull),

      // Csv.
      "from_csv" -> parseCsvToStructs _,
      "schema_of_csv" -> parseSchemaOfCsv _,
      "to_csv" -> parseStructsToCsv _,

      // Datasketches.
      "hll_sketch_estimate" -> unaryParser(HllSketchEstimate),
      "hll_union" -> ternaryParser(HllUnion),

      // Datetime.
      "current_timezone" -> ((_: String) => CurrentTimeZone()),
      "current_date" -> ((_: String) => CurrentDate()),
      "current_timestamp" -> ((_: String) => CurrentTimestamp()),
      "now" -> ((_: String) => Now()),
      "localtimestamp" -> ((_: String) => LocalTimestamp()),
      "year" -> unaryParser(Year),
      "yearofweek" -> unaryParser(YearOfWeek),
      "quarter" -> unaryParser(Quarter),
      "month" -> unaryParser(Month),
      "day" -> unaryParser(DayOfMonth),
      "dayofmonth" -> unaryParser(DayOfMonth),
      "dayofyear" -> unaryParser(DayOfYear),
      "dayofweek" -> unaryParser(DayOfWeek),
      "weekday" -> unaryParser(WeekDay),
      "weekofyear" -> unaryParser(WeekOfYear),
      "hour" -> unaryWithTimeZoneParser(Hour),
      "minute" -> unaryWithTimeZoneParser(Minute),
      "second" -> unaryWithTimeZoneParser(Second),
      "secondwithfraction" -> unaryWithTimeZoneParser(SecondWithFraction),
      "unix_date" -> unaryParser(UnixDate),
      "date_from_unix_date" -> unaryParser(DateFromUnixDate),
      "last_day" -> unaryParser(LastDay),
      "timestamp_seconds" -> unaryParser(SecondsToTimestamp),
      "timestamp_millis" -> unaryParser(MillisToTimestamp),
      "timestamp_micros" -> unaryParser(MicrosToTimestamp),
      "unix_seconds" -> unaryParser(UnixSeconds),
      "cast_timestamp_ntz_to_long" -> unaryParser(CastTimestampNTZToLong),
      "unix_millis" -> unaryParser(UnixMillis),
      "unix_micros" -> unaryParser(UnixMicros),
      "date_add" -> binaryParser(DateAdd),
      "date_sub" -> binaryParser(DateSub),
      "next_day" -> binaryParser(NextDay),
      "add_months" -> binaryParser(AddMonths),
      "trunc" -> binaryParser(TruncDate),
      "date_format" -> binaryWithTimeZoneParser(DateFormatClass),
      "to_unix_timestamp" -> binaryWithTimeZoneParser(ToUnixTimestamp),
      "unix_timestamp" -> binaryWithTimeZoneParser(UnixTimestamp),
      "from_unixtime" -> binaryWithTimeZoneParser(FromUnixTime),
      "datediff" -> binaryParser(DateDiff),
      "date_diff" -> binaryParser(DateDiff),
      "from_utc_timestamp" -> binaryParser(FromUTCTimestamp),
      "to_utc_timestamp" -> binaryParser(ToUTCTimestamp),
      "date_trunc" -> binaryWithTimeZoneParser(TruncTimestamp),
      "months_between" -> ternaryWithTimeZoneParser(MonthsBetween),
      "make_date" -> ternaryParser(MakeDate),
      "make_timestamp" -> parseMakeTimestamp _,
      "make_timestamp_ltz" -> parseMakeTimestamp _,
      "make_timestamp_ntz" -> parseMakeTimestamp _,
      "try_make_timestamp_ltz" -> parseMakeTimestamp _,
      "try_make_timestamp_ntz" -> parseMakeTimestamp _,
      "gettimestamp" -> parseGetTimestamp _,
      "convert_timezone" -> ternaryParser(ConvertTimezone),
      "timestampadd" -> parseTimestampAdd _,
      "timestampdiff" -> parseTimestampDiff _,
      "dayname" -> unaryParser(DayName),
      "monthname" -> unaryParser(MonthName),

      // Decimal.
      "UnscaledValue" -> unaryParser(UnscaledValue),
      "MakeDecimal" -> parseMakeDecimal _,
      "promote_precision" -> unaryParser(PromotePrecision),
      "CheckOverflow" -> unaryParser(CheckOverflow),

      // Dynamic Pruning.
      "dynamicpruningexpression" -> unaryParser(DynamicPruningExpression),

      // Generator.
      "explode" -> unaryParser(Explode.apply),
      "posexplode" -> unaryParser(PosExplode.apply),
      "inline" -> unaryParser(Inline),
      "json_tuple" -> variableLengthParser(JsonTuple),
      "stack" -> variableLengthParser(Stack),
      "replicaterows" -> variableLengthParser(ReplicateRows),
      "sql_keywords" -> ((_: String) => SQLKeywords()),
      "collations" -> ((_: String) => Collations()),
      "variant_explode" -> unaryParser(VariantExplode),

      // Hash.
      "md5" -> unaryParser(Md5),
      "sha2" -> binaryParser(Sha2),
      "sha" -> unaryParser(Sha1),
      "sha1" -> unaryParser(Sha1),
      "crc32" -> unaryParser(Crc32),
      "hash" -> parseMurmur3Hash _,
      "xxhash64" -> parseXxHash64 _,

      // HighOrderFunction.
      "lambdafunction" -> parseLambdaFunction _,
      "transform" -> binaryParser(ArrayTransform),
      "array_sort" -> parseArraySort _,
      "map_filter" -> binaryParser(MapFilter),
      "filter" -> binaryParser(ArrayFilter),
      "exists" -> binaryParser(ArrayExists),
      "forall" -> binaryParser(ArrayForAll),
      "aggregate" -> quaternaryParser(ArrayAggregate),
      "reduce" -> quaternaryParser(ArrayAggregate),
      "transform_keys" -> binaryParser(TransformKeys),
      "transform_values" -> binaryParser(TransformValues),
      "map_zip_with" -> ternaryParser(MapZipWith),
      "zip_with" -> ternaryParser(ZipWith),

      // InputFileBlock.
      "input_file_name" -> ((_: String) => InputFileName()),
      "input_file_block_start" -> ((_: String) => InputFileBlockStart()),
      "input_file_block_length" -> ((_: String) => InputFileBlockLength()),

      // Interval.
      "extractintervalyears" -> unaryParser(ExtractIntervalYears),
      "extractintervalmonths" -> unaryParser(ExtractIntervalMonths),
      "extractintervaldays" -> unaryParser(ExtractIntervalDays),
      "extractintervalhours" -> unaryParser(ExtractIntervalHours),
      "extractintervalminutes" -> unaryParser(ExtractIntervalMinutes),
      "extractintervalseconds" -> unaryParser(ExtractIntervalSeconds),
      "extractansiintervalyears" -> unaryParser(ExtractANSIIntervalYears),
      "extractansiintervalmonths" -> unaryParser(ExtractANSIIntervalMonths),
      "extractansiintervaldays" -> unaryParser(ExtractANSIIntervalDays),
      "extractansiintervalhours" -> unaryParser(ExtractANSIIntervalHours),
      "extractansiintervalminutes" -> unaryParser(ExtractANSIIntervalMinutes),
      "extractansiintervalseconds" -> unaryParser(ExtractANSIIntervalSeconds),
      "multiply_interval" -> binaryParser(MultiplyInterval),
      "divide_interval" -> binaryParser(DivideInterval),
      "make_interval" -> parseMakeInterval _,
      "make_dt_interval" -> quaternaryParser(MakeDTInterval),
      "make_ym_interval" -> binaryParser(MakeYMInterval),

      // Json.
      "get_json_object" -> parseGetJsonObject _,
      "from_json" -> parseJsonToStructs _,
      "to_json" -> parseStructsToJson _,
      "schema_of_json" -> parseSchemaOfJson _,
      "json_array_length" -> ((s: String) => LengthOfJsonArray(parse(s))),
      "json_object_keys" -> ((s: String) => JsonObjectKeys(parse(s))),

      // Kll.
      "kll_sketch_to_string_bigint" -> unaryParser(KllSketchToStringBigint),
      "kll_sketch_to_string_float" -> unaryParser(KllSketchToStringFloat),
      "kll_sketch_to_string_double" -> unaryParser(KllSketchToStringDouble),
      "kll_sketch_get_n_bigint" -> unaryParser(KllSketchGetNBigint),
      "kll_sketch_get_n_float" -> unaryParser(KllSketchGetNFloat),
      "kll_sketch_get_n_double" -> unaryParser(KllSketchGetNDouble),
      "kll_sketch_merge_bigint" -> binaryParser(KllSketchMergeBigint),
      "kll_sketch_merge_float" -> binaryParser(KllSketchMergeFloat),
      "kll_sketch_merge_double" -> binaryParser(KllSketchMergeDouble),
      "kll_sketch_get_quantile_bigint" -> binaryParser(KllSketchGetQuantileBigint),
      "kll_sketch_get_quantile_float" -> binaryParser(KllSketchGetQuantileFloat),
      "kll_sketch_get_quantile_double" -> binaryParser(KllSketchGetQuantileDouble),
      "kll_sketch_get_rank_bigint" -> binaryParser(KllSketchGetRankBigint),
      "kll_sketch_get_rank_float" -> binaryParser(KllSketchGetRankFloat),
      "kll_sketch_get_rank_double" -> binaryParser(KllSketchGetRankDouble),

      // Mask.
      "mask" -> quinaryParser(Mask),

      // Math.
      "E" -> ((_: String) => EulerNumber()),
      "PI" -> ((_: String) => Pi()),
      "ACOS" -> unaryParser(Acos),
      "ASIN" -> unaryParser(Asin),
      "ATAN" -> unaryParser(Atan),
      "CBRT" -> unaryParser(Cbrt),
      "COS" -> unaryParser(Cos),
      "SEC" -> unaryParser(Sec),
      "COSH" -> unaryParser(Cosh),
      "ACOSH" -> unaryParser(Acosh),
      "EXP" -> unaryParser(Exp),
      "EXPM1" -> unaryParser(Expm1),
      "ln" -> unaryParser(Log),
      "LOG2" -> unaryParser(Log2),
      "LOG10" -> unaryParser(Log10),
      "LOG1P" -> unaryParser(Log1p),
      "rint" -> unaryParser(Rint),
      "sign" -> unaryParser(Signum),
      "SIGNUM" -> unaryParser(Signum),
      "SIN" -> unaryParser(Sin),
      "CSC" -> unaryParser(Csc),
      "SINH" -> unaryParser(Sinh),
      "ASINH" -> unaryParser(Asinh),
      "SQRT" -> unaryParser(Sqrt),
      "TAN" -> unaryParser(Tan),
      "COT" -> unaryParser(Cot),
      "TANH" -> unaryParser(Tanh),
      "ATANH" -> unaryParser(Atanh),
      "DEGREES" -> unaryParser(ToDegrees),
      "RADIANS" -> unaryParser(ToRadians),
      "CEIL" -> parseCeil _,
      "ceil" -> parseCeil _,
      "ceiling" -> parseCeil _,
      "FLOOR" -> parseFloor _,
      "floor" -> parseFloor _,
      "factorial" -> unaryParser(Factorial),
      "bin" -> unaryParser(Bin),
      "hex" -> unaryParser(Hex),
      "unhex" -> unaryParser(Unhex),
      "ATAN2" -> binaryParser(Atan2),
      "pow" -> binaryParser(Pow),
      "POWER" -> binaryParser(Pow),
      "HYPOT" -> binaryParser(Hypot),
      "LOG" -> binaryParser(Logarithm),
      "shiftleft" -> binaryParser(ShiftLeft),
      "shiftright" -> binaryParser(ShiftRight),
      "shiftrightunsigned" -> binaryParser(ShiftRightUnsigned),
      "round" -> binaryParser(Round),
      "bround" -> binaryParser(BRound),
      "conv" -> ternaryParser(Conv),
      "width_bucket" -> quaternaryParser(WidthBucket),

      // Misc.
      "raise_error" -> parseRaiseError _,
      "uuid" -> parseUuid _,
      "version" -> ((_: String) => SparkVersion()),
      "SPARK_PARTITION_ID" -> ((_: String) => SparkPartitionID()),
      "typeof" -> unaryParser(TypeOf),
      "monotonically_increasing_id" -> ((_: String) => MonotonicallyIncreasingID()),
      "reflect" -> variableLengthParser(CallMethodViaReflection),
      "java_method" -> variableLengthParser(CallMethodViaReflection),

      // Null.
      "coalesce" -> variableLengthParser(Coalesce),
      "isnan" -> unaryParser(IsNaN),
      "nanvl" -> binaryParser(NaNvl),
      "isnull" -> unaryParser(IsNull),
      "isnotnull" -> unaryParser(IsNotNull),
      "atleastnnonnulls" -> parseAtLeastNNonNulls _,

      // Number Format.
      "to_number" -> binaryParser(ToNumber),
      "try_to_number" -> binaryParser(TryToNumber),
      "to_char" -> binaryParser(ToCharacter),

      // Objects.
      "staticinvoke" -> parseStaticInvoke _,
      "static_invoke" -> parseStaticInvoke4x _,
      "invoke" -> parseInvoke _,
      "newInstance" -> parseNewInstance _,
      "unwrapoption" -> parseUnwrapOption _,
      "wrapoption" -> parseWrapOption _,
      "lambdavariable" -> parseLambdaVariable _,
      "mapobjects" -> ternaryWithTimeZoneParser(MapObjects),
      "catalysttoexternalmap" -> parseCatalystToExternalMap _,
      "externalmaptocatalyst" -> quinaryParser(ExternalMapToCatalyst),
      "createexternalrow" -> parseCreateExternalRow _,
      "encodeusingserializer" -> unaryParser(EncodeUsingSerializer),
      "decodeusingserializer" -> parseDecodeUsingSerializer _,
      "initializejavabean" -> parseInitializeJavaBean _,
      "assertnotnull" -> unaryParser(AssertNotNull),
      "getexternalrowfield" -> parseGetExternalRowField _,
      "validateexternaltype" -> parseValidateExternalType _,

      // Optimizer.
      "normalizenanandzero" -> unaryParser(NormalizeNaNAndZero),

      // Protobuf.
      "from_protobuf" -> unaryParser(ProtobufDataToCatalyst),
      "to_protobuf" -> unaryParser(CatalystDataToProtobuf),

      // Random.
      "rand" -> unaryParser(Rand),
      "random" -> unaryParser(Rand),
      "randn" -> unaryParser(Randn),
      "randstr" -> binaryParser(RandStr),

      // Regexp.
      "RLIKE" -> binaryParser(RLike),
      "likeall" -> parseMultiLike(LikeAll) _,
      "notlikeall" -> parseMultiLike(NotLikeAll) _,
      "likeany" -> parseMultiLike(LikeAny) _,
      "notlikeany" -> parseMultiLike(NotLikeAny) _,
      "split" -> ternaryParser(StringSplit),
      "regexp_replace" -> parseRegexFunction(RegExpReplace.apply) _,
      "regexp_extract" -> parseRegexFunction(RegExpExtract.apply) _,
      "regexp_extract_all" -> parseRegexFunction(RegExpExtractAll.apply) _,
      "regexp_instr" -> parseRegexFunction(RegExpInStr.apply) _,

      // String.
      "concat_ws" -> variableLengthParser(ConcatWs),
      "elt" -> parseElt _,
      "ucase" -> unaryParser(Upper),
      "upper" -> unaryParser(Upper),
      "lcase" -> unaryParser(Lower),
      "lower" -> unaryParser(Lower),
      "Contains" -> stringPredicateParser(Contains),
      "StartsWith" -> stringPredicateParser(StartsWith),
      "EndsWith" -> stringPredicateParser(EndsWith),
      "replace" -> ternaryParser(StringReplace),
      "overlay" -> quaternaryParser(Overlay),
      "translate" -> ternaryParser(StringTranslate),
      "find_in_set" -> binaryParser(FindInSet),
      "trim" -> unaryOptionParser(StringTrim),
      "ltrim" -> unaryOptionParser(StringTrimLeft),
      "rtrim" -> unaryOptionParser(StringTrimRight),
      "instr" -> binaryParser(StringInstr),
      "substring_index" -> ternaryParser(SubstringIndex),
      "locate" -> ternaryParser(StringLocate),
      "position" -> ternaryParser(StringLocate),
      "lpad" -> ternaryParser(StringLPad),
      "rpad" -> ternaryParser(StringRPad),
      "printf" -> variableLengthParser(FormatString),
      "format_string" -> variableLengthParser(FormatString),
      "initcap" -> unaryParser(InitCap),
      "repeat" -> binaryParser(StringRepeat),
      "space" -> unaryParser(StringSpace),
      "substr" -> ternaryParser(Substring),
      "substring" -> ternaryParser(Substring),
      "len" -> unaryParser(Length),
      "length" -> unaryParser(Length),
      "char_length" -> unaryParser(Length),
      "character_length" -> unaryParser(Length),
      "bit_length" -> unaryParser(BitLength),
      "octet_length" -> unaryParser(OctetLength),
      "levenshtein" -> binaryOptionParser(Levenshtein),
      "soundex" -> unaryParser(SoundEx),
      "ascii" -> unaryParser(Ascii),
      "chr" -> unaryParser(Chr),
      "char" -> unaryParser(Chr),
      "base64" -> unaryParser(Base64),
      "unbase64" -> unaryParser(UnBase64),
      "decode" -> binaryParser(StringDecode),
      "encode" -> binaryParser(Encode),
      "format_number" -> binaryParser(FormatNumber),
      "sentences" -> ternaryParser(Sentences),
      "stringsplitsql" -> binaryParser(StringSplitSQL),

      // Thetasketches.
      "theta_sketch_estimate" -> unaryParser(ThetaSketchEstimate),
      "theta_union" -> ternaryParser(ThetaUnion),
      "theta_difference" -> binaryParser(ThetaDifference),
      "theta_intersection" -> binaryParser(ThetaIntersection),

      // TimeWindow.
      "precisetimestampconversion" -> parsePreciseTimestampConversion _,

      // TryEval.
      "tryeval" -> unaryParser(TryEval),

      // Url.
      "parse_url" -> parseParseUrl _,

      // Variant.
      "to_variant_object" -> unaryParser(ToVariantObject),
      "variant_get" -> parseVariantGet _,
      "try_variant_get" -> parseVariantGet _,

      // Window.
      "rank" -> variableLengthParser(Rank),
      "dense_rank" -> variableLengthParser(DenseRank),
      "percent_rank" -> variableLengthParser(PercentRank),
      "row_number" -> ((_: String) => RowNumber()),
      "ntile" -> unaryParser(NTile),
      "nth_value" -> parseNthValue _,
      "lead" -> ternaryParser(Lead),
      "lag" -> ternaryParser(Lag),
      "cume_dist" -> ((_: String) => CumeDist()),
      "null_index" -> unaryParser(NullIndex),
      "ewm" -> parseEWM _,
      "last_non_null" -> unaryParser(LastNonNull),

      // XML.
      "from_xml" -> parseXmlToStructs _,
      "schema_of_xml" -> unaryParser(SchemaOfXml),
      "to_xml" -> parseStructsToXml _,

      // XPath.
      "xpath_boolean" -> xpathParser(XPathBoolean),
      "xpath_short" -> xpathParser(XPathShort),
      "xpath_int" -> xpathParser(XPathInt),
      "xpath_long" -> xpathParser(XPathLong),
      "xpath_float" -> xpathParser(XPathFloat),
      "xpath_double" -> xpathParser(XPathDouble),
      "xpath_number" -> xpathParser(XPathDouble),
      "xpath_string" -> xpathParser(XPathString),
      "xpath" -> xpathParser(XPathList)
    ) ++ aggregateParsers

  private def unaryParser[T](build: Expression => T): String => T = {
    (str: String) =>
      {
        val children = parseList(str)
        build(children.head)
      }
  }

  private def unaryOptionParser[T](build: (Expression, Option[Expression]) => T): String => T = {
    (str: String) =>
      {
        val children = splitList(str)
        require(children.length > 1)
        build(parse(children.head), getOption(children(1)).map(parse))
      }
  }

  private def unaryWithTimeZoneParser[T](build: (Expression, Option[String]) => T): String => T = {
    (str: String) =>
      {
        val children = splitList(str)
        require(children.length > 1)
        build(parse(children.head), getOption(children(1)))
      }
  }

  private def binaryParser[T](build: (Expression, Expression) => T): String => T = {
    (str: String) =>
      {
        val children = parseList(str)
        require(children.length >= 2)
        build(children(0), children(1))
      }
  }

  private def binaryOptionParser[T](
      build: (Expression, Expression, Option[Expression]) => T
  ): String => T = {
    (str: String) =>
      {
        val children = splitList(str)
        require(children.length > 2)
        build(parse(children.head), parse(children(1)), getOption(children(2)).map(parse))
      }
  }

  private def binaryWithTimeZoneParser[T](
      build: (Expression, Expression, Option[String]) => T
  ): String => T = {
    (str: String) =>
      {
        val children = splitList(str)
        require(children.length > 2)
        build(parse(children.head), parse(children(1)), getOption(children(2)))
      }
  }

  private def ternaryParser[T](build: (Expression, Expression, Expression) => T): String => T = {
    (str: String) =>
      {
        val children = parseList(str)
        require(children.length >= 3)
        build(children(0), children(1), children(2))
      }
  }

  private def ternaryWithTimeZoneParser[T](
      build: (Expression, Expression, Expression, Option[String]) => T
  ): String => T = {
    (str: String) =>
      {
        val children = splitList(str)
        require(children.length > 3)
        build(parse(children.head), parse(children(1)), parse(children(2)), getOption(children(3)))
      }
  }

  private def quaternaryParser[T](
      build: (Expression, Expression, Expression, Expression) => T
  ): String => T = {
    (str: String) =>
      {
        val children = parseList(str)
        require(children.length >= 4)
        build(children(0), children(1), children(2), children(3))
      }
  }

  private def quinaryParser[T](
      build: (Expression, Expression, Expression, Expression, Expression) => T
  ): String => T = {
    (str: String) =>
      {
        val children = parseList(str)
        require(children.length >= 5)
        build(children(0), children(1), children(2), children(3), children(4))
      }
  }

  private def variableLengthParser[T](build: IndexedSeq[Expression] => T): String => T = {
    (str: String) =>
      {
        val children = parseList(str)
        build(children)
      }
  }

  private def binaryOperatorParser(
      build: (Expression, Expression) => Expression
  ): (String, String) => Expression = {
    (leftStr: String, rightStr: String) =>
      {
        val left = parse(leftStr)
        val right = parse(rightStr)
        build(left, right)
      }
  }

  def setInput(input: Seq[Attribute]): Unit = {
    inputById =
      input
        .groupBy(_.exprId)
        .filter(_._2.length == 1)
        .map { case (exprId, attributes) => (exprId, attributes.head) }
    inputByName =
      input
        .groupBy(_.name)
        .filter(_._2.length == 1)
        .map { case (name, attributes) => (name, attributes.head) }
  }

  def withInput[T](input: Seq[Attribute])(f: ExpressionParser => T): T = {
    setInput(input)
    try {
      f.apply(this)
    } finally {
      inputById = Map.empty
      inputByName = Map.empty
    }
  }

  def parse(str: String): Expression = {
    val s = str.trim

    // Attribute or Alias.
    val (attribute, alias) = parseAttributeOrAlias(s, parse)
    if (alias.isDefined) return alias.get

    // UnaryMinus.
    if (s.startsWith("-") && s.length > 1) {
      val expression = parse(s.tail)
      expression match {
        case _: Literal =>
          return Literal(s)
        case in: In =>
          return In(UnaryMinus(in.value), in.list)
        case inset: InSet =>
          return InSet(UnaryMinus(inset.child), inset.set)
        case _ =>
      }
      return UnaryMinus(expression)
    }

    // BitwiseNot.
    if (s.startsWith("~") && s.length > 1) {
      val expression = parse(s.tail)
      expression match {
        case in: In =>
          return In(BitwiseNot(in.value), in.list)
        case inset: InSet =>
          return InSet(BitwiseNot(inset.child), inset.set)
        case _ =>
      }
      return BitwiseNot(expression)
    }

    // Not.
    if (s.startsWith("NOT ")) {
      val child = parse(s.substring(4))
      return Not(child)
    }

    // If.
    if (s.startsWith("if (")) {
      val conditionEnd = indexOfParentheses(s, 3)
      val conditionStr = s.substring(4, conditionEnd)
      val trueEnd = findElse(s, conditionEnd)
      val trueStr = s.substring(conditionEnd + 2, trueEnd)
      val falseStr = s.substring(trueEnd + 6, s.length)

      val condition = parse(conditionStr)
      val trueValue = parse(trueStr)
      val falseValue = parse(falseStr)

      return buildIf(condition, trueValue, falseValue)
    }

    // Ends with parentheses.
    if (s.endsWith(")") && s.length > 1) {
      val startIndex = lastIndexOfScope(s, '(', ')')
      if (startIndex == -1) {
        throw ParseException(s, "Unbalanced parentheses")
      } else if (startIndex == 0) {
        return parseBinaryOperator(s.substring(1, s.length - 1))
      }

      // Function call style.
      val functionName = s.substring(0, startIndex)
      val childrenStr = s.substring(startIndex + 1, s.length - 1)
      val expressionParser = functionParsers.get(functionName)
      if (expressionParser.isDefined) {
        return expressionParser.get.apply(childrenStr)
      }

      // In.
      if (functionName.endsWith(" IN ")) {
        val valueStr = functionName.substring(0, functionName.length - 4)
        val value = parse(valueStr)
        val list = splitList(childrenStr, Some(_)).map(parse)
        return In(value, list)
      }

      if (isFunctionName(functionName)) {
        val children = parseList(childrenStr)
        return UnknownSQLFunction(functionName, children)
      }
    }

    // CaseWhen.
    if (s.startsWith("CASE WHEN ") && s.endsWith(" END")) {
      val branches = ArrayBuffer[(Expression, Expression)]()
      var index = 10
      while (index < s.length) {
        val (conditionEnd, thenEnd, end) = findWhenThen(s, index)
        val conditionStr = s.substring(index, conditionEnd)
        val thenStr = s.substring(conditionEnd + 6, thenEnd)
        branches += (parse(conditionStr) -> parse(thenStr))

        if (end == -1) {
          index = thenEnd + 6
        } else if (end + 4 == s.length) {
          val elseValue =
            if (thenEnd != end) {
              val elseStr = s.substring(thenEnd + 6, end)
              Some(parse(elseStr))
            } else {
              None
            }
          return CaseWhen(branches.toIndexedSeq, elseValue)
        } else {
          // There is more after END, exit loop.
          index = s.length
        }
      }
    }

    // In subquery.
    {
      val index = s.lastIndexOf(" IN ")
      if (index != -1) {
        val subqueryStr = s.substring(index + 4)
        val subquery = subqueries(subqueryStr)
        if (subquery.isDefined) {
          val valueStr = s.substring(0, index)
          val value = parse(valueStr)
          return InSubqueryExec(value, subquery.get)
        }
      }
    }

    // InSet.
    {
      val index = rootLevelLastIndexOf(s, " INSET ", s.length - 1)
      if (index != -1) {
        val valueStr = s.substring(0, index)
        val setStr = s.substring(index + 7)
        val set = setStr.split(", ").map(Literal(_))
        return InSet(parse(valueStr), set.toIndexedSeq)
      }
    }

    // Like.
    {
      val index = rootLevelLastIndexOf(s, " LIKE ", s.length - 1)
      if (index != -1) {
        val valueStr = s.substring(0, index)
        val (patternEnd, escapeChar) =
          if (
            index + 16 < s.length &&
            s.endsWith("'") &&
            s.substring(s.length - 11).startsWith(" ESCAPE '")
          ) {
            (s.length - 11, s.charAt(s.length - 2))
          } else {
            (s.length, '\\')
          }
        val patternStr = s.substring(index + 6, patternEnd)

        return Like(parse(valueStr), parse(patternStr), escapeChar)
      }
    }

    // Date and Timestamp Add.
    {
      val index = rootLevelLastIndexOf(s, " + ", s.length - 1)
      if (index != -1) {
        val leftStr = s.substring(0, index)
        val rightStr = s.substring(index + 3, s.length)
        val left = parse(leftStr)
        val right = parse(rightStr)
        return buildDateTimeAdd(left, right)
      }
    }

    // Subquery.
    if (s.startsWith("Subquery ") || s.startsWith("ReusedSubquery ")) {
      val subquery = subqueries(s)
      if (subquery.isDefined) {
        return ScalarSubquery(subquery.get)
      }
      if (isSubquery(s)) {
        return UnknownSQLExpression(s)
      }
    }

    // BoundReference, GetArrayItem, and GetMapValue.
    if (s.endsWith("]") && s.length > 3 && !s.startsWith("[")) {
      val startIndex = lastIndexOfScope(s, '[', ']')
      if (startIndex == -1) {
        throw ParseException(s, "Unbalanced square brackets")
      }
      val prefixStr = s.substring(0, startIndex)
      val contentStr = s.substring(startIndex, s.length)
      val parts = splitSquareBracketsList(contentStr)
      if (prefixStr == "input" && parts.size == 3) {
        val ordinal = parts.head.toInt
        val dataType = DataTypeParser.parseSimpleString(parts(1))
        return BoundReference(ordinal, dataType)
      } else if (parts.size == 1) {
        val prefix = parse(prefixStr)
        val element = parse(parts.head)

        val arrayType =
          prefix.dataType.collectFirst {
            case arrayType: ArrayType =>
              arrayType
          }
        val mapType =
          prefix.dataType.collectFirst {
            case mapType: MapType =>
              mapType
          }

        if (arrayType.isDefined && mapType.isEmpty) {
          return GetArrayItem(prefix, element)
        } else if (arrayType.isEmpty && mapType.isDefined) {
          return GetMapValue(prefix, element)
        } else if (prefix.dataType == AnyType || arrayType.isDefined && mapType.isDefined) {
          return AmbiguousExpression(
            IndexedSeq(
              GetArrayItem(prefix, element),
              GetMapValue(prefix, element)
            )
          )
        }
      }
    }

    // GetStructField, GetArrayStructFields, and Invoke.
    {
      var dotIndex = rootLevelLastIndexOf(s, ".", s.length - 1)
      while (dotIndex > 0 && dotIndex < s.length - 1) {
        val name = s.substring(dotIndex + 1)
        val childStr = s.substring(0, dotIndex)
        val child = parse(childStr)
        child.dataType match {
          // GetStructField.
          case structType: StructType =>
            val ordinal = structType.indexOf(name)
            if (ordinal != -1) {
              val fieldName = structType.fields(ordinal).name
              val optionalName = if (name != fieldName) Some(name) else None
              return GetStructField(child, ordinal, optionalName, structType.fields.length)
            }

          // GetArrayStructFields.
          case ArrayType(structType: StructType) =>
            val ordinal = structType.indexOf(name)
            if (ordinal != -1) {
              val field = structType.fields(ordinal).copy(name = name)
              return GetArrayStructFields(child, field, ordinal, structType.fields.length)
            }

          // Invoke.
          case _: ObjectType =>
            return Invoke(child, name)
          case AnyType if !child.isInstanceOf[Literal] =>
            return Invoke(child, name)

          case _ =>
        }

        dotIndex = rootLevelLastIndexOf(s, ".", dotIndex - 1)
      }
    }

    if (attribute.isDefined) {
      if (s.startsWith("lambda ")) {
        return NamedLambdaVariable(
          attribute.get.name.substring(7),
          attribute.get.dataType,
          attribute.get.exprId
        )
      }
      return attribute.get
    }

    Literal(str)
  }

  def parseList(str: String): IndexedSeq[Expression] = {
    splitSpacedList(str).map(parse)
  }

  def parseSquareList(str: String): IndexedSeq[Expression] = {
    require(str.head == '[')
    require(str.last == ']')
    val listStr = str.substring(1, str.length - 1)
    splitList(listStr).map(parse)
  }

  def parseNamedList(str: String): Seq[NamedExpression] = {
    require(str.head == '[')
    val endOffset = if (str.last == ']') 1 else 0
    val listStr = str.substring(1, str.length - endOffset)
    splitList(
      listStr,
      part => {
        val (attribute, alias) = parseAttributeOrAlias(part.trim, parse)
        attribute.orElse(alias)
      }
    )
  }

  def parseSortOrderList(str: String): Seq[SortOrder] = {
    require(str.head == '[')
    require(str.last == ']')
    val listStr = str.substring(1, str.length - 1)
    splitList(listStr, s => parseSortOrder(s.trim))
  }

  def parseAggregateList(str: String): Seq[AggregateExpression] = {
    splitSquareBracketsList(str).map(parseAggregate)
  }

  def parseWindowList(str: String): Seq[NamedExpression] = {
    require(str.head == '[')
    val endOffset = if (str.last == ']') 1 else 0
    val listStr = str.substring(1, str.length - endOffset)
    splitList(
      listStr,
      part => {
        val (attribute, alias) = parseAttributeOrAlias(part.trim, parseWindow)
        attribute.orElse(alias)
      }
    )
  }

  /**
   * [[org.apache.spark.sql.catalyst.expressions.AttributeReference]].
   * [[org.apache.spark.sql.catalyst.expressions.Alias]].
   */
  private def parseAttributeOrAlias(
      str: String,
      childParse: String => Expression
  ): (Option[Attribute], Option[Alias]) = {
    if (str.length < 3) {
      return (None, None)
    }

    // Validate last character `L` or alphanumeric.
    val lastChar = str.last
    var endIndex = str.length
    if (lastChar == 'L') {
      endIndex -= 1
    } else if (lastChar < '0' || lastChar > '9') {
      return (None, None)
    }

    // Ensure alphanumeric until `#`.
    var i = str.length - 2
    while (i >= 0 && str.apply(i) >= '0' && str.apply(i) <= '9') i -= 1
    if (i <= 0 || str.apply(i) != '#') {
      return (None, None)
    }
    val idIndex = i + 1
    val id = str.substring(idIndex, endIndex).toLong

    // Iterate until ` AS ` is found.
    i -= 1
    var opened = 0
    while (
      i > 3 && !(
        str.charAt(i) == ' ' &&
          str.charAt(i - 1) == 'S' &&
          str.charAt(i - 2) == 'A' &&
          str.charAt(i - 3) == ' ' &&
          opened == 0
      )
    ) {
      str.charAt(i) match {
        case ')' | ']' =>
          opened += 1
        case '(' | '[' =>
          opened -= 1
        case _ =>
          ()
      }

      i -= 1
    }

    // Alias was not found.
    if (
      i <= 3 ||
      str.apply(i) != ' ' ||
      str.apply(i - 1) != 'S' ||
      str.apply(i - 2) != 'A' ||
      str.apply(i - 3) != ' ' ||
      opened > 0
    ) {
      val name = str.substring(0, idIndex - 1)
      val dataType = inputById
        .get(id)
        .map(_.dataType)
        .getOrElse(inputByName.get(name).fold[DataType](AnyType)(_.dataType))
      val attribute = AttributeReference(name, dataType, id)
      return (Some(attribute), None)
    }

    // Alias found.
    val childStr = str.substring(0, i - 3)
    val child = childParse(childStr)
    val nameIndex = i + 1
    val name = str.substring(nameIndex, idIndex - 1)

    val alias = Alias(child, name, id)
    (None, Some(alias))
  }

  /**
   * [[org.apache.spark.sql.catalyst.expressions.SortOrder]].
   */
  def parseSortOrder(str: String): Option[SortOrder] = {
    val nullOrdering =
      if (str.endsWith("NULLS FIRST")) {
        NullsFirst
      } else if (str.endsWith("NULLS LAST")) {
        NullsLast
      } else {
        return None
      }

    val remainingEnd = str.lastIndexOf(' ', str.length - 11)
    val remainingStr = str.substring(0, remainingEnd)
    val direction =
      if (remainingStr.endsWith("ASC")) {
        Ascending
      } else if (remainingStr.endsWith("DESC")) {
        Descending
      } else {
        return None
      }

    val childEnd = remainingStr.lastIndexOf(' ', remainingStr.length - 4)
    val childStr = remainingStr.substring(0, childEnd)
    val child = parse(childStr)

    Some(SortOrder(child, direction, nullOrdering))
  }

  /**
   * [[org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression.toString]].
   */
  def parseAggregate(str: String): AggregateExpression = {
    val (modePrefix, startIndex) =
      if (str.startsWith("partial_")) {
        ("partial", 8)
      } else if (str.startsWith("merge_")) {
        ("merge", 6)
      } else {
        ("", 0)
      }

    val parenthesesStart = str.indexOf("(", startIndex)
    val parenthesesEnd = indexOfParentheses(str, parenthesesStart)
    val functionName = str.substring(startIndex, parenthesesStart)

    val isDistinct = str.indexOf("distinct ", parenthesesStart + 1) != -1
    val distinctOffset = if (isDistinct) 9 else 0
    val functionStr = str.substring(parenthesesStart + distinctOffset + 1, parenthesesEnd)
    val function =
      if (aggregateParsers.contains(functionName)) {
        aggregateParsers(functionName)(functionStr)
      } else {
        UnknownAggregateFunction(functionName, parseList(functionStr))
      }

    val filterStr = str
      .substring(parenthesesEnd)
      .stripPrefix(") FILTER (WHERE ")
      .dropRight(1)
    val filter =
      if (filterStr.isEmpty) {
        None
      } else {
        Some(parse(filterStr))
      }

    AggregateExpression(function, modePrefix, isDistinct, filter)
  }

  /**
   * [[org.apache.spark.sql.catalyst.expressions.WindowExpression]].
   */
  def parseWindow(str: String): WindowExpression = {
    // Parse spec.
    val specIndex = str.indexOf("windowspecdefinition")
    val specStr = str.substring(specIndex)
    val spec = parseWindowSpec(specStr)

    // Parse function.
    val functionStr = str.substring(0, specIndex)
    val function = parse(functionStr)

    WindowExpression(function, spec)
  }

  /**
   * [[org.apache.spark.sql.catalyst.expressions.WindowSpecDefinition]].
   *
   * Example:
   *
   * {{{
   * windowspecdefinition(
   *  byte#1,
   *  long#4L ASC NULLS FIRST,
   *  specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())
   * )
   * }}}
   */
  def parseWindowSpec(str: String): WindowSpecDefinition = {
    val arguments = splitParenthesesList(str.substring("windowspecdefinition".length))
    require(arguments.nonEmpty)

    // Parse partitions and order.
    val partitionSpec = new ArrayBuffer[Expression]()
    val orderSpec = new ArrayBuffer[SortOrder]()
    var i = arguments.length - 2
    while (i >= 0) {
      parseSortOrder(arguments(i))
        .map(orderSpec += _)
        .getOrElse(partitionSpec += parse(arguments(i)))

      i -= 1
    }

    // Parse frame.
    val frame = parseWindowFrame(arguments.last)

    WindowSpecDefinition(
      partitionSpec.reverse.toIndexedSeq,
      orderSpec.reverse.toIndexedSeq,
      frame
    )
  }

  /**
   * [[org.apache.spark.sql.catalyst.expressions.SpecifiedWindowFrame]].
   *
   * Example:
   *
   * {{{
   * specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())
   * }}}
   */
  def parseWindowFrame(str: String): WindowFrame = {
    val arguments = splitParenthesesList(str.substring("specifiedwindowframe".length))
    require(arguments.length > 2)

    val frameType =
      if (arguments.head == "RangeFrame") {
        RangeFrame
      } else if (arguments.head == "RowFrame") {
        RowFrame
      } else {
        throw ParseException(str, "Unknow window frame type")
      }
    val lower = parseWindowFrameBoundary(arguments(1))
    val upper = parseWindowFrameBoundary(arguments(2))

    SpecifiedWindowFrame(frameType, lower, upper)
  }

  /**
   * [[org.apache.spark.sql.catalyst.expressions.SpecialFrameBoundary]].
   */
  def parseWindowFrameBoundary(str: String): Expression = {
    str match {
      case "unboundedpreceding$()" =>
        UnboundedPreceding
      case "unboundedfollowing$()" =>
        UnboundedFollowing
      case "currentrow$()" =>
        CurrentRow
      case _ =>
        parse(str)
    }
  }

  private def isFunctionName(str: String): Boolean = {
    str.forall {
      c =>
        (c >= 'a' && c <= 'z') ||
        (c >= 'A' && c <= 'Z') ||
        (c >= '0' && c <= '9') ||
        c == '_'
    }
  }

  /**
   * Attempts to parse a string in the format `... <op> ...` as an expression.
   */
  private def parseBinaryOperator(str: String): Expression = {
    val length = str.length
    var i = 0
    var opened = 0
    while (i < length) {
      str.charAt(i) match {
        case '(' =>
          opened += 1
        case ')' =>
          opened -= 1
        case ' ' =>
          if (opened == 0) {
            // Operators have size 1, 2 or 3.
            val end = Seq(2, 3, 4).find(end => i + end < length && str.charAt(i + end) == ' ')
            val operator = end.map(e => str.substring(i + 1, i + e))
            if (operator.isDefined && binaryOperatorParsers.contains(operator.get)) {
              val left = str.substring(0, i)
              val right = str.substring(i + end.get + 1)
              return binaryOperatorParsers.apply(operator.get)(left, right)
            }
          }
        case _ =>
          ()
      }
      i += 1
    }

    UnknownSQLExpression(str)
  }

  /**
   * For an expression `if (...) ... else ...` returns the index where the `trueValue` ends.
   */
  private def findElse(str: String, start: Int): Int = {
    var i = start
    var opened = 0
    while (i < str.length) {
      str.charAt(i) match {
        case 'i' if str.substring(i).startsWith("if (") =>
          opened += 1
          i += 3
        case 'e' if str.substring(i).startsWith("else ") =>
          if (opened == 0) {
            return i - 1
          }
          opened -= 1
          i += 4
        case _ =>
          ()
      }
      i += 1
    }

    -1
  }

  private def buildIf(
      condition: Expression,
      trueValue: Expression,
      falseValue: Expression
  ): Expression = {
    val ifExpr = If(condition, trueValue, falseValue)

    // Try to resolve conflicting precedence.
    falseValue match {
      case _: GetArrayItem =>
        if (!trueValue.resolvedIsCompatibleWith(ArrayType(AnyType))) {
          return ifExpr
        }
      case _: GetMapValue =>
        if (!trueValue.resolvedIsCompatibleWith(MapType(AnyType, AnyType))) {
          return ifExpr
        }
      case _ =>
    }

    val falsePossibilities =
      falseValue match {
        case expr: AmbiguousExpression =>
          expr.children.filter(_.resolvedIsCompatibleWith(trueValue.dataType))
        case _ =>
          Seq(falseValue)
      }

    val alternatives: Seq[Expression] =
      falsePossibilities.map {
        case in: In =>
          In(If(condition, trueValue, in.value), in.list)
        case inSet: InSet =>
          InSet(If(condition, trueValue, inSet.child), inSet.set)
        case like: Like =>
          Like(If(condition, trueValue, like.left), like.right, like.escapeChar)
        case get: GetArrayItem =>
          GetArrayItem(If(condition, trueValue, get.child), get.ordinal)
        case get: GetMapValue =>
          GetMapValue(If(condition, trueValue, get.child), get.key)
        case time: TimeAdd =>
          TimeAdd(If(condition, trueValue, time.start), time.interval)
        case time: TimestampAddYMInterval =>
          TimestampAddYMInterval(If(condition, trueValue, time.timestamp), time.interval)
        case date: DateAddInterval =>
          DateAddInterval(If(condition, trueValue, date.start), date.interval)
        case date: DateAddYMInterval =>
          DateAddYMInterval(If(condition, trueValue, date.date), date.interval)
        case _ =>
          ifExpr
      }

    falseValue match {
      case _: In | _: InSet | _: Like =>
        if (!ifExpr.resolvedIsCompatibleWith(BooleanType)) {
          return alternatives.head
        }
      case _ =>
    }

    if (alternatives.length == 1 && ifExpr == alternatives.head) {
      ifExpr
    } else {
      val possibilities = falsePossibilities.indices.flatMap { i =>
        val falseValue = falsePossibilities(i)
        val ifExpr = If(condition, trueValue, falseValue)
        val alternative = alternatives(i)
        if (ifExpr == alternative) {
          Seq(ifExpr)
        } else {
          Seq(ifExpr, alternative)
        }
      }

      AmbiguousExpression(possibilities)
    }
  }

  /**
   * For an expression `CASE WHEN ... THEN ... ELSE ... END` it returns the indices of: `when`
   * expression end; `then` expression end; and end of last expression if `end` is reached.
   */
  private def findWhenThen(str: String, start: Int): (Int, Int, Int) = {
    var i = start
    var opened = 0
    var whenEnd = -1
    var elseIndex = -1
    while (i < str.length) {
      str.charAt(i) match {
        case 'C'
            if str.substring(i).startsWith("CASE WHEN ") =>
          opened += 1
          i += 9
        case 'T'
            if str.substring(i).startsWith("THEN ") =>
          if (opened == 0) {
            whenEnd = i - 1
          }
          i += 4
        case 'W'
            if str.substring(i).startsWith("WHEN ") =>
          if (opened == 0) {
            return (whenEnd, i - 1, -1)
          }
          i += 4
        case 'E'
            if str.substring(i).startsWith("ELSE ") =>
          if (opened == 0) {
            elseIndex = i - 1
          }
          i += 4
        case 'E'
            if str.substring(i).startsWith("END") =>
          if (opened == 0) {
            if (elseIndex == -1) {
              return (whenEnd, i - 1, i - 1)
            } else {
              return (whenEnd, elseIndex, i - 1)
            }
          }
          opened -= 1
          i += 2
        case _ =>
          ()
      }
      i += 1
    }

    (-1, -1, -1)
  }

  private def isSubquery(str: String): Boolean = {
    val remaining =
      str
        .stripPrefix("ReusedSubquery ")
        .stripPrefix("Subquery ")
        .stripPrefix("scalar-")
        .stripPrefix("subquery#")
        .dropWhile(c => c >= '0' && c <= '9')

    if (remaining.isEmpty) {
      true
    } else {
      remaining
        .stripPrefix(", [id=#")
        .dropWhile(c => c >= '0' && c <= '9')
        .stripPrefix("]")
        .isEmpty
    }
  }

  private def parseMultiply(leftStr: String, rightStr: String): Expression = {
    val left = parse(leftStr)
    val right = parse(rightStr)

    val expressions =
      IndexedSeq[(DataType, (Expression, Expression) => Expression)](
        (TypeSet.Numeric, Multiply.apply),
        (YearMonthIntervalType, MultiplyYMInterval.apply),
        (DayTimeIntervalType, MultiplyDTInterval.apply)
      ).flatMap { case (inputType, build) =>
        if (left.resolvedIsCompatibleWith(inputType)) {
          Seq(build(left, right))
        } else {
          Seq.empty
        }
      }

    AmbiguousExpression(expressions)
  }

  private def parseDivide(leftStr: String, rightStr: String): Expression = {
    val left = parse(leftStr)
    val right = parse(rightStr)

    val expressions =
      IndexedSeq[(DataType, DataType, (Expression, Expression) => Expression)](
        (TypeSet(DoubleType, DecimalType), TypeSet(DoubleType, DecimalType), Divide.apply),
        (YearMonthIntervalType, TypeSet.Numeric, DivideYMInterval.apply),
        (DayTimeIntervalType, TypeSet.Numeric, DivideDTInterval.apply)
      ).flatMap { case (leftInputType, rightInputType, build) =>
        if (
          left.resolvedIsCompatibleWith(leftInputType) &&
          right.resolvedIsCompatibleWith(rightInputType)
        ) {
          Seq(build(left, right))
        } else {
          Seq.empty
        }
      }

    AmbiguousExpression(expressions)
  }

  private def parseSubtract(leftStr: String, rightStr: String): Expression = {
    val left = parse(leftStr)
    val right = parse(rightStr)

    val expressions =
      IndexedSeq[(DataType, (Expression, Expression) => Expression)](
        (TypeSet.NumericAndInterval, Subtract.apply),
        (DateType, SubtractDates.apply),
        (TimestampType + TimestampNTZType, SubtractTimestamps.apply)
      ).flatMap { case (inputType, build) =>
        if (
          left.resolvedIsCompatibleWith(inputType) &&
          right.resolvedIsCompatibleWith(inputType)
        ) {
          Seq(build(left, right))
        } else {
          Seq.empty
        }
      }

    AmbiguousExpression(expressions)
  }

  private def parsePercentile(str: String): Percentile = {
    val children = parseList(str)
    require(children.length > 4)
    val reverse = children.length > 5 && children(5).asInstanceOf[Literal].booleanValue
    Percentile(children(0), children(1), children(2), reverse)
  }

  private def parsePercentileDisc(str: String): PercentileDisc = {
    val children = parseList(str)
    require(children.length > 4)
    val reverse = children(2).asInstanceOf[Literal].booleanValue
    val legacyCalculation = children.length < 6 || children(5).asInstanceOf[Literal].booleanValue
    PercentileDisc(children(0), children(1), reverse, legacyCalculation)
  }

  private def parseApproximatePercentile(str: String): ApproximatePercentile = {
    val children = parseList(str)
    require(children.length > 2)
    ApproximatePercentile(children(0), children(1), children(2))
  }

  /**
   * [[org.apache.spark.sql.catalyst.expressions.aggregate.BoolAnd]].
   */
  private def parseBoolAnd(str: String): Min = {
    Min(parse(str).resolveDataType(BooleanType))
  }

  /**
   * [[org.apache.spark.sql.catalyst.expressions.aggregate.BoolOr]].
   */
  private def parseBoolOr(str: String): Max = {
    Max(parse(str).resolveDataType(BooleanType))
  }

  private def parseHistogramNumeric(str: String): HistogramNumeric = {
    val parts = splitSpacedList(str)
    require(parts.length > 3)
    val (child, nBins) =
      if (str.startsWith("histogram_numeric, HiveFunctionWrapper(")) {
        (parts(2), parts(3))
      } else {
        (parts(0), parts(1))
      }
    HistogramNumeric(parse(child), parse(nBins))
  }

  private def parsePivotFirst(str: String): PivotFirst = {
    val children = parseList(str)
    require(children.length > 4)
    val values = children.tail.tail.dropRight(2).asInstanceOf[IndexedSeq[Literal]]
    PivotFirst(children(0), children(1), values)
  }

  private def parseHyperLogLogInitSimpleAgg(str: String): HyperLogLogInitSimpleAgg = {
    val children = parseList(str)
    require(children.length > 4)
    HyperLogLogInitSimpleAgg(
      children.head,
      children(1).asInstanceOf[Literal].doubleValue,
      children(2).asInstanceOf[Literal].stringValue
    )
  }

  private def parseListAgg(str: String): ListAgg = {
    val parts = splitSpacedList(str)
    require(parts.length > 3)
    ListAgg(
      parse(parts.head),
      parse(parts(1)),
      parts.tail.tail.map(parseSortOrder).collect { case Some(e) => e }
    )
  }

  private def parseHyperLogLogCardinality(str: String): HyperLogLogCardinality = {
    val children = parseList(str)
    require(children.length > 1)
    HyperLogLogCardinality(
      children.head,
      children(1).asInstanceOf[Literal].stringValue
    )
  }

  private def parseCast(isTryCast: Boolean)(str: String): Cast = {
    val asIndex = str.lastIndexOf(" as ")
    val valueStr = str.substring(0, asIndex)
    val typeStr = str.substring(asIndex + 4)
    val dataType = DataTypeParser.parseSimpleString(typeStr)
    Cast(parse(valueStr), dataType, isTryCast)
  }

  private def parseShuffle(str: String): Shuffle = {
    val children = splitList(str)
    require(children.length > 1)
    Shuffle(
      parse(children.head),
      getOption(children(1)).map(_.toLong)
    )
  }

  private def parseArrayJoin(str: String): ArrayJoin = {
    val children = splitSpacedList(str)
    require(children.length > 2)
    ArrayJoin(
      parse(children.head),
      parse(children(1)),
      getOption(children(2)).map(parse)
    )
  }

  private def parseElementAt(str: String): Expression = {
    val children = splitList(str)
    require(children.length > 2)

    val defaultValueOutOfBound =
      if (children.length > 3) {
        getOption(children(2)).map(Literal(_))
      } else {
        None
      }
    val failOnError = children.last.toBoolean

    ElementAt(
      parse(children.head),
      parse(children(1)),
      defaultValueOutOfBound,
      failOnError
    )
  }

  private def parseSequence(str: String): Sequence = {
    val children = splitList(str)
    require(children.length > 3)
    Sequence(
      parse(children.head),
      parse(children(1)),
      getOption(children(2)).map(parse),
      getOption(children(3))
    )
  }

  private def parseArrayInsert(str: String): ArrayInsert = {
    val children = splitSpacedList(str)
    require(children.length > 2)

    val legacyNegativeIndex =
      if (children.length > 3) {
        children(3).toBoolean
      } else {
        true
      }

    ArrayInsert(
      parse(children.head),
      parse(children(1)),
      parse(children(2)),
      legacyNegativeIndex
    )
  }

  private def parseArraysZip(str: String): ArraysZip = {
    val children = splitList(str)
    val arrayCount = children.length / 2
    ArraysZip(
      children.take(arrayCount).map(parse),
      children.drop(arrayCount)
    )
  }

  private def parseMap(str: String): Expression = {
    if (str.startsWith("keys: ")) {
      Literal(str)
    } else {
      CreateMap(parseList(str))
    }
  }

  private def parseCreateNamedStruct(str: String): CreateNamedStruct = {
    val parts = splitSpacedList(str)
    val children = parts.indices.map { i =>
      if (i % 2 == 0) {
        Literal(parts(i))
      } else {
        parse(parts(i))
      }
    }
    CreateNamedStruct(children)
  }

  private def parseCsvToStructs(str: String): CsvToStructs = {
    val parts = splitSpacedList(str)

    // Parse schema.
    val fields = parts.takeWhile(_.startsWith("StructField")).map(DataTypeParser.parseStructField)
    val schema = StructType(fields.toArray)

    // Parse options.
    val options = parts
      .drop(fields.length)
      .takeWhile(s => s.startsWith("(") && s.endsWith(")"))
      .map(splitTuple2)
      .toMap

    // Parse required schema.
    val beforeLastPart = parts.apply(parts.length - 2)
    val hasRequiredSchema = beforeLastPart == "None" || beforeLastPart.startsWith("Some(")
    val requiredSchema =
      if (hasRequiredSchema) {
        getOption(parts.last).map(DataTypeParser.parseToString(_).asInstanceOf[StructType])
      } else {
        None
      }

    // Parse timezone id.
    val timeZoneId = getOption(if (hasRequiredSchema) beforeLastPart else parts.last)

    // Parse child.
    val childStr = parts
      .drop(fields.length + options.size)
      .dropRight(1 + (if (hasRequiredSchema) 1 else 0))
      .mkString(", ")
    val child = parse(childStr)

    CsvToStructs(
      schema,
      options,
      child,
      timeZoneId,
      requiredSchema
    )
  }

  private def parseSchemaOfCsv(str: String): SchemaOfCsv = {
    val parts = splitSpacedList(str)

    // Parse options.
    var i = parts.length - 1
    while (i > 0 && parts(i).startsWith("(") && parts(i).endsWith(")")) {
      i -= 1
    }
    val options = parts
      .drop(i + 1)
      .map(splitTuple2)
      .toMap

    // Parse child.
    val childStr = parts
      .dropRight(options.size)
      .mkString(", ")
    val child = parse(childStr)

    SchemaOfCsv(child, options)
  }

  private def parseStructsToCsv(str: String): StructsToCsv = {
    val parts = splitSpacedList(str)

    // Parse options.
    val options = parts
      .takeWhile(s => s.startsWith("(") && s.endsWith(")"))
      .map(splitTuple2)
      .toMap

    // Parse child.
    val childStr = parts
      .drop(options.size)
      .dropRight(1)
      .mkString(", ")
    val child = parse(childStr)

    // Parse timezone id.
    val timeZoneId = getOption(parts.last)

    StructsToCsv(options, child, timeZoneId)
  }

  private def parseMakeTimestamp(str: String): MakeTimestamp = {
    val children = splitList(str)
    require(children.length > 7)

    val failOnError =
      if (children.length > 8) {
        children(8).toBoolean
      } else {
        false
      }

    val dataType =
      if (children.length > 9) {
        DataTypeParser.parseToString(children(9))
      } else {
        TimestampType
      }

    MakeTimestamp(
      parse(children.head),
      parse(children(1)),
      parse(children(2)),
      parse(children(3)),
      parse(children(4)),
      parse(children(5)),
      getOption(children(6)).map(parse),
      getOption(children(7)),
      failOnError,
      dataType
    )
  }

  private def parseGetTimestamp(str: String): GetTimestamp = {
    val children = splitList(str)
    require(children.length > 2)

    val (dataType, timeZoneId) =
      if (children(2).startsWith("Timestamp")) {
        (
          DataTypeParser.parseToString(children(2)),
          getOption(children(3))
        )
      } else {
        (
          TimestampType,
          getOption(children(2))
        )
      }

    GetTimestamp(
      parse(children.head),
      parse(children(1)),
      dataType,
      timeZoneId
    )
  }

  private def parseTimestampAdd(str: String): TimestampAdd = {
    val children = splitList(str)
    require(children.length > 3)

    TimestampAdd(
      children.head,
      parse(children(1)),
      parse(children(2)),
      getOption(children(3))
    )
  }

  private def parseTimestampDiff(str: String): TimestampDiff = {
    val children = splitList(str)
    require(children.length > 3)

    TimestampDiff(
      children.head,
      parse(children(1)),
      parse(children(2)),
      getOption(children(3))
    )
  }

  private def buildDateTimeAdd(left: Expression, right: Expression): Expression = {
    val expressions =
      (
        if (left.dataType.intersect(DateType) == DateType) {
          (
            if (right.dataType.intersect(CalendarIntervalType) == CalendarIntervalType) {
              Seq(DateAddInterval(left, right))
            } else {
              Seq.empty
            }
          ) ++ (
            if (right.dataType.intersect(YearMonthIntervalType) == YearMonthIntervalType) {
              Seq(DateAddYMInterval(left, right))
            } else {
              Seq.empty
            }
          )
        } else {
          Seq.empty
        }
      ) ++ (
        if (
          left.dataType.intersect(TimestampType) == TimestampType ||
          left.dataType.intersect(TimestampNTZType) == TimestampNTZType
        ) {
          (
            if (
              right.dataType.intersect(CalendarIntervalType) == CalendarIntervalType ||
              right.dataType.intersect(DayTimeIntervalType) == DayTimeIntervalType
            ) {
              Seq(TimeAdd(left, right))
            } else {
              Seq.empty
            }
          ) ++ (
            if (right.dataType.intersect(YearMonthIntervalType) == YearMonthIntervalType) {
              Seq(TimestampAddYMInterval(left, right))
            } else {
              Seq.empty
            }
          )
        } else {
          Seq.empty
        }
      )

    AmbiguousExpression(expressions.toIndexedSeq)
  }

  private def parseMakeDecimal(str: String): MakeDecimal = {
    val children = splitList(str)
    require(children.length > 2)

    MakeDecimal(
      parse(children.head),
      precision = children(1).toInt,
      scale = children(2).toInt
    )
  }

  private def parseMurmur3Hash(str: String): Murmur3Hash = {
    val children = splitList(str)
    require(children.length > 1)
    Murmur3Hash(
      children.dropRight(1).map(parse),
      seed = children.last.toInt
    )
  }

  private def parseXxHash64(str: String): XxHash64 = {
    val children = splitList(str)
    require(children.length > 1)
    XxHash64(
      children.dropRight(1).map(parse),
      seed = children.last.toLong
    )
  }

  private def parseLambdaFunction(str: String): LambdaFunction = {
    val children = parseList(str)
    require(children.length > 1)
    LambdaFunction(
      function = children.head,
      arguments = children.tail.dropRight(1).asInstanceOf[IndexedSeq[NamedExpression]]
    )
  }

  private def parseArraySort(str: String): ArraySort = {
    val children = parseList(str)
    require(children.nonEmpty)
    ArraySort(
      argument = children.head,
      function =
        if (children.length == 1) {
          UndefinedExpression(IntegerType)
        } else {
          children(1)
        }
    )
  }

  private def parseMakeInterval(str: String): MakeInterval = {
    val children = parseList(str)
    require(children.length > 6)
    MakeInterval(
      children.head,
      children(1),
      children(2),
      children(3),
      children(4),
      children(5),
      children(6)
    )
  }

  private def parseGetJsonObject(str: String): GetJsonObject = {
    val parts = splitSpacedList(str)
    require(parts.length > 1)
    val jsonStr = parts.dropRight(1).mkString(", ")
    GetJsonObject(parse(jsonStr), parse(parts.last))
  }

  private def parseJsonToStructs(str: String): JsonToStructs = {
    val parts = splitSpacedList(str)

    // Parse schema.
    val fields = parts.takeWhile(_.startsWith("StructField")).map(DataTypeParser.parseStructField)
    val schema = StructType(fields.toArray)

    // Parse options.
    val options = parts
      .drop(fields.length)
      .takeWhile(s => s.startsWith("(") && s.endsWith(")"))
      .map(splitTuple2)
      .toMap

    // Parse timezone id.
    val lastPart = parts.last
    val finishesWithBoolean = lastPart == "false" || lastPart == "true"
    val timeZoneIdStr = if (finishesWithBoolean) parts(parts.length - 2) else parts.last
    val timeZoneId = getOption(timeZoneIdStr)

    // Parse child.
    val childStr = parts
      .drop(fields.length + options.size)
      .dropRight(1 + (if (finishesWithBoolean) 1 else 0))
      .mkString(", ")
    val child = parse(childStr)

    JsonToStructs(
      schema,
      options,
      child,
      timeZoneId
    )
  }

  private def parseStructsToJson(str: String): StructsToJson = {
    val parts = splitSpacedList(str)

    // Parse options.
    val options = parts
      .takeWhile(s => s.startsWith("(") && s.endsWith(")"))
      .map(splitTuple2)
      .toMap

    // Parse child.
    val childStr = parts
      .drop(options.size)
      .dropRight(1)
      .mkString(", ")
    val child = parse(childStr)

    // Parse timezone id.
    val timeZoneId = getOption(parts.last)

    StructsToJson(options, child, timeZoneId)
  }

  private def parseSchemaOfJson(str: String): SchemaOfJson = {
    val parts = splitSpacedList(str)

    // Parse options.
    var i = parts.length - 1
    while (i > 0 && parts(i).startsWith("(") && parts(i).endsWith(")")) {
      i -= 1
    }
    val options = parts
      .drop(i + 1)
      .map(splitTuple2)
      .toMap

    // Parse child.
    val childStr = parts
      .dropRight(options.size)
      .mkString(", ")
    val child = parse(childStr)

    SchemaOfJson(child, options)
  }

  private def parseCeil(str: String): Expression = {
    val children = parseList(str)
    if (children.length > 1) {
      RoundCeil(children.head, children(1))
    } else {
      Ceil(children.head)
    }
  }

  private def parseFloor(str: String): Expression = {
    val children = parseList(str)
    if (children.length > 1) {
      RoundFloor(children.head, children(1))
    } else {
      Floor(children.head)
    }
  }

  private def parseRaiseError(str: String): RaiseError = {
    val parts = splitSpacedList(str)
    require(parts.length > 1)

    RaiseError(
      parse(parts.head),
      DataTypeParser.parseToString(parts.apply(1))
    )
  }

  private def parseUuid(str: String): Uuid = {
    Uuid(getOption(str).map(_.toLong))
  }

  private def parseAtLeastNNonNulls(str: String): AtLeastNNonNulls = {
    val children = parseList(str)
    require(children.nonEmpty)

    AtLeastNNonNulls(
      children.head.asInstanceOf[Literal].intValue,
      children.tail
    )
  }

  private def parseStaticInvoke(str: String): Expression = {
    val children = splitList(str)
    require(children.length > 2)

    lazy val className = getClassName(children.head)
    lazy val dataType = DataTypeParser.parseToString(children(1))
    val functionName = children(2)

    // Parse arguments.
    val thirdLast = children(children.length - 3)
    val booleanCount = if (thirdLast == "false" || thirdLast == "true") 3 else 2
    val arguments =
      children
        .drop(3)
        .dropRight(booleanCount)
        .takeWhile(DataTypeParser.parseToString(_).isInstanceOf[UnknownType])
        .map(parse)

    functionName match {
      case "bitmapBucketNumber" =>
        BitmapBucketNumber(arguments.head)
      case "bitmapBitPosition" =>
        BitmapBitPosition(arguments.head)
      case "bitmapCount" =>
        BitmapCount(arguments.head)
      case "contains" =>
        Contains(
          arguments.head.resolveDataType(BinaryType),
          arguments.apply(1).resolveDataType(BinaryType)
        )
      case "startsWith" =>
        StartsWith(
          arguments.head.resolveDataType(BinaryType),
          arguments.apply(1).resolveDataType(BinaryType)
        )
      case "endsWith" =>
        EndsWith(
          arguments.head.resolveDataType(BinaryType),
          arguments.apply(1).resolveDataType(BinaryType)
        )
      case "lpad" | "rpad" =>
        BinaryPad(functionName, arguments.head, arguments.apply(1), arguments.apply(2))
      case "encode" if className.contains("Url") =>
        UrlEncode(arguments.head)
      case "decode" if className.contains("Url") =>
        UrlDecode(arguments.head)
      case "encode" =>
        Base64(arguments.head)
      case "isLuhnNumber" =>
        Luhncheck(arguments.head)
      case "aesEncrypt" =>
        AesEncrypt(
          arguments.head,
          arguments.apply(1),
          arguments.apply(2),
          arguments.apply(3),
          arguments.apply(4),
          arguments.apply(5)
        )
      case "aesDecrypt" =>
        AesDecrypt(
          arguments.head,
          arguments.apply(1),
          arguments.apply(2),
          arguments.apply(3),
          arguments.apply(4)
        )
      case _ =>
        StaticInvoke(className, dataType, functionName, arguments)
    }
  }

  private def parseStaticInvoke4x(str: String): Expression = {
    val (invocation, argumentsStr) = parseFunction(str)
    lazy val arguments = splitSpacedList(argumentsStr).map(parse)
    val functionIndex = invocation.lastIndexOf(".")
    val functionName = invocation.substring(functionIndex + 1)
    val staticObject = invocation.substring(0, functionIndex)

    functionName match {
      case "validateUTF8String" =>
        ValidateUTF8(arguments.head)
      case "tryValidateUTF8String" =>
        TryValidateUTF8(arguments.head)
      case "quote" =>
        Quote(arguments.head)
      case "makeTime" =>
        MakeTime(arguments.head, arguments(1), arguments(2))
      case "timeDiff" =>
        TimeDiff(arguments.head, arguments(1), arguments(2))
      case "timeTrunc" =>
        TimeTrunc(arguments.head, arguments(1))
      case "parseJson" =>
        ParseJson(arguments.head)
      case "isVariantNull" =>
        IsVariantNull(arguments.head)
      case "schemaOfVariant" =>
        SchemaOfVariant(arguments.head)
      case "schemaOfXml" =>
        SchemaOfXml(arguments(1))
      case "bitmapBucketNumber" =>
        BitmapBucketNumber(arguments.head)
      case "bitmapBitPosition" =>
        BitmapBitPosition(arguments.head)
      case "bitmapCount" =>
        BitmapCount(arguments.head)
      case "lengthOfJsonArray" =>
        LengthOfJsonArray(parse(argumentsStr))
      case "jsonObjectKeys" =>
        JsonObjectKeys(parse(argumentsStr))
      case "encode"
          if staticObject == "Encode" =>
        Encode(arguments.head, arguments(1))
      case "decode"
          if staticObject == "StringDecode" =>
        StringDecode(arguments.head, arguments(1))
      case "encode"
          if staticObject == "Base64" =>
        Base64(arguments.head)
      case "encode"
          if staticObject == "UrlCodec" =>
        UrlEncode(arguments.head)
      case "decode"
          if staticObject == "UrlCodec" =>
        UrlDecode(arguments.head)
      case "lpad" | "rpad" =>
        BinaryPad(functionName, arguments.head, arguments.apply(1), arguments.apply(2))
      case "aesEncrypt" =>
        AesEncrypt(
          arguments.head,
          arguments(1),
          arguments(2),
          arguments(3),
          arguments(4),
          arguments(5)
        )
      case "aesDecrypt" =>
        AesDecrypt(
          arguments.head,
          arguments(1),
          arguments(2),
          arguments(3),
          arguments(4)
        )
      case "getSentences" =>
        Sentences(arguments.head, arguments(1), arguments(2))
      case "isLuhnNumber" =>
        Luhncheck(arguments.head)
      case "getSparkVersion" =>
        SparkVersion()
      case "stAsBinary" =>
        ST_AsBinary(arguments.head)
      case "stGeogFromWKB" =>
        ST_GeogFromWKB(arguments.head)
      case "stGeomFromWKB" =>
        ST_GeomFromWKB(arguments.head)
      case "stSrid" =>
        ST_Srid(arguments.head)
      case "stSetSrid" =>
        ST_SetSrid(arguments.head, arguments(1))
      case functionName =>
        StaticInvoke(staticObject, AnyType, functionName, arguments)
    }
  }

  private def parseInvoke(str: String): Expression = {
    val (invocation, argumentsStr) = parseFunction(str)
    lazy val arguments = splitSpacedList(argumentsStr).map(parse)
    val functionIndex = invocation.lastIndexOf(".")
    val functionName = invocation.substring(functionIndex + 1)
    val targetStr = invocation.substring(0, functionIndex)
    lazy val (target, targetArgumentsStr) = parseFunction(targetStr)
    lazy val targetArguments = splitList(targetArgumentsStr)

    functionName match {
      case "isValid" =>
        IsValidUTF8(parse(targetStr))
      case "makeValid" =>
        MakeValidUTF8(parse(targetStr))
      case "parse"
          if targetStr.startsWith("ToTimeParser") =>
        ToTime(
          arguments.head,
          getOption(targetArgumentsStr).map(parse)
        )
      case "evaluate"
          if targetStr.startsWith("XPath") =>
        val xml = arguments.head
        val path = xpathParsePath(targetArgumentsStr)
        target match {
          case "XPathBooleanEvaluator" =>
            XPathBoolean(xml, path)
          case "XPathShortEvaluator" =>
            XPathShort(xml, path)
          case "XPathIntEvaluator" =>
            XPathInt(xml, path)
          case "XPathLongEvaluator" =>
            XPathLong(xml, path)
          case "XPathFloatEvaluator" =>
            XPathFloat(xml, path)
          case "XPathDoubleEvaluator" =>
            XPathDouble(xml, path)
          case "XPathStringEvaluator" =>
            XPathString(xml, path)
          case "XPathListEvaluator" =>
            XPathList(xml, path)
        }
      case "evaluate"
          if targetStr.startsWith("SchemaOfJsonEvaluator") =>
        SchemaOfJson(
          parse(argumentsStr),
          getMap(targetArgumentsStr)
        )
      case "evaluate"
          if targetStr.startsWith("StructsToJsonEvaluator") =>
        StructsToJson(
          getMap(targetArguments.head),
          parse(argumentsStr),
          getOption(targetArguments(2))
        )
      case "evaluate"
          if targetStr.startsWith("SchemaOfCsvEvaluator") =>
        SchemaOfCsv(
          parse(argumentsStr),
          getMap(targetArgumentsStr)
        )
      case "evaluate"
          if targetStr.startsWith("ParseUrlEvaluator") =>
        ParseUrl(arguments)
      case functionName =>
        Invoke(parse(targetStr), functionName, AnyType, arguments)
    }
  }

  private def parseFunction(str: String): (String, String) = {
    val argumentsIndex = lastIndexOfScope(str, '(', ')')
    val argumentsStr = str.substring(argumentsIndex + 1, str.length - 1)
    val invocation = str.substring(0, argumentsIndex)

    (invocation, argumentsStr)
  }

  private def parseNewInstance(str: String): NewInstance = {
    NewInstance(getClassName(str))
  }

  private def parseUnwrapOption(str: String): UnwrapOption = {
    val parts = splitSpacedList(str)
    require(parts.length > 1)

    UnwrapOption(
      DataTypeParser.parseToString(parts.head),
      parse(parts(1))
    )
  }

  private def parseWrapOption(str: String): WrapOption = {
    val parts = splitSpacedList(str)
    require(parts.length > 1)

    WrapOption(
      parse(parts.head),
      DataTypeParser.parseToString(parts(1))
    )
  }

  private def parseLambdaVariable(str: String): LambdaVariable = {
    val parts = splitSpacedList(str)
    require(parts.length > 3)
    val id = Try(parts(3).toLong)
    val dataTypeStr = if (id.isSuccess) parts(1) else parts(2)
    val dataType = DataTypeParser.parseToString(dataTypeStr)

    LambdaVariable(parts.head, dataType)
  }

  private def parseCatalystToExternalMap(str: String): CatalystToExternalMap = {
    val parts = splitSpacedList(str)
    require(parts.length > 5)

    CatalystToExternalMap(
      parse(parts.head),
      parse(parts(1)),
      parse(parts(2)),
      parse(parts(3)),
      parse(parts(4)),
      getClassName(parts(5))
    )
  }

  private def parseCreateExternalRow(str: String): CreateExternalRow = {
    val parts = splitSpacedList(str)
    require(parts.nonEmpty)

    // Parse children.
    val children = parts.takeWhile(!_.startsWith("StructField")).map(parse)

    // Parse schema.
    val fields = parts.drop(children.length).map(DataTypeParser.parseStructField)
    val schema = StructType(fields.toArray)

    CreateExternalRow(children, schema)
  }

  private def parseDecodeUsingSerializer(str: String): DecodeUsingSerializer = {
    val parts = splitSpacedList(str)
    require(parts.length > 1)

    DecodeUsingSerializer(parse(parts.head), parts(1))
  }

  private def parseInitializeJavaBean(str: String): InitializeJavaBean = {
    val parts = splitSpacedList(str)
    require(parts.length > 1)

    val setters = parts
      .tail
      .map { part =>
        val setterParts = splitParenthesesList(part)
        require(setterParts.length > 1)
        setterParts.head -> parse(setterParts(1))
      }
      .toMap

    InitializeJavaBean(parse(parts.head), setters)
  }

  private def parseGetExternalRowField(str: String): GetExternalRowField = {
    val parts = splitSpacedList(str)
    require(parts.length > 2)

    GetExternalRowField(parse(parts.head), parts(1).toInt, parts(2))
  }

  private def parseValidateExternalType(str: String): ValidateExternalType = {
    val parts = splitSpacedList(str)
    require(parts.length > 2)

    ValidateExternalType(
      parse(parts.head),
      DataTypeParser.parseToString(parts(1)),
      DataTypeParser.parseToString(parts(2))
    )
  }

  private def parseMultiLike(
      build: (Expression, Seq[String]) => Expression
  )(str: String): Expression = {
    val children = splitSpacedList(str)
    require(children.nonEmpty)
    build.apply(parse(children.head), children.tail)
  }

  private def parseRegexFunction(
      build: IndexedSeq[Expression] => Expression
  )(str: String): Expression = {
    val parts = splitSpacedList(str)
    require(parts.length > 2)

    val children = parts.map(parse)
    val finalChildren =
      if (children.apply(1).isInstanceOf[UnknownNode]) {
        children.indices.map { i =>
          if (i == 1) {
            Literal(parts.apply(1))
          } else {
            children.apply(i)
          }
        }
      } else {
        children
      }

    build(finalChildren)
  }

  private def parseElt(str: String): Elt = {
    val children = parseList(str)
    val last = children.last
    last match {
      case literal: Literal
          if literal.stringValue == "false" || literal.stringValue == "true" =>
        Elt(children.dropRight(1))
      case _ =>
        Elt(children)
    }
  }

  private def stringPredicateParser[T](build: (Expression, Expression) => T): String => T = {
    binaryParser { case (left, right) =>
      build(
        left.resolveDataType(StringType),
        right.resolveDataType(StringType)
      )
    }
  }

  private def parsePreciseTimestampConversion(str: String): PreciseTimestampConversion = {
    val parts = splitSpacedList(str)
    require(parts.length > 2)
    PreciseTimestampConversion(
      parse(parts.head),
      fromType = DataTypeParser.parseToString(parts(1)),
      toType = DataTypeParser.parseToString(parts(2))
    )
  }

  private def parseParseUrl(str: String): ParseUrl = {
    val children = parseList(str)
    require(children.length > 2)
    ParseUrl(children.dropRight(1))
  }

  private def parseVariantGet(str: String): VariantGet = {
    val parts = splitSpacedList(str)
    require(parts.length > 3)
    VariantGet(
      parse(parts.head),
      parse(parts(1)),
      DataTypeParser.parseToString(parts(2)),
      parts(3).toBoolean
    )
  }

  private def parseNthValue(str: String): NthValue = {
    val children = parseList(str)
    require(children.length > 2)
    NthValue(
      children.head,
      children(1),
      children(2).asInstanceOf[Literal].booleanValue
    )
  }

  private def parseEWM(str: String): EWM = {
    val children = parseList(str)
    require(children.length > 2)
    EWM(
      children.head,
      children(1).asInstanceOf[Literal].doubleValue,
      children(2).asInstanceOf[Literal].booleanValue
    )
  }

  private def parseXmlToStructs(str: String): XmlToStructs = {
    val parts = splitSpacedList(str)

    // Parse schema.
    val fields = parts.takeWhile(_.startsWith("StructField")).map(DataTypeParser.parseStructField)
    val schema = StructType(fields.toArray)

    // Parse options.
    val options = parts
      .drop(fields.length)
      .takeWhile(s => s.startsWith("(") && s.endsWith(")"))
      .map(splitTuple2)
      .toMap

    // Parse timezone id.
    val timeZoneId = getOption(parts.last)

    // Parse child.
    val childStr = parts
      .drop(fields.length + options.size)
      .dropRight(1)
      .mkString(", ")
    val child = parse(childStr)

    XmlToStructs(
      schema,
      options,
      child,
      timeZoneId
    )
  }

  private def parseStructsToXml(str: String): StructsToXml = {
    val parts = splitSpacedList(str)

    // Parse options.
    val options = parts
      .takeWhile(s => s.startsWith("(") && s.endsWith(")"))
      .map(splitTuple2)
      .toMap

    // Parse child.
    val childStr = parts
      .drop(options.size)
      .dropRight(1)
      .mkString(", ")
    val child = parse(childStr)

    StructsToXml(child)
  }

  private def xpathParser[T](build: (Expression, Expression) => T): String => T = {
    (str: String) =>
      {
        val parts = splitSpacedList(str)
        require(parts.length >= 2)
        val xml = parse(parts(0))
        val path = xpathParsePath(parts(1))

        build(xml, path)
      }
  }

  private def xpathParsePath(path: String): Expression = {
    val literalPath = Literal(path)
    val parsedPath = Try(parse(path))
    if (
      parsedPath.isSuccess &&
      !parsedPath.get.isInstanceOf[UnknownNode] &&
      parsedPath.get.resolvedIsCompatibleWith(StringType)
    ) {
      if (parsedPath.get.isInstanceOf[Concat]) {
        AmbiguousExpression(IndexedSeq(literalPath, parsedPath.get))
      } else {
        parsedPath.get
      }
    } else {
      literalPath
    }
  }
}

object ExpressionParser {

  def default: ExpressionParser = {
    new ExpressionParser(_ => None)
  }
}
