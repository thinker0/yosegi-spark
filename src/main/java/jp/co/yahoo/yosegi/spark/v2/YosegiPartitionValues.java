/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jp.co.yahoo.yosegi.spark.v2;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualNullSafe;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.sources.IsNull;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.Not;
import org.apache.spark.sql.sources.Or;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Converts Hive-style path values to Spark partition values and evaluates partition pruning.
 *
 * <p>The typed partition contract includes strings, integral and floating-point values, booleans,
 * dates, decimals, and Hive's default partition marker. The default marker is materialized as SQL
 * NULL.
 *
 * <p>Pruning is deliberately conservative. A file is removed only when a supported partition-only
 * predicate can be evaluated as definitively false. Parse failures and unknown comparisons retain
 * the file so pruning cannot silently cause data loss.
 *
 * <p>Compound {@code AND}, {@code OR}, and {@code NOT} expressions are considered supported only
 * when their required children are supported by this evaluator.
 */
public final class YosegiPartitionValues {
  public static final String HIVE_DEFAULT_PARTITION = "__HIVE_DEFAULT_PARTITION__";

  private YosegiPartitionValues() {}

  /**
   * Resolves discovered partition fields against an externally supplied table schema.
   *
   * <p>When a requested field exists, its Spark type and metadata are used while the partition
   * field remains nullable because the path may represent Hive's NULL partition marker.
   *
   * @param discoveredSchema partition schema inferred from path strings
   * @param requestedTableSchema Spark table schema supplied by the caller
   * @return partition schema using requested Spark types where available
   */
  public static StructType resolveSchema(
      final StructType discoveredSchema, final StructType requestedTableSchema) {
    final StructField[] fields = discoveredSchema.fields().clone();
    for (int i = 0; i < fields.length; i++) {
      final StructField requested = findField(requestedTableSchema, fields[i].name());
      if (requested != null) {
        fields[i] =
            new StructField(
                fields[i].name(), requested.dataType(), true, requested.metadata());
      }
    }
    return new StructType(fields);
  }

  /**
   * Converts raw partition path values to Spark's internal row representation.
   *
   * @param partitionSchema ordered typed partition schema
   * @param rawValues decoded raw partition values in schema order
   * @return Spark internal row used when appending partition columns to a batch
   * @throws IllegalArgumentException if the value count or typed value is invalid
   */
  public static InternalRow toInternalRow(
      final StructType partitionSchema, final String[] rawValues) {
    if (partitionSchema.length() != rawValues.length) {
      throw new IllegalArgumentException(
          "Partition value count does not match schema. schema="
              + partitionSchema.length()
              + " values="
              + rawValues.length);
    }
    final Object[] values = new Object[rawValues.length];
    for (int i = 0; i < rawValues.length; i++) {
      values[i] = parseInternal(rawValues[i], partitionSchema.fields()[i].dataType());
    }
    return new GenericInternalRow(values);
  }

  public static boolean isPartitionOnlyFilter(
      final Filter filter, final StructType partitionSchema) {
    final Set<String> names =
        Arrays.stream(partitionSchema.fieldNames()).collect(Collectors.toSet());
    final String[] references = filter.references();
    if (references.length == 0) {
      return false;
    }
    for (String reference : references) {
      if (!names.contains(reference)) {
        return false;
      }
    }
    return isSupported(filter);
  }

  /**
   * Returns whether a file should be retained after conservative partition pruning.
   *
   * <p>Unknown or malformed values return {@code true}; pruning must not discard a file merely
   * because this local evaluator cannot prove the predicate false.
   *
   * @param filters partition-only filters accepted by planning
   * @param partitionSchema ordered typed partition schema
   * @param rawValues raw values keyed by partition column name
   * @return {@code false} only when a filter is definitively false
   */
  public static boolean matches(
      final Filter[] filters,
      final StructType partitionSchema,
      final Map<String, String> rawValues) {
    if (filters.length == 0) {
      return true;
    }
    final Map<String, Object> values = new HashMap<>();
    try {
      for (StructField field : partitionSchema.fields()) {
        values.put(field.name(), parseComparable(rawValues.get(field.name()), field.dataType()));
      }
    } catch (RuntimeException e) {
      // A malformed typed partition must not be silently removed by pruning. Keep the file and let
      // normal partition-value materialization report the conversion error if the column is read.
      return true;
    }
    for (Filter filter : filters) {
      if (evaluate(filter, values) == Result.FALSE) {
        return false;
      }
    }
    return true;
  }

  private static boolean isSupported(final Filter filter) {
    if (filter instanceof EqualTo
        || filter instanceof EqualNullSafe
        || filter instanceof GreaterThan
        || filter instanceof GreaterThanOrEqual
        || filter instanceof LessThan
        || filter instanceof LessThanOrEqual
        || filter instanceof In
        || filter instanceof IsNull
        || filter instanceof IsNotNull) {
      return true;
    }
    if (filter instanceof And) {
      final And and = (And) filter;
      return isSupported(and.left()) && isSupported(and.right());
    }
    if (filter instanceof Or) {
      final Or or = (Or) filter;
      return isSupported(or.left()) && isSupported(or.right());
    }
    if (filter instanceof Not) {
      return isSupported(((Not) filter).child());
    }
    return false;
  }

  private static Result evaluate(final Filter filter, final Map<String, Object> values) {
    if (filter instanceof EqualTo) {
      final EqualTo equal = (EqualTo) filter;
      return compare(values.get(equal.attribute()), equal.value(), Comparison.EQ, false);
    }
    if (filter instanceof EqualNullSafe) {
      final EqualNullSafe equal = (EqualNullSafe) filter;
      return compare(values.get(equal.attribute()), equal.value(), Comparison.EQ, true);
    }
    if (filter instanceof GreaterThan) {
      final GreaterThan value = (GreaterThan) filter;
      return compare(values.get(value.attribute()), value.value(), Comparison.GT, false);
    }
    if (filter instanceof GreaterThanOrEqual) {
      final GreaterThanOrEqual value = (GreaterThanOrEqual) filter;
      return compare(values.get(value.attribute()), value.value(), Comparison.GE, false);
    }
    if (filter instanceof LessThan) {
      final LessThan value = (LessThan) filter;
      return compare(values.get(value.attribute()), value.value(), Comparison.LT, false);
    }
    if (filter instanceof LessThanOrEqual) {
      final LessThanOrEqual value = (LessThanOrEqual) filter;
      return compare(values.get(value.attribute()), value.value(), Comparison.LE, false);
    }
    if (filter instanceof In) {
      final In in = (In) filter;
      final Object actual = values.get(in.attribute());
      if (actual == null) {
        return Result.FALSE;
      }
      for (Object candidate : in.values()) {
        if (compare(actual, candidate, Comparison.EQ, false) == Result.TRUE) {
          return Result.TRUE;
        }
      }
      return Result.FALSE;
    }
    if (filter instanceof IsNull) {
      return values.get(((IsNull) filter).attribute()) == null ? Result.TRUE : Result.FALSE;
    }
    if (filter instanceof IsNotNull) {
      return values.get(((IsNotNull) filter).attribute()) == null ? Result.FALSE : Result.TRUE;
    }
    if (filter instanceof And) {
      final And and = (And) filter;
      return evaluate(and.left(), values).and(evaluate(and.right(), values));
    }
    if (filter instanceof Or) {
      final Or or = (Or) filter;
      return evaluate(or.left(), values).or(evaluate(or.right(), values));
    }
    if (filter instanceof Not) {
      return evaluate(((Not) filter).child(), values).not();
    }
    return Result.UNKNOWN;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static Result compare(
      final Object actual,
      final Object literal,
      final Comparison comparison,
      final boolean nullSafe) {
    if (actual == null || literal == null) {
      if (nullSafe && comparison == Comparison.EQ) {
        return actual == null && literal == null ? Result.TRUE : Result.FALSE;
      }
      return Result.FALSE;
    }
    final Object normalized = normalizeLiteral(literal, actual);
    if (normalized == null) {
      return Result.UNKNOWN;
    }
    final int result;
    if (actual instanceof Comparable && actual.getClass().isInstance(normalized)) {
      result = ((Comparable) actual).compareTo(normalized);
    } else {
      result = actual.toString().compareTo(normalized.toString());
    }
    switch (comparison) {
      case EQ:
        return result == 0 ? Result.TRUE : Result.FALSE;
      case GT:
        return result > 0 ? Result.TRUE : Result.FALSE;
      case GE:
        return result >= 0 ? Result.TRUE : Result.FALSE;
      case LT:
        return result < 0 ? Result.TRUE : Result.FALSE;
      case LE:
        return result <= 0 ? Result.TRUE : Result.FALSE;
      default:
        return Result.UNKNOWN;
    }
  }

  private static Object normalizeLiteral(final Object literal, final Object actual) {
    try {
      if (actual instanceof String) {
        return literal.toString();
      }
      if (actual instanceof Boolean) {
        return literal instanceof Boolean ? literal : Boolean.valueOf(literal.toString());
      }
      if (actual instanceof BigDecimal) {
        return literal instanceof BigDecimal
            ? literal
            : new BigDecimal(literal.toString());
      }
      if (actual instanceof LocalDate) {
        if (literal instanceof java.sql.Date) {
          return ((java.sql.Date) literal).toLocalDate();
        }
        return LocalDate.parse(literal.toString());
      }
      if (actual instanceof Byte) {
        return literal instanceof Number
            ? ((Number) literal).byteValue()
            : Byte.valueOf(literal.toString());
      }
      if (actual instanceof Short) {
        return literal instanceof Number
            ? ((Number) literal).shortValue()
            : Short.valueOf(literal.toString());
      }
      if (actual instanceof Integer) {
        return literal instanceof Number
            ? ((Number) literal).intValue()
            : Integer.valueOf(literal.toString());
      }
      if (actual instanceof Long) {
        return literal instanceof Number
            ? ((Number) literal).longValue()
            : Long.valueOf(literal.toString());
      }
      if (actual instanceof Float) {
        return literal instanceof Number
            ? ((Number) literal).floatValue()
            : Float.valueOf(literal.toString());
      }
      if (actual instanceof Double) {
        return literal instanceof Number
            ? ((Number) literal).doubleValue()
            : Double.valueOf(literal.toString());
      }
      return literal;
    } catch (RuntimeException e) {
      return null;
    }
  }

  private static Object parseComparable(final String rawValue, final DataType dataType) {
    if (isNull(rawValue)) {
      return null;
    }
    if (dataType.equals(DataTypes.StringType)) {
      return rawValue;
    }
    if (dataType.equals(DataTypes.BooleanType)) {
      if ("true".equalsIgnoreCase(rawValue)) {
        return Boolean.TRUE;
      }
      if ("false".equalsIgnoreCase(rawValue)) {
        return Boolean.FALSE;
      }
      throw new IllegalArgumentException("Invalid Boolean partition value: " + rawValue);
    }
    if (dataType.equals(DataTypes.ByteType)) {
      return Byte.valueOf(rawValue);
    }
    if (dataType.equals(DataTypes.ShortType)) {
      return Short.valueOf(rawValue);
    }
    if (dataType.equals(DataTypes.IntegerType)) {
      return Integer.valueOf(rawValue);
    }
    if (dataType.equals(DataTypes.LongType)) {
      return Long.valueOf(rawValue);
    }
    if (dataType.equals(DataTypes.FloatType)) {
      return Float.valueOf(rawValue);
    }
    if (dataType.equals(DataTypes.DoubleType)) {
      return Double.valueOf(rawValue);
    }
    if (dataType instanceof DecimalType) {
      return new BigDecimal(rawValue);
    }
    if (dataType.equals(DataTypes.DateType)) {
      return LocalDate.parse(rawValue);
    }
    return rawValue;
  }

  private static Object parseInternal(final String rawValue, final DataType dataType) {
    final Object comparable = parseComparable(rawValue, dataType);
    if (comparable == null) {
      return null;
    }
    if (dataType.equals(DataTypes.StringType)) {
      return UTF8String.fromString((String) comparable);
    }
    if (dataType.equals(DataTypes.DateType)) {
      return Math.toIntExact(((LocalDate) comparable).toEpochDay());
    }
    if (dataType instanceof DecimalType) {
      final DecimalType decimalType = (DecimalType) dataType;
      return Decimal.apply((BigDecimal) comparable, decimalType.precision(), decimalType.scale());
    }
    return comparable;
  }

  private static boolean isNull(final String rawValue) {
    return rawValue == null || HIVE_DEFAULT_PARTITION.equals(rawValue);
  }

  private static StructField findField(final StructType schema, final String name) {
    for (StructField field : schema.fields()) {
      if (field.name().equals(name)) {
        return field;
      }
    }
    return null;
  }

  private enum Comparison {
    EQ,
    GT,
    GE,
    LT,
    LE
  }

  private enum Result {
    TRUE,
    FALSE,
    UNKNOWN;

    private Result and(final Result other) {
      if (this == FALSE || other == FALSE) {
        return FALSE;
      }
      if (this == TRUE && other == TRUE) {
        return TRUE;
      }
      return UNKNOWN;
    }

    private Result or(final Result other) {
      if (this == TRUE || other == TRUE) {
        return TRUE;
      }
      if (this == FALSE && other == FALSE) {
        return FALSE;
      }
      return UNKNOWN;
    }

    private Result not() {
      if (this == TRUE) {
        return FALSE;
      }
      if (this == FALSE) {
        return TRUE;
      }
      return UNKNOWN;
    }
  }
}
