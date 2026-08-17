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
package jp.co.yahoo.yosegi.spark.inmemory.loader;

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.VariantType;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.sql.vectorized.ColumnarRow;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.types.variant.Variant;
import org.apache.spark.types.variant.VariantBuilder;
import org.apache.spark.types.variant.VariantBuilder.FieldEntry;
import org.apache.spark.unsafe.types.VariantVal;

import java.util.ArrayList;

/** Converts a Spark columnar value into the public Spark Variant binary representation. */
final class SparkVariantValueWriter {

  private SparkVariantValueWriter() {}

  static Variant write(final StructType type, final ColumnarRow row) {
    final VariantBuilder builder = new VariantBuilder(false);
    appendStruct(builder, type, row);
    return builder.result();
  }

  static Variant write(
      final DataType type, final ColumnVector vector, final int rowId) {
    final VariantBuilder builder = new VariantBuilder(false);
    appendColumnValue(builder, vector, rowId, type);
    return builder.result();
  }

  static Variant writeNull() {
    final VariantBuilder builder = new VariantBuilder(false);
    builder.appendNull();
    return builder.result();
  }

  static Variant writeEmptyArray() {
    final VariantBuilder builder = new VariantBuilder(false);
    builder.finishWritingArray(builder.getWritePos(), new ArrayList<>());
    return builder.result();
  }


  static Variant writeVariantArray(
      final ColumnVector elements, final int startOrdinal, final int length) {
    final VariantBuilder builder = new VariantBuilder(false);
    final int start = builder.getWritePos();
    final ArrayList<Integer> offsets = new ArrayList<>(length);
    for (int i = 0; i < length; i++) {
      offsets.add(builder.getWritePos() - start);
      final int ordinal = startOrdinal + i;
      if (elements.isNullAt(ordinal)) {
        builder.appendNull();
      } else {
        appendVariant(builder, elements.getVariant(ordinal));
      }
    }
    builder.finishWritingArray(start, offsets);
    return builder.result();
  }

  static Variant writeVariantObject(
      final java.util.List<String> names,
      final java.util.List<? extends ColumnVector> children,
      final int rowId) {
    if (names.size() != children.size()) {
      throw new IllegalArgumentException("Variant object names and children must have equal size.");
    }
    final VariantBuilder builder = new VariantBuilder(false);
    final int start = builder.getWritePos();
    final ArrayList<FieldEntry> fields = new ArrayList<>(names.size());
    for (int i = 0; i < names.size(); i++) {
      final String name = names.get(i);
      final int id = builder.addKey(name);
      fields.add(new FieldEntry(name, id, builder.getWritePos() - start));
      final ColumnVector child = children.get(i);
      if (child.isNullAt(rowId)) {
        builder.appendNull();
      } else {
        appendVariant(builder, child.getVariant(rowId));
      }
    }
    builder.finishWritingObject(start, fields);
    return builder.result();
  }

  static Variant writeEmptyObject() {
    final VariantBuilder builder = new VariantBuilder(false);
    builder.finishWritingObject(builder.getWritePos(), new ArrayList<>());
    return builder.result();
  }

  private static void appendColumnValue(
      final VariantBuilder builder,
      final ColumnVector vector,
      final int rowId,
      final DataType type) {
    if (vector.isNullAt(rowId)) {
      builder.appendNull();
      return;
    }
    if (sameType(type, DataTypes.BooleanType)) {
      builder.appendBoolean(vector.getBoolean(rowId));
    } else if (sameType(type, DataTypes.ByteType)) {
      builder.appendLong(vector.getByte(rowId));
    } else if (sameType(type, DataTypes.ShortType)) {
      builder.appendLong(vector.getShort(rowId));
    } else if (sameType(type, DataTypes.IntegerType)) {
      builder.appendLong(vector.getInt(rowId));
    } else if (sameType(type, DataTypes.LongType)) {
      builder.appendLong(vector.getLong(rowId));
    } else if (sameType(type, DataTypes.FloatType)) {
      builder.appendFloat(vector.getFloat(rowId));
    } else if (sameType(type, DataTypes.DoubleType)) {
      builder.appendDouble(vector.getDouble(rowId));
    } else if (sameType(type, DataTypes.StringType)) {
      builder.appendString(vector.getUTF8String(rowId).toString());
    } else if (sameType(type, DataTypes.BinaryType)) {
      builder.appendBinary(vector.getBinary(rowId));
    } else if (sameType(type, DataTypes.DateType)) {
      builder.appendDate(vector.getInt(rowId));
    } else if (sameType(type, DataTypes.TimestampType)) {
      builder.appendTimestamp(vector.getLong(rowId));
    } else if (sameType(type, DataTypes.TimestampNTZType)) {
      builder.appendTimestampNtz(vector.getLong(rowId));
    } else if (type instanceof DecimalType) {
      final DecimalType decimalType = (DecimalType) type;
      builder.appendDecimal(
          vector.getDecimal(rowId, decimalType.precision(), decimalType.scale())
              .toJavaBigDecimal());
    } else if (type instanceof StructType) {
      appendStruct(builder, (StructType) type, vector.getStruct(rowId));
    } else if (type instanceof ArrayType) {
      appendArray(builder, (ArrayType) type, vector.getArray(rowId));
    } else if (type instanceof MapType) {
      appendMap(builder, (MapType) type, vector.getMap(rowId));
    } else if (type instanceof VariantType) {
      appendVariant(builder, vector.getVariant(rowId));
    } else {
      throw unsupported(type);
    }
  }

  private static void appendStruct(
      final VariantBuilder builder, final StructType type, final ColumnarRow row) {
    final int start = builder.getWritePos();
    final ArrayList<FieldEntry> fields = new ArrayList<>(type.size());
    final StructField[] structFields = type.fields();
    for (int i = 0; i < structFields.length; i++) {
      final StructField field = structFields[i];
      final int id = builder.addKey(field.name());
      fields.add(new FieldEntry(field.name(), id, builder.getWritePos() - start));
      appendRowValue(builder, row, i, field.dataType());
    }
    builder.finishWritingObject(start, fields);
  }

  private static void appendArray(
      final VariantBuilder builder, final ArrayType type, final ColumnarArray array) {
    final int start = builder.getWritePos();
    final ArrayList<Integer> offsets = new ArrayList<>(array.numElements());
    for (int i = 0; i < array.numElements(); i++) {
      offsets.add(builder.getWritePos() - start);
      appendArrayValue(builder, array, i, type.elementType());
    }
    builder.finishWritingArray(start, offsets);
  }

  private static void appendMap(
      final VariantBuilder builder, final MapType type, final ColumnarMap map) {
    if (!sameType(type.keyType(), DataTypes.StringType)) {
      throw new UnsupportedOperationException(
          "Full Variant reconstruction supports only string-keyed maps: " + type);
    }
    final ColumnarArray keys = map.keyArray();
    final ColumnarArray values = map.valueArray();
    final int start = builder.getWritePos();
    final ArrayList<FieldEntry> fields = new ArrayList<>(map.numElements());
    for (int i = 0; i < map.numElements(); i++) {
      if (keys.isNullAt(i)) {
        throw new IllegalArgumentException("Variant object key must not be null.");
      }
      final String key = keys.getUTF8String(i).toString();
      final int id = builder.addKey(key);
      fields.add(new FieldEntry(key, id, builder.getWritePos() - start));
      appendArrayValue(builder, values, i, type.valueType());
    }
    builder.finishWritingObject(start, fields);
  }

  private static void appendRowValue(
      final VariantBuilder builder,
      final ColumnarRow row,
      final int ordinal,
      final DataType type) {
    if (row.isNullAt(ordinal)) {
      builder.appendNull();
      return;
    }
    if (sameType(type, DataTypes.BooleanType)) {
      builder.appendBoolean(row.getBoolean(ordinal));
    } else if (sameType(type, DataTypes.ByteType)) {
      builder.appendLong(row.getByte(ordinal));
    } else if (sameType(type, DataTypes.ShortType)) {
      builder.appendLong(row.getShort(ordinal));
    } else if (sameType(type, DataTypes.IntegerType)) {
      builder.appendLong(row.getInt(ordinal));
    } else if (sameType(type, DataTypes.LongType)) {
      builder.appendLong(row.getLong(ordinal));
    } else if (sameType(type, DataTypes.FloatType)) {
      builder.appendFloat(row.getFloat(ordinal));
    } else if (sameType(type, DataTypes.DoubleType)) {
      builder.appendDouble(row.getDouble(ordinal));
    } else if (sameType(type, DataTypes.StringType)) {
      builder.appendString(row.getUTF8String(ordinal).toString());
    } else if (sameType(type, DataTypes.BinaryType)) {
      builder.appendBinary(row.getBinary(ordinal));
    } else if (sameType(type, DataTypes.DateType)) {
      builder.appendDate(row.getInt(ordinal));
    } else if (sameType(type, DataTypes.TimestampType)) {
      builder.appendTimestamp(row.getLong(ordinal));
    } else if (sameType(type, DataTypes.TimestampNTZType)) {
      builder.appendTimestampNtz(row.getLong(ordinal));
    } else if (type instanceof DecimalType) {
      final DecimalType decimalType = (DecimalType) type;
      builder.appendDecimal(
          row.getDecimal(ordinal, decimalType.precision(), decimalType.scale()).toJavaBigDecimal());
    } else if (type instanceof StructType) {
      appendStruct(builder, (StructType) type, row.getStruct(ordinal, ((StructType) type).size()));
    } else if (type instanceof ArrayType) {
      appendArray(builder, (ArrayType) type, row.getArray(ordinal));
    } else if (type instanceof MapType) {
      appendMap(builder, (MapType) type, row.getMap(ordinal));
    } else if (type instanceof VariantType) {
      appendVariant(builder, row.getVariant(ordinal));
    } else {
      throw unsupported(type);
    }
  }

  private static void appendArrayValue(
      final VariantBuilder builder,
      final ColumnarArray array,
      final int ordinal,
      final DataType type) {
    if (array.isNullAt(ordinal)) {
      builder.appendNull();
      return;
    }
    if (sameType(type, DataTypes.BooleanType)) {
      builder.appendBoolean(array.getBoolean(ordinal));
    } else if (sameType(type, DataTypes.ByteType)) {
      builder.appendLong(array.getByte(ordinal));
    } else if (sameType(type, DataTypes.ShortType)) {
      builder.appendLong(array.getShort(ordinal));
    } else if (sameType(type, DataTypes.IntegerType)) {
      builder.appendLong(array.getInt(ordinal));
    } else if (sameType(type, DataTypes.LongType)) {
      builder.appendLong(array.getLong(ordinal));
    } else if (sameType(type, DataTypes.FloatType)) {
      builder.appendFloat(array.getFloat(ordinal));
    } else if (sameType(type, DataTypes.DoubleType)) {
      builder.appendDouble(array.getDouble(ordinal));
    } else if (sameType(type, DataTypes.StringType)) {
      builder.appendString(array.getUTF8String(ordinal).toString());
    } else if (sameType(type, DataTypes.BinaryType)) {
      builder.appendBinary(array.getBinary(ordinal));
    } else if (sameType(type, DataTypes.DateType)) {
      builder.appendDate(array.getInt(ordinal));
    } else if (sameType(type, DataTypes.TimestampType)) {
      builder.appendTimestamp(array.getLong(ordinal));
    } else if (sameType(type, DataTypes.TimestampNTZType)) {
      builder.appendTimestampNtz(array.getLong(ordinal));
    } else if (type instanceof DecimalType) {
      final DecimalType decimalType = (DecimalType) type;
      builder.appendDecimal(
          array
              .getDecimal(ordinal, decimalType.precision(), decimalType.scale())
              .toJavaBigDecimal());
    } else if (type instanceof StructType) {
      appendStruct(
          builder, (StructType) type, array.getStruct(ordinal, ((StructType) type).size()));
    } else if (type instanceof ArrayType) {
      appendArray(builder, (ArrayType) type, array.getArray(ordinal));
    } else if (type instanceof MapType) {
      appendMap(builder, (MapType) type, array.getMap(ordinal));
    } else if (type instanceof VariantType) {
      appendVariant(builder, array.getVariant(ordinal));
    } else {
      throw unsupported(type);
    }
  }

  private static void appendVariant(
      final VariantBuilder builder, final VariantVal variantValue) {
    builder.appendVariant(new Variant(variantValue.getValue(), variantValue.getMetadata()));
  }

  private static boolean sameType(final DataType left, final DataType right) {
    return left.getClass() == right.getClass();
  }

  private static UnsupportedOperationException unsupported(final DataType type) {
    return new UnsupportedOperationException(
        "Unsupported datatype in full Variant reconstruction: " + type);
  }
}
