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

import jp.co.yahoo.yosegi.spark.v2.VariantPath;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.types.variant.Variant;
import org.apache.spark.unsafe.types.UTF8String;

import java.time.ZoneOffset;
import java.util.List;

/** Evaluates a pushed Variant path against a reconstructed Variant value. */
final class SparkVariantPathValueWriter {
  private SparkVariantPathValueWriter() {}

  static void evaluateAndWrite(
      final Variant root,
      final List<VariantPath.Segment> segments,
      final WritableColumnVector output,
      final int rowId) {
    Variant current = root;
    try {
      for (VariantPath.Segment segment : segments) {
        if (current == null || isVariantNull(current)) {
          output.putNull(rowId);
          return;
        }
        if (segment instanceof VariantPath.Field) {
          current = current.getFieldByKey(((VariantPath.Field) segment).name());
        } else if (segment instanceof VariantPath.Index) {
          final int index = ((VariantPath.Index) segment).index();
          if (index < 0 || index >= current.arraySize()) {
            output.putNull(rowId);
            return;
          }
          current = current.getElementAtIndex(index);
        } else {
          output.putNull(rowId);
          return;
        }
      }
      if (current == null || isVariantNull(current)) {
        output.putNull(rowId);
        return;
      }
      writeScalar(current, output, rowId);
    } catch (RuntimeException e) {
      // A path/type mismatch is represented as missing for pushed extraction. Cast/error semantics
      // remain with Spark because Yosegi accepts only exact physical leaf types.
      output.putNull(rowId);
    }
  }

  private static void writeScalar(
      final Variant value, final WritableColumnVector output, final int rowId) {
    final DataType type = output.dataType();
    output.putNotNull(rowId);
    if (same(type, DataTypes.BooleanType)) {
      output.putBoolean(rowId, value.getBoolean());
    } else if (same(type, DataTypes.ByteType)) {
      output.putByte(rowId, (byte) value.getLong());
    } else if (same(type, DataTypes.ShortType)) {
      output.putShort(rowId, (short) value.getLong());
    } else if (same(type, DataTypes.IntegerType) || same(type, DataTypes.DateType)) {
      output.putInt(rowId, (int) value.getLong());
    } else if (same(type, DataTypes.LongType)
        || same(type, DataTypes.TimestampType)
        || same(type, DataTypes.TimestampNTZType)) {
      output.putLong(rowId, value.getLong());
    } else if (same(type, DataTypes.FloatType)) {
      output.putFloat(rowId, value.getFloat());
    } else if (same(type, DataTypes.DoubleType)) {
      output.putDouble(rowId, value.getDouble());
    } else if (same(type, DataTypes.StringType)) {
      output.putByteArray(rowId, UTF8String.fromString(value.getString()).getBytes());
    } else if (same(type, DataTypes.BinaryType)) {
      output.putByteArray(rowId, value.getBinary());
    } else {
      output.putNull(rowId);
    }
  }

  private static boolean isVariantNull(final Variant value) {
    return "null".equals(value.toJson(ZoneOffset.UTC));
  }

  private static boolean same(final DataType left, final DataType right) {
    return left.getClass() == right.getClass();
  }
}
