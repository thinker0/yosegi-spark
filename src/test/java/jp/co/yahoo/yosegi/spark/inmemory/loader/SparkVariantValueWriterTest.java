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

import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.types.variant.Variant;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.ZoneOffset;

public class SparkVariantValueWriterTest {
  @Test
  public void T_write_RebuildsSparkVariantValueAndMetadata() {
    final StructType type =
        new StructType()
            .add("id", DataTypes.LongType, true)
            .add("name", DataTypes.StringType, true);
    final WritableColumnVector vector = new OnHeapColumnVector(1, type);
    try {
      vector.putNotNull(0);
      vector.getChild(0).putLong(0, 7L);
      vector.getChild(1).putByteArray(0, UTF8String.fromString("alice").getBytes());
      final Variant variant = SparkVariantValueWriter.write(type, vector.getStruct(0));

      Assertions.assertEquals(7L, variant.getFieldByKey("id").getLong());
      Assertions.assertEquals("alice", variant.getFieldByKey("name").getString());
      Assertions.assertTrue(variant.getValue().length > 0);
      Assertions.assertTrue(variant.getMetadata().length > 0);
    } finally {
      vector.close();
    }
  }

  @Test
  public void T_write_RebuildsDecimalDateAndTimestampTypes() {
    final DecimalType decimalType = DataTypes.createDecimalType(12, 3);
    final StructType type =
        new StructType()
            .add("decimal_value", decimalType, true)
            .add("date_value", DataTypes.DateType, true)
            .add("timestamp_value", DataTypes.TimestampType, true)
            .add("timestamp_ntz_value", DataTypes.TimestampNTZType, true);
    final WritableColumnVector vector = new OnHeapColumnVector(1, type);
    try {
      vector.putNotNull(0);
      vector.getChild(0).putDecimal(
          0, Decimal.apply(new BigDecimal("123456.789")), decimalType.precision());
      // Spark DateType is epoch days.
      vector.getChild(1).putInt(0, 0);
      // Spark TimestampType/TimestampNTZType columnar representation is microseconds.
      vector.getChild(2).putLong(0, 0L);
      vector.getChild(3).putLong(0, 0L);

      final Variant variant = SparkVariantValueWriter.write(type, vector.getStruct(0));

      Assertions.assertEquals(
          new BigDecimal("123456.789"),
          variant.getFieldByKey("decimal_value").getDecimalWithOriginalScale());
      Assertions.assertEquals(
          "\"1970-01-01\"",
          variant.getFieldByKey("date_value").toJson(ZoneOffset.UTC));
      Assertions.assertEquals(
          "\"1970-01-01 00:00:00+00:00\"",
          variant.getFieldByKey("timestamp_value").toJson(ZoneOffset.UTC));
      Assertions.assertEquals(
          "\"1970-01-01 00:00:00\"",
          variant.getFieldByKey("timestamp_ntz_value").toJson(ZoneOffset.UTC));
    } finally {
      vector.close();
    }
  }

  @Test
  public void T_write_PreservesTimestampMicrosAndDateBeforeEpoch() {
    final StructType type =
        new StructType()
            .add("date_value", DataTypes.DateType, true)
            .add("timestamp_value", DataTypes.TimestampType, true)
            .add("timestamp_ntz_value", DataTypes.TimestampNTZType, true);
    final WritableColumnVector vector = new OnHeapColumnVector(1, type);
    try {
      vector.putNotNull(0);
      vector.getChild(0).putInt(0, -1);
      vector.getChild(1).putLong(0, 1_234_567L);
      vector.getChild(2).putLong(0, -1L);

      final Variant variant = SparkVariantValueWriter.write(type, vector.getStruct(0));

      Assertions.assertEquals(
          "\"1969-12-31\"",
          variant.getFieldByKey("date_value").toJson(ZoneOffset.UTC));
      Assertions.assertEquals(
          "\"1970-01-01 00:00:01.234567+00:00\"",
          variant.getFieldByKey("timestamp_value").toJson(ZoneOffset.UTC));
      Assertions.assertEquals(
          "\"1969-12-31 23:59:59.999999\"",
          variant.getFieldByKey("timestamp_ntz_value").toJson(ZoneOffset.UTC));
    } finally {
      vector.close();
    }
  }

  @Test
  public void T_write_PreservesNonFiniteNumbersAndBinary() {
    final StructType type =
        new StructType()
            .add("nan", DataTypes.FloatType, true)
            .add("positive_infinity", DataTypes.DoubleType, true)
            .add("negative_infinity", DataTypes.DoubleType, true)
            .add("binary", DataTypes.BinaryType, true)
            .add("empty_binary", DataTypes.BinaryType, true);
    final WritableColumnVector vector = new OnHeapColumnVector(1, type);
    try {
      vector.putNotNull(0);
      vector.getChild(0).putFloat(0, Float.NaN);
      vector.getChild(1).putDouble(0, Double.POSITIVE_INFINITY);
      vector.getChild(2).putDouble(0, Double.NEGATIVE_INFINITY);
      vector.getChild(3).putByteArray(0, new byte[] {0, 1, 2, (byte) 0xff});
      vector.getChild(4).putByteArray(0, new byte[0]);

      final Variant variant = SparkVariantValueWriter.write(type, vector.getStruct(0));

      Assertions.assertTrue(Float.isNaN(variant.getFieldByKey("nan").getFloat()));
      Assertions.assertEquals(
          Double.POSITIVE_INFINITY,
          variant.getFieldByKey("positive_infinity").getDouble());
      Assertions.assertEquals(
          Double.NEGATIVE_INFINITY,
          variant.getFieldByKey("negative_infinity").getDouble());
      Assertions.assertArrayEquals(
          new byte[] {0, 1, 2, (byte) 0xff},
          variant.getFieldByKey("binary").getBinary());
      Assertions.assertArrayEquals(
          new byte[0],
          variant.getFieldByKey("empty_binary").getBinary());
    } finally {
      vector.close();
    }
  }
}
