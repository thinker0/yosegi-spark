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
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.IsNull;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Contract coverage for typed Hive-style partition values used by the V2 reader. */
class YosegiPartitionCoverageTest {

  @Test
  void T_toInternalRow_ConvertsSupportedPartitionTypesAndNulls() {
    final DecimalType decimalType = DataTypes.createDecimalType(12, 2);
    final StructType schema =
        new StructType()
            .add("p_string", DataTypes.StringType, true)
            .add("p_int", DataTypes.IntegerType, true)
            .add("p_long", DataTypes.LongType, true)
            .add("p_boolean", DataTypes.BooleanType, true)
            .add("p_date", DataTypes.DateType, true)
            .add("p_decimal", decimalType, true)
            .add("p_hive_null", DataTypes.StringType, true)
            .add("p_raw_null", DataTypes.StringType, true);

    final InternalRow row =
        YosegiPartitionValues.toInternalRow(
            schema,
            new String[] {
              "tokyo",
              "42",
              "922337203685477000",
              "true",
              "2026-08-12",
              "12345.67",
              YosegiPartitionValues.HIVE_DEFAULT_PARTITION,
              null
            });

    assertEquals("tokyo", row.getUTF8String(0).toString());
    assertEquals(42, row.getInt(1));
    assertEquals(922337203685477000L, row.getLong(2));
    assertTrue(row.getBoolean(3));
    assertEquals((int) LocalDate.of(2026, 8, 12).toEpochDay(), row.getInt(4));
    assertEquals(new BigDecimal("12345.67"), row.getDecimal(5, 12, 2).toJavaBigDecimal());
    assertTrue(row.isNullAt(6));
    assertTrue(row.isNullAt(7));
  }

  @Test
  void T_matches_PrunesTypedPartitionsAndTreatsHiveDefaultAsNull() {
    final DecimalType decimalType = DataTypes.createDecimalType(12, 2);
    final StructType schema =
        new StructType()
            .add("p_long", DataTypes.LongType, true)
            .add("p_boolean", DataTypes.BooleanType, true)
            .add("p_date", DataTypes.DateType, true)
            .add("p_decimal", decimalType, true)
            .add("p_null", DataTypes.StringType, true);
    final Map<String, String> values = new LinkedHashMap<>();
    values.put("p_long", "1000");
    values.put("p_boolean", "true");
    values.put("p_date", "2026-08-12");
    values.put("p_decimal", "123.45");
    values.put("p_null", YosegiPartitionValues.HIVE_DEFAULT_PARTITION);

    assertTrue(
        YosegiPartitionValues.matches(
            new Filter[] {
              new GreaterThan("p_long", 999L),
              new EqualTo("p_boolean", true),
              new EqualTo("p_date", Date.valueOf("2026-08-12")),
              new EqualTo("p_decimal", new BigDecimal("123.45")),
              new IsNull("p_null")
            },
            schema,
            values));

    assertFalse(
        YosegiPartitionValues.matches(
            new Filter[] {new EqualTo("p_boolean", false)}, schema, values));
  }

  @Test
  void T_matches_MalformedTypedValueIsKeptForConservativePruning() {
    final StructType schema =
        new StructType().add("p_boolean", DataTypes.BooleanType, true);

    assertTrue(
        YosegiPartitionValues.matches(
            new Filter[] {new EqualTo("p_boolean", true)},
            schema,
            Map.of("p_boolean", "not-a-boolean")));
  }
}
