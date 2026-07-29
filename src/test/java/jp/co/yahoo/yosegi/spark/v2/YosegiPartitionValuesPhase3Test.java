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
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class YosegiPartitionValuesPhase3Test {

  @Test
  void T_toInternalRow_ConvertsStringIntegerAndDate() {
    final StructType schema =
        new StructType()
            .add("date_text", DataTypes.StringType, true)
            .add("hour", DataTypes.IntegerType, true)
            .add("event_date", DataTypes.DateType, true);

    final InternalRow row =
        YosegiPartitionValues.toInternalRow(
            schema, new String[] {"2026-07-29", "10", "2026-07-29"});

    assertEquals("2026-07-29", row.getUTF8String(0).toString());
    assertEquals(10, row.getInt(1));
    assertEquals((int) LocalDate.of(2026, 7, 29).toEpochDay(), row.getInt(2));
  }

  @Test
  void T_matches_EvaluatesPartitionFiltersConservatively() {
    final StructType schema =
        new StructType()
            .add("date", DataTypes.StringType, true)
            .add("hour", DataTypes.IntegerType, true);
    final Map<String, String> values = Map.of("date", "2026-07-29", "hour", "10");

    assertTrue(
        YosegiPartitionValues.matches(
            new Filter[] {new EqualTo("date", "2026-07-29")}, schema, values));
    assertFalse(
        YosegiPartitionValues.matches(
            new Filter[] {new EqualTo("date", "2026-07-30")}, schema, values));
    assertTrue(
        YosegiPartitionValues.matches(
            new Filter[] {
              new And(
                  new In("date", new Object[] {"2026-07-28", "2026-07-29"}),
                  new GreaterThanOrEqual("hour", 10))
            },
            schema,
            values));
    assertTrue(
        YosegiPartitionValues.matches(
            new Filter[] {new EqualTo("hour", 10)},
            schema,
            Map.of("date", "2026-07-29", "hour", "not-an-integer")));
  }
}
