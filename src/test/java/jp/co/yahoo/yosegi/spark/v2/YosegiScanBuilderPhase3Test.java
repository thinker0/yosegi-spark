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

import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class YosegiScanBuilderPhase3Test {

  @Test
  void T_pushFilters_SplitsPartitionAndDataConjunctsButKeepsSparkResidual() {
    final StructType physicalSchema = new StructType().add("id", DataTypes.LongType, true);
    final StructType partitionSchema =
        new StructType().add("date", DataTypes.StringType, true);
    final StructType tableSchema =
        new StructType()
            .add("id", DataTypes.LongType, true)
            .add("date", DataTypes.StringType, true);
    final YosegiScanBuilder builder =
        new YosegiScanBuilder(
            tableSchema,
            physicalSchema,
            partitionSchema,
            new CaseInsensitiveStringMap(Map.of()));
    final Filter mixed =
        new And(new EqualTo("date", "2026-07-29"), new EqualTo("id", 1L));

    final Filter[] residual = builder.pushFilters(new Filter[] {mixed});

    assertEquals(1, residual.length);
    assertEquals(1, builder.partitionFilters().length);
    assertEquals(2, builder.pushedFilters().length);
  }
}
