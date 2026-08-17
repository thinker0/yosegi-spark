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

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.read.VariantExtraction;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

public class YosegiScanPhase1Test {

  @Test
  public void T_readSchema_UnionsNormalProjectionAndVariantSources() {
    final StructType logicalSchema =
        new StructType()
            .add("row_id", DataTypes.LongType, false)
            .add("unused", DataTypes.StringType, true)
            .add("v", DataTypes.VariantType, true);
    final StructType physicalSchema =
        new StructType()
            .add("row_id", DataTypes.LongType, false)
            .add("unused", DataTypes.StringType, true)
            .add(
                "v",
                new StructType()
                    .add("id", DataTypes.LongType, true)
                    .add("name", DataTypes.StringType, true),
                true);
    final StructType requiredSchema =
        new StructType().add("row_id", DataTypes.LongType, false);
    final VariantExtraction[] extractions =
        new VariantExtraction[] {
          new YosegiScanBuilderPhase1Test.TestVariantExtraction(
              "v", "$.id", DataTypes.LongType, true),
          new YosegiScanBuilderPhase1Test.TestVariantExtraction(
              "v", "$.name", DataTypes.StringType, false)
        };

    final YosegiScan scan =
        new YosegiScan(
            logicalSchema,
            physicalSchema,
            requiredSchema,
            new Filter[0],
            extractions,
            Collections.emptyMap(),
            new SerializableConfiguration(new Configuration(false)));

    Assertions.assertArrayEquals(new String[] {"row_id", "v"}, scan.readSchema().fieldNames());
    final StructType extractionStruct =
        (StructType) scan.readSchema().apply("v").dataType();
    Assertions.assertEquals(DataTypes.LongType, extractionStruct.apply("0").dataType());
    Assertions.assertEquals(DataTypes.StringType, extractionStruct.apply("1").dataType());
    Assertions.assertFalse(Arrays.asList(scan.readSchema().fieldNames()).contains("unused"));
  }

  @Test
  public void T_readSchema_FullVariantProjectionKeepsLogicalVariantType() {
    final StructType logicalSchema =
        new StructType()
            .add("row_id", DataTypes.LongType, false)
            .add("v", DataTypes.VariantType, true);
    final StructType physicalSchema =
        new StructType()
            .add("row_id", DataTypes.LongType, false)
            .add("v", new StructType().add("id", DataTypes.LongType, true), true);
    final StructType requiredSchema =
        new StructType().add("v", DataTypes.VariantType, true);

    final YosegiScan scan =
        new YosegiScan(
            logicalSchema,
            physicalSchema,
            requiredSchema,
            new Filter[0],
            new VariantExtraction[0],
            Collections.emptyMap(),
            new SerializableConfiguration(new Configuration(false)));

    Assertions.assertEquals(DataTypes.VariantType, scan.readSchema().apply("v").dataType());
  }

  @Test
  public void T_readSchema_AcceptedExtractionWinsOverStaleLogicalVariant() {
    final StructType logicalSchema =
        new StructType().add("v", DataTypes.VariantType, true);
    final StructType physicalSchema =
        new StructType()
            .add("v", new StructType().add("id", DataTypes.LongType, true), true);
    final StructType requiredSchema =
        new StructType().add("v", DataTypes.VariantType, true);
    final VariantExtraction[] extractions =
        new VariantExtraction[] {
          new YosegiScanBuilderPhase1Test.TestVariantExtraction(
              "v", "$.id", DataTypes.LongType, true)
        };

    final YosegiScan scan =
        new YosegiScan(
            logicalSchema,
            physicalSchema,
            requiredSchema,
            new Filter[0],
            extractions,
            Collections.emptyMap(),
            new SerializableConfiguration(new Configuration(false)));

    Assertions.assertTrue(scan.readSchema().apply("v").dataType() instanceof StructType);
    final StructType extractionStruct =
        (StructType) scan.readSchema().apply("v").dataType();
    Assertions.assertEquals(DataTypes.LongType, extractionStruct.apply("0").dataType());
  }
}
