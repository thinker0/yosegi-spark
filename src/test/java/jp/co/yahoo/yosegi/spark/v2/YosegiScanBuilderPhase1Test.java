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

import org.apache.spark.sql.connector.read.VariantExtraction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class YosegiScanBuilderPhase1Test {

  @Test
  public void T_pushVariantExtractions_AcceptsOnlyExactPhysicalType() {
    final StructType logicalSchema =
        new StructType()
            .add("row_id", DataTypes.LongType, false)
            .add("v", DataTypes.VariantType, true);
    final StructType physicalSchema =
        new StructType()
            .add("row_id", DataTypes.LongType, false)
            .add(
                "v",
                new StructType()
                    .add("id", DataTypes.LongType, true)
                    .add("name", DataTypes.StringType, true),
                true);
    final YosegiScanBuilder builder =
        new YosegiScanBuilder(
            logicalSchema,
            physicalSchema,
            new CaseInsensitiveStringMap(Collections.emptyMap()));

    final boolean[] accepted =
        builder.pushVariantExtractions(
            new VariantExtraction[] {
              new TestVariantExtraction("v", "$.id", DataTypes.LongType, true)
            });
    Assertions.assertArrayEquals(new boolean[] {true}, accepted);

    final boolean[] castRequired =
        builder.pushVariantExtractions(
            new VariantExtraction[] {
              new TestVariantExtraction("v", "$.id", DataTypes.IntegerType, true)
            });
    Assertions.assertArrayEquals(new boolean[] {false}, castRequired);
  }

  @Test
  public void T_pushVariantExtractions_AcceptsNestedPathWithExactPhysicalType() {
    final StructType logicalSchema =
        new StructType().add("v", DataTypes.VariantType, true);
    final StructType physicalSchema =
        new StructType()
            .add(
                "v",
                new StructType()
                    .add(
                        "user",
                        new StructType().add("id", DataTypes.LongType, true),
                        true),
                true);
    final YosegiScanBuilder builder =
        new YosegiScanBuilder(
            logicalSchema,
            physicalSchema,
            new CaseInsensitiveStringMap(Collections.emptyMap()));

    final boolean[] accepted =
        builder.pushVariantExtractions(
            new VariantExtraction[] {
              new TestVariantExtraction("v", "$.user.id", DataTypes.LongType, false)
            });
    Assertions.assertArrayEquals(new boolean[] {true}, accepted);
  }

  @Test
  public void T_pushVariantExtractions_RejectsNestedPathWhenCastIsRequired() {
    final StructType logicalSchema =
        new StructType().add("v", DataTypes.VariantType, true);
    final StructType physicalSchema =
        new StructType()
            .add(
                "v",
                new StructType()
                    .add(
                        "user",
                        new StructType().add("id", DataTypes.LongType, true),
                        true),
                true);
    final YosegiScanBuilder builder =
        new YosegiScanBuilder(
            logicalSchema,
            physicalSchema,
            new CaseInsensitiveStringMap(Collections.emptyMap()));

    final boolean[] accepted =
        builder.pushVariantExtractions(
            new VariantExtraction[] {
              new TestVariantExtraction("v", "$.user.id", DataTypes.IntegerType, false)
            });
    Assertions.assertArrayEquals(new boolean[] {false}, accepted);
  }

  static Metadata extractionMetadata(
      final String path, final boolean failOnError) {
    final Metadata variantMetadata =
        new MetadataBuilder()
            .putString(VariantStructUtil.PATH_METADATA_KEY, path)
            .putBoolean(VariantStructUtil.FAIL_ON_ERROR_METADATA_KEY, failOnError)
            .putString(VariantStructUtil.TIME_ZONE_ID_METADATA_KEY, "UTC")
            .build();
    return new MetadataBuilder()
        .putMetadata(VariantStructUtil.VARIANT_METADATA_KEY, variantMetadata)
        .build();
  }

  static final class TestVariantExtraction implements VariantExtraction {
    private final String[] columnName;
    private final DataType expectedDataType;
    private final Metadata metadata;

    TestVariantExtraction(
        final String columnName,
        final String path,
        final DataType expectedDataType,
        final boolean failOnError) {
      this.columnName = new String[] {columnName};
      this.expectedDataType = expectedDataType;
      this.metadata = extractionMetadata(path, failOnError);
    }

    @Override
    public String[] columnName() {
      return columnName.clone();
    }

    @Override
    public DataType expectedDataType() {
      return expectedDataType;
    }

    @Override
    public Metadata metadata() {
      return metadata;
    }
  }
}
