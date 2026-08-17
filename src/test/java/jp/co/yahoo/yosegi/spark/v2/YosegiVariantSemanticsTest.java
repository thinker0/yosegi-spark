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

import java.util.Collections;

import org.apache.spark.sql.connector.read.VariantExtraction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Regression tests for the semantic boundary of Variant extraction pushdown. */
public class YosegiVariantSemanticsTest {

  @Test
  public void T_exactType_IsSafeForBothFailOnErrorModes() {
    final YosegiScanBuilder builder = builderWithLongId();
    Assertions.assertArrayEquals(
        new boolean[] {true},
        builder.pushVariantExtractions(
            new VariantExtraction[] {extraction("$.id", DataTypes.LongType, true, "UTC")}));

    Assertions.assertArrayEquals(
        new boolean[] {true},
        builder.pushVariantExtractions(
            new VariantExtraction[] {extraction("$.id", DataTypes.LongType, false, "UTC")}));
  }

  @Test
  public void T_castAndOverflowSemantics_RemainOwnedBySpark() {
    final YosegiScanBuilder builder = builderWithLongId();

    // LONG -> INT can overflow. Rejecting the extraction leaves both variant_get and
    // try_variant_get cast/error behavior to Spark.
    Assertions.assertArrayEquals(
        new boolean[] {false},
        builder.pushVariantExtractions(
            new VariantExtraction[] {extraction("$.id", DataTypes.IntegerType, true, "UTC")}));
    Assertions.assertArrayEquals(
        new boolean[] {false},
        builder.pushVariantExtractions(
            new VariantExtraction[] {extraction("$.id", DataTypes.IntegerType, false, "UTC")}));
  }

  @Test
  public void T_missingField_RemainsOwnedBySpark() {
    final YosegiScanBuilder builder = builderWithLongId();
    Assertions.assertArrayEquals(
        new boolean[] {false},
        builder.pushVariantExtractions(
            new VariantExtraction[] {extraction("$.missing", DataTypes.LongType, false, "UTC")}));
  }

  @Test
  public void T_nestedCast_RemainsOwnedBySpark() {
    final StructType logical = new StructType().add("v", DataTypes.VariantType, true);
    final StructType physical =
        new StructType()
            .add(
                "v",
                new StructType()
                    .add("user", new StructType().add("id", DataTypes.LongType, true), true),
                true);
    final YosegiScanBuilder builder = builder(logical, physical);

    Assertions.assertArrayEquals(
        new boolean[] {false},
        builder.pushVariantExtractions(
            new VariantExtraction[] {
              extraction("$.user.id", DataTypes.IntegerType, false, "UTC")
            }));
  }

  @Test
  public void T_timestampExactType_DoesNotPerformTimezoneCastInYosegi() {
    final StructType logical = new StructType().add("v", DataTypes.VariantType, true);
    final StructType physical =
        new StructType()
            .add("v", new StructType().add("ts", DataTypes.TimestampType, true), true);
    final YosegiScanBuilder builder = builder(logical, physical);

    // The physical and requested Spark types are identical, so no timezone/cast operation is
    // introduced by the data source. The metadata timezone remains Spark metadata only.
    Assertions.assertArrayEquals(
        new boolean[] {true},
        builder.pushVariantExtractions(
            new VariantExtraction[] {
              extraction("$.ts", DataTypes.TimestampType, true, "Asia/Tokyo")
            }));
  }

  @Test
  public void T_timestampTypeChange_RemainsOwnedBySpark() {
    final StructType logical = new StructType().add("v", DataTypes.VariantType, true);
    final StructType physical =
        new StructType()
            .add("v", new StructType().add("ts", DataTypes.TimestampType, true), true);
    final YosegiScanBuilder builder = builder(logical, physical);

    Assertions.assertArrayEquals(
        new boolean[] {false},
        builder.pushVariantExtractions(
            new VariantExtraction[] {
              extraction("$.ts", DataTypes.TimestampNTZType, false, "Asia/Tokyo")
            }));
  }

  @Test
  public void T_mixedSafeAndUnsafeExtractions_PreserveCurrentAllOrNoneContract() {
    final YosegiScanBuilder builder = builderWithLongId();
    final boolean[] result =
        builder.pushVariantExtractions(
            new VariantExtraction[] {
              extraction("$.id", DataTypes.LongType, true, "UTC"),
              extraction("$.id", DataTypes.IntegerType, false, "UTC")
            });
    Assertions.assertArrayEquals(new boolean[] {false, false}, result);
  }

  private static YosegiScanBuilder builderWithLongId() {
    final StructType logical = new StructType().add("v", DataTypes.VariantType, true);
    final StructType physical =
        new StructType().add("v", new StructType().add("id", DataTypes.LongType, true), true);
    return builder(logical, physical);
  }

  private static YosegiScanBuilder builder(
      final StructType logical, final StructType physical) {
    return new YosegiScanBuilder(
        logical, physical, new CaseInsensitiveStringMap(Collections.emptyMap()));
  }

  private static VariantExtraction extraction(
      final String path,
      final DataType expectedType,
      final boolean failOnError,
      final String timeZoneId) {
    final Metadata nested =
        new MetadataBuilder()
            .putString(VariantStructUtil.PATH_METADATA_KEY, path)
            .putBoolean(VariantStructUtil.FAIL_ON_ERROR_METADATA_KEY, failOnError)
            .putString(VariantStructUtil.TIME_ZONE_ID_METADATA_KEY, timeZoneId)
            .build();
    final Metadata metadata =
        new MetadataBuilder()
            .putMetadata(VariantStructUtil.VARIANT_METADATA_KEY, nested)
            .build();
    return new TestExtraction(expectedType, metadata);
  }

  private static final class TestExtraction implements VariantExtraction {
    private final DataType expectedType;
    private final Metadata metadata;

    TestExtraction(final DataType expectedType, final Metadata metadata) {
      this.expectedType = expectedType;
      this.metadata = metadata;
    }

    @Override
    public String[] columnName() {
      return new String[] {"v"};
    }

    @Override
    public DataType expectedDataType() {
      return expectedType;
    }

    @Override
    public Metadata metadata() {
      return metadata;
    }
  }
}
