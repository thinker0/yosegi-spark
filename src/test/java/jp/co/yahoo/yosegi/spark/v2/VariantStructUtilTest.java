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

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VariantStructUtilTest {
  @Test
  void T_isVariantStruct() {
    final StructType type =
        new StructType(
            new StructField[] {
              new StructField(
                  "0", DataTypes.IntegerType, true, createVariantMetadata("$.id"))
            });
    assertTrue(VariantStructUtil.isVariantStruct(type));
    assertEquals("id", VariantStructUtil.getDirectFieldName(type.fields()[0].metadata()));
  }

  @Test
  void T_directPathMetadataWithoutSparkWrapperIsNotVariantStruct() {
    final StructType type =
        new StructType(
            new StructField[] {
              new StructField(
                  "0",
                  DataTypes.IntegerType,
                  true,
                  new MetadataBuilder().putString("path", "$.id").build())
            });
    assertFalse(VariantStructUtil.isVariantStruct(type));
    assertNull(VariantStructUtil.getDirectFieldName(type.fields()[0].metadata()));
  }

  @Test
  void T_nestedPathIsNotSupported() {
    assertNull(VariantStructUtil.getDirectFieldName("$.user.id"));
  }

  private static Metadata createVariantMetadata(final String path) {
    final Metadata variantMetadata =
        new MetadataBuilder()
            .putString(VariantStructUtil.PATH_METADATA_KEY, path)
            .putBoolean(VariantStructUtil.FAIL_ON_ERROR_METADATA_KEY, true)
            .putString(VariantStructUtil.TIME_ZONE_ID_METADATA_KEY, "UTC")
            .build();
    return new MetadataBuilder()
        .putMetadata(VariantStructUtil.VARIANT_METADATA_KEY, variantMetadata)
        .build();
  }
}
