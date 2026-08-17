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
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.util.SerializableConfiguration;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class YosegiScanBuilderTest {
  @Test
  void T_pushVariantExtraction_DirectField() {
    final YosegiScanBuilder builder = createBuilder();
    final boolean[] result =
        builder.pushVariantExtractions(
            new VariantExtraction[] {new TestVariantExtraction("v", "$.id", DataTypes.IntegerType)});
    assertTrue(result[0]);
  }

  @Test
  void T_pushVariantExtraction_NestedFieldIsAccepted() {
    final YosegiScanBuilder builder = createBuilder();
    final boolean[] result =
        builder.pushVariantExtractions(
            new VariantExtraction[] {
              new TestVariantExtraction("v", "$.user.id", DataTypes.IntegerType)
            });
    assertTrue(result[0]);
  }

  @Test
  void T_pushVariantExtraction_DirectAndNestedRequestAreAcceptedAsOneBatch() {
    final YosegiScanBuilder builder = createBuilder();
    final boolean[] result =
        builder.pushVariantExtractions(
            new VariantExtraction[] {
              new TestVariantExtraction("v", "$.id", DataTypes.IntegerType),
              new TestVariantExtraction("v", "$.user.id", DataTypes.IntegerType)
            });
    assertTrue(result[0]);
    assertTrue(result[1]);
  }

  @Test
  void T_scanReadSchema_RewritesVariantToOrdinalStruct() {
    final StructType tableSchema = createVariantSchema();
    final VariantExtraction extraction =
        new TestVariantExtraction("v", "$.id", DataTypes.IntegerType);
    final YosegiScan scan =
        new YosegiScan(
            tableSchema,
            tableSchema,
            new Filter[0],
            new VariantExtraction[] {extraction},
            Map.of("path", "/tmp/not-used"),
            new SerializableConfiguration(new Configuration(false)));
    final StructField variantField = scan.readSchema().fields()[0];
    assertTrue(variantField.dataType() instanceof StructType);
    final StructField extractedField = ((StructType) variantField.dataType()).fields()[0];
    assertEquals("0", extractedField.name());
    assertEquals(DataTypes.IntegerType, extractedField.dataType());
    assertEquals("id", VariantStructUtil.getDirectFieldName(extractedField.metadata()));
  }

  private static YosegiScanBuilder createBuilder() {
    return new YosegiScanBuilder(
        createVariantSchema(),
        new CaseInsensitiveStringMap(Map.of("path", "/tmp/not-used")));
  }

  private static StructType createVariantSchema() {
    return new StructType(
        new StructField[] {
          new StructField("v", DataTypes.VariantType, true, Metadata.empty())
        });
  }

  private static final class TestVariantExtraction implements VariantExtraction {
    private final String[] columnName;
    private final DataType expectedDataType;
    private final Metadata metadata;

    private TestVariantExtraction(
        final String columnName, final String path, final DataType expectedDataType) {
      this.columnName = new String[] {columnName};
      this.expectedDataType = expectedDataType;
      this.metadata = createVariantMetadata(path);
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
