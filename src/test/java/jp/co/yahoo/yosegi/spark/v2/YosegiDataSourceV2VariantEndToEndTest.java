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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

/** End-to-end coverage for the complete DataSource V2 Variant read path. */
class YosegiDataSourceV2VariantEndToEndTest {
  private static final StructType PROFILE_TYPE =
      new StructType().add("name", DataTypes.StringType, true);
  private static final StructType USER_TYPE =
      new StructType().add("profile", PROFILE_TYPE, true);
  private static final StructType ARRAY_USER_TYPE =
      new StructType().add("id", DataTypes.LongType, true);
  private static final ArrayType ITEMS_TYPE =
      DataTypes.createArrayType(DataTypes.LongType, true);
  private static final ArrayType USERS_TYPE =
      DataTypes.createArrayType(ARRAY_USER_TYPE, true);
  private static final StructType PHYSICAL_VARIANT_TYPE =
      new StructType()
          .add("id", DataTypes.LongType, true)
          .add("name", DataTypes.StringType, true)
          .add("user", USER_TYPE, true)
          .add("items", ITEMS_TYPE, true)
          .add("users", USERS_TYPE, true);
  private static final StructType PHYSICAL_SCHEMA =
      new StructType()
          .add("row_id", DataTypes.LongType, false)
          .add("v", PHYSICAL_VARIANT_TYPE, true);

  private static SparkSession spark;
  private Path outputPath;

  @BeforeAll
  static void setUpSpark() {
    spark =
        SparkSession.builder()
            .appName("YosegiDataSourceV2VariantEndToEndTest")
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.variant.pushVariantIntoScan", "true")
            .getOrCreate();
    spark.conf().set("spark.sql.variant.pushVariantIntoScan", "true");
    spark.sparkContext().setLogLevel("WARN");
  }

  @AfterAll
  static void closeSpark() throws IOException {
    if (spark != null) {
      spark.close();
      spark = null;
    }
  }

  @AfterEach
  void deleteOutput() throws IOException {
    if (outputPath == null || !Files.exists(outputPath)) {
      return;
    }
    try (Stream<Path> paths = Files.walk(outputPath)) {
      paths
          .sorted((left, right) -> right.compareTo(left))
          .map(Path::toFile)
          .forEach(File::delete);
    }
  }

  @Test
  void T_variantColumnsOption_InfersLogicalVariantSchema() throws Exception {
    writeFixture();
    final Dataset<Row> dataset = readVariant();

    assertEquals(DataTypes.LongType, dataset.schema().apply("row_id").dataType());
    assertEquals(DataTypes.VariantType, dataset.schema().apply("v").dataType());
  }

  @Test
  void T_selectFullVariant_ReadsEveryRowThroughV2() throws Exception {
    writeFixture();
    final List<Row> rows =
        readVariant().selectExpr("row_id", "v").orderBy("row_id").collectAsList();

    assertEquals(4, rows.size());
    for (Row row : rows) {
      assertFalse(row.isNullAt(1));
    }
  }

  @Test
  void T_variantGet_DirectNestedAndArrayPaths_ReturnExpectedValues() throws Exception {
    writeFixture();
    final List<Row> rows =
        readVariant()
            .selectExpr(
                "row_id",
                "variant_get(v, '$.id', 'bigint') AS id",
                "variant_get(v, '$.user.profile.name', 'string') AS profile_name",
                "variant_get(v, '$.items[1]', 'bigint') AS second_item",
                "variant_get(v, '$.users[0].id', 'bigint') AS first_user_id")
            .orderBy("row_id")
            .collectAsList();

    assertEquals(4, rows.size());

    assertEquals(1L, rows.get(0).getLong(0));
    assertEquals(10L, rows.get(0).getLong(1));
    assertEquals("alice-p", rows.get(0).getString(2));
    assertEquals(2L, rows.get(0).getLong(3));
    assertEquals(7L, rows.get(0).getLong(4));

    assertEquals(2L, rows.get(1).getLong(0));
    assertEquals(20L, rows.get(1).getLong(1));
    assertNull(rows.get(1).get(2));
    assertNull(rows.get(1).get(3));
    assertEquals(9L, rows.get(1).getLong(4));

    assertEquals(3L, rows.get(2).getLong(0));
    assertNull(rows.get(2).get(1));
    assertNull(rows.get(2).get(2));
    assertNull(rows.get(2).get(3));
    assertNull(rows.get(2).get(4));

    assertEquals(4L, rows.get(3).getLong(0));
    assertNull(rows.get(3).get(1));
    assertNull(rows.get(3).get(2));
    assertNull(rows.get(3).get(3));
    assertNull(rows.get(3).get(4));
  }

  @Test
  void T_tryVariantGet_MissingFieldFallsBackToSparkAndReturnsNull() throws Exception {
    writeFixture();
    final List<Row> rows =
        readVariant()
            .selectExpr(
                "row_id",
                "try_variant_get(v, '$.missing', 'bigint') AS missing_value")
            .orderBy("row_id")
            .collectAsList();

    assertEquals(4, rows.size());
    for (Row row : rows) {
      assertNull(row.get(1));
    }
  }

  private Dataset<Row> readVariant() {
    return spark
        .read()
        .format("yosegi-v2")
        .option(YosegiV2Options.VARIANT_COLUMNS, "v")
        .load(outputPath.toString());
  }

  private void writeFixture() throws Exception {
    outputPath = Files.createTempDirectory("yosegi-v2-variant-e2e-");
    final URL resource =
        Thread.currentThread().getContextClassLoader().getResource("v2/VariantEndToEnd_1.txt");
    if (resource == null) {
      throw new IOException("Test resource was not found: v2/VariantEndToEnd_1.txt");
    }
    final String resourcePath = Paths.get(resource.toURI()).toString();
    spark
        .read()
        .schema(PHYSICAL_SCHEMA)
        .json(resourcePath)
        .write()
        .mode(SaveMode.Overwrite)
        .format("jp.co.yahoo.yosegi.spark.YosegiFileFormat")
        .save(outputPath.toString());
  }
}
