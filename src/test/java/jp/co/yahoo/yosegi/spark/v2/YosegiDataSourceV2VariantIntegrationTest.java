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
import static org.junit.jupiter.api.Assertions.assertNull;

/** End-to-end tests for the public DataSource V2 read path and Variant extraction pushdown. */
class YosegiDataSourceV2VariantIntegrationTest {
  private static final StructType PHYSICAL_SCHEMA =
      new StructType()
          .add("row_id", DataTypes.LongType, false)
          .add(
              "v",
              new StructType()
                  .add("id", DataTypes.LongType, true)
                  .add("name", DataTypes.StringType, true),
              true);
  private static SparkSession spark;
  private Path outputPath;

  @BeforeAll
  static void setUpSpark() {
    spark =
        SparkSession.builder()
            .appName("YosegiDataSourceV2VariantIntegrationTest")
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.variant.pushVariantIntoScan", "true")
            .getOrCreate();
    // getOrCreate() may reuse a SparkSession created by another test. Set the SQLConf again on
    // the actual session so the V2 optimizer invokes SupportsPushDownVariantExtractions.
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
  void T_read_NormalStructThroughV2ShortName() throws Exception {
    writeFixture();

    final Dataset<Row> actual =
        spark
            .read()
            .schema(PHYSICAL_SCHEMA)
            .format("yosegi-v2")
            .load(outputPath.toString())
            .selectExpr("row_id", "v.name AS name")
            .orderBy("row_id");
    final List<Row> rows = actual.collectAsList();
    assertEquals(3, rows.size());
    assertEquals("alice", rows.get(0).getString(1));
    assertEquals("bob", rows.get(1).getString(1));
    assertEquals("charlie", rows.get(2).getString(1));
  }

  @Test
  void T_variantGet_UsesVariantStructLoaderAndReturnsValues() throws Exception {
    writeFixture();
    final StructType logicalSchema =
        new StructType()
            .add("row_id", DataTypes.LongType, false)
            .add("v", DataTypes.VariantType, true);

    final Dataset<Row> actual =
        spark
            .read()
            .schema(logicalSchema)
            .format("yosegi-v2")
            .load(outputPath.toString())
            .selectExpr("row_id", "variant_get(v, '$.id', 'bigint') AS id")
            .orderBy("row_id");
    final List<Row> rows = actual.collectAsList();
    assertEquals(3, rows.size());
    assertEquals(1L, rows.get(0).getLong(0));
    assertEquals(10L, rows.get(0).getLong(1));
    assertEquals(2L, rows.get(1).getLong(0));
    assertEquals(20L, rows.get(1).getLong(1));
    assertEquals(3L, rows.get(2).getLong(0));
    assertNull(rows.get(2).get(1));
  }

  private void writeFixture() throws Exception {
    outputPath = Files.createTempDirectory("yosegi-v2-variant-");
    final URL resource =
        Thread.currentThread()
            .getContextClassLoader()
            .getResource("v2/VariantPushdown_1.txt");
    if (resource == null) {
      throw new IOException("Test resource was not found: v2/VariantPushdown_1.txt");
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
