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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** End-to-end contract tests for the DataSource V2 physical explain plan. */
class YosegiDataSourceV2ExplainPlanTest {
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
  private static final StructType PHYSICAL_VARIANT_SCHEMA =
      new StructType()
          .add("row_id", DataTypes.LongType, false)
          .add("v", PHYSICAL_VARIANT_TYPE, true);

  private static SparkSession spark;
  private final List<Path> outputPaths = new ArrayList<>();

  @BeforeAll
  static void setUpSpark() {
    spark =
        SparkSession.builder()
            .appName("YosegiDataSourceV2ExplainPlanTest")
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
  void deleteOutputs() throws IOException {
    for (Path outputPath : outputPaths) {
      if (!Files.exists(outputPath)) {
        continue;
      }
      try (Stream<Path> paths = Files.walk(outputPath)) {
        paths
            .sorted((left, right) -> right.compareTo(left))
            .map(Path::toFile)
            .forEach(File::delete);
      }
    }
    outputPaths.clear();
  }

  @Test
  void T_explainPlan_ShowsBatchScanReadSchemaAndFilterContracts() throws Exception {
    final Path output = newOutput("yosegi-v2-explain-filter-");
    final StructType schema =
        new StructType()
            .add("id", DataTypes.LongType, false)
            .add("name", DataTypes.StringType, true)
            .add("date", DataTypes.StringType, false);
    final List<Row> rows =
        List.of(
            RowFactory.create(1L, "alice", "2026-08-12"),
            RowFactory.create(2L, "bob", "2026-08-13"));

    spark
        .createDataFrame(rows, schema)
        .write()
        .mode(SaveMode.Overwrite)
        .partitionBy("date")
        .format("yosegi")
        .save(output.toString());

    final Dataset<Row> query =
        spark
            .read()
            .format("yosegi-v2")
            .load(output.toString())
            .where("id = 1 AND date = '2026-08-12'")
            .select("id");
    final String plan = physicalPlan(query);

    assertTrue(plan.contains("BatchScan"), plan);
    assertTrue(plan.contains("Yosegi V2 scan"), plan);
    assertTrue(plan.contains("ReadSchema: struct<"), plan);
    assertTrue(plan.contains("PushedFilters: ["), plan);
    assertTrue(plan.contains("id"), plan);
    assertTrue(plan.contains("PartitionFilters: ["), plan);
    assertTrue(plan.contains("date"), plan);
  }

  @Test
  void T_explainPlan_VariantExactTypeIsPushed() throws Exception {
    final Dataset<Row> variant = readVariantFixture();
    final Dataset<Row> query =
        variant.selectExpr("row_id", "variant_get(v, '$.id', 'bigint') AS id");
    final String plan = physicalPlan(query);

    assertTrue(plan.contains("BatchScan"), plan);
    assertTrue(plan.contains("PushedVariantExtractions: 1"), plan);
  }

  @Test
  void T_explainPlan_VariantCastRequiredFallsBackToSpark() throws Exception {
    final Dataset<Row> variant = readVariantFixture();
    final Dataset<Row> query =
        variant.selectExpr("row_id", "variant_get(v, '$.id', 'int') AS id");
    final String plan = physicalPlan(query);

    assertTrue(plan.contains("BatchScan"), plan);
    assertTrue(plan.contains("PushedVariantExtractions: 0"), plan);
  }

  private Dataset<Row> readVariantFixture() throws Exception {
    final Path output = newOutput("yosegi-v2-explain-variant-");
    final URL resource =
        Thread.currentThread().getContextClassLoader().getResource("v2/VariantEndToEnd_1.txt");
    if (resource == null) {
      throw new IOException("Test resource was not found: v2/VariantEndToEnd_1.txt");
    }
    final String resourcePath = Paths.get(resource.toURI()).toString();
    spark
        .read()
        .schema(PHYSICAL_VARIANT_SCHEMA)
        .json(resourcePath)
        .write()
        .mode(SaveMode.Overwrite)
        .format("jp.co.yahoo.yosegi.spark.YosegiFileFormat")
        .save(output.toString());

    return spark
        .read()
        .format("yosegi-v2")
        .option(YosegiV2Options.VARIANT_COLUMNS, "v")
        .load(output.toString());
  }

  private static String physicalPlan(final Dataset<Row> dataset) {
    return dataset.queryExecution().executedPlan().toString();
  }

  private Path newOutput(final String prefix) throws IOException {
    final Path path = Files.createTempDirectory(prefix);
    outputPaths.add(path);
    return path;
  }
}
