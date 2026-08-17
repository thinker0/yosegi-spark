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
import org.apache.spark.sql.RowFactory;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** End-to-end coverage for partition columns, pruning, splitting and multi-file schema merge. */
class YosegiDataSourceV2Phase3IntegrationTest {
  private static SparkSession spark;
  private final List<Path> outputPaths = new ArrayList<>();

  @BeforeAll
  static void setUpSpark() {
    spark =
        SparkSession.builder()
            .appName("YosegiDataSourceV2Phase3IntegrationTest")
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate();
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
  void T_partitionColumnsAndPruning_ReturnCorrectRows() throws Exception {
    final Path output = newOutput("yosegi-v2-partition-");
    final StructType schema =
        new StructType()
            .add("id", DataTypes.LongType, false)
            .add("date", DataTypes.StringType, false);
    final List<Row> rows =
        List.of(
            RowFactory.create(1L, "2026-07-29"),
            RowFactory.create(2L, "2026-07-29"),
            RowFactory.create(3L, "2026-07-30"));
    spark
        .createDataFrame(rows, schema)
        .write()
        .mode(SaveMode.Overwrite)
        .partitionBy("date")
        .format("yosegi")
        .save(output.toString());

    final List<Row> actual =
        spark
            .read()
            .format("yosegi-v2")
            .load(output.toString())
            .where("date = '2026-07-29'")
            .select("id", "date")
            .orderBy("id")
            .collectAsList();

    assertEquals(2, actual.size());
    assertEquals(1L, actual.get(0).getLong(0));
    assertEquals("2026-07-29", actual.get(0).getString(1));
    assertEquals(2L, actual.get(1).getLong(0));
  }

  @Test
  void T_splitRead_MatchesWholeFileRead() throws Exception {
    final Path output = newOutput("yosegi-v2-split-");
    final StructType schema = new StructType().add("id", DataTypes.LongType, false);
    final List<Row> rows = new ArrayList<>();
    for (long i = 0; i < 200; i++) {
      rows.add(RowFactory.create(i));
    }
    spark
        .createDataFrame(rows, schema)
        .coalesce(1)
        .write()
        .mode(SaveMode.Overwrite)
        .format("yosegi")
        .save(output.toString());

    final List<Row> whole =
        spark
            .read()
            .format("yosegi-v2")
            .load(output.toString())
            .orderBy("id")
            .collectAsList();
    final List<Row> split =
        spark
            .read()
            .option(YosegiV2Options.SPLIT_SIZE, "128")
            .format("yosegi-v2")
            .load(output.toString())
            .orderBy("id")
            .collectAsList();

    assertEquals(whole, split);
    assertEquals(200, split.size());
  }

  @Test
  void T_multiFileSchema_MergesColumnsAcrossAllFiles() throws Exception {
    final Path first = newOutput("yosegi-v2-schema-a-");
    final Path second = newOutput("yosegi-v2-schema-b-");
    spark
        .createDataFrame(
            List.of(RowFactory.create(1L)),
            new StructType().add("id", DataTypes.LongType, false))
        .write()
        .mode(SaveMode.Overwrite)
        .format("yosegi")
        .save(first.toString());
    spark
        .createDataFrame(
            List.of(RowFactory.create(2L, "bob")),
            new StructType()
                .add("id", DataTypes.LongType, false)
                .add("name", DataTypes.StringType, true))
        .write()
        .mode(SaveMode.Overwrite)
        .format("yosegi")
        .save(second.toString());

    final Dataset<Row> data =
        spark.read().format("yosegi-v2").load(first.toString(), second.toString());
    final List<Row> actual = data.select("id", "name").orderBy("id").collectAsList();

    assertTrue(List.of(data.schema().fieldNames()).contains("name"));
    assertEquals(2, actual.size());
    assertEquals(1L, actual.get(0).getLong(0));
    assertNull(actual.get(0).get(1));
    assertEquals("bob", actual.get(1).getString(1));
  }

  private Path newOutput(final String prefix) throws IOException {
    final Path path = Files.createTempDirectory(prefix);
    outputPaths.add(path);
    return path;
  }
}
