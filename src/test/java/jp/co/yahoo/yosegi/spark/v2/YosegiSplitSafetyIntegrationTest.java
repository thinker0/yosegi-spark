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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** End-to-end safety coverage for Yosegi V2 byte-range splitting. */
class YosegiSplitSafetyIntegrationTest {
  private static SparkSession spark;
  private final List<Path> outputPaths = new ArrayList<>();

  @BeforeAll
  static void setUpSpark() {
    spark =
        SparkSession.builder()
            .appName("YosegiSplitSafetyIntegrationTest")
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
  void T_oneByteSplits_ReturnSingleRowExactlyOnce() throws Exception {
    final Path output = newOutput("yosegi-v2-split-byte-");
    final StructType schema = new StructType().add("id", DataTypes.LongType, false);
    writeSingleFile(output, List.of(RowFactory.create(7L)), schema);

    final long fileLength = dataFileLength(output);
    assertTrue(fileLength > 0L);

    final Dataset<Row> data =
        spark
            .read()
            .option(YosegiV2Options.SPLIT_SIZE, "1")
            .format("yosegi-v2")
            .load(output.toString())
            .select("id");

    // splitSize=1 creates a split boundary at every byte offset. This exercises both block-start
    // and inside-block boundaries without depending on Yosegi's private block metadata APIs.
    assertEquals(fileLength, data.rdd().getNumPartitions());
    final List<Row> rows = data.collectAsList();
    assertEquals(1, rows.size());
    assertEquals(7L, rows.get(0).getLong(0));
  }

  @Test
  void T_irregularSplits_HaveNoMissingOrDuplicateIds() throws Exception {
    final Path output = newOutput("yosegi-v2-split-irregular-");
    final StructType schema =
        new StructType()
            .add("id", DataTypes.LongType, false)
            .add("payload", DataTypes.StringType, false);
    final List<Row> rows = new ArrayList<>();
    final String payload = "0123456789abcdef".repeat(16);
    final int rowCount = 4096;
    for (long id = 0; id < rowCount; id++) {
      rows.add(RowFactory.create(id, payload + id));
    }
    writeSingleFile(output, rows, schema);

    final long fileLength = dataFileLength(output);
    assertTrue(fileLength > 4093L);

    // An intentionally awkward size makes boundaries highly unlikely to line up with Yosegi
    // blocks and verifies the reader's half-open range ownership across many splits.
    final List<Row> actual = readIds(output, 4093L);
    assertExactlyOnce(actual, rowCount);
  }

  @Test
  void T_splitLargerThanFile_MatchesWholeFileRead() throws Exception {
    final Path output = newOutput("yosegi-v2-split-oversize-");
    final StructType schema = new StructType().add("id", DataTypes.LongType, false);
    final List<Row> rows = new ArrayList<>();
    for (long id = 0; id < 128; id++) {
      rows.add(RowFactory.create(id));
    }
    writeSingleFile(output, rows, schema);

    final long fileLength = dataFileLength(output);
    final long oversizeSplit = Math.addExact(fileLength, 1L);
    final Dataset<Row> splitRead =
        spark
            .read()
            .option(YosegiV2Options.SPLIT_SIZE, Long.toString(oversizeSplit))
            .format("yosegi-v2")
            .load(output.toString())
            .select("id");

    assertEquals(1, splitRead.rdd().getNumPartitions());
    assertExactlyOnce(splitRead.collectAsList(), rows.size());
  }

  @Test
  void T_emptyDataset_WithTinySplit_ReturnsNoRows() throws Exception {
    final Path output = newOutput("yosegi-v2-split-empty-");
    final StructType schema = new StructType().add("id", DataTypes.LongType, true);

    spark
        .createDataFrame(List.<Row>of(), schema)
        .coalesce(1)
        .write()
        .mode(SaveMode.Overwrite)
        .format("yosegi")
        .save(output.toString());

    // Supplying the schema makes this check about split/read safety rather than schema inference
    // when the writer produces no data-bearing block.
    final List<Row> actual =
        spark
            .read()
            .schema(schema)
            .option(YosegiV2Options.SPLIT_SIZE, "1")
            .format("yosegi-v2")
            .load(output.toString())
            .collectAsList();

    assertTrue(actual.isEmpty());
  }

  private void writeSingleFile(
      final Path output, final List<Row> rows, final StructType schema) {
    spark
        .createDataFrame(rows, schema)
        .coalesce(1)
        .write()
        .mode(SaveMode.Overwrite)
        .format("yosegi")
        .save(output.toString());
  }

  private List<Row> readIds(final Path output, final long splitSize) {
    return spark
        .read()
        .option(YosegiV2Options.SPLIT_SIZE, Long.toString(splitSize))
        .format("yosegi-v2")
        .load(output.toString())
        .select("id")
        .collectAsList();
  }

  private static void assertExactlyOnce(final List<Row> rows, final int expectedCount) {
    final Map<Long, Integer> counts = new HashMap<>();
    for (Row row : rows) {
      counts.merge(row.getLong(0), 1, Integer::sum);
    }

    assertEquals(expectedCount, rows.size(), "row count");
    assertEquals(expectedCount, counts.size(), "unique id count");
    for (long id = 0; id < expectedCount; id++) {
      assertEquals(1, counts.getOrDefault(id, 0), "id=" + id);
    }
  }

  private static long dataFileLength(final Path output) throws IOException {
    try (Stream<Path> paths = Files.walk(output)) {
      final Path dataFile =
          paths
              .filter(Files::isRegularFile)
              .filter(path -> path.getFileName().toString().startsWith("part-"))
              .findFirst()
              .orElseThrow(() -> new IllegalStateException("Yosegi data file was not created."));
      return Files.size(dataFile);
    }
  }

  private Path newOutput(final String prefix) throws IOException {
    final Path path = Files.createTempDirectory(prefix);
    outputPaths.add(path);
    return path;
  }
}
