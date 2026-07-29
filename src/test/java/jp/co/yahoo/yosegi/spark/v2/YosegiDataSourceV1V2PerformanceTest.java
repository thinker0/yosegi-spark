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
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Manual release-gate benchmark for detecting large V2 read regressions against the legacy V1
 * reader. This test is intentionally skipped during the normal test suite because wall-clock
 * assertions are sensitive to host load.
 *
 * <p>Enable with:
 *
 * <pre>
 * mvn -Dtest=YosegiDataSourceV1V2PerformanceTest \
 *     -Dyosegi.performance.enabled=true test
 * </pre>
 */
class YosegiDataSourceV1V2PerformanceTest {

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
          .add("user", USER_TYPE, true)
          .add("items", ITEMS_TYPE, true)
          .add("users", USERS_TYPE, true);
  private static final StructType PHYSICAL_SCHEMA =
      new StructType()
          .add("id", DataTypes.LongType, false)
          .add("name", DataTypes.StringType, true)
          .add("score", DataTypes.LongType, true)
          .add("payload", DataTypes.StringType, true)
          .add("v", PHYSICAL_VARIANT_TYPE, true);

  private static SparkSession spark;
  private Path outputPath;

  @BeforeAll
  static void setUpSpark() {
    spark =
        SparkSession.builder()
            .appName("YosegiDataSourceV1V2PerformanceTest")
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
      paths.sorted((left, right) -> right.compareTo(left)).map(Path::toFile).forEach(File::delete);
    }
  }

  @Test
  void T_releaseGate_NoLargeV2ReadRegression() throws Exception {
    Assumptions.assumeTrue(
        Boolean.getBoolean("yosegi.performance.enabled"),
        "manual performance gate; enable with -Dyosegi.performance.enabled=true");

    final int rows = Integer.getInteger("yosegi.performance.rows", 20000);
    final int warmups = Integer.getInteger("yosegi.performance.warmups", 2);
    final int iterations = Integer.getInteger("yosegi.performance.iterations", 5);
    final double maxRatio =
        Double.parseDouble(System.getProperty("yosegi.performance.maxV2ToV1Ratio", "5.0"));

    writeFixture(rows);

    final Dataset<Row> v1 = readV1();
    final Dataset<Row> v2 = readV2();
    final Dataset<Row> variant = readVariant();
    final long targetId = rows / 2L;

    final List<Scenario> comparable =
        List.of(
            new Scenario(
                "select_all",
                dataset ->
                    dataset
                        .selectExpr(
                            "sum(id) AS id_sum",
                            "sum(score) AS score_sum",
                            "sum(length(name)) AS name_len",
                            "sum(length(payload)) AS payload_len",
                            "sum(v.id) AS variant_id_sum",
                            "count(v.user.profile.name) AS profile_name_count")
                        .collectAsList()),
            new Scenario(
                "select_id",
                dataset -> dataset.selectExpr("sum(id) AS id_sum").collectAsList()),
            new Scenario(
                "filter_id",
                dataset ->
                    dataset
                        .where("id = " + targetId)
                        .select("id", "name", "score")
                        .collectAsList()));

    System.out.println("\n=== Yosegi V1/V2 performance regression gate ===");
    System.out.printf(
        Locale.ROOT,
        "rows=%d warmups=%d iterations=%d maxV2ToV1Ratio=%.2f%n",
        rows,
        warmups,
        iterations,
        maxRatio);

    for (Scenario scenario : comparable) {
      final PairResult result = benchmarkPair(v1, v2, scenario, warmups, iterations);
      System.out.printf(
          Locale.ROOT,
          "%-24s V1=%8.2f ms V2=%8.2f ms ratio=%5.2fx%n",
          scenario.name,
          nanosToMillis(result.v1MedianNanos),
          nanosToMillis(result.v2MedianNanos),
          result.ratio());
      assertTrue(
          result.ratio() <= maxRatio,
          () ->
              scenario.name
                  + " V2/V1 ratio "
                  + String.format(Locale.ROOT, "%.2fx", result.ratio())
                  + " exceeded release-gate limit "
                  + String.format(Locale.ROOT, "%.2fx", maxRatio));
    }

    System.out.println("\n=== V2 Variant baseline (report only) ===");
    benchmarkVariant(
        "select_v",
        () -> variant.select("v").collectAsList(),
        warmups,
        iterations);
    benchmarkVariant(
        "variant_get_$.id",
        () ->
            variant
                .selectExpr("variant_get(v, '$.id', 'bigint') AS value")
                .selectExpr("sum(value) AS total")
                .collectAsList(),
        warmups,
        iterations);
    benchmarkVariant(
        "variant_get_$.user.profile.name",
        () ->
            variant
                .selectExpr("variant_get(v, '$.user.profile.name', 'string') AS value")
                .selectExpr("count(value) AS total")
                .collectAsList(),
        warmups,
        iterations);
    benchmarkVariant(
        "variant_get_$.items[0]",
        () ->
            variant
                .selectExpr("variant_get(v, '$.items[0]', 'bigint') AS value")
                .selectExpr("sum(value) AS total")
                .collectAsList(),
        warmups,
        iterations);
  }

  private PairResult benchmarkPair(
      final Dataset<Row> v1,
      final Dataset<Row> v2,
      final Scenario scenario,
      final int warmups,
      final int iterations) {
    for (int i = 0; i < warmups; i++) {
      if ((i & 1) == 0) {
        scenario.action.run(v1);
        scenario.action.run(v2);
      } else {
        scenario.action.run(v2);
        scenario.action.run(v1);
      }
    }

    final long[] v1Times = new long[iterations];
    final long[] v2Times = new long[iterations];
    for (int i = 0; i < iterations; i++) {
      if ((i & 1) == 0) {
        v1Times[i] = measure(() -> scenario.action.run(v1));
        v2Times[i] = measure(() -> scenario.action.run(v2));
      } else {
        v2Times[i] = measure(() -> scenario.action.run(v2));
        v1Times[i] = measure(() -> scenario.action.run(v1));
      }
    }
    return new PairResult(median(v1Times), median(v2Times));
  }

  private void benchmarkVariant(
      final String name, final Runnable action, final int warmups, final int iterations) {
    for (int i = 0; i < warmups; i++) {
      action.run();
    }
    final long[] times = new long[iterations];
    for (int i = 0; i < iterations; i++) {
      times[i] = measure(action);
    }
    System.out.printf(
        Locale.ROOT, "%-34s %8.2f ms%n", name, nanosToMillis(median(times)));
  }

  private static long measure(final Runnable action) {
    final long start = System.nanoTime();
    action.run();
    return System.nanoTime() - start;
  }

  private static long median(final long[] values) {
    final long[] sorted = values.clone();
    Arrays.sort(sorted);
    return sorted[sorted.length / 2];
  }

  private static double nanosToMillis(final long nanos) {
    return nanos / 1_000_000.0d;
  }

  private void writeFixture(final int rowCount) throws IOException {
    outputPath = Files.createTempDirectory("yosegi-v1-v2-performance-");
    final List<Row> rows = new ArrayList<>(rowCount);
    for (int i = 0; i < rowCount; i++) {
      final long id = i;
      final String name = "name-" + (i % 1000);
      final long score = i % 10000L;
      final String payload = "payload-" + (i % 100) + "-abcdefghijklmnopqrstuvwxyz0123456789";
      final Row profile = RowFactory.create("profile-" + (i % 100));
      final Row user = RowFactory.create(profile);
      final List<Long> items = List.of(id, id + 1L, id + 2L);
      final List<Row> users = List.of(RowFactory.create(id % 1000L));
      final Row variant = RowFactory.create(id, user, items, users);
      rows.add(RowFactory.create(id, name, score, payload, variant));
    }

    spark
        .createDataFrame(rows, PHYSICAL_SCHEMA)
        .write()
        .mode(SaveMode.Overwrite)
        .format("jp.co.yahoo.yosegi.spark.YosegiFileFormat")
        .save(outputPath.toString());
  }

  private Dataset<Row> readV1() {
    return spark.read().format("yosegi").load(outputPath.toString());
  }

  private Dataset<Row> readV2() {
    return spark.read().format("yosegi-v2").load(outputPath.toString());
  }

  private Dataset<Row> readVariant() {
    return spark
        .read()
        .format("yosegi-v2")
        .option(YosegiV2Options.VARIANT_COLUMNS, "v")
        .load(outputPath.toString());
  }

  private interface DatasetAction {
    void run(Dataset<Row> dataset);
  }

  private static final class Scenario {
    private final String name;
    private final DatasetAction action;

    private Scenario(final String name, final DatasetAction action) {
      this.name = name;
      this.action = action;
    }
  }

  private static final class PairResult {
    private final long v1MedianNanos;
    private final long v2MedianNanos;

    private PairResult(final long v1MedianNanos, final long v2MedianNanos) {
      this.v1MedianNanos = v1MedianNanos;
      this.v2MedianNanos = v2MedianNanos;
    }

    private double ratio() {
      return ((double) v2MedianNanos) / Math.max(1L, v1MedianNanos);
    }
  }
}
