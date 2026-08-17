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
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Black-box parity tests between the legacy V1 file source and the DataSource V2 reader. */
class YosegiDataSourceV1V2ParityTest {
  private static final StructType CHILD_TYPE =
      new StructType()
          .add("score", DataTypes.LongType, true)
          .add("label", DataTypes.StringType, true);
  private static final ArrayType VALUES_TYPE = DataTypes.createArrayType(DataTypes.LongType, true);
  private static final MapType ATTRIBUTES_TYPE =
      DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true);
  private static final DecimalType DECIMAL_TYPE = DataTypes.createDecimalType(18, 4);
  private static final StructType SCHEMA =
      new StructType()
          .add("id", DataTypes.LongType, false)
          .add("enabled", DataTypes.BooleanType, true)
          .add("int_value", DataTypes.IntegerType, true)
          .add("double_value", DataTypes.DoubleType, true)
          .add("name", DataTypes.StringType, true)
          .add("payload", DataTypes.BinaryType, true)
          .add("decimal_value", DECIMAL_TYPE, true)
          .add("date_value", DataTypes.DateType, true)
          .add("timestamp_value", DataTypes.TimestampType, true)
          .add("child", CHILD_TYPE, true)
          .add("values", VALUES_TYPE, true)
          .add("attributes", ATTRIBUTES_TYPE, true);

  private static SparkSession spark;
  private final List<Path> outputPaths = new ArrayList<>();

  @BeforeAll
  static void setUpSpark() {
    spark =
        SparkSession.builder()
            .appName("YosegiDataSourceV1V2ParityTest")
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
  void T_schemaAndValues_V2MatchV1() throws Exception {
    final Path output = writeFixture();
    final Dataset<Row> v1 = readV1(output);
    final Dataset<Row> v2 = readV2(output);

    assertEquals(v1.schema(), v2.schema());

    final List<Row> v1Rows = v1.orderBy("id").collectAsList();
    final List<Row> v2Rows = v2.orderBy("id").collectAsList();
    assertEquals(v1Rows.size(), v2Rows.size());
    for (int i = 0; i < v1Rows.size(); i++) {
      assertRowEquals(v1Rows.get(i), v2Rows.get(i));
    }
  }

  @Test
  void T_projection_V2MatchesV1() throws Exception {
    final Path output = writeFixture();
    final List<Row> v1 =
        readV1(output).select("id", "name", "child.score").orderBy("id").collectAsList();
    final List<Row> v2 =
        readV2(output).select("id", "name", "child.score").orderBy("id").collectAsList();

    assertEquals(v1, v2);
  }

  @Test
  void T_filter_V2MatchesV1() throws Exception {
    final Path output = writeFixture();
    final String condition = "enabled = true AND int_value >= 10 AND name IS NOT NULL";
    final List<Row> v1 =
        readV1(output).where(condition).select("id", "name").orderBy("id").collectAsList();
    final List<Row> v2 =
        readV2(output).where(condition).select("id", "name").orderBy("id").collectAsList();

    assertEquals(v1, v2);
  }

  @Test
  void T_partitionedRead_V2MatchesV1() throws Exception {
    final Path output = newOutput("yosegi-v1-v2-partition-parity-");
    final StructType partitionSchema =
        new StructType()
            .add("id", DataTypes.LongType, false)
            .add("date", DataTypes.StringType, false);
    spark
        .createDataFrame(
            List.of(
                RowFactory.create(1L, "2026-08-11"),
                RowFactory.create(2L, "2026-08-12"),
                RowFactory.create(3L, "2026-08-12")),
            partitionSchema)
        .write()
        .mode(SaveMode.Overwrite)
        .partitionBy("date")
        .format("yosegi")
        .save(output.toString());

    final String condition = "date = '2026-08-12'";
    // Spark may infer YYYY-MM-DD partition values as DateType. On some Java 17 runtimes,
    // external-row decoding of an inferred partition Date touches JDK-internal ZoneInfo classes.
    // The parity contract here is the partition value itself, so compare its stable SQL string
    // representation instead of depending on that external Date decoder.
    final List<Row> v1 =
        readV1(output)
            .where(condition)
            .selectExpr("id", "CAST(date AS STRING) AS date")
            .orderBy("id")
            .collectAsList();
    final List<Row> v2 =
        readV2(output)
            .where(condition)
            .selectExpr("id", "CAST(date AS STRING) AS date")
            .orderBy("id")
            .collectAsList();

    assertEquals(v1, v2);
  }

  private Path writeFixture() throws IOException {
    final Path output = newOutput("yosegi-v1-v2-parity-");
    final Map<String, String> firstAttributes = new LinkedHashMap<>();
    firstAttributes.put("region", "jp");
    firstAttributes.put("tier", "gold");
    final Map<String, String> secondAttributes = new LinkedHashMap<>();
    secondAttributes.put("region", "us");

    final List<Row> rows =
        List.of(
            RowFactory.create(
                1L,
                true,
                10,
                1.5d,
                "alice",
                new byte[] {1, 2, 3},
                new BigDecimal("123.4500"),
                Date.valueOf("2026-08-12"),
                Timestamp.valueOf("2026-08-12 10:11:12.123456"),
                RowFactory.create(100L, "first"),
                List.of(1L, 2L, 3L),
                firstAttributes),
            RowFactory.create(
                2L,
                false,
                5,
                -2.25d,
                "bob",
                new byte[0],
                new BigDecimal("-0.5000"),
                Date.valueOf("1970-01-01"),
                Timestamp.valueOf("1970-01-01 00:00:00"),
                RowFactory.create(null, "second"),
                List.of(9L),
                secondAttributes),
            RowFactory.create(
                3L,
                true,
                20,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null));

    spark
        .createDataFrame(rows, SCHEMA)
        .write()
        .mode(SaveMode.Overwrite)
        .format("yosegi")
        .save(output.toString());
    return output;
  }

  private Dataset<Row> readV1(final Path output) {
    return spark.read().format("yosegi").load(output.toString());
  }

  private Dataset<Row> readV2(final Path output) {
    return spark.read().format("yosegi-v2").load(output.toString());
  }

  private Path newOutput(final String prefix) throws IOException {
    final Path path = Files.createTempDirectory(prefix);
    outputPaths.add(path);
    return path;
  }

  private static void assertRowEquals(final Row expected, final Row actual) {
    assertEquals(expected.schema(), actual.schema());
    for (int i = 0; i < expected.size(); i++) {
      final Object expectedValue = expected.get(i);
      final Object actualValue = actual.get(i);
      final String fieldName = expected.schema().fields()[i].name();
      assertTrue(
          java.util.Objects.deepEquals(expectedValue, actualValue),
          "field="
              + fieldName
              + ", index="
              + i
              + ", expected="
              + valueDescription(expectedValue)
              + ", actual="
              + valueDescription(actualValue));
    }
  }

  private static String valueDescription(final Object value) {
    if (value == null) {
      return "null";
    }
    if (value instanceof byte[]) {
      return java.util.Arrays.toString((byte[]) value) + " (byte[])";
    }
    return value + " (" + value.getClass().getName() + ")";
  }
}
