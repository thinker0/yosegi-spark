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
import org.apache.spark.sql.types.DecimalType;
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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** End-to-end contract coverage for typed partitions and partition/data-column conflicts. */
class YosegiPartitionCoverageIntegrationTest {
  private static SparkSession spark;
  private final List<Path> outputPaths = new ArrayList<>();

  @BeforeAll
  static void setUpSpark() {
    spark =
        SparkSession.builder()
            .appName("YosegiPartitionCoverageIntegrationTest")
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
  void T_typedPartitionColumns_AreMaterializedFromHiveStylePaths() throws Exception {
    final Path table = newOutput("yosegi-v2-typed-partition-");
    final Path partitionPath =
        table
            .resolve("p_string=tokyo")
            .resolve("p_int=42")
            .resolve("p_long=922337203685477000")
            .resolve("p_boolean=true")
            .resolve("p_date=2026-08-12")
            .resolve("p_decimal=12345.67")
            .resolve("p_null=" + YosegiPartitionValues.HIVE_DEFAULT_PARTITION);

    spark
        .createDataFrame(
            List.of(RowFactory.create(7L)),
            new StructType().add("id", DataTypes.LongType, false))
        .write()
        .mode(SaveMode.Overwrite)
        .format("yosegi")
        .save(partitionPath.toString());

    final DecimalType decimalType = DataTypes.createDecimalType(12, 2);
    final StructType requestedSchema =
        new StructType()
            .add("id", DataTypes.LongType, true)
            .add("p_string", DataTypes.StringType, true)
            .add("p_int", DataTypes.IntegerType, true)
            .add("p_long", DataTypes.LongType, true)
            .add("p_boolean", DataTypes.BooleanType, true)
            .add("p_date", DataTypes.DateType, true)
            .add("p_decimal", decimalType, true)
            .add("p_null", DataTypes.StringType, true);

    final Dataset<Row> data =
        spark.read().schema(requestedSchema).format("yosegi-v2").load(table.toString());

    assertEquals(DataTypes.IntegerType, data.schema().apply("p_int").dataType());
    assertEquals(DataTypes.LongType, data.schema().apply("p_long").dataType());
    assertEquals(DataTypes.BooleanType, data.schema().apply("p_boolean").dataType());
    assertEquals(DataTypes.DateType, data.schema().apply("p_date").dataType());
    assertEquals(decimalType, data.schema().apply("p_decimal").dataType());

    // Cast Date to String before collecting because Spark 4.2 + Java 17 external Row conversion of
    // partition Date can hit module-access restrictions unrelated to the V2 reader contract.
    final Row row =
        data.selectExpr(
                "id",
                "p_string",
                "p_int",
                "p_long",
                "p_boolean",
                "cast(p_date as string) as p_date",
                "p_decimal",
                "p_null")
            .head();

    assertEquals(7L, row.getLong(0));
    assertEquals("tokyo", row.getString(1));
    assertEquals(42, row.getInt(2));
    assertEquals(922337203685477000L, row.getLong(3));
    assertTrue(row.getBoolean(4));
    assertEquals("2026-08-12", row.getString(5));
    assertEquals(new BigDecimal("12345.67"), row.getDecimal(6));
    assertNull(row.get(7));
  }

  @Test
  void T_partitionNameConflictWithPhysicalDataColumn_FailsFast() throws Exception {
    final Path table = newOutput("yosegi-v2-partition-conflict-");
    final Path partitionPath = table.resolve("date=2026-08-12");

    spark
        .createDataFrame(
            List.of(RowFactory.create(1L, "physical-date")),
            new StructType()
                .add("id", DataTypes.LongType, false)
                .add("date", DataTypes.StringType, false))
        .write()
        .mode(SaveMode.Overwrite)
        .format("yosegi")
        .save(partitionPath.toString());

    final RuntimeException error =
        assertThrows(
            RuntimeException.class,
            () -> spark.read().format("yosegi-v2").load(table.toString()).schema());

    assertTrue(
        containsMessage(error, "Partition column conflicts with a Yosegi data column: date"),
        () -> "Unexpected exception chain: " + error);
  }

  private static boolean containsMessage(final Throwable error, final String expected) {
    Throwable current = error;
    while (current != null) {
      if (current.getMessage() != null && current.getMessage().contains(expected)) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  private Path newOutput(final String prefix) throws IOException {
    final Path path = Files.createTempDirectory(prefix);
    outputPaths.add(path);
    return path;
  }
}
