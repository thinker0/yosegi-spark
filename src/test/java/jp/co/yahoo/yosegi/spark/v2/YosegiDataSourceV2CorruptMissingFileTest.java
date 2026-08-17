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

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertThrows;

/** Read 1.0 contract for corrupt and missing files: fail fast. */
class YosegiDataSourceV2CorruptMissingFileTest {
  private static SparkSession spark;
  private final List<Path> cleanupPaths = new ArrayList<>();

  @BeforeAll
  static void setUpSpark() {
    spark =
        SparkSession.builder()
            .appName("YosegiDataSourceV2CorruptMissingFileTest")
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
  void cleanup() throws IOException {
    spark.conf().set("spark.sql.files.ignoreMissingFiles", "false");
    spark.conf().set("spark.sql.files.ignoreCorruptFiles", "false");
    for (Path cleanupPath : cleanupPaths) {
      if (!Files.exists(cleanupPath)) {
        continue;
      }
      try (Stream<Path> paths = Files.walk(cleanupPath)) {
        paths.sorted((left, right) -> right.compareTo(left)).map(Path::toFile).forEach(File::delete);
      }
    }
    cleanupPaths.clear();
  }

  @Test
  void T_missingRootPath_FailsFastByDefault() throws Exception {
    final Path parent = newCleanupDirectory("yosegi-v2-missing-default-");
    final Path missing = parent.resolve("does-not-exist");

    assertThrows(
        Exception.class,
        () -> spark.read().format("yosegi-v2").load(missing.toString()).collectAsList());
  }

  @Test
  void T_missingRootPath_StillFailsFastWhenSparkIgnoreMissingFilesIsTrue() throws Exception {
    final Path parent = newCleanupDirectory("yosegi-v2-missing-ignore-");
    final Path missing = parent.resolve("does-not-exist");
    spark.conf().set("spark.sql.files.ignoreMissingFiles", "true");

    assertThrows(
        Exception.class,
        () -> spark.read().format("yosegi-v2").load(missing.toString()).collectAsList());
  }

  @Test
  void T_corruptFile_FailsFastByDefault() throws Exception {
    final Path directory = newCleanupDirectory("yosegi-v2-corrupt-default-");
    final Path corrupt = directory.resolve("corrupt.yosegi");
    Files.write(corrupt, "not-a-yosegi-file".getBytes(StandardCharsets.UTF_8));

    assertThrows(
        Exception.class,
        () -> spark.read().format("yosegi-v2").load(corrupt.toString()).collectAsList());
  }

  @Test
  void T_corruptFile_StillFailsFastWhenSparkIgnoreCorruptFilesIsTrue() throws Exception {
    final Path directory = newCleanupDirectory("yosegi-v2-corrupt-ignore-");
    final Path corrupt = directory.resolve("corrupt.yosegi");
    Files.write(corrupt, "not-a-yosegi-file".getBytes(StandardCharsets.UTF_8));
    spark.conf().set("spark.sql.files.ignoreCorruptFiles", "true");

    assertThrows(
        Exception.class,
        () -> spark.read().format("yosegi-v2").load(corrupt.toString()).collectAsList());
  }

  private Path newCleanupDirectory(final String prefix) throws IOException {
    final Path path = Files.createTempDirectory(prefix);
    cleanupPaths.add(path);
    return path;
  }
}
