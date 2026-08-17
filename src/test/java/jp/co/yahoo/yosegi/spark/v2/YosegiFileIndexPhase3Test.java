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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class YosegiFileIndexPhase3Test {

  @TempDir Path tempDir;

  @Test
  void T_listPartitionedFiles_DiscoversHivePartitionsAndSkipsHiddenFiles() throws Exception {
    final Path first = tempDir.resolve("date=2026-07-29/hour=10/part-00000.yosegi");
    final Path second = tempDir.resolve("date=2026-07-30/hour=11/part-00001.yosegi");
    final Path hidden = tempDir.resolve("date=2026-07-30/hour=11/_SUCCESS");
    Files.createDirectories(first.getParent());
    Files.createDirectories(second.getParent());
    Files.write(first, new byte[] {1});
    Files.write(second, new byte[] {2});
    Files.write(hidden, new byte[] {3});

    final YosegiFileIndex.Listing listing =
        YosegiFileIndex.listPartitionedFiles(
            new Configuration(), Map.of(YosegiV2Options.PATH, tempDir.toString()));

    assertEquals(2, listing.files().length);
    assertArrayEquals(new String[] {"date", "hour"}, listing.partitionSchema().fieldNames());
    assertArrayEquals(
        new String[] {"2026-07-29", "10"},
        listing.files()[0].partitionValues(listing.partitionSchema()));
    assertArrayEquals(
        new String[] {"2026-07-30", "11"},
        listing.files()[1].partitionValues(listing.partitionSchema()));
  }

  @Test
  void T_listPartitionedFiles_BasePathIncludesPartitionAboveLoadRoot() throws Exception {
    final Path base = tempDir.resolve("table");
    final Path loadRoot = base.resolve("date=2026-07-29");
    final Path file = loadRoot.resolve("part-00000.yosegi");
    Files.createDirectories(loadRoot);
    Files.write(file, new byte[] {1});

    final YosegiFileIndex.Listing listing =
        YosegiFileIndex.listPartitionedFiles(
            new Configuration(),
            Map.of(
                YosegiV2Options.PATH,
                loadRoot.toString(),
                YosegiV2Options.BASE_PATH,
                base.toString()));

    assertArrayEquals(new String[] {"date"}, listing.partitionSchema().fieldNames());
    assertArrayEquals(
        new String[] {"2026-07-29"},
        listing.files()[0].partitionValues(listing.partitionSchema()));
  }

  @Test
  void T_listPartitionedFiles_DecodesPercentEscapesWithoutChangingPlus() throws Exception {
    final Path file = tempDir.resolve("key=a+b%20c/part-00000.yosegi");
    Files.createDirectories(file.getParent());
    Files.write(file, new byte[] {1});

    final YosegiFileIndex.Listing listing =
        YosegiFileIndex.listPartitionedFiles(
            new Configuration(), Map.of(YosegiV2Options.PATH, tempDir.toString()));

    assertArrayEquals(
        new String[] {"a+b c"},
        listing.files()[0].partitionValues(listing.partitionSchema()));
  }
}
