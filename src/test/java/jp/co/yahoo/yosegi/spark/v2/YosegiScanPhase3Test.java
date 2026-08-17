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
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class YosegiScanPhase3Test {

  @TempDir Path tempDir;

  @Test
  void T_planInputPartitions_PrunesPartitionsAndPreservesValuesAcrossSplits() throws Exception {
    final Path first = tempDir.resolve("date=2026-07-29/part-00000.yosegi");
    final Path second = tempDir.resolve("date=2026-07-30/part-00001.yosegi");
    Files.createDirectories(first.getParent());
    Files.createDirectories(second.getParent());
    Files.write(first, new byte[10]);
    Files.write(second, new byte[10]);

    final StructType physicalSchema = new StructType().add("id", DataTypes.LongType, true);
    final StructType partitionSchema =
        new StructType().add("date", DataTypes.StringType, true);
    final StructType tableSchema =
        new StructType()
            .add("id", DataTypes.LongType, true)
            .add("date", DataTypes.StringType, true);
    final YosegiScan scan =
        new YosegiScan(
            tableSchema,
            physicalSchema,
            partitionSchema,
            tableSchema,
            new Filter[0],
            new Filter[] {new EqualTo("date", "2026-07-29")},
            new org.apache.spark.sql.connector.read.VariantExtraction[0],
            Map.of(
                YosegiV2Options.PATH,
                tempDir.toString(),
                YosegiV2Options.SPLIT_SIZE,
                "4"),
            new SerializableConfiguration(new Configuration()));

    final InputPartition[] planned = scan.planInputPartitions();

    assertEquals(3, planned.length);
    assertEquals(1, scan.dataReadSchema().length());
    assertEquals(1, scan.partitionReadSchema().length());
    assertEquals(2, scan.readSchema().length());
    assertEquals(0L, ((YosegiInputPartition) planned[0]).start());
    assertEquals(4L, ((YosegiInputPartition) planned[1]).start());
    assertEquals(8L, ((YosegiInputPartition) planned[2]).start());
    assertArrayEquals(
        new String[] {"2026-07-29"},
        ((YosegiInputPartition) planned[0]).partitionValues());
  }
}
