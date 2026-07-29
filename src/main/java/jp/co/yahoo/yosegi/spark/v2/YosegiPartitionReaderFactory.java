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
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.util.SerializableConfiguration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Creates executor-side columnar readers for planned Yosegi input partitions.
 *
 * <p>Schema values are serialized as Spark JSON because this factory crosses the driver/executor
 * boundary. The logical read schema, physical Yosegi schema, and partition schema remain separate
 * so Variant backing fields and path-derived partition values can be materialized correctly.
 *
 * <p>Read 1.0 is columnar-only in the DataSource V2 path. {@link #createReader(InputPartition)}
 * deliberately rejects row readers while {@link #supportColumnarReads(InputPartition)} advertises
 * columnar execution to Spark.
 */
public class YosegiPartitionReaderFactory implements PartitionReaderFactory {
  private final SerializableConfiguration hadoopConf;
  private final String readSchemaJson;
  private final String physicalSchemaJson;
  private final String partitionSchemaJson;
  private final Filter[] pushedFilters;
  private final Map<String, String> options;

  public YosegiPartitionReaderFactory(
      final SerializableConfiguration hadoopConf,
      final String readSchemaJson,
      final Filter[] pushedFilters,
      final Map<String, String> options) {
    this(hadoopConf, readSchemaJson, readSchemaJson, pushedFilters, options);
  }

  public YosegiPartitionReaderFactory(
      final SerializableConfiguration hadoopConf,
      final String readSchemaJson,
      final String physicalSchemaJson,
      final Filter[] pushedFilters,
      final Map<String, String> options) {
    this(
        hadoopConf,
        readSchemaJson,
        physicalSchemaJson,
        DataTypes.createStructType(Collections.emptyList()).json(),
        pushedFilters,
        options);
  }

  public YosegiPartitionReaderFactory(
      final SerializableConfiguration hadoopConf,
      final String readSchemaJson,
      final String physicalSchemaJson,
      final String partitionSchemaJson,
      final Filter[] pushedFilters,
      final Map<String, String> options) {
    this.hadoopConf = hadoopConf;
    this.readSchemaJson = readSchemaJson;
    this.physicalSchemaJson = physicalSchemaJson;
    this.partitionSchemaJson = partitionSchemaJson;
    this.pushedFilters = pushedFilters.clone();
    this.options = new HashMap<>(options);
  }

  @Override
  public PartitionReader<InternalRow> createReader(final InputPartition partition) {
    throw new UnsupportedOperationException("Yosegi V2 supports columnar reads only.");
  }

  @Override
  public boolean supportColumnarReads(final InputPartition partition) {
    return true;
  }

  @Override
  public PartitionReader<ColumnarBatch> createColumnarReader(final InputPartition partition) {
    try {
      return new YosegiColumnarPartitionReader(
          (YosegiInputPartition) partition,
          hadoopConf,
          readSchemaJson,
          physicalSchemaJson,
          partitionSchemaJson,
          pushedFilters,
          options);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to create Yosegi columnar reader.", e);
    }
  }
}
