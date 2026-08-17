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
import jp.co.yahoo.yosegi.spark.pushdown.FilterConnectorFactory;
import jp.co.yahoo.yosegi.spark.reader.IColumnarBatchReader;
import jp.co.yahoo.yosegi.spark.reader.SparkColumnarBatchReader;
import jp.co.yahoo.yosegi.spark.utils.ProjectionPushdownUtil;
import jp.co.yahoo.yosegi.spread.expression.AndExpressionNode;
import jp.co.yahoo.yosegi.spread.expression.IExpressionNode;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.util.SerializableConfiguration;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Executor-side Spark partition reader backed by the existing Yosegi columnar reader.
 *
 * <p>The reader converts raw path partition values to Spark internal values, opens the Hadoop input
 * stream, configures physical projection and conservative block-skip filters, and delegates batch
 * decoding to {@code SparkColumnarBatchReader}.
 *
 * <p>The logical read schema may contain Spark Variant values while the physical Yosegi schema
 * contains their struct backing. Both schemas are therefore passed to the lower-level converter.
 *
 * <p>After successful construction the wrapped reader owns the input stream and vector resources.
 * {@link #close()} propagates cleanup failures through the Spark {@link PartitionReader} contract.
 */
public class YosegiColumnarPartitionReader implements PartitionReader<ColumnarBatch> {
  private final IColumnarBatchReader reader;
  private ColumnarBatch current;

  public YosegiColumnarPartitionReader(
      final YosegiInputPartition partition,
      final SerializableConfiguration hadoopConf,
      final String readSchemaJson,
      final Filter[] pushedFilters,
      final Map<String, String> options)
      throws IOException {
    this(
        partition,
        hadoopConf,
        readSchemaJson,
        readSchemaJson,
        DataTypes.createStructType(Collections.emptyList()).json(),
        pushedFilters,
        options);
  }

  public YosegiColumnarPartitionReader(
      final YosegiInputPartition partition,
      final SerializableConfiguration hadoopConf,
      final String readSchemaJson,
      final String physicalSchemaJson,
      final Filter[] pushedFilters,
      final Map<String, String> options)
      throws IOException {
    this(
        partition,
        hadoopConf,
        readSchemaJson,
        physicalSchemaJson,
        DataTypes.createStructType(Collections.emptyList()).json(),
        pushedFilters,
        options);
  }

  public YosegiColumnarPartitionReader(
      final YosegiInputPartition partition,
      final SerializableConfiguration hadoopConf,
      final String readSchemaJson,
      final String physicalSchemaJson,
      final String partitionSchemaJson,
      final Filter[] pushedFilters,
      final Map<String, String> options)
      throws IOException {
    final StructType readSchema = (StructType) DataType.fromJson(readSchemaJson);
    final StructType physicalSchema = (StructType) DataType.fromJson(physicalSchemaJson);
    final StructType partitionSchema = (StructType) DataType.fromJson(partitionSchemaJson);
    final InternalRow partitionValues =
        YosegiPartitionValues.toInternalRow(partitionSchema, partition.partitionValues());
    final Path path = new Path(partition.path());
    final FileSystem fileSystem = path.getFileSystem(hadoopConf.value());
    final AndExpressionNode filterNode = createFilterNode(pushedFilters);
    final jp.co.yahoo.yosegi.config.Configuration yosegiConfiguration =
        YosegiV2Options.createYosegiConfiguration(options);
    yosegiConfiguration.set(
        "spread.reader.read.column.names",
        ProjectionPushdownUtil.createProjectionPushdownJson(readSchema));
    reader =
        new SparkColumnarBatchReader(
            partitionSchema,
            partitionValues,
            readSchema,
            physicalSchema,
            fileSystem.open(path),
            partition.fileLength(),
            partition.start(),
            partition.length(),
            yosegiConfiguration,
            filterNode);
    reader.setLineFilterNode(filterNode);
  }

  @Override
  public boolean next() throws IOException {
    if (!reader.hasNext()) {
      current = null;
      return false;
    }
    current = reader.next();
    return true;
  }

  @Override
  public ColumnarBatch get() {
    return current;
  }

  @Override
  public void close() throws IOException {
    try {
      reader.close();
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("Failed to close Yosegi reader.", e);
    }
  }

  private static AndExpressionNode createFilterNode(final Filter[] filters) {
    final AndExpressionNode result = new AndExpressionNode();
    for (Filter filter : filters) {
      final IExpressionNode node = FilterConnectorFactory.get(filter);
      if (node != null) {
        result.addChildNode(node);
      }
    }
    return result;
  }
}
