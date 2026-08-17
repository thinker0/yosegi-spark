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
import org.apache.hadoop.fs.FileStatus;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.VariantExtraction;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
/**
 * Planned batch scan for the Yosegi DataSource V2 reader.
 *
 * <p>The scan freezes the logical, physical, required, and partition schemas together with the
 * pushdown state negotiated during planning. It performs file listing and partition pruning,
 * byte-range split planning, read-schema construction, and creation of executor-side readers.
 *
 * <p>Split ranges are planning hints. Yosegi assigns complete blocks to those ranges, so split
 * correctness is defined by zero missing rows and zero duplicate rows even when a boundary falls
 * inside a block.
 *
 * <p>The effective read schema is intentionally distinct from the table schema. Accepted Variant
 * extractions read only their required physical backing fields. {@link #description()} exposes
 * this state for Spark {@code explain} output.
 */
public class YosegiScan implements Scan, Batch {
  private final StructType tableSchema;
  private final StructType physicalSchema;
  private final StructType partitionSchema;
  private final StructType requiredSchema;
  private final Filter[] pushedFilters;
  private final Filter[] partitionFilters;
  private final VariantExtraction[] pushedVariantExtractions;
  private final Map<String, String> options;
  private final SerializableConfiguration hadoopConf;
  private final StructType readSchema;
  private final StructType dataReadSchema;
  private final StructType partitionReadSchema;
  public YosegiScan(
      final StructType tableSchema,
      final StructType requiredSchema,
      final Filter[] pushedFilters,
      final VariantExtraction[] pushedVariantExtractions,
      final Map<String, String> options,
      final SerializableConfiguration hadoopConf) {
    this(
        tableSchema,
        tableSchema,
        DataTypes.createStructType(Collections.emptyList()),
        requiredSchema,
        pushedFilters,
        new Filter[0],
        pushedVariantExtractions,
        options,
        hadoopConf);
  }
  public YosegiScan(
      final StructType tableSchema,
      final StructType physicalSchema,
      final StructType requiredSchema,
      final Filter[] pushedFilters,
      final VariantExtraction[] pushedVariantExtractions,
      final Map<String, String> options,
      final SerializableConfiguration hadoopConf) {
    this(
        tableSchema,
        physicalSchema,
        DataTypes.createStructType(Collections.emptyList()),
        requiredSchema,
        pushedFilters,
        new Filter[0],
        pushedVariantExtractions,
        options,
        hadoopConf);
  }
  public YosegiScan(
      final StructType tableSchema,
      final StructType physicalSchema,
      final StructType partitionSchema,
      final StructType requiredSchema,
      final Filter[] pushedFilters,
      final Filter[] partitionFilters,
      final VariantExtraction[] pushedVariantExtractions,
      final Map<String, String> options,
      final SerializableConfiguration hadoopConf) {
    this.tableSchema = tableSchema;
    this.physicalSchema = physicalSchema;
    this.partitionSchema = partitionSchema;
    this.requiredSchema = requiredSchema;
    this.pushedFilters = pushedFilters.clone();
    this.partitionFilters = partitionFilters.clone();
    this.pushedVariantExtractions = pushedVariantExtractions.clone();
    this.options = Map.copyOf(options);
    this.hadoopConf = hadoopConf;
    this.readSchema = createReadSchema();
    this.dataReadSchema = selectFields(readSchema, partitionSchema, false);
    this.partitionReadSchema = selectFields(readSchema, partitionSchema, true);
  }
  @Override
  public StructType readSchema() {
    return readSchema;
  }

  StructType dataReadSchema() {
    return dataReadSchema;
  }

  StructType partitionReadSchema() {
    return partitionReadSchema;
  }

  @Override
  public String description() {
    return "Yosegi V2 scan"
        + ", ReadSchema: "
        + readSchema.simpleString()
        + ", PushedFilters: "
        + Arrays.toString(pushedFilters)
        + ", PartitionFilters: "
        + Arrays.toString(partitionFilters)
        + ", PushedVariantExtractions: "
        + pushedVariantExtractions.length;
  }
  @Override
  public Batch toBatch() {
    return this;
  }

  @Override
  public ColumnarSupportMode columnarSupportMode() {
    return ColumnarSupportMode.SUPPORTED;
  }
  @Override
  public InputPartition[] planInputPartitions() {
    try {
      final YosegiFileIndex.Listing listing =
          YosegiFileIndex.listPartitionedFiles(hadoopConf.value(), options);
      final List<InputPartition> partitions = new ArrayList<>();
      final long splitSize =
          YosegiV2Options.getLong(options, YosegiV2Options.SPLIT_SIZE, 0L);
      for (YosegiFileIndex.PartitionedFile partitionedFile : listing.files()) {
        if (!YosegiPartitionValues.matches(
            partitionFilters, partitionSchema, partitionedFile.partitionValues())) {
          continue;
        }
        final FileStatus file = partitionedFile.fileStatus();
        final String[] partitionValues = partitionedFile.partitionValues(partitionReadSchema);
        if (splitSize <= 0L || file.getLen() <= splitSize) {
          partitions.add(createPartition(file, 0L, file.getLen(), partitionValues));
        } else {
          // YosegiReader always reads the file header, maps each byte range to block start offsets,
          // and assigns a block to exactly one half-open [start, start + length) range. Therefore
          // contiguous byte splits are safe even when splitSize is not block aligned.
          for (long start = 0L; start < file.getLen(); start += splitSize) {
            final long length = Math.min(splitSize, file.getLen() - start);
            partitions.add(createPartition(file, start, length, partitionValues));
          }
        }
      }
      return partitions.toArray(new InputPartition[0]);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to plan Yosegi input partitions.", e);
    }
  }
  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new YosegiPartitionReaderFactory(
        hadoopConf,
        dataReadSchema.json(),
        physicalSchema.json(),
        partitionReadSchema.json(),
        pushedFilters,
        options);
  }
  private YosegiInputPartition createPartition(
      final FileStatus file,
      final long start,
      final long length,
      final String[] partitionValues)
      throws IOException {
    final String[] locations =
        YosegiFileIndex.getPreferredLocations(hadoopConf.value(), file, start, length);
    return new YosegiInputPartition(
        file.getPath().toString(),
        file.getLen(),
        start,
        length,
        partitionValues,
        locations);
  }
  private StructType createReadSchema() {
    if (pushedVariantExtractions.length == 0) {
      return requiredSchema;
    }
    final Map<String, StructField> requiredByName = new LinkedHashMap<>();
    for (StructField field : requiredSchema.fields()) {
      requiredByName.put(field.name(), field);
    }
    final Map<String, List<VariantExtraction>> byColumn = new LinkedHashMap<>();
    for (VariantExtraction extraction : pushedVariantExtractions) {
      byColumn
          .computeIfAbsent(extraction.columnName()[0], ignored -> new ArrayList<>())
          .add(extraction);
    }
    // Only read ordinary required columns plus the physical fields needed by accepted Variant
    // extractions. This fixes the previous tableSchema-based all-column read.
    final List<StructField> fields = new ArrayList<>();
    for (StructField tableField : tableSchema.fields()) {
      final StructField requiredField = requiredByName.get(tableField.name());
      final List<VariantExtraction> extractions = byColumn.get(tableField.name());
      // Once Spark accepts a Variant extraction it rewrites the relation column to an ordinal
      // Struct. requiredSchema may still contain the original VariantType because the Variant
      // pushdown rule runs before or independently of ordinary pruneColumns. Therefore the accepted
      // extraction Struct must take precedence here; otherwise Spark sees VariantType where it
      // expects Struct<0,...> and analysis fails with an unresolved GetStructField.
      if (extractions != null) {
        fields.add(createVariantExtractionField(tableField, extractions));
      } else if (requiredField != null) {
        fields.add(requiredField);
      }
    }
    return new StructType(fields.toArray(new StructField[0]));
  }
  private static StructField createVariantExtractionField(
      final StructField sourceField, final List<VariantExtraction> extractions) {
    final StructField[] extractedFields = new StructField[extractions.size()];
    for (int ordinal = 0; ordinal < extractions.size(); ordinal++) {
      final VariantExtraction extraction = extractions.get(ordinal);
      extractedFields[ordinal] =
          new StructField(
              Integer.toString(ordinal),
              extraction.expectedDataType(),
              true,
              extraction.metadata());
    }
    return new StructField(
        sourceField.name(),
        new StructType(extractedFields),
        sourceField.nullable(),
        sourceField.metadata());
  }
  private static StructType selectFields(
      final StructType schema, final StructType partitionSchema, final boolean selectPartitions) {
    final Set<String> partitionNames =
        new HashSet<>(Arrays.asList(partitionSchema.fieldNames()));
    final List<StructField> fields = new ArrayList<>();
    for (StructField field : schema.fields()) {
      if (partitionNames.contains(field.name()) == selectPartitions) {
        fields.add(field);
      }
    }
    return new StructType(fields.toArray(new StructField[0]));
  }
}
