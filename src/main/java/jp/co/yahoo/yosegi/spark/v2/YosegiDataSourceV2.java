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

import jp.co.yahoo.yosegi.spark.schema.SchemaFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.VariantType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Spark DataSource V2 entry point for Yosegi batch reads.
 *
 * <p>The provider is registered as {@code yosegi-v2}. The existing DataSource V1 provider remains
 * registered as {@code yosegi}; the two names intentionally coexist so applications can migrate
 * read paths independently from existing V1 write paths.
 *
 * <p>This class owns the driver-side boundary between Spark and Yosegi schema discovery. It lists
 * input files, infers the physical Yosegi schema, discovers Hive-style partition columns, applies
 * configured Variant columns to the logical Spark schema, and rejects partition/data-column name
 * conflicts before a {@link YosegiTable} is created.
 *
 * <p>Variant columns are logical Spark {@code VariantType} columns backed by Yosegi structs. The
 * physical backing schema is kept separately because projection and Variant extraction need the
 * original typed Yosegi structure even when the table schema exposes a Variant value.
 *
 * <p>Read 1.0 is batch-read only. Writer capabilities are deliberately not advertised here.
 */
public class YosegiDataSourceV2 implements TableProvider, DataSourceRegister {

  @Override
  public String shortName() {
    return "yosegi-v2";
  }

  @Override
  public StructType inferSchema(final CaseInsensitiveStringMap options) {
    final SourceInfo source = inferSource(options);
    final StructType logicalDataSchema =
        applyVariantColumns(
            source.physicalDataSchema, options.get(YosegiV2Options.VARIANT_COLUMNS));
    validateNoPartitionNameConflict(logicalDataSchema, source.discoveredPartitionSchema);
    return append(logicalDataSchema, source.discoveredPartitionSchema);
  }

  @Override
  public Table getTable(
      final StructType schema,
      final Transform[] partitioning,
      final Map<String, String> properties) {
    final CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(properties);
    final SourceInfo source = inferSource(options);

    final StructType partitionSchema =
        schema == null
            ? source.discoveredPartitionSchema
            : YosegiPartitionValues.resolveSchema(source.discoveredPartitionSchema, schema);
    final StructType logicalDataSchema;
    if (schema == null) {
      logicalDataSchema =
          applyVariantColumns(
              source.physicalDataSchema, options.get(YosegiV2Options.VARIANT_COLUMNS));
    } else {
      logicalDataSchema = removePartitionFields(schema, partitionSchema);
    }

    validateNoPartitionNameConflict(source.physicalDataSchema, partitionSchema);
    validateVariantBackings(logicalDataSchema, source.physicalDataSchema);
    final StructType tableSchema = append(logicalDataSchema, partitionSchema);
    return new YosegiTable(
        tableSchema,
        source.physicalDataSchema,
        partitionSchema,
        new HashMap<>(properties));
  }

  @Override
  public boolean supportsExternalMetadata() {
    return true;
  }

  private static SourceInfo inferSource(final CaseInsensitiveStringMap options) {
    final SparkSession sparkSession = SparkSession.active();
    try {
      final Configuration hadoopConfiguration =
          new Configuration(sparkSession.sparkContext().hadoopConfiguration());
      final YosegiFileIndex.Listing listing =
          YosegiFileIndex.listPartitionedFiles(
              hadoopConfiguration, options.asCaseSensitiveMap());
      if (listing.files().length == 0) {
        throw new IllegalArgumentException("No Yosegi files were found for schema inference.");
      }
      final FileStatus[] files =
          Arrays.stream(listing.files())
              .map(YosegiFileIndex.PartitionedFile::fileStatus)
              .toArray(FileStatus[]::new);
      final StructType physicalDataSchema =
          SchemaFactory.create(
              sparkSession,
              YosegiV2Options.createYosegiConfiguration(options.asCaseSensitiveMap()),
              files);
      return new SourceInfo(physicalDataSchema, listing.partitionSchema());
    } catch (IOException e) {
      throw new IllegalStateException("Failed to infer Yosegi schema.", e);
    }
  }

  private static StructType applyVariantColumns(
      final StructType schema, final String variantColumnsOption) {
    if (variantColumnsOption == null || variantColumnsOption.trim().isEmpty()) {
      return schema;
    }
    final Set<String> variantColumns =
        new HashSet<>(Arrays.asList(variantColumnsOption.split("\\s*,\\s*")));
    variantColumns.removeIf(String::isBlank);
    final Set<String> found = new HashSet<>();
    final StructField[] fields = schema.fields().clone();
    for (int i = 0; i < fields.length; i++) {
      if (!variantColumns.contains(fields[i].name())) {
        continue;
      }
      if (!(fields[i].dataType() instanceof StructType)) {
        throw new IllegalArgumentException(
            "Variant column must be backed by a Yosegi Struct: " + fields[i].name());
      }
      found.add(fields[i].name());
      fields[i] =
          new StructField(
              fields[i].name(),
              DataTypes.VariantType,
              fields[i].nullable(),
              fields[i].metadata());
    }
    variantColumns.removeAll(found);
    if (!variantColumns.isEmpty()) {
      throw new IllegalArgumentException(
          "Variant columns were not found in the inferred schema: " + variantColumns);
    }
    return new StructType(fields);
  }

  private static StructType removePartitionFields(
      final StructType schema, final StructType partitionSchema) {
    final Set<String> partitionNames = new HashSet<>(Arrays.asList(partitionSchema.fieldNames()));
    final List<StructField> fields = new ArrayList<>();
    for (StructField field : schema.fields()) {
      if (!partitionNames.contains(field.name())) {
        fields.add(field);
      }
    }
    return new StructType(fields.toArray(new StructField[0]));
  }

  private static StructType append(final StructType left, final StructType right) {
    final StructField[] fields = new StructField[left.length() + right.length()];
    System.arraycopy(left.fields(), 0, fields, 0, left.length());
    System.arraycopy(right.fields(), 0, fields, left.length(), right.length());
    return new StructType(fields);
  }

  private static void validateNoPartitionNameConflict(
      final StructType dataSchema, final StructType partitionSchema) {
    final Set<String> dataNames = new HashSet<>(Arrays.asList(dataSchema.fieldNames()));
    for (String partitionName : partitionSchema.fieldNames()) {
      if (dataNames.contains(partitionName)) {
        throw new IllegalArgumentException(
            "Partition column conflicts with a Yosegi data column: " + partitionName);
      }
    }
  }

  private static void validateVariantBackings(
      final StructType logicalSchema, final StructType physicalSchema) {
    for (StructField logicalField : logicalSchema.fields()) {
      if (!(logicalField.dataType() instanceof VariantType)) {
        continue;
      }
      final StructField physicalField = findField(physicalSchema, logicalField.name());
      if (physicalField == null || !(physicalField.dataType() instanceof StructType)) {
        throw new IllegalArgumentException(
            "Variant column must be backed by a Yosegi Struct: " + logicalField.name());
      }
    }
  }

  private static StructField findField(final StructType schema, final String name) {
    for (StructField field : schema.fields()) {
      if (field.name().equals(name)) {
        return field;
      }
    }
    return null;
  }

  private static final class SourceInfo {
    private final StructType physicalDataSchema;
    private final StructType discoveredPartitionSchema;

    private SourceInfo(
        final StructType physicalDataSchema, final StructType discoveredPartitionSchema) {
      this.physicalDataSchema = physicalDataSchema;
      this.discoveredPartitionSchema = discoveredPartitionSchema;
    }
  }
}
