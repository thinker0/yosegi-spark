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
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsPushDownVariantExtractions;
import org.apache.spark.sql.connector.read.VariantExtraction;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.VariantType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.util.SerializableConfiguration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Builds a Yosegi DataSource V2 scan and negotiates Spark pushdown contracts.
 *
 * <p>The builder handles required-column pruning, regular Spark filters, partition filters, and
 * Spark 4.2 Variant extractions. These optimizations have different correctness contracts and must
 * not be conflated.
 *
 * <p>Regular data filters are used as conservative Yosegi block-skip hints. Spark receives the
 * original filters back as residual predicates and therefore remains responsible for final SQL,
 * NULL, and cast semantics. Compound filters are decomposed only where doing so is safe; in
 * particular an unsupported child must not be hidden inside a translated {@code OR} or {@code NOT}
 * expression.
 *
 * <p>Partition filters are evaluated on discovered path values to prune files. Malformed or
 * otherwise uncertain values are kept rather than silently pruned.
 *
 * <p>Variant extraction follows a stricter rule: an extraction is accepted only when the Yosegi
 * physical leaf already has exactly the Spark type requested by the extraction. Cast, overflow,
 * timezone, {@code failOnError}, and related semantics remain owned by Spark. The current Variant
 * ordinal contract is all-or-none for a push request.
 */
public class YosegiScanBuilder
    implements ScanBuilder,
        SupportsPushDownRequiredColumns,
        SupportsPushDownFilters,
        SupportsPushDownVariantExtractions {
  private final StructType tableSchema;
  private final StructType physicalSchema;
  private final StructType partitionSchema;
  private final boolean validatePhysicalSchema;
  private final CaseInsensitiveStringMap options;
  private StructType requiredSchema;
  private Filter[] pushedFilters = new Filter[0];
  private Filter[] partitionFilters = new Filter[0];
  private VariantExtraction[] pushedVariantExtractions = new VariantExtraction[0];

  public YosegiScanBuilder(
      final StructType tableSchema, final CaseInsensitiveStringMap options) {
    this(
        tableSchema,
        tableSchema,
        DataTypes.createStructType(Collections.emptyList()),
        false,
        options);
  }

  public YosegiScanBuilder(
      final StructType tableSchema,
      final StructType physicalSchema,
      final CaseInsensitiveStringMap options) {
    this(
        tableSchema,
        physicalSchema,
        DataTypes.createStructType(Collections.emptyList()),
        true,
        options);
  }

  public YosegiScanBuilder(
      final StructType tableSchema,
      final StructType physicalSchema,
      final StructType partitionSchema,
      final CaseInsensitiveStringMap options) {
    this(tableSchema, physicalSchema, partitionSchema, true, options);
  }

  private YosegiScanBuilder(
      final StructType tableSchema,
      final StructType physicalSchema,
      final StructType partitionSchema,
      final boolean validatePhysicalSchema,
      final CaseInsensitiveStringMap options) {
    this.tableSchema = tableSchema;
    this.physicalSchema = physicalSchema;
    this.partitionSchema = partitionSchema;
    this.validatePhysicalSchema = validatePhysicalSchema;
    this.requiredSchema = tableSchema;
    this.options = options;
  }

  @Override
  public void pruneColumns(final StructType requiredSchema) {
    this.requiredSchema = requiredSchema;
  }

  @Override
  public Filter[] pushFilters(final Filter[] filters) {
    final List<Filter> acceptedDataFilters = new ArrayList<>();
    final List<Filter> acceptedPartitionFilters = new ArrayList<>();
    for (Filter filter : filters) {
      collectPartitionFilters(filter, acceptedPartitionFilters);
      collectDataFilters(filter, acceptedDataFilters);
    }
    pushedFilters = acceptedDataFilters.toArray(new Filter[0]);
    partitionFilters = acceptedPartitionFilters.toArray(new Filter[0]);
    // Data filters are block-skip hints and partition filters are used for file pruning. Spark still
    // evaluates every predicate, which preserves correctness for null and cast semantics.
    return filters;
  }

  @Override
  public Filter[] pushedFilters() {
    final Filter[] result = new Filter[pushedFilters.length + partitionFilters.length];
    System.arraycopy(pushedFilters, 0, result, 0, pushedFilters.length);
    System.arraycopy(partitionFilters, 0, result, pushedFilters.length, partitionFilters.length);
    return result;
  }

  Filter[] partitionFilters() {
    return partitionFilters.clone();
  }

  private void collectPartitionFilters(
      final Filter filter, final List<Filter> acceptedPartitionFilters) {
    if (YosegiPartitionValues.isPartitionOnlyFilter(filter, partitionSchema)) {
      acceptedPartitionFilters.add(filter);
      return;
    }
    if (filter instanceof And) {
      final And and = (And) filter;
      collectPartitionFilters(and.left(), acceptedPartitionFilters);
      collectPartitionFilters(and.right(), acceptedPartitionFilters);
    }
  }

  private void collectDataFilters(
      final Filter filter, final List<Filter> acceptedDataFilters) {
    if (!referencesPartitionColumn(filter) && FilterConnectorFactory.get(filter) != null) {
      acceptedDataFilters.add(filter);
      return;
    }
    if (filter instanceof And) {
      final And and = (And) filter;
      collectDataFilters(and.left(), acceptedDataFilters);
      collectDataFilters(and.right(), acceptedDataFilters);
    }
  }

  private boolean referencesPartitionColumn(final Filter filter) {
    final List<String> partitionNames = Arrays.asList(partitionSchema.fieldNames());
    for (String reference : filter.references()) {
      if (partitionNames.contains(reference)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean[] pushVariantExtractions(final VariantExtraction[] extractions) {
    final boolean[] results = new boolean[extractions.length];
    // Keep the current all-or-none ordinal contract. More importantly, only accept an extraction
    // when the physical Yosegi field already has exactly the requested Spark type. Cast, overflow,
    // failOnError, try_variant_get and timezone behavior then remain owned by Spark.
    for (VariantExtraction extraction : extractions) {
      if (!isSupported(extraction)) {
        pushedVariantExtractions = new VariantExtraction[0];
        return results;
      }
    }
    Arrays.fill(results, true);
    pushedVariantExtractions = extractions.clone();
    return results;
  }

  @Override
  public Scan build() {
    final SparkSession sparkSession = SparkSession.active();
    return new YosegiScan(
        tableSchema,
        physicalSchema,
        partitionSchema,
        requiredSchema,
        pushedFilters,
        partitionFilters,
        pushedVariantExtractions,
        options.asCaseSensitiveMap(),
        new SerializableConfiguration(
            new Configuration(sparkSession.sparkContext().hadoopConfiguration())));
  }

  private boolean isSupported(final VariantExtraction extraction) {
    final String[] columnName = extraction.columnName();
    if (columnName.length != 1) {
      return false;
    }

    final StructField logicalField = findTopLevelField(tableSchema, columnName[0]);
    if (logicalField == null || !(logicalField.dataType() instanceof VariantType)) {
      return false;
    }

    if (!isSupportedOutputType(extraction.expectedDataType())) {
      return false;
    }
    final VariantPath sourcePath = VariantStructUtil.getVariantPath(extraction.metadata());
    if (sourcePath == null || sourcePath.segments().isEmpty()) {
      return false;
    }

    // The legacy two-argument constructor has no independent physical schema. Preserve its
    // original capability check for unit tests and direct callers. The DataSource V2 path uses
    // the full constructor and therefore performs the exact physical type check below.
    if (!validatePhysicalSchema) {
      return true;
    }
    final StructField physicalField = findTopLevelField(physicalSchema, columnName[0]);
    if (physicalField == null || !(physicalField.dataType() instanceof StructType)) {
      return false;
    }
    final DataType physicalLeaf =
        findNestedType(physicalField.dataType(), sourcePath.segments());
    return physicalLeaf != null
        && physicalLeaf.equals(extraction.expectedDataType());
  }

  private static DataType findNestedType(
      final DataType rootType, final List<VariantPath.Segment> path) {
    DataType current = rootType;
    for (VariantPath.Segment segment : path) {
      if (segment instanceof VariantPath.Field) {
        if (!(current instanceof StructType)) {
          return null;
        }
        final StructField field =
            findTopLevelField((StructType) current, ((VariantPath.Field) segment).name());
        if (field == null) {
          return null;
        }
        current = field.dataType();
      } else if (segment instanceof VariantPath.Index) {
        if (!(current instanceof ArrayType)) {
          return null;
        }
        current = ((ArrayType) current).elementType();
      } else {
        return null;
      }
    }
    return current;
  }

  private static boolean isSupportedOutputType(final DataType dataType) {
    if (dataType instanceof VariantType
        || dataType instanceof ArrayType
        || dataType instanceof StructType
        || dataType instanceof MapType) {
      return false;
    }
    final Class<?> klass = dataType.getClass();
    return klass == DataTypes.StringType.getClass()
        || klass == DataTypes.BinaryType.getClass()
        || klass == DataTypes.BooleanType.getClass()
        || klass == DataTypes.ByteType.getClass()
        || klass == DataTypes.ShortType.getClass()
        || klass == DataTypes.IntegerType.getClass()
        || klass == DataTypes.LongType.getClass()
        || klass == DataTypes.FloatType.getClass()
        || klass == DataTypes.DoubleType.getClass()
        || klass == DataTypes.DateType.getClass()
        || klass == DataTypes.TimestampType.getClass()
        || klass == DataTypes.TimestampNTZType.getClass()
        || klass == DecimalType.class;
  }

  private static StructField findTopLevelField(final StructType schema, final String name) {
    return Arrays.stream(schema.fields())
        .filter(field -> field.name().equals(name))
        .findFirst()
        .orElse(null);
  }
}
