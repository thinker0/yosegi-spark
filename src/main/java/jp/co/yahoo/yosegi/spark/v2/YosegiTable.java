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

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Read-only Spark DataSource V2 table for a Yosegi dataset.
 *
 * <p>The table keeps three related schemas: the logical table schema visible to Spark, the physical
 * Yosegi data schema used by readers, and the discovered partition schema. Keeping these schemas
 * separate is required for Variant columns, whose logical Spark type may differ from their Yosegi
 * struct backing, and for partition columns, which are materialized from paths rather than files.
 *
 * <p>The only advertised capability is {@link TableCapability#BATCH_READ}. DataSource V2 writing is
 * intentionally outside the Read 1.0 contract.
 */
public class YosegiTable implements SupportsRead {
  private final StructType schema;
  private final StructType physicalSchema;
  private final StructType partitionSchema;
  private final Map<String, String> properties;

  public YosegiTable(final StructType schema, final Map<String, String> properties) {
    this(
        schema,
        schema,
        DataTypes.createStructType(Collections.emptyList()),
        properties);
  }

  public YosegiTable(
      final StructType schema,
      final StructType physicalSchema,
      final Map<String, String> properties) {
    this(
        schema,
        physicalSchema,
        DataTypes.createStructType(Collections.emptyList()),
        properties);
  }

  public YosegiTable(
      final StructType schema,
      final StructType physicalSchema,
      final StructType partitionSchema,
      final Map<String, String> properties) {
    this.schema = schema;
    this.physicalSchema = physicalSchema;
    this.partitionSchema = partitionSchema;
    this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
  }

  @Override
  public String name() {
    final String path = new CaseInsensitiveStringMap(properties).get(YosegiV2Options.PATH);
    return path == null ? "YosegiV2" : "YosegiV2[" + path + "]";
  }

  @Override
  public StructType schema() {
    return schema;
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public Set<TableCapability> capabilities() {
    return Collections.singleton(TableCapability.BATCH_READ);
  }

  @Override
  public ScanBuilder newScanBuilder(final CaseInsensitiveStringMap scanOptions) {
    final Map<String, String> merged = new HashMap<>(properties);
    merged.putAll(scanOptions.asCaseSensitiveMap());
    return new YosegiScanBuilder(
        schema,
        physicalSchema,
        partitionSchema,
        new CaseInsensitiveStringMap(merged));
  }
}
