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

import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
/**
 * Utilities for Spark Variant extraction metadata and Yosegi struct-backed Variant columns.
 *
 * <p>Spark 4.2 communicates extraction information through public Variant metadata. This helper
 * reads the {@code __VARIANT_METADATA_KEY} structure, parses its path into {@link VariantPath}, and
 * provides compatibility helpers shared by scan planning and Variant loaders.
 *
 * <p>A successfully decoded path is not permission to reproduce Spark cast, overflow, timezone, or
 * error semantics in Yosegi. Exact-type acceptance remains the responsibility of
 * {@link YosegiScanBuilder}.
 */
public final class VariantStructUtil {
  public static final String VARIANT_METADATA_KEY = "__VARIANT_METADATA_KEY";
  public static final String PATH_METADATA_KEY = "path";
  public static final String FAIL_ON_ERROR_METADATA_KEY = "failOnError";
  public static final String TIME_ZONE_ID_METADATA_KEY = "timeZoneId";

  private VariantStructUtil() {}

  public static boolean isVariantStruct(final StructType structType) {
    if (structType == null || structType.fields().length == 0) {
      return false;
    }
    for (StructField field : structType.fields()) {
      if (!field.metadata().contains(VARIANT_METADATA_KEY)) {
        return false;
      }
    }
    return true;
  }

  public static Metadata getVariantMetadata(final Metadata metadata) {
    if (metadata == null || !metadata.contains(VARIANT_METADATA_KEY)) {
      return null;
    }
    return metadata.getMetadata(VARIANT_METADATA_KEY);
  }

  public static String getPath(final Metadata metadata) {
    final Metadata nested = getVariantMetadata(metadata);
    if (nested == null || !nested.contains(PATH_METADATA_KEY)) {
      return null;
    }
    return nested.getString(PATH_METADATA_KEY);
  }

  public static VariantPath getVariantPath(final Metadata metadata) {
    return VariantPath.parse(getPath(metadata));
  }

  /** Compatibility helper retained for existing direct-field callers. */
  public static String getDirectFieldName(final Metadata metadata) {
    return getDirectFieldName(getPath(metadata));
  }

  public static String getDirectFieldName(final String path) {
    final VariantPath parsed = VariantPath.parse(path);
    if (parsed == null || parsed.segments().size() != 1
        || !(parsed.segments().get(0) instanceof VariantPath.Field)) {
      return null;
    }
    return ((VariantPath.Field) parsed.segments().get(0)).name();
  }

  /** Object-only nested paths can be translated into Yosegi projection columns. */
  public static String[] getObjectFieldPath(final Metadata metadata) {
    final VariantPath path = getVariantPath(metadata);
    return path == null ? null : path.objectFieldPath();
  }

  /** Physical columns needed for a path. Array indexes are evaluated after reading the array. */
  public static String[] getPhysicalProjectionPath(final Metadata metadata) {
    final VariantPath path = getVariantPath(metadata);
    return path == null ? null : path.physicalProjectionPath();
  }

}
