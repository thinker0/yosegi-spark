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
import java.util.Map;
/**
 * Option names and configuration helpers used by the Yosegi DataSource V2 reader.
 *
 * <p>Spark data-source options are treated case-insensitively at this boundary. Only options owned
 * by the lower-level Yosegi reader are copied into Yosegi's configuration object. V2 planning
 * options such as input paths, {@code basePath}, Variant columns, and split size remain Spark-side
 * concerns.
 */
public final class YosegiV2Options {
  public static final String PATH = "path";
  public static final String PATHS = "paths";
  public static final String BASE_PATH = "basePath";
  public static final String EXPAND_COLUMN = "spread.reader.expand.column";
  public static final String FLATTEN_COLUMN = "spread.reader.flatten.column";
  public static final String VARIANT_COLUMNS = "yosegi.variant.columns";
  public static final String SPLIT_SIZE = "spark.yosegi.v2.split.size";

  private YosegiV2Options() {}

  public static jp.co.yahoo.yosegi.config.Configuration createYosegiConfiguration(
      final Map<String, String> options) {
    final jp.co.yahoo.yosegi.config.Configuration result =
        new jp.co.yahoo.yosegi.config.Configuration();
    copyIfPresent(options, result, EXPAND_COLUMN);
    copyIfPresent(options, result, FLATTEN_COLUMN);
    return result;
  }

  public static long getLong(
      final Map<String, String> options, final String key, final long defaultValue) {
    final String value = getIgnoreCase(options, key);
    return value == null ? defaultValue : Long.parseLong(value);
  }

  public static String getIgnoreCase(final Map<String, String> options, final String key) {
    for (Map.Entry<String, String> entry : options.entrySet()) {
      if (entry.getKey().equalsIgnoreCase(key)) {
        return entry.getValue();
      }
    }
    return null;
  }

  private static void copyIfPresent(
      final Map<String, String> options,
      final jp.co.yahoo.yosegi.config.Configuration configuration,
      final String key) {
    final String value = getIgnoreCase(options, key);
    if (value != null) {
      configuration.set(key, value);
    }
  }
}
