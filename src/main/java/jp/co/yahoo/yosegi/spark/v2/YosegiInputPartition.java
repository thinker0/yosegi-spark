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
import org.apache.spark.sql.connector.read.InputPartition;

/**
 * Serializable executor input describing one byte range of a Yosegi file.
 *
 * <p>The partition carries the complete file length, requested byte-range start and length, raw
 * Hive-style partition values, and Spark preferred locations. The range is a planning hint to the
 * Yosegi reader; complete Yosegi blocks are still assigned so that arbitrary split boundaries do
 * not create missing or duplicate rows.
 *
 * <p>Partition values and preferred locations are defensively copied on construction and access so
 * driver-side planning state cannot be mutated after the partition has been serialized.
 */
public class YosegiInputPartition implements InputPartition {
  private final String path;
  private final long fileLength;
  private final long start;
  private final long length;
  private final String[] partitionValues;
  private final String[] preferredLocations;

  public YosegiInputPartition(
      final String path,
      final long fileLength,
      final long start,
      final long length,
      final String[] preferredLocations) {
    this(path, fileLength, start, length, new String[0], preferredLocations);
  }

  public YosegiInputPartition(
      final String path,
      final long fileLength,
      final long start,
      final long length,
      final String[] partitionValues,
      final String[] preferredLocations) {
    this.path = path;
    this.fileLength = fileLength;
    this.start = start;
    this.length = length;
    this.partitionValues = partitionValues.clone();
    this.preferredLocations = preferredLocations.clone();
  }

  public String path() {
    return path;
  }

  public long fileLength() {
    return fileLength;
  }

  public long start() {
    return start;
  }

  public long length() {
    return length;
  }

  public String[] partitionValues() {
    return partitionValues.clone();
  }

  @Override
  public String[] preferredLocations() {
    return preferredLocations.clone();
  }
}
