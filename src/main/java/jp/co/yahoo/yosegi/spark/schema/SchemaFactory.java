/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jp.co.yahoo.yosegi.spark.schema;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import jp.co.yahoo.yosegi.message.design.IField;
import jp.co.yahoo.yosegi.message.design.StructContainerField;
import jp.co.yahoo.yosegi.message.design.spark.SparkSchemaFactory;
import jp.co.yahoo.yosegi.reader.YosegiReader;

/**
 * Infers and merges Spark schemas from one or more Yosegi files.
 *
 * <p>Files are processed in deterministic path order. Each physical Yosegi schema is converted to a
 * Spark schema and then merged using the conservative Read 1.0 evolution contract. Compatible
 * numeric widening and nullable missing fields are allowed; incompatible logical types fail rather
 * than being guessed or automatically promoted to Variant/UNION.
 *
 * <p>Nested Struct, Array, and Map values are merged recursively when their component types remain
 * compatible. Map key types must match.
 *
 * <p>Only schema metadata is consumed from each input file and the Yosegi reader is closed on every
 * path so driver-side schema inference does not leak file-system resources.
 */
public final class SchemaFactory {

  private SchemaFactory() {}

  /**
   * Infers and merges the Spark schema for a set of Yosegi files.
   *
   * @param sparkSession active Spark session used to obtain Hadoop configuration
   * @param yosegiConfig Yosegi reader configuration
   * @param files Yosegi files participating in the dataset
   * @return merged Spark schema
   * @throws IOException if schema metadata cannot be read
   * @throws IllegalArgumentException if no files are supplied or schemas are incompatible
   */
  public static StructType create(
      final SparkSession sparkSession,
      final jp.co.yahoo.yosegi.config.Configuration yosegiConfig,
      final FileStatus[] files)
      throws IOException {
    if (files == null || files.length == 0) {
      throw new IllegalArgumentException(
          "At least one Yosegi file is required for schema inference.");
    }

    final Configuration conf = sparkSession.sessionState().newHadoopConf();
    final FileStatus[] sortedFiles = files.clone();
    Arrays.sort(sortedFiles, Comparator.comparing(file -> file.getPath().toString()));

    StructType merged = null;
    String mergedFrom = null;
    for (FileStatus file : sortedFiles) {
      final StructType current =
          SparkSchemaFactory.getSparkSchema(readSchemaFromFile(conf, yosegiConfig, file));
      if (merged == null) {
        merged = current;
        mergedFrom = file.getPath().toString();
      } else {
        merged =
            mergeSchemas(
                merged,
                current,
                mergedFrom,
                file.getPath().toString());
        mergedFrom = mergedFrom + "," + file.getPath();
      }
    }
    return merged;
  }

  static StructType mergeSchemas(final StructType left, final StructType right) {
    return mergeSchemas(left, right, "left schema", "right schema");
  }

  private static StructType mergeSchemas(
      final StructType left,
      final StructType right,
      final String leftSource,
      final String rightSource) {
    return mergeStructTypes(left, right, "root", leftSource, rightSource);
  }

  private static StructType mergeStructTypes(
      final StructType left,
      final StructType right,
      final String path,
      final String leftSource,
      final String rightSource) {
    final Map<String, StructField> rightFields = new LinkedHashMap<>();
    for (StructField field : right.fields()) {
      rightFields.put(field.name(), field);
    }

    final Map<String, StructField> mergedFields = new LinkedHashMap<>();
    for (StructField leftField : left.fields()) {
      final StructField rightField = rightFields.remove(leftField.name());
      if (rightField == null) {
        mergedFields.put(leftField.name(), withNullable(leftField, true));
        continue;
      }

      final String fieldPath = path + "." + leftField.name();
      final DataType mergedType =
          mergeDataTypes(
              leftField.dataType(),
              rightField.dataType(),
              fieldPath,
              leftSource,
              rightSource);
      mergedFields.put(
          leftField.name(),
          new StructField(
              leftField.name(),
              mergedType,
              leftField.nullable() || rightField.nullable(),
              leftField.metadata()));
    }

    for (StructField rightField : rightFields.values()) {
      mergedFields.put(rightField.name(), withNullable(rightField, true));
    }

    return new StructType(mergedFields.values().toArray(new StructField[0]));
  }

  private static DataType mergeDataTypes(
      final DataType left,
      final DataType right,
      final String path,
      final String leftSource,
      final String rightSource) {
    if (left.equals(right)) {
      return left;
    }

    if (left instanceof StructType && right instanceof StructType) {
      return mergeStructTypes(
          (StructType) left,
          (StructType) right,
          path,
          leftSource,
          rightSource);
    }

    if (left instanceof ArrayType && right instanceof ArrayType) {
      final ArrayType leftArray = (ArrayType) left;
      final ArrayType rightArray = (ArrayType) right;
      return new ArrayType(
          mergeDataTypes(
              leftArray.elementType(),
              rightArray.elementType(),
              path + "[]",
              leftSource,
              rightSource),
          leftArray.containsNull() || rightArray.containsNull());
    }

    if (left instanceof MapType && right instanceof MapType) {
      final MapType leftMap = (MapType) left;
      final MapType rightMap = (MapType) right;
      if (!leftMap.keyType().equals(rightMap.keyType())) {
        throw incompatible(path + "<key>", leftMap.keyType(), rightMap.keyType(), leftSource, rightSource);
      }
      return new MapType(
          leftMap.keyType(),
          mergeDataTypes(
              leftMap.valueType(),
              rightMap.valueType(),
              path + "<value>",
              leftSource,
              rightSource),
          leftMap.valueContainsNull() || rightMap.valueContainsNull());
    }

    if (isPair(left, right, DataTypes.IntegerType, DataTypes.LongType)) {
      return DataTypes.LongType;
    }
    if (isPair(left, right, DataTypes.FloatType, DataTypes.DoubleType)) {
      return DataTypes.DoubleType;
    }

    throw incompatible(path, left, right, leftSource, rightSource);
  }

  private static boolean isPair(
      final DataType left,
      final DataType right,
      final DataType first,
      final DataType second) {
    return (left.equals(first) && right.equals(second))
        || (left.equals(second) && right.equals(first));
  }

  private static StructField withNullable(final StructField field, final boolean nullable) {
    if (field.nullable() == nullable) {
      return field;
    }
    return new StructField(field.name(), field.dataType(), nullable, field.metadata());
  }

  private static IllegalArgumentException incompatible(
      final String path,
      final DataType left,
      final DataType right,
      final String leftSource,
      final String rightSource) {
    return new IllegalArgumentException(
        "Incompatible Yosegi schemas at '"
            + path
            + "': "
            + left.catalogString()
            + " ("
            + leftSource
            + ") vs "
            + right.catalogString()
            + " ("
            + rightSource
            + "). Yosegi V2 Read 1.0 does not automatically evolve incompatible types to Variant/UNION.");
  }

  /**
   * Reads physical Yosegi schema metadata from one file.
   *
   * @param config Hadoop configuration used to open the file
   * @param yosegiConfig Yosegi reader configuration
   * @param file file whose schema is read
   * @return Yosegi schema tree rooted at {@code root}
   * @throws IOException if the file cannot be opened or parsed
   */
  public static IField readSchemaFromFile(
      final Configuration config,
      final jp.co.yahoo.yosegi.config.Configuration yosegiConfig,
      final FileStatus file)
      throws IOException {
    final YosegiReader reader = new YosegiReader();
    final IField result = new StructContainerField("root");
    try {
      final Path filePath = file.getPath();
      final FileSystem fs = filePath.getFileSystem(config);
      final InputStream in = fs.open(filePath);
      reader.setNewStream(in, file.getLen(), yosegiConfig);
      for (int i = 0; i < 1 && reader.hasNext(); i++) {
        result.merge(reader.next().getSchema());
      }
    } finally {
      reader.close();
    }
    return result;
  }
}
