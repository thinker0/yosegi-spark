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
package jp.co.yahoo.yosegi.spark.blackbox;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class PartitionLoadTest {
  private static SparkSession spark;
  private static SQLContext sqlContext;
  private static String appName = "PartitionLoadTest";

  public boolean deleteDirectory(final File directory) {
    final File[] allContents = directory.listFiles();
    if (allContents != null) {
      for (final File file : allContents) {
        deleteDirectory(file);
      }
    }
    return directory.delete();
  }

  public String getResourcePath(final String resource) {
    return Thread.currentThread().getContextClassLoader().getResource(resource).getPath();
  }

  public String getTmpPath() {
    String tmpdir = System.getProperty("java.io.tmpdir");
    if (tmpdir.endsWith("/")) {
      tmpdir = tmpdir.substring(0, tmpdir.length() - 1);
    }
    return tmpdir + "/" + appName + ".yosegi";
  }

  public Dataset<Row> loadJsonFile(final String resource, final StructType schema) {
    final String resourcePath = getResourcePath(resource);
    if (schema == null) {
      return sqlContext.read().json(resourcePath).orderBy(col("id").asc());
    }
    return sqlContext.read().schema(schema).json(resourcePath).orderBy(col("id").asc());
  }

  public void createYosegiFile(final String resource, final String... partitions) {
    final Dataset<Row> df = loadJsonFile(resource, null);
    final String tmpPath = getTmpPath();
    df.write()
        .mode(SaveMode.Overwrite)
        .partitionBy(partitions)
        .format("jp.co.yahoo.yosegi.spark.YosegiFileFormat")
        .save(tmpPath);
  }

  public Dataset<Row> loadYosegiFile(final StructType schema) {
    final String tmpPath = getTmpPath();
    if (schema == null) {
      return sqlContext
          .read()
          .format("jp.co.yahoo.yosegi.spark.YosegiFileFormat")
          .load(tmpPath)
          .orderBy(col("id").asc());
    }
    return sqlContext
        .read()
        .format("jp.co.yahoo.yosegi.spark.YosegiFileFormat")
        .schema(schema)
        .load(tmpPath)
        .orderBy(col("id").asc());
  }

  @BeforeAll
  static void initAll() {
    spark = SparkSession.builder().appName(appName).master("local[*]").getOrCreate();
    sqlContext = spark.sqlContext();
  }

  @AfterAll
  static void tearDownAll() {
    spark.close();
  }

  @AfterEach
  void tearDown() {
    deleteDirectory(new File(getTmpPath()));
  }

  /*
   * FIXME: The rows cannot be loaded if rows in a partition have only null values.
   *  * {"id":1}
   */
  @Test
  void T_load_Partition_Primitive_1() throws IOException {
    // NOTE: create yosegi file
    final String resource = "blackbox/Partition_Primitive_1.txt";
    createYosegiFile(resource, "id");

    // NOTE: schema
    final int precision = DecimalType.MAX_PRECISION();
    final int scale = DecimalType.MINIMUM_ADJUSTED_SCALE();
    final List<StructField> fields =
        Arrays.asList(
            DataTypes.createStructField("id", DataTypes.IntegerType, true),
            DataTypes.createStructField("bo", DataTypes.BooleanType, true),
            DataTypes.createStructField("by", DataTypes.ByteType, true),
            DataTypes.createStructField("de", DataTypes.createDecimalType(precision, scale), true),
            DataTypes.createStructField("do", DataTypes.DoubleType, true),
            DataTypes.createStructField("fl", DataTypes.FloatType, true),
            DataTypes.createStructField("in", DataTypes.IntegerType, true),
            DataTypes.createStructField("lo", DataTypes.LongType, true),
            DataTypes.createStructField("sh", DataTypes.ShortType, true),
            DataTypes.createStructField("st", DataTypes.StringType, true));
    final StructType structType = DataTypes.createStructType(fields);
    // NOTE: load
    final Dataset<Row> dfj = loadJsonFile(resource, structType);
    final Dataset<Row> dfy = loadYosegiFile(structType);

    // NOTE: assert
    final List<Row> ldfj = dfj.collectAsList();
    final List<Row> ldfy = dfy.collectAsList();
    for (int i = 0; i < ldfj.size(); i++) {
      for (final StructField field : fields) {
        final String name = field.name();
        final DataType dataType = field.dataType();
        assertEquals((Object) ldfj.get(i).getAs(name), (Object) ldfy.get(i).getAs(name));
      }
    }
  }

  @Test
  void T_load_Partition_Primitive_2() throws IOException {
    // NOTE: create yosegi file
    final String resource = "blackbox/Partition_Primitive_2.txt";
    createYosegiFile(resource, "p1", "p2");

    // NOTE: schema
    final List<StructField> fields =
        Arrays.asList(
            DataTypes.createStructField("id", DataTypes.IntegerType, true),
            DataTypes.createStructField("p1", DataTypes.IntegerType, true),
            DataTypes.createStructField("p2", DataTypes.StringType, true),
            DataTypes.createStructField("v", DataTypes.StringType, true));
    final StructType structType = DataTypes.createStructType(fields);
    // NOTE: load
    final Dataset<Row> dfj = loadJsonFile(resource, structType);
    final Dataset<Row> dfy = loadYosegiFile(structType);

    // NOTE: assert
    final List<Row> ldfj = dfj.collectAsList();
    final List<Row> ldfy = dfy.collectAsList();
    for (int i = 0; i < ldfj.size(); i++) {
      for (final StructField field : fields) {
        final String name = field.name();
        final DataType dataType = field.dataType();
        assertEquals((Object) ldfj.get(i).getAs(name), (Object) ldfy.get(i).getAs(name));
      }
    }
  }
}
