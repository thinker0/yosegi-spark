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
package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.binary.FindColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.DumpSpreadColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.IColumnBinaryMaker;
import jp.co.yahoo.yosegi.inmemory.ILoader;
import jp.co.yahoo.yosegi.inmemory.ILoaderFactory;
import jp.co.yahoo.yosegi.inmemory.ISpreadLoader;
import jp.co.yahoo.yosegi.message.parser.json.JacksonMessageReader;
import jp.co.yahoo.yosegi.spark.inmemory.SparkLoaderFactoryUtil;
import jp.co.yahoo.yosegi.spark.inmemory.factory.SparkVariantStructLoaderFactory;
import jp.co.yahoo.yosegi.spark.test.Utils;
import jp.co.yahoo.yosegi.spark.v2.VariantStructUtil;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.SpreadColumn;

import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SparkVariantStructLoaderV2Test {
  @Test
  void T_load_DirectVariantFieldsIntoSparkStruct() throws IOException {
    final String resource = "SparkVariantStructLoaderV2Test/VariantDirectFields.txt";
    final int loadSize = 3;
    final IColumn column = toSpreadColumn(resource);
    final IColumnBinaryMaker binaryMaker =
        FindColumnBinaryMaker.get(DumpSpreadColumnBinaryMaker.class.getName());
    final ColumnBinary columnBinary =
        Utils.getColumnBinary(binaryMaker, column, null, null, null);
    final StructType type =
        new StructType(
            new StructField[] {
              new StructField(
                  "0", DataTypes.IntegerType, true, createVariantMetadata("$.id")),
              new StructField(
                  "1", DataTypes.StringType, true, createVariantMetadata("$.name"))
            });
    final WritableColumnVector vector = new OnHeapColumnVector(loadSize, type);
    final ILoaderFactory<WritableColumnVector> loaderFactory =
        SparkLoaderFactoryUtil.createLoaderFactory(vector);
    assertTrue(loaderFactory instanceof SparkVariantStructLoaderFactory);
    final ILoader loader = loaderFactory.createLoader(columnBinary, loadSize);
    assertTrue(loader instanceof SparkVariantStructLoader);
    @SuppressWarnings("unchecked")
    final ISpreadLoader<WritableColumnVector> spreadLoader =
        (ISpreadLoader<WritableColumnVector>) loader;
    binaryMaker.load(columnBinary, spreadLoader);
    spreadLoader.build();
    assertFalse(vector.isNullAt(0));
    assertEquals(1, vector.getChild(0).getInt(0));
    assertEquals(UTF8String.fromString("alice"), vector.getChild(1).getUTF8String(0));

    assertFalse(vector.isNullAt(1));
    assertEquals(2, vector.getChild(0).getInt(1));
    assertTrue(vector.getChild(1).isNullAt(1));

    assertFalse(vector.isNullAt(2));
    assertTrue(vector.getChild(0).isNullAt(2));
    assertEquals(UTF8String.fromString("charlie"), vector.getChild(1).getUTF8String(2));
  }

  private static Metadata createVariantMetadata(final String path) {
    final Metadata variantMetadata =
        new MetadataBuilder()
            .putString(VariantStructUtil.PATH_METADATA_KEY, path)
            .putBoolean(VariantStructUtil.FAIL_ON_ERROR_METADATA_KEY, true)
            .putString(VariantStructUtil.TIME_ZONE_ID_METADATA_KEY, "UTC")
            .build();
    return new MetadataBuilder()
        .putMetadata(VariantStructUtil.VARIANT_METADATA_KEY, variantMetadata)
        .build();
  }

  private static IColumn toSpreadColumn(final String resource) throws IOException {
    final JacksonMessageReader jsonReader = new JacksonMessageReader();
    final SpreadColumn column = new SpreadColumn("column");
    final InputStream input =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
    if (input == null) {
      throw new IOException("Test resource was not found: " + resource);
    }
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(input))) {
      int index = 0;
      String json;
      while ((json = reader.readLine()) != null) {
        column.add(ColumnType.SPREAD, jsonReader.create(json), index);
        index++;
      }
    }
    return column;
  }
}
