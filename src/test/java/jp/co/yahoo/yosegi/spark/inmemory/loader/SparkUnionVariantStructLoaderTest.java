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
import jp.co.yahoo.yosegi.message.parser.json.JacksonMessageReader;
import jp.co.yahoo.yosegi.spark.test.Utils;
import jp.co.yahoo.yosegi.spark.v2.VariantStructUtil;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
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

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SparkUnionVariantStructLoaderTest {
  private static StructType extractionType() {
    return new StructType(
        new StructField[] {
          new StructField("0", DataTypes.LongType, true, variantMetadata("$.id")),
          new StructField("1", DataTypes.StringType, true, variantMetadata("$.name"))
        });
  }

  private static StructType physicalType() {
    return DataTypes.createStructType(
        new StructField[] {
          DataTypes.createStructField("id", DataTypes.LongType, true),
          DataTypes.createStructField("name", DataTypes.StringType, true)
        });
  }

  private static Metadata variantMetadata(final String path) {
    final Metadata nested =
        new MetadataBuilder()
            .putString(VariantStructUtil.PATH_METADATA_KEY, path)
            .putBoolean(VariantStructUtil.FAIL_ON_ERROR_METADATA_KEY, true)
            .putString(VariantStructUtil.TIME_ZONE_ID_METADATA_KEY, "UTC")
            .build();
    return new MetadataBuilder()
        .putMetadata(VariantStructUtil.VARIANT_METADATA_KEY, nested)
        .build();
  }

  private static ColumnBinary spreadBinary() throws IOException {
    final JacksonMessageReader reader = new JacksonMessageReader();
    final SpreadColumn column = new SpreadColumn("column");
    column.add(ColumnType.SPREAD, reader.create("{\"id\":10,\"name\":\"alice\"}"), 0);
    column.add(ColumnType.SPREAD, reader.create("{\"id\":30,\"name\":\"charlie\"}"), 2);
    final IColumnBinaryMaker maker =
        FindColumnBinaryMaker.get(DumpSpreadColumnBinaryMaker.class.getName());
    return Utils.getColumnBinary(maker, column, null, null, null);
  }

  @Test
  void T_load_SPREAD_rows_and_null_non_SPREAD_rows() throws IOException {
    final int loadSize = 3;
    final WritableColumnVector vector = new OnHeapColumnVector(loadSize, extractionType());
    final SparkUnionVariantStructLoader loader =
        new SparkUnionVariantStructLoader(vector, loadSize, physicalType());

    loader.loadChild(spreadBinary(), loadSize);
    loader.setIndexAndColumnType(0, ColumnType.SPREAD);
    loader.setIndexAndColumnType(1, ColumnType.LONG);
    loader.setIndexAndColumnType(2, ColumnType.SPREAD);
    loader.finish();
    final WritableColumnVector actual = loader.build();

    assertSame(vector, actual);
    assertFalse(actual.isNullAt(0));
    assertEquals(10L, actual.getChild(0).getLong(0));
    assertEquals(UTF8String.fromString("alice"), actual.getChild(1).getUTF8String(0));

    assertTrue(actual.isNullAt(1));

    assertFalse(actual.isNullAt(2));
    assertEquals(30L, actual.getChild(0).getLong(2));
    assertEquals(UTF8String.fromString("charlie"), actual.getChild(1).getUTF8String(2));
  }

  @Test
  void T_build_without_SPREAD_child_returns_empty_variant_struct() throws IOException {
    final int loadSize = 2;
    final WritableColumnVector vector = new OnHeapColumnVector(loadSize, extractionType());
    final SparkUnionVariantStructLoader loader =
        new SparkUnionVariantStructLoader(vector, loadSize, physicalType());

    loader.setIndexAndColumnType(0, ColumnType.LONG);
    loader.setNull(1);
    final WritableColumnVector actual = loader.build();

    assertSame(vector, actual);
    for (int rowId = 0; rowId < loadSize; rowId++) {
      assertTrue(actual.isNullAt(rowId));
      assertTrue(actual.getChild(0).isNullAt(rowId));
      assertTrue(actual.getChild(1).isNullAt(rowId));
    }
  }
}
