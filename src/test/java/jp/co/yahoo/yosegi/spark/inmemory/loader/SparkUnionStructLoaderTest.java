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
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.SpreadColumn;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SparkUnionStructLoaderTest {
  private static StructType structType() {
    return DataTypes.createStructType(
        new org.apache.spark.sql.types.StructField[] {
          DataTypes.createStructField("id", DataTypes.LongType, true),
          DataTypes.createStructField("name", DataTypes.StringType, true)
        });
  }

  private static ColumnBinary spreadBinary() throws IOException {
    final JacksonMessageReader reader = new JacksonMessageReader();
    final SpreadColumn column = new SpreadColumn("column");
    column.add(ColumnType.SPREAD, reader.create("{\"id\":10,\"name\":\"alice\"}"), 0);
    column.add(ColumnType.SPREAD, reader.create("{\"id\":20,\"name\":\"bob\"}"), 2);
    final IColumnBinaryMaker maker =
        FindColumnBinaryMaker.get(DumpSpreadColumnBinaryMaker.class.getName());
    return Utils.getColumnBinary(maker, column, null, null, null);
  }

  @Test
  void T_load_SPREAD_rows_and_null_non_SPREAD_rows() throws IOException {
    final int loadSize = 3;
    final WritableColumnVector vector = new OnHeapColumnVector(loadSize, structType());
    final SparkUnionStructLoader loader = new SparkUnionStructLoader(vector, loadSize);

    loader.loadChild(spreadBinary(), loadSize);
    loader.setIndexAndColumnType(0, ColumnType.SPREAD);
    loader.setIndexAndColumnType(1, ColumnType.LONG);
    loader.setIndexAndColumnType(2, ColumnType.SPREAD);
    loader.finish();
    final WritableColumnVector actual = loader.build();

    assertSame(vector, actual);
    assertFalse(actual.isNullAt(0));
    assertTrue(actual.isNullAt(1));
    assertFalse(actual.isNullAt(2));
    assertEquals(10L, actual.getChild(0).getLong(0));
    assertEquals(UTF8String.fromString("alice"), actual.getChild(1).getUTF8String(0));
    assertEquals(20L, actual.getChild(0).getLong(2));
    assertEquals(UTF8String.fromString("bob"), actual.getChild(1).getUTF8String(2));
  }

  @Test
  void T_build_without_SPREAD_child_returns_empty_struct() throws IOException {
    final int loadSize = 3;
    final WritableColumnVector vector = new OnHeapColumnVector(loadSize, structType());
    final SparkUnionStructLoader loader = new SparkUnionStructLoader(vector, loadSize);

    loader.setNull(0);
    loader.setIndexAndColumnType(1, ColumnType.LONG);
    loader.setIndexAndColumnType(2, ColumnType.NULL);
    final WritableColumnVector actual = loader.build();

    assertSame(vector, actual);
    for (int index = 0; index < loadSize; index++) {
      assertTrue(actual.isNullAt(index));
      assertTrue(actual.getChild(0).isNullAt(index));
      assertTrue(actual.getChild(1).isNullAt(index));
    }
  }
}
