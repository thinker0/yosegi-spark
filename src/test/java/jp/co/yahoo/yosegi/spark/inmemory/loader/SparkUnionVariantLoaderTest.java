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
import jp.co.yahoo.yosegi.message.parser.json.JacksonMessageReader;
import jp.co.yahoo.yosegi.spark.inmemory.factory.SparkVariantLoaderFactory;
import jp.co.yahoo.yosegi.spark.test.UnionColumnUtils;
import jp.co.yahoo.yosegi.spark.test.Utils;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.SpreadColumn;
import jp.co.yahoo.yosegi.spread.column.ArrayColumn;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.UnionColumn;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.types.variant.Variant;
import org.apache.spark.unsafe.types.VariantVal;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SparkUnionVariantLoaderTest {

  @Test
  void T_load_PrimitiveUnion_ReconstructsFullVariantValues() throws IOException {
    final int loadSize = 4;
    final UnionColumnUtils union = new UnionColumnUtils(loadSize);
    union.add(ColumnType.LONG, Map.of(0, 10L, 2, 30L));
    union.add(ColumnType.STRING, Map.of(1, "alice", 3, "charlie"));
    final ColumnBinary binary = union.createColumnBinary();

    final WritableColumnVector vector =
        new OnHeapColumnVector(loadSize, DataTypes.VariantType);
    try {
      final StructType spreadType = new StructType();
      final ILoader loader =
          new SparkVariantLoaderFactory(vector, spreadType).createLoader(binary, loadSize);
      FindColumnBinaryMaker.get(binary.makerClassName).load(binary, loader);
      loader.build();

      assertFalse(vector.isNullAt(0));
      assertEquals(10L, variantAt(vector, 0).getLong());
      assertEquals("alice", variantAt(vector, 1).getString());
      assertEquals(30L, variantAt(vector, 2).getLong());
      assertEquals("charlie", variantAt(vector, 3).getString());
    } finally {
      vector.close();
    }
  }

  @Test
  void T_typeTags_ReconstructVariantNullAndEmptyContainers() throws IOException {
    final int loadSize = 4;
    final WritableColumnVector vector =
        new OnHeapColumnVector(loadSize, DataTypes.VariantType);
    try {
      final SparkUnionVariantLoader loader =
          new SparkUnionVariantLoader(vector, loadSize, new StructType());
      loader.setIndexAndColumnType(0, ColumnType.NULL);
      loader.setIndexAndColumnType(1, ColumnType.EMPTY_ARRAY);
      loader.setIndexAndColumnType(2, ColumnType.EMPTY_SPREAD);
      loader.setNull(3);
      loader.build();

      assertFalse(vector.isNullAt(0));
      assertTrue(variantAt(vector, 0).getValue().length > 0);
      assertTrue(variantAt(vector, 1).getValue().length > 0);
      assertTrue(variantAt(vector, 2).getValue().length > 0);
      assertTrue(vector.isNullAt(3));
    } finally {
      vector.close();
    }
  }


  @Test
  void T_load_SpreadMember_ReconstructsObjectVariant() throws IOException {
    final int loadSize = 3;
    final WritableColumnVector vector =
        new OnHeapColumnVector(loadSize, DataTypes.VariantType);
    final StructType spreadType =
        new StructType()
            .add("id", DataTypes.LongType, true)
            .add("name", DataTypes.StringType, true);
    try {
      final SparkUnionVariantLoader loader =
          new SparkUnionVariantLoader(vector, loadSize, spreadType);
      loader.loadChild(spreadBinary(2), loadSize);
      loader.setIndexAndColumnType(0, ColumnType.SPREAD);
      loader.setNull(1);
      loader.setIndexAndColumnType(2, ColumnType.SPREAD);
      loader.build();

      assertEquals(10L, variantAt(vector, 0).getFieldByKey("id").getLong());
      assertEquals("alice", variantAt(vector, 0).getFieldByKey("name").getString());
      assertTrue(vector.isNullAt(1));
      assertEquals(30L, variantAt(vector, 2).getFieldByKey("id").getLong());
      assertEquals("charlie", variantAt(vector, 2).getFieldByKey("name").getString());
    } finally {
      vector.close();
    }
  }


  @Test
  void T_logicalMapAndStructTags_UseSpreadChildRepresentation() throws IOException {
    final int loadSize = 2;
    final WritableColumnVector vector =
        new OnHeapColumnVector(loadSize, DataTypes.VariantType);
    try {
      final SparkUnionVariantLoader loader =
          new SparkUnionVariantLoader(vector, loadSize, new StructType());
      loader.loadChild(spreadBinary(1), loadSize);
      loader.setIndexAndColumnType(0, ColumnType.MAP);
      loader.setIndexAndColumnType(1, ColumnType.STRUCT);
      loader.build();

      assertEquals(10L, variantAt(vector, 0).getFieldByKey("id").getLong());
      assertEquals("alice", variantAt(vector, 0).getFieldByKey("name").getString());
      assertEquals(30L, variantAt(vector, 1).getFieldByKey("id").getLong());
      assertEquals("charlie", variantAt(vector, 1).getFieldByKey("name").getString());
    } finally {
      vector.close();
    }
  }

  @Test
  void T_load_ArrayAndNestedUnion_ReconstructsVariantWithoutDeclaredElementSchema()
      throws IOException {
    final int loadSize = 3;
    final JacksonMessageReader reader = new JacksonMessageReader();
    final ArrayColumn arrays = new ArrayColumn("column");
    arrays.add(ColumnType.ARRAY, reader.create("[1,2,3]"), 0);
    arrays.add(ColumnType.ARRAY, reader.create("[{\"id\":7},\"x\",null]"), 2);

    final Map<ColumnType, IColumn> children = new HashMap<>();
    children.put(ColumnType.ARRAY, arrays);
    children.put(ColumnType.LONG, Utils.toLongColumn(Map.of(1, 99L), loadSize));
    final UnionColumn unionColumn = new UnionColumn("column", children);
    unionColumn.addCell(ColumnType.ARRAY, arrays.get(0), 0);
    unionColumn.addCell(ColumnType.LONG, children.get(ColumnType.LONG).get(1), 1);
    unionColumn.addCell(ColumnType.ARRAY, arrays.get(2), 2);

    final IColumnBinaryMaker maker =
        FindColumnBinaryMaker.get(
            jp.co.yahoo.yosegi.binary.maker.DumpUnionColumnBinaryMaker.class.getName());
    final ColumnBinary binary = Utils.getColumnBinary(maker, unionColumn, null, null, null);
    final WritableColumnVector vector =
        new OnHeapColumnVector(loadSize, DataTypes.VariantType);
    try {
      final ILoader loader =
          new SparkVariantLoaderFactory(vector, new StructType()).createLoader(binary, loadSize);
      FindColumnBinaryMaker.get(binary.makerClassName).load(binary, loader);
      loader.build();

      final Variant firstArray = variantAt(vector, 0);
      assertFalse(vector.isNullAt(0));
      assertEquals(3, firstArray.arraySize());
      assertEquals(1L, firstArray.getElementAtIndex(0).getLong());
      assertEquals(2L, firstArray.getElementAtIndex(1).getLong());
      assertEquals(3L, firstArray.getElementAtIndex(2).getLong());

      assertEquals(99L, variantAt(vector, 1).getLong());

      final Variant nestedArray = variantAt(vector, 2);
      assertFalse(vector.isNullAt(2));
      assertEquals(3, nestedArray.arraySize());
      assertEquals(
          7L,
          nestedArray.getElementAtIndex(0).getFieldByKey("id").getLong());
      assertEquals("x", nestedArray.getElementAtIndex(1).getString());
      assertEquals(
          "null",
          nestedArray.getElementAtIndex(2).toJson(java.time.ZoneOffset.UTC));
    } finally {
      vector.close();
    }
  }

  private static ColumnBinary spreadBinary(final int secondRowIndex) throws IOException {
    final JacksonMessageReader reader = new JacksonMessageReader();
    final SpreadColumn column = new SpreadColumn("column");
    column.add(ColumnType.SPREAD, reader.create("{\"id\":10,\"name\":\"alice\"}"), 0);
    column.add(
        ColumnType.SPREAD,
        reader.create("{\"id\":30,\"name\":\"charlie\"}"),
        secondRowIndex);
    final IColumnBinaryMaker maker =
        FindColumnBinaryMaker.get(DumpSpreadColumnBinaryMaker.class.getName());
    return Utils.getColumnBinary(maker, column, null, null, null);
  }

  private static Variant variantAt(
      final WritableColumnVector vector, final int rowId) {
    final VariantVal value = vector.getVariant(rowId);
    return new Variant(value.getValue(), value.getMetadata());
  }
}
