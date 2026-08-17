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

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.binary.FindColumnBinaryMaker;
import jp.co.yahoo.yosegi.inmemory.IArrayLoader;
import jp.co.yahoo.yosegi.inmemory.ILoader;
import jp.co.yahoo.yosegi.inmemory.ISpreadLoader;
import jp.co.yahoo.yosegi.spark.inmemory.SparkLoaderFactoryUtil;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.types.variant.Variant;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Loads an arbitrary Yosegi value into a Spark Variant vector without a predeclared nested schema. */
final class SparkRecursiveVariantLoader {

  private SparkRecursiveVariantLoader() {}

  static void load(
      final ColumnBinary columnBinary,
      final int loadSize,
      final WritableColumnVector output)
      throws IOException {
    switch (columnBinary.columnType) {
      case SPREAD:
      case MAP:
      case STRUCT:
        // Yosegi MAP/STRUCT values use the same child-by-name representation as SPREAD.
        loadWith(columnBinary, new ObjectLoader(output, loadSize));
        return;
      case ARRAY:
        loadWith(columnBinary, new ArrayLoader(output, loadSize));
        return;
      case UNION:
        loadWith(columnBinary, new SparkUnionVariantLoader(output, loadSize, null));
        return;
      case NULL:
        fillVariantNull(output, loadSize);
        return;
      case EMPTY_ARRAY:
        fillConstant(output, loadSize, SparkVariantValueWriter.writeEmptyArray());
        return;
      case EMPTY_SPREAD:
        fillConstant(output, loadSize, SparkVariantValueWriter.writeEmptyObject());
        return;
      default:
        loadPrimitive(columnBinary, loadSize, output);
    }
  }

  private static void loadPrimitive(
      final ColumnBinary columnBinary,
      final int loadSize,
      final WritableColumnVector output)
      throws IOException {
    final DataType type = primitiveType(columnBinary.columnType);
    if (type == null) {
      throw new UnsupportedOperationException(
          "Unsupported recursive Variant child type: " + columnBinary.columnType);
    }
    final WritableColumnVector temporary = new OnHeapColumnVector(loadSize, type);
    try {
      final ILoader loader =
          SparkLoaderFactoryUtil.createLoaderFactory(temporary)
              .createLoader(columnBinary, loadSize);
      FindColumnBinaryMaker.get(columnBinary.makerClassName).load(columnBinary, loader);
      loader.build();
      for (int i = 0; i < loadSize; i++) {
        if (temporary.isNullAt(i)) {
          putSqlNull(output, i);
        } else {
          putVariant(output, i, SparkVariantValueWriter.write(type, temporary, i));
        }
      }
    } finally {
      temporary.close();
    }
  }

  private static DataType primitiveType(final ColumnType type) {
    switch (type) {
      case BOOLEAN:
        return DataTypes.BooleanType;
      case BYTE:
        return DataTypes.ByteType;
      case SHORT:
        return DataTypes.ShortType;
      case INTEGER:
        return DataTypes.IntegerType;
      case LONG:
        return DataTypes.LongType;
      case FLOAT:
        return DataTypes.FloatType;
      case DOUBLE:
        return DataTypes.DoubleType;
      case STRING:
        return DataTypes.StringType;
      case BYTES:
        return DataTypes.BinaryType;
      default:
        return null;
    }
  }

  private static void loadWith(final ColumnBinary binary, final ILoader loader)
      throws IOException {
    FindColumnBinaryMaker.get(binary.makerClassName).load(binary, loader);
    loader.build();
  }

  private static void fillVariantNull(final WritableColumnVector output, final int loadSize) {
    fillConstant(output, loadSize, SparkVariantValueWriter.writeNull());
  }

  private static void fillConstant(
      final WritableColumnVector output, final int loadSize, final Variant value) {
    for (int i = 0; i < loadSize; i++) {
      putVariant(output, i, value);
    }
  }

  private static void putVariant(
      final WritableColumnVector output, final int rowId, final Variant variant) {
    output.putNotNull(rowId);
    output.getChild(0).putByteArray(rowId, variant.getValue());
    output.getChild(1).putByteArray(rowId, variant.getMetadata());
  }

  private static void putSqlNull(final WritableColumnVector output, final int rowId) {
    output.putNull(rowId);
    output.getChild(0).putNull(rowId);
    output.getChild(1).putNull(rowId);
  }

  private static final class ArrayLoader implements IArrayLoader<WritableColumnVector> {
    private final WritableColumnVector output;
    private final int loadSize;
    private final int[] starts;
    private final int[] lengths;
    private final boolean[] nullRows;
    private WritableColumnVector elements;

    private ArrayLoader(final WritableColumnVector output, final int loadSize) {
      this.output = output;
      this.loadSize = loadSize;
      this.starts = new int[loadSize];
      this.lengths = new int[loadSize];
      this.nullRows = new boolean[loadSize];
    }

    @Override
    public int getLoadSize() {
      return loadSize;
    }

    @Override
    public void setNull(final int index) {
      nullRows[index] = true;
    }

    @Override
    public void setArrayIndex(final int index, final int start, final int length) {
      starts[index] = start;
      lengths[index] = length;
    }

    @Override
    public void loadChild(final ColumnBinary child, final int childLength) throws IOException {
      elements = new OnHeapColumnVector(childLength, DataTypes.VariantType);
      SparkRecursiveVariantLoader.load(child, childLength, elements);
    }

    @Override
    public void finish() {}

    @Override
    public WritableColumnVector build() {
      try {
        for (int rowId = 0; rowId < loadSize; rowId++) {
          if (nullRows[rowId]) {
            putSqlNull(output, rowId);
          } else {
            putVariant(
                output,
                rowId,
                SparkVariantValueWriter.writeVariantArray(elements, starts[rowId], lengths[rowId]));
          }
        }
        return output;
      } finally {
        if (elements != null) {
          elements.close();
          elements = null;
        }
      }
    }
  }

  private static final class ObjectLoader implements ISpreadLoader<WritableColumnVector> {
    private final WritableColumnVector output;
    private final int loadSize;
    private final boolean[] nullRows;
    private final List<String> names = new ArrayList<>();
    private final List<WritableColumnVector> children = new ArrayList<>();

    private ObjectLoader(final WritableColumnVector output, final int loadSize) {
      this.output = output;
      this.loadSize = loadSize;
      this.nullRows = new boolean[loadSize];
    }

    @Override
    public int getLoadSize() {
      return loadSize;
    }

    @Override
    public void setNull(final int index) {
      nullRows[index] = true;
    }

    @Override
    public void loadChild(final ColumnBinary child, final int childLoadSize) throws IOException {
      final WritableColumnVector childVector =
          new OnHeapColumnVector(loadSize, DataTypes.VariantType);
      boolean success = false;
      try {
        SparkRecursiveVariantLoader.load(child, childLoadSize, childVector);
        names.add(child.columnName);
        children.add(childVector);
        success = true;
      } finally {
        if (!success) {
          childVector.close();
        }
      }
    }

    @Override
    public void finish() {}

    @Override
    public WritableColumnVector build() {
      try {
        for (int rowId = 0; rowId < loadSize; rowId++) {
          if (nullRows[rowId]) {
            putSqlNull(output, rowId);
          } else {
            putVariant(
                output,
                rowId,
                SparkVariantValueWriter.writeVariantObject(names, children, rowId));
          }
        }
        return output;
      } finally {
        for (final WritableColumnVector child : children) {
          child.close();
        }
        children.clear();
        names.clear();
      }
    }
  }
}
