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
import jp.co.yahoo.yosegi.inmemory.ILoader;
import jp.co.yahoo.yosegi.inmemory.IUnionLoader;
import jp.co.yahoo.yosegi.spark.inmemory.SparkLoaderFactoryUtil;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.VariantType;
import org.apache.spark.types.variant.Variant;

import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;

/** Reconstructs full Spark Variant values from a Yosegi UNION column. */
public class SparkUnionVariantLoader implements IUnionLoader<WritableColumnVector> {
  private static final int VALUE_CHILD_ORDINAL = 0;
  private static final int METADATA_CHILD_ORDINAL = 1;

  private final WritableColumnVector vector;
  private final int loadSize;
  private final StructType spreadType;
  private final ColumnType[] rowTypes;
  private final Map<ColumnType, WritableColumnVector> childVectors;
  private final Map<ColumnType, DataType> childDataTypes;

  public SparkUnionVariantLoader(
      final WritableColumnVector vector,
      final int loadSize,
      final StructType spreadType) {
    if (!(vector.dataType() instanceof VariantType)) {
      throw new IllegalArgumentException("Output vector must be VariantType.");
    }
    if (vector.getNumChildren() != 2) {
      throw new IllegalArgumentException(
          "Spark Variant vector must have value and metadata children.");
    }
    this.vector = vector;
    this.loadSize = loadSize;
    this.spreadType = spreadType;
    this.rowTypes = new ColumnType[loadSize];
    this.childVectors = new EnumMap<>(ColumnType.class);
    this.childDataTypes = new EnumMap<>(ColumnType.class);

    for (int i = 0; i < vector.getNumChildren(); i++) {
      vector.getChild(i).reset();
      vector.getChild(i).reserve(loadSize);
    }
  }

  @Override
  public int getLoadSize() {
    return loadSize;
  }

  @Override
  public void setNull(final int index) throws IOException {
    rowTypes[index] = null;
  }

  @Override
  public void finish() throws IOException {
    // Child makers finish their loaders while loadChild is executing.
  }

  @Override
  public WritableColumnVector build() throws IOException {
    final WritableColumnVector values = vector.getChild(VALUE_CHILD_ORDINAL);
    final WritableColumnVector metadata = vector.getChild(METADATA_CHILD_ORDINAL);
    try {
      for (int rowId = 0; rowId < loadSize; rowId++) {
        final ColumnType rowType = rowTypes[rowId];
        if (rowType == null || rowType == ColumnType.UNKNOWN) {
          putSqlNull(rowId, values, metadata);
          continue;
        }

        final Variant variant;
        switch (rowType) {
          case NULL:
            variant = SparkVariantValueWriter.writeNull();
            break;
          case EMPTY_ARRAY:
            variant = SparkVariantValueWriter.writeEmptyArray();
            break;
          case EMPTY_SPREAD:
            variant = SparkVariantValueWriter.writeEmptyObject();
            break;
          default:
            final ColumnType childKey = childKey(rowType);
            final WritableColumnVector childVector = childVectors.get(childKey);
            final DataType childType = childDataTypes.get(childKey);
            if (childVector == null || childType == null) {
              throw new UnsupportedOperationException(
                  "Full Variant UNION child type is not supported: " + rowType);
            }
            if (childVector.isNullAt(rowId)) {
              variant = SparkVariantValueWriter.writeNull();
            } else {
              variant = SparkVariantValueWriter.write(childType, childVector, rowId);
            }
            break;
        }
        vector.putNotNull(rowId);
        values.putByteArray(rowId, variant.getValue());
        metadata.putByteArray(rowId, variant.getMetadata());
      }
      return vector;
    } finally {
      for (final WritableColumnVector childVector : childVectors.values()) {
        childVector.close();
      }
      childVectors.clear();
      childDataTypes.clear();
    }
  }

  @Override
  public void setIndexAndColumnType(final int index, final ColumnType columnType)
      throws IOException {
    rowTypes[index] = columnType;
  }

  @Override
  public void loadChild(final ColumnBinary columnBinary, final int childLoadSize)
      throws IOException {
    if (childVectors.containsKey(columnBinary.columnType)) {
      throw new IOException(
          "A Variant UNION contains duplicate child type: " + columnBinary.columnType);
    }

    // Composite and nested UNION members are reconstructed recursively as Variant values.
    if (columnBinary.columnType == ColumnType.ARRAY
        || columnBinary.columnType == ColumnType.SPREAD
        || columnBinary.columnType == ColumnType.MAP
        || columnBinary.columnType == ColumnType.STRUCT
        || columnBinary.columnType == ColumnType.UNION) {
      final WritableColumnVector childVector =
          new OnHeapColumnVector(loadSize, DataTypes.VariantType);
      boolean success = false;
      try {
        SparkRecursiveVariantLoader.load(columnBinary, childLoadSize, childVector);
        childVectors.put(columnBinary.columnType, childVector);
        childDataTypes.put(columnBinary.columnType, DataTypes.VariantType);
        success = true;
      } finally {
        if (!success) {
          childVector.close();
        }
      }
      return;
    }

    final DataType dataType = sparkType(columnBinary.columnType);
    if (dataType == null) {
      if (columnBinary.columnType == ColumnType.NULL
          || columnBinary.columnType == ColumnType.EMPTY_ARRAY
          || columnBinary.columnType == ColumnType.EMPTY_SPREAD) {
        return;
      }
      throw new UnsupportedOperationException(
          "Full Variant UNION child type is not supported: " + columnBinary.columnType);
    }

    final WritableColumnVector childVector = new OnHeapColumnVector(loadSize, dataType);
    boolean success = false;
    try {
      final ILoader childLoader =
          SparkLoaderFactoryUtil.createLoaderFactory(childVector)
              .createLoader(columnBinary, childLoadSize);
      FindColumnBinaryMaker.get(columnBinary.makerClassName).load(columnBinary, childLoader);
      childLoader.build();
      childVectors.put(columnBinary.columnType, childVector);
      childDataTypes.put(columnBinary.columnType, dataType);
      success = true;
    } finally {
      if (!success) {
        childVector.close();
      }
    }
  }


  private ColumnType childKey(final ColumnType rowType) {
    // MAP and STRUCT are logical schema tags. Their binary child representation is SPREAD.
    if ((rowType == ColumnType.MAP || rowType == ColumnType.STRUCT)
        && childVectors.containsKey(ColumnType.SPREAD)) {
      return ColumnType.SPREAD;
    }
    return rowType;
  }

  private DataType sparkType(final ColumnType columnType) {
    switch (columnType) {
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

  private void putSqlNull(
      final int rowId,
      final WritableColumnVector values,
      final WritableColumnVector metadata) {
    vector.putNull(rowId);
    values.putNull(rowId);
    metadata.putNull(rowId);
  }
}
