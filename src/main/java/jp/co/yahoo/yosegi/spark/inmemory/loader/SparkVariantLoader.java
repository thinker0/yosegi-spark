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
import jp.co.yahoo.yosegi.inmemory.ISpreadLoader;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.VariantType;
import org.apache.spark.types.variant.Variant;

import java.io.IOException;

/** Rebuilds Spark Variant value/metadata binaries from a Yosegi SPREAD-backed Struct. */
public class SparkVariantLoader implements ISpreadLoader<WritableColumnVector> {
  private static final int VALUE_CHILD_ORDINAL = 0;
  private static final int METADATA_CHILD_ORDINAL = 1;

  private final WritableColumnVector vector;
  private final int loadSize;
  private final StructType physicalType;
  private final WritableColumnVector physicalVector;
  private final SparkStructLoader delegate;

  public SparkVariantLoader(
      final WritableColumnVector vector,
      final int loadSize,
      final StructType physicalType) {
    if (!(vector.dataType() instanceof VariantType)) {
      throw new IllegalArgumentException("Output vector must be VariantType.");
    }
    if (vector.getNumChildren() != 2) {
      throw new IllegalArgumentException(
          "Spark Variant vector must have value and metadata children.");
    }
    this.vector = vector;
    this.loadSize = loadSize;
    this.physicalType = physicalType;
    this.physicalVector = new OnHeapColumnVector(0, physicalType);
    this.physicalVector.reserve(loadSize);
    this.delegate = new SparkStructLoader(physicalVector, loadSize);

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
    delegate.setNull(index);
  }

  @Override
  public void finish() throws IOException {
    delegate.finish();
  }

  @Override
  public WritableColumnVector build() throws IOException {
    try {
      delegate.build();
      final WritableColumnVector values = vector.getChild(VALUE_CHILD_ORDINAL);
      final WritableColumnVector metadata = vector.getChild(METADATA_CHILD_ORDINAL);
      for (int rowId = 0; rowId < loadSize; rowId++) {
        if (physicalVector.isNullAt(rowId)) {
          vector.putNull(rowId);
          values.putNull(rowId);
          metadata.putNull(rowId);
          continue;
        }
        final Variant variant =
            SparkVariantValueWriter.write(
                physicalType, physicalVector.getStruct(rowId));
        vector.putNotNull(rowId);
        values.putByteArray(rowId, variant.getValue());
        metadata.putByteArray(rowId, variant.getMetadata());
      }
      return vector;
    } finally {
      physicalVector.close();
    }
  }

  @Override
  public void loadChild(final ColumnBinary columnBinary, final int childLoadSize)
      throws IOException {
    delegate.loadChild(columnBinary, childLoadSize);
  }
}
