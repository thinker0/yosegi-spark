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
import jp.co.yahoo.yosegi.inmemory.IArrayLoader;
import jp.co.yahoo.yosegi.spark.v2.VariantPath;
import jp.co.yahoo.yosegi.spark.v2.VariantStructUtil;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.types.variant.Variant;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Evaluates root-array paths such as $[0] and $[0].id. */
public final class SparkRootArrayVariantStructLoader
    implements IArrayLoader<WritableColumnVector> {
  private final WritableColumnVector vector;
  private final int loadSize;
  private final int[] starts;
  private final int[] lengths;
  private final boolean[] nullRows;
  private final List<Target> targets = new ArrayList<>();
  private WritableColumnVector elements;

  public SparkRootArrayVariantStructLoader(
      final WritableColumnVector vector, final int loadSize) {
    this.vector = vector;
    this.loadSize = loadSize;
    this.starts = new int[loadSize];
    this.lengths = new int[loadSize];
    this.nullRows = new boolean[loadSize];
    final StructType type = (StructType) vector.dataType();
    final StructField[] fields = type.fields();
    for (int i = 0; i < fields.length; i++) {
      final VariantPath path = VariantStructUtil.getVariantPath(fields[i].metadata());
      if (path == null || path.segments().isEmpty()
          || !(path.segments().get(0) instanceof VariantPath.Index)) {
        throw new UnsupportedOperationException("Root ARRAY requires $[index] path.");
      }
      targets.add(new Target(i, path.segments()));
    }
  }

  @Override public int getLoadSize() { return loadSize; }
  @Override public void setNull(final int index) { nullRows[index] = true; }
  @Override public void setArrayIndex(final int index, final int start, final int length) {
    starts[index] = start;
    lengths[index] = length;
  }
  @Override public void loadChild(final ColumnBinary child, final int childLength)
      throws IOException {
    elements = new OnHeapColumnVector(childLength, DataTypes.VariantType);
    SparkRecursiveVariantLoader.load(child, childLength, elements);
  }
  @Override public void finish() {}
  @Override public WritableColumnVector build() {
    try {
      for (int rowId = 0; rowId < loadSize; rowId++) {
        for (Target target : targets) {
          final WritableColumnVector output = vector.getChild(target.ordinal);
          if (nullRows[rowId]) {
            output.putNull(rowId);
            continue;
          }
          final int index = ((VariantPath.Index) target.segments.get(0)).index();
          if (index < 0 || index >= lengths[rowId] || elements == null) {
            output.putNull(rowId);
            continue;
          }
          final int elementIndex = starts[rowId] + index;
          if (elements.isNullAt(elementIndex)) {
            output.putNull(rowId);
            continue;
          }
          final Variant element = new Variant(
              elements.getChild(0).getBinary(elementIndex),
              elements.getChild(1).getBinary(elementIndex));
          SparkVariantPathValueWriter.evaluateAndWrite(
              element, target.segments.subList(1, target.segments.size()), output, rowId);
        }
      }
      return vector;
    } finally {
      if (elements != null) {
        elements.close();
        elements = null;
      }
    }
  }

  private static final class Target {
    private final int ordinal;
    private final List<VariantPath.Segment> segments;
    private Target(final int ordinal, final List<VariantPath.Segment> segments) {
      this.ordinal = ordinal;
      this.segments = new ArrayList<>(segments);
    }
  }
}
