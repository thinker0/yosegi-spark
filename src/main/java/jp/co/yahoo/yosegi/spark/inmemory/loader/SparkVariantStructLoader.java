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
import jp.co.yahoo.yosegi.inmemory.ILoaderFactory;
import jp.co.yahoo.yosegi.inmemory.ISpreadLoader;
import jp.co.yahoo.yosegi.inmemory.IUnionLoader;
import jp.co.yahoo.yosegi.spark.inmemory.SparkLoaderFactoryUtil;
import jp.co.yahoo.yosegi.spark.v2.VariantPath;
import jp.co.yahoo.yosegi.spark.v2.VariantStructUtil;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.types.variant.Variant;

/** Loads a Spark Variant extraction Struct from a Yosegi SPREAD column. */
public class SparkVariantStructLoader implements ISpreadLoader<WritableColumnVector> {
  private final WritableColumnVector vector;
  private final int loadSize;
  private final PathNode root = new PathNode();
  private final Map<String, List<ArrayTarget>> arrayTargetsBySourceName = new LinkedHashMap<>();
  private final boolean[] loaded;

  public SparkVariantStructLoader(final WritableColumnVector vector, final int loadSize) {
    this(vector, loadSize, null);
  }

  public SparkVariantStructLoader(
      final WritableColumnVector vector, final int loadSize, final StructType physicalType) {
    this.vector = vector;
    this.loadSize = loadSize;
    final StructType structType = (StructType) vector.dataType();
    if (!VariantStructUtil.isVariantStruct(structType)) {
      throw new IllegalArgumentException("The vector is not a Variant extraction Struct.");
    }
    final StructField[] fields = structType.fields();
    loaded = new boolean[fields.length];
    for (int i = 0; i < fields.length; i++) {
      final WritableColumnVector child = vector.getChild(i);
      child.reset();
      child.reserve(loadSize);
      if (child.hasDictionary()) {
        child.reserveDictionaryIds(0);
        child.setDictionary(null);
      }
      final VariantPath path = VariantStructUtil.getVariantPath(fields[i].metadata());
      if (path == null || path.segments().isEmpty()
          || !(path.segments().get(0) instanceof VariantPath.Field)) {
        throw new UnsupportedOperationException(
            "Unsupported Variant path for SPREAD root: " + fields[i].metadata().json());
      }
      if (path.containsArrayIndex()) {
        final String sourceName = ((VariantPath.Field) path.segments().get(0)).name();
        arrayTargetsBySourceName.computeIfAbsent(sourceName, ignored -> new ArrayList<>())
            .add(new ArrayTarget(i, path.segments().subList(1, path.segments().size())));
        continue;
      }
      final String[] segments = path.objectFieldPath();
      final StructField physicalField = findField(physicalType, segments);
      if (physicalType != null && physicalField == null) {
        continue;
      }
      if (physicalField != null && !physicalField.dataType().equals(child.dataType())) {
        throw new UnsupportedOperationException(
            "Variant extraction cast must be evaluated by Spark. path="
                + path
                + ", physicalType="
                + physicalField.dataType()
                + ", requestedType="
                + child.dataType());
      }
      PathNode node = root;
      for (String segment : segments) {
        node = node.children.computeIfAbsent(segment, ignored -> new PathNode());
      }
      node.targets.add(new Target(i, SparkLoaderFactoryUtil.createLoaderFactory(child)));
    }
  }

  @Override
  public int getLoadSize() {
    return loadSize;
  }

  @Override
  public void setNull(final int index) throws IOException {
    vector.putNull(index);
  }

  @Override
  public void finish() throws IOException {
    // no-op
  }

  @Override
  public WritableColumnVector build() throws IOException {
    for (int i = 0; i < loaded.length; i++) {
      if (!loaded[i]) {
        SparkEmptyLoader.load(vector.getChild(i), loadSize);
      }
    }
    return vector;
  }

  @Override
  public void loadChild(final ColumnBinary columnBinary, final int childLoadSize)
      throws IOException {
    final PathNode node = root.children.get(columnBinary.columnName);
    if (node != null) {
      loadNode(columnBinary, childLoadSize, node);
    }
    final List<ArrayTarget> arrayTargets = arrayTargetsBySourceName.get(columnBinary.columnName);
    if (arrayTargets != null) {
      loadArrayTargets(columnBinary, childLoadSize, arrayTargets);
    }
  }


  private void loadArrayTargets(
      final ColumnBinary columnBinary,
      final int childLoadSize,
      final List<ArrayTarget> targets) throws IOException {
    final WritableColumnVector temporary =
        new OnHeapColumnVector(childLoadSize, DataTypes.VariantType);
    try {
      SparkRecursiveVariantLoader.load(columnBinary, childLoadSize, temporary);
      for (int rowId = 0; rowId < childLoadSize; rowId++) {
        if (temporary.isNullAt(rowId)) {
          for (ArrayTarget target : targets) {
            vector.getChild(target.ordinal).putNull(rowId);
          }
          continue;
        }
        final Variant rootValue =
            new Variant(
                temporary.getChild(0).getBinary(rowId),
                temporary.getChild(1).getBinary(rowId));
        for (ArrayTarget target : targets) {
          SparkVariantPathValueWriter.evaluateAndWrite(
              rootValue, target.remainingSegments, vector.getChild(target.ordinal), rowId);
          loaded[target.ordinal] = true;
        }
      }
    } finally {
      temporary.close();
    }
  }

  private void loadNode(
      final ColumnBinary columnBinary, final int childLoadSize, final PathNode node)
      throws IOException {
    for (Target target : node.targets) {
      target.loaderFactory.create(columnBinary, childLoadSize);
      loaded[target.ordinal] = true;
    }
    if (node.children.isEmpty()) {
      return;
    }
    if (columnBinary.columnType == ColumnType.SPREAD) {
      FindColumnBinaryMaker.get(columnBinary.makerClassName)
          .load(columnBinary, new NestedSpreadLoader(node, childLoadSize));
    } else if (columnBinary.columnType == ColumnType.UNION) {
      FindColumnBinaryMaker.get(columnBinary.makerClassName)
          .load(columnBinary, new NestedUnionLoader(node, childLoadSize));
    }
  }

  private final class NestedSpreadLoader implements ISpreadLoader<WritableColumnVector> {
    private final PathNode node;
    private final int nestedLoadSize;

    private NestedSpreadLoader(final PathNode node, final int nestedLoadSize) {
      this.node = node;
      this.nestedLoadSize = nestedLoadSize;
    }

    @Override
    public int getLoadSize() {
      return nestedLoadSize;
    }

    @Override
    public void setNull(final int index) throws IOException {
      putDescendantNulls(node, index);
    }

    @Override
    public void finish() throws IOException {
      // no-op
    }

    @Override
    public WritableColumnVector build() throws IOException {
      return vector;
    }

    @Override
    public void loadChild(final ColumnBinary columnBinary, final int childLoadSize)
        throws IOException {
      final PathNode child = node.children.get(columnBinary.columnName);
      if (child != null) {
        loadNode(columnBinary, childLoadSize, child);
      }
    }
  }

  private final class NestedUnionLoader implements IUnionLoader<WritableColumnVector> {
    private final PathNode node;
    private final int nestedLoadSize;

    private NestedUnionLoader(final PathNode node, final int nestedLoadSize) {
      this.node = node;
      this.nestedLoadSize = nestedLoadSize;
    }

    @Override
    public int getLoadSize() {
      return nestedLoadSize;
    }

    @Override
    public void setNull(final int index) throws IOException {
      putDescendantNulls(node, index);
    }

    @Override
    public void finish() throws IOException {
      // no-op
    }

    @Override
    public WritableColumnVector build() throws IOException {
      return vector;
    }

    @Override
    public void setIndexAndColumnType(final int index, final ColumnType columnType)
        throws IOException {
      if (columnType != ColumnType.SPREAD
          && columnType != ColumnType.MAP
          && columnType != ColumnType.STRUCT) {
        putDescendantNulls(node, index);
      }
    }

    @Override
    public void loadChild(final ColumnBinary columnBinary, final int childLoadSize)
        throws IOException {
      if (columnBinary.columnType == ColumnType.SPREAD) {
        FindColumnBinaryMaker.get(columnBinary.makerClassName)
            .load(columnBinary, new NestedSpreadLoader(node, childLoadSize));
      }
    }
  }

  private void putDescendantNulls(final PathNode node, final int index) {
    for (Target target : node.targets) {
      vector.getChild(target.ordinal).putNull(index);
    }
    for (PathNode child : node.children.values()) {
      putDescendantNulls(child, index);
    }
  }

  private static StructField findField(final StructType type, final String[] path) {
    if (type == null) {
      return null;
    }
    StructType current = type;
    StructField field = null;
    for (int i = 0; i < path.length; i++) {
      field = findDirectField(current, path[i]);
      if (field == null) {
        return null;
      }
      if (i + 1 < path.length) {
        if (!(field.dataType() instanceof StructType)) {
          return null;
        }
        current = (StructType) field.dataType();
      }
    }
    return field;
  }

  private static StructField findDirectField(final StructType type, final String name) {
    for (StructField field : type.fields()) {
      if (field.name().equals(name)) {
        return field;
      }
    }
    return null;
  }

  private static final class PathNode {
    private final Map<String, PathNode> children = new LinkedHashMap<>();
    private final List<Target> targets = new ArrayList<>();
  }


  private static final class ArrayTarget {
    private final int ordinal;
    private final List<VariantPath.Segment> remainingSegments;

    private ArrayTarget(
        final int ordinal, final List<VariantPath.Segment> remainingSegments) {
      this.ordinal = ordinal;
      this.remainingSegments = new ArrayList<>(remainingSegments);
    }
  }

  private static final class Target {
    private final int ordinal;
    private final ILoaderFactory<WritableColumnVector> loaderFactory;

    private Target(
        final int ordinal, final ILoaderFactory<WritableColumnVector> loaderFactory) {
      this.ordinal = ordinal;
      this.loaderFactory = loaderFactory;
    }
  }
}
