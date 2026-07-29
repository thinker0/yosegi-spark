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
import jp.co.yahoo.yosegi.inmemory.ILoader;
import jp.co.yahoo.yosegi.inmemory.IUnionLoader;
import jp.co.yahoo.yosegi.spark.inmemory.SparkLoaderFactoryUtil;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

/** Loads the SPREAD member of a Yosegi UNION into a Spark struct vector. */
public class SparkUnionStructLoader implements IUnionLoader<WritableColumnVector> {
  private final WritableColumnVector vector;
  private final int loadSize;
  private boolean childLoaded;

  public SparkUnionStructLoader(final WritableColumnVector vector, final int loadSize) {
    this.vector = vector;
    this.loadSize = loadSize;
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
    // The child loader completes the SPREAD vector while loadChild is executing.
  }

  @Override
  public WritableColumnVector build() throws IOException {
    if (!childLoaded) {
      return new SparkEmptyStructLoader(vector, loadSize).build();
    }
    return vector;
  }

  @Override
  public void setIndexAndColumnType(final int index, final ColumnType columnType)
      throws IOException {
    if (columnType != ColumnType.SPREAD) {
      vector.putNull(index);
    }
  }

  @Override
  public void loadChild(final ColumnBinary columnBinary, final int childLoadSize)
      throws IOException {
    if (columnBinary.columnType == ColumnType.SPREAD) {
      childLoaded = true;
      final ILoader childLoader =
          SparkLoaderFactoryUtil.createLoaderFactory(vector)
              .createLoader(columnBinary, childLoadSize);
      FindColumnBinaryMaker.get(columnBinary.makerClassName).load(columnBinary, childLoader);
      childLoader.build();
    }
  }
}
