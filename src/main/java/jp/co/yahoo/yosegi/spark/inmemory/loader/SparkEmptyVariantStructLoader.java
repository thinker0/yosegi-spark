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

import jp.co.yahoo.yosegi.inmemory.ILoader;
import jp.co.yahoo.yosegi.inmemory.LoadType;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

/** Null loader shared by pushed Variant Structs and native Spark Variant vectors. */
public class SparkEmptyVariantStructLoader implements ILoader<WritableColumnVector> {
  private final WritableColumnVector vector;
  private final int loadSize;

  public SparkEmptyVariantStructLoader(
      final WritableColumnVector vector, final int loadSize) {
    this.vector = vector;
    this.loadSize = loadSize;
    for (int i = 0; i < vector.getNumChildren(); i++) {
      final WritableColumnVector child = vector.getChild(i);
      child.reset();
      child.reserve(loadSize);
      if (child.hasDictionary()) {
        child.reserveDictionaryIds(0);
        child.setDictionary(null);
      }
    }
  }

  @Override
  public LoadType getLoaderType() {
    return LoadType.NULL;
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
    for (int i = 0; i < vector.getNumChildren(); i++) {
      SparkEmptyLoader.load(vector.getChild(i), loadSize);
    }
    for (int i = 0; i < loadSize; i++) {
      vector.putNull(i);
    }
    return vector;
  }

  @Override
  public boolean isLoadingSkipped() {
    return true;
  }
}
