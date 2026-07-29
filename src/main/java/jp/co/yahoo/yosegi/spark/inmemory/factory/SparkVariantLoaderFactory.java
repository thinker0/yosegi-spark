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
package jp.co.yahoo.yosegi.spark.inmemory.factory;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.inmemory.ILoader;
import jp.co.yahoo.yosegi.inmemory.ILoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.loader.SparkEmptyVariantStructLoader;
import jp.co.yahoo.yosegi.spark.inmemory.loader.SparkVariantLoader;
import jp.co.yahoo.yosegi.spark.inmemory.loader.SparkUnionVariantLoader;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;

/** Reconstructs a Spark VariantType value from a Yosegi SPREAD-backed Struct. */
public class SparkVariantLoaderFactory implements ILoaderFactory<WritableColumnVector> {
  private final WritableColumnVector vector;
  private final StructType physicalType;

  public SparkVariantLoaderFactory(
      final WritableColumnVector vector, final StructType physicalType) {
    this.vector = vector;
    this.physicalType = physicalType;
  }

  @Override
  public ILoader createLoader(final ColumnBinary columnBinary, final int loadSize)
      throws IOException {
    if (columnBinary == null) {
      return new SparkEmptyVariantStructLoader(vector, loadSize);
    }
    switch (getLoadType(columnBinary, loadSize)) {
      case SPREAD:
        return new SparkVariantLoader(vector, loadSize, physicalType);
      case UNION:
        return new SparkUnionVariantLoader(vector, loadSize, physicalType);
      default:
        return new SparkEmptyVariantStructLoader(vector, loadSize);
    }
  }
}
