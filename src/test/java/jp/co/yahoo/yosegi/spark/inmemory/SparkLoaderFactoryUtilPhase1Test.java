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
package jp.co.yahoo.yosegi.spark.inmemory;

import jp.co.yahoo.yosegi.inmemory.ILoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.factory.SparkVariantLoaderFactory;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SparkLoaderFactoryUtilPhase1Test {

  @Test
  public void T_createLoaderFactory_FullVariantUsesPhysicalStruct() {
    final StructType physicalType =
        new StructType().add("id", DataTypes.LongType, true);
    final WritableColumnVector output =
        new OnHeapColumnVector(1, DataTypes.VariantType);
    try {
      final ILoaderFactory<WritableColumnVector> factory =
          SparkLoaderFactoryUtil.createLoaderFactory(
              output, DataTypes.VariantType, physicalType);
      Assertions.assertTrue(factory instanceof SparkVariantLoaderFactory);
    } finally {
      output.close();
    }
  }
}
