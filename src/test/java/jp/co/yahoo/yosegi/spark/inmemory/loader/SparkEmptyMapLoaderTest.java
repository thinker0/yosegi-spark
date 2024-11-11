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
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class SparkEmptyMapLoaderTest {
    public static Stream<Arguments> data() {
        final int loadSize = 5;
        return Stream.of(
            arguments(DataTypes.BooleanType, loadSize),
            arguments(DataTypes.ByteType, loadSize),
            arguments(DataTypes.StringType, loadSize),
            arguments(DataTypes.createDecimalType(), loadSize),
            arguments(DataTypes.DoubleType, loadSize),
            arguments(DataTypes.FloatType, loadSize),
            arguments(DataTypes.IntegerType, loadSize),
            arguments(DataTypes.LongType, loadSize),
            arguments(DataTypes.ShortType, loadSize)
        );
    }

    @ParameterizedTest
    @MethodSource("data")
    void T_load(final DataType elmDataType, final int loadSize) throws Exception {
        final DataType dataType = DataTypes.createMapType(DataTypes.StringType, elmDataType, true);
        final WritableColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
        final ILoader<WritableColumnVector> loader = new SparkEmptyMapLoader(vector, loadSize);
        loader.build();
        for (int i = 0; i < loadSize; i++) {
            assertFalse(vector.isNullAt(i));
            final ColumnarMap cm = vector.getMap(i);
            assertEquals(0, cm.numElements());
        }
    }
}