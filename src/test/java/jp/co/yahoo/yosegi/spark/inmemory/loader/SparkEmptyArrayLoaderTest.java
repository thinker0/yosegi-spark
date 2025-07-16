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
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class SparkEmptyArrayLoaderTest {
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
    void T_load_Array_Primitive(final DataType elmDataType, final int loadSize) throws IOException {
        final DataType dataType = DataTypes.createArrayType(elmDataType, true);
        final WritableColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
        final ILoader<WritableColumnVector> loader = new SparkEmptyArrayLoader(vector, loadSize);
        loader.build();
        for (int i = 0; i < loadSize; i++) {
            assertFalse(vector.isNullAt(i));
            assertEquals(0, vector.getArrayLength(i));
        }
    }

    @ParameterizedTest
    @MethodSource("data")
    void T_load_Array_Array(final DataType elmDataType, final int loadSize) throws IOException {
        final DataType arrayDataType = DataTypes.createArrayType(elmDataType);
        final DataType dataType = DataTypes.createArrayType(arrayDataType, true);
        final WritableColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
        final ILoader<WritableColumnVector> loader = new SparkEmptyArrayLoader(vector, loadSize);
        loader.build();
        for (int i = 0; i < loadSize; i++) {
            assertFalse(vector.isNullAt(i));
            assertEquals(0, vector.getArrayLength(i));
        }
    }

    @ParameterizedTest
    @MethodSource("data")
    void T_load_Array_Map(final DataType elmDataType, final int loadSize) throws IOException {
        final DataType mapDataType = DataTypes.createMapType(DataTypes.StringType, elmDataType, true);
        final DataType dataType = DataTypes.createArrayType(mapDataType, true);
        final WritableColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
        final ILoader<WritableColumnVector> loader = new SparkEmptyArrayLoader(vector, loadSize);
        loader.build();
        for (int i = 0; i < loadSize; i++) {
            assertFalse(vector.isNullAt(i));
            assertEquals(0, vector.getArrayLength(i));
        }
    }

    @Test
    void T_load_Array_Struct() throws IOException {
        final List<StructField> fields =
            Arrays.asList(
                DataTypes.createStructField("bo", DataTypes.BooleanType, true),
                DataTypes.createStructField("by", DataTypes.ByteType, true),
                DataTypes.createStructField("bi", DataTypes.BinaryType, true),
                DataTypes.createStructField("do", DataTypes.DoubleType, true),
                DataTypes.createStructField("fl", DataTypes.FloatType, true),
                DataTypes.createStructField("in", DataTypes.IntegerType, true),
                DataTypes.createStructField("lo", DataTypes.LongType, true),
                DataTypes.createStructField("sh", DataTypes.ShortType, true),
                DataTypes.createStructField("st", DataTypes.StringType, true));
        final DataType structDataType = DataTypes.createStructType(fields);
        final DataType dataType = DataTypes.createArrayType(structDataType, true);
        final int loadSize = 5;
        final WritableColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
        final ILoader<WritableColumnVector> loader = new SparkEmptyArrayLoader(vector, loadSize);
        loader.build();
        for (int i = 0; i < loadSize; i++) {
            assertFalse(vector.isNullAt(i));
            assertEquals(0, vector.getArrayLength(i));
        }
    }
}