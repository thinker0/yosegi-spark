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
package jp.co.yahoo.yosegi.message.parser.spark;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

/** Regression coverage for nullable complex values in the V1 Yosegi writer parser path. */
class SparkParserFactoryNullTest {

  @Test
  void T_get_NullComplexFields_ReturnNullParser() throws Exception {
    final StructType structType = new StructType().add("id", DataTypes.LongType, true);
    final ArrayType arrayType = DataTypes.createArrayType(DataTypes.LongType, true);
    final MapType mapType =
        DataTypes.createMapType(DataTypes.StringType, DataTypes.LongType, true);
    final GenericInternalRow row = new GenericInternalRow(new Object[] {null, null, null});

    assertInstanceOf(SparkNullParser.class, SparkParserFactory.get(structType, row, 0));
    assertInstanceOf(SparkNullParser.class, SparkParserFactory.get(arrayType, row, 1));
    assertInstanceOf(SparkNullParser.class, SparkParserFactory.get(mapType, row, 2));
  }

  @Test
  void T_getFromArray_NullComplexElements_ReturnNullParser() throws Exception {
    final StructType structType = new StructType().add("id", DataTypes.LongType, true);
    final ArrayType arrayType = DataTypes.createArrayType(DataTypes.LongType, true);
    final MapType mapType =
        DataTypes.createMapType(DataTypes.StringType, DataTypes.LongType, true);
    final GenericArrayData row = new GenericArrayData(new Object[] {null});

    assertInstanceOf(
        SparkNullParser.class, SparkParserFactory.getFromArray(structType, row)[0]);
    assertInstanceOf(
        SparkNullParser.class, SparkParserFactory.getFromArray(arrayType, row)[0]);
    assertInstanceOf(
        SparkNullParser.class, SparkParserFactory.getFromArray(mapType, row)[0]);
  }
}
