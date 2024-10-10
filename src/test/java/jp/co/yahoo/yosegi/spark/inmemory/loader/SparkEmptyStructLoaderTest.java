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
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SparkEmptyStructLoaderTest {
    @Test
    void T_load() throws IOException {
        final List<StructField> e4Fields =
            Arrays.asList(
                DataTypes.createStructField("e1", DataTypes.LongType, true),
                DataTypes.createStructField("e2", DataTypes.createArrayType(DataTypes.LongType, true), true),
                DataTypes.createStructField("e3",
                    DataTypes.createMapType(DataTypes.StringType, DataTypes.LongType, true), true)
            );
        final List<StructField> fields =
            Arrays.asList(
                DataTypes.createStructField("e1", DataTypes.StringType, true),
                DataTypes.createStructField("e2", DataTypes.createArrayType(DataTypes.StringType, true), true),
                DataTypes.createStructField("e3",
                    DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true), true),
                DataTypes.createStructField("e4", DataTypes.createStructType(e4Fields), true)
            );
        final DataType dataType = DataTypes.createStructType(fields);
        final int loadSize = 5;
        final WritableColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
        final ILoader<WritableColumnVector> loader = new SparkEmptyStructLoader(vector, loadSize);
        loader.build();
        for (int i = 0; i < loadSize; i++) {
            for (int j = 0; j < fields.size(); j++) {
                final StructField field = fields.get(j);
                final String name = field.name();
                final DataType type = field.dataType();
                if (name.equals("e1")) {
                    assertTrue(vector.getChild(j).isNullAt(i));
                } else if (name.equals("e2")) {
                    assertFalse(vector.getChild(j).isNullAt(i));
                    assertEquals(0, vector.getChild(j).getArrayLength(i));
                } else if (name.equals("e3")) {
                    assertFalse(vector.getChild(j).isNullAt(i));
                    final ColumnarMap cm = vector.getChild(j).getMap(i);
                    assertEquals(0, cm.numElements());
                } else if (name.equals("e4")) {
                    for (int k = 0; k < e4Fields.size(); k++) {
                        final StructField e4Field = e4Fields.get(k);
                        final String e4Name = e4Field.name();
                        final DataType e4Type = e4Field.dataType();
                        if (e4Name.equals("e1")) {
                            assertTrue(vector.getChild(j).getChild(k).isNullAt(i));
                        } else if (e4Name.equals("e2")) {
                            assertFalse(vector.getChild(j).isNullAt(i));
                            assertEquals(0, vector.getChild(j).getChild(k).getArrayLength(i));
                        } else if (e4Name.equals("e3")) {
                            assertFalse(vector.getChild(j).isNullAt(i));
                            final ColumnarMap e4Cm = vector.getChild(j).getChild(k).getMap(i);
                            assertEquals(0, e4Cm.numElements());
                        } else {
                            assertTrue(false);
                        }
                    }
                } else {
                    assertTrue(false);
                }
            }
        }
    }
}