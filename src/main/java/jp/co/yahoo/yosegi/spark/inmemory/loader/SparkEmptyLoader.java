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

import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;

public class SparkEmptyLoader {
    public static void load(final WritableColumnVector vector, final int loadSize) throws IOException {
        final Class klass = vector.dataType().getClass();
        if (klass == ArrayType.class) {
            new SparkEmptyArrayLoader(vector, loadSize).build();
        } else if (klass == StructType.class) {
            new SparkEmptyStructLoader(vector, loadSize).build();
        } else if (klass == MapType.class) {
            new SparkEmptyMapLoader(vector, loadSize).build();
        } else {
            new SparkNullLoader(vector, loadSize).build();
        }
    }
}
