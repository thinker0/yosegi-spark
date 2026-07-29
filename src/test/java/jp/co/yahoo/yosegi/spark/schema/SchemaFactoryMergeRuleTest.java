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
package jp.co.yahoo.yosegi.spark.schema;

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaFactoryMergeRuleTest {

  @Test
  void T_sameType_KeepsType() {
    final StructType merged =
        SchemaFactory.mergeSchemas(
            new StructType().add("id", DataTypes.IntegerType, false),
            new StructType().add("id", DataTypes.IntegerType, false));

    assertEquals(DataTypes.IntegerType, merged.apply("id").dataType());
  }

  @Test
  void T_integerAndLong_PromotesToLong() {
    final StructType merged =
        SchemaFactory.mergeSchemas(
            new StructType().add("id", DataTypes.IntegerType, false),
            new StructType().add("id", DataTypes.LongType, false));

    assertEquals(DataTypes.LongType, merged.apply("id").dataType());
  }

  @Test
  void T_floatAndDouble_PromotesToDouble() {
    final StructType merged =
        SchemaFactory.mergeSchemas(
            new StructType().add("score", DataTypes.FloatType, false),
            new StructType().add("score", DataTypes.DoubleType, false));

    assertEquals(DataTypes.DoubleType, merged.apply("score").dataType());
  }

  @Test
  void T_missingColumn_BecomesNullable() {
    final StructType merged =
        SchemaFactory.mergeSchemas(
            new StructType().add("id", DataTypes.LongType, false),
            new StructType()
                .add("id", DataTypes.LongType, false)
                .add("name", DataTypes.StringType, false));

    assertTrue(merged.apply("name").nullable());
  }

  @Test
  void T_existingColumnMissingFromLaterFile_BecomesNullable() {
    final StructType merged =
        SchemaFactory.mergeSchemas(
            new StructType()
                .add("id", DataTypes.LongType, false)
                .add("name", DataTypes.StringType, false),
            new StructType().add("id", DataTypes.LongType, false));

    assertTrue(merged.apply("name").nullable());
  }

  @Test
  void T_stringAndLong_IsRejected() {
    final IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                SchemaFactory.mergeSchemas(
                    new StructType().add("value", DataTypes.StringType, true),
                    new StructType().add("value", DataTypes.LongType, true)));

    assertTrue(error.getMessage().contains("root.value"));
  }

  @Test
  void T_structAndString_IsRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            SchemaFactory.mergeSchemas(
                new StructType()
                    .add(
                        "value",
                        new StructType().add("id", DataTypes.LongType, true),
                        true),
                new StructType().add("value", DataTypes.StringType, true)));
  }

  @Test
  void T_nestedStruct_MergesRecursively() {
    final StructType merged =
        SchemaFactory.mergeSchemas(
            new StructType()
                .add(
                    "user",
                    new StructType().add("id", DataTypes.IntegerType, false),
                    false),
            new StructType()
                .add(
                    "user",
                    new StructType()
                        .add("id", DataTypes.LongType, false)
                        .add("name", DataTypes.StringType, false),
                    false));

    final StructType user = (StructType) merged.apply("user").dataType();
    assertEquals(DataTypes.LongType, user.apply("id").dataType());
    assertTrue(user.apply("name").nullable());
  }

  @Test
  void T_arrayElementNumericType_PromotesSafely() {
    final StructType merged =
        SchemaFactory.mergeSchemas(
            new StructType()
                .add("values", DataTypes.createArrayType(DataTypes.IntegerType, false), false),
            new StructType()
                .add("values", DataTypes.createArrayType(DataTypes.LongType, true), false));

    final ArrayType values = (ArrayType) merged.apply("values").dataType();
    assertEquals(DataTypes.LongType, values.elementType());
    assertTrue(values.containsNull());
  }

  @Test
  void T_mapKeyTypeConflict_IsRejected() {
    final MapType stringKey =
        DataTypes.createMapType(DataTypes.StringType, DataTypes.LongType, true);
    final MapType longKey = DataTypes.createMapType(DataTypes.LongType, DataTypes.LongType, true);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            SchemaFactory.mergeSchemas(
                new StructType().add("attrs", stringKey, true),
                new StructType().add("attrs", longKey, true)));
  }
}
