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
package jp.co.yahoo.yosegi.spark.v2;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.Map;

import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.IsNull;
import org.apache.spark.sql.sources.Not;
import org.apache.spark.sql.sources.Or;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;

class YosegiScanBuilderFilterPushdownTest {

  private YosegiScanBuilder builder() {
    final StructType schema =
        new StructType()
            .add("id", DataTypes.LongType, true)
            .add("name", DataTypes.StringType, true);
    return new YosegiScanBuilder(
        schema,
        schema,
        new StructType(),
        new CaseInsensitiveStringMap(Map.of()));
  }

  @Test
  void T_supportedLeaf_IsHintButRemainsSparkResidual() {
    final YosegiScanBuilder builder = builder();
    final Filter filter = new EqualTo("id", 10L);
    final Filter[] residual = builder.pushFilters(new Filter[] {filter});
    assertEquals(1, builder.pushedFilters().length);
    assertSame(filter, builder.pushedFilters()[0]);
    assertArrayEquals(new Filter[] {filter}, residual);
  }

  @Test
  void T_and_WithUnsupportedChild_PushesOnlySafeConjunct() {
    final YosegiScanBuilder builder = builder();
    final Filter safe = new EqualTo("id", 10L);
    final Filter unsupported = new IsNull("name");
    final Filter whole = new And(safe, unsupported);
    final Filter[] residual = builder.pushFilters(new Filter[] {whole});
    assertEquals(1, builder.pushedFilters().length);
    assertSame(safe, builder.pushedFilters()[0]);
    assertArrayEquals(new Filter[] {whole}, residual);
  }

  @Test
  void T_or_WithUnsupportedChild_DoesNotPushEitherSide() {
    final YosegiScanBuilder builder = builder();
    final Filter whole = new Or(new EqualTo("id", 10L), new IsNull("name"));
    final Filter[] residual = builder.pushFilters(new Filter[] {whole});
    assertEquals(0, builder.pushedFilters().length);
    assertArrayEquals(new Filter[] {whole}, residual);
  }

  @Test
  void T_or_AllChildrenSupported_PushesWholeExpression() {
    final YosegiScanBuilder builder = builder();
    final Filter whole = new Or(new EqualTo("id", 10L), new EqualTo("name", "alice"));
    final Filter[] residual = builder.pushFilters(new Filter[] {whole});
    assertEquals(1, builder.pushedFilters().length);
    assertSame(whole, builder.pushedFilters()[0]);
    assertArrayEquals(new Filter[] {whole}, residual);
  }

  @Test
  void T_not_UnsupportedChild_DoesNotPush() {
    final YosegiScanBuilder builder = builder();
    final Filter whole = new Not(new IsNull("name"));
    final Filter[] residual = builder.pushFilters(new Filter[] {whole});
    assertEquals(0, builder.pushedFilters().length);
    assertArrayEquals(new Filter[] {whole}, residual);
  }
}
