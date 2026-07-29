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

package jp.co.yahoo.yosegi.spark.v2;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

public class VariantPathTest {
  @Test
  public void T_parse_DirectField() {
    assertArrayEquals(new String[] {"id"}, VariantPath.parse("$.id").objectFieldPath());
  }

  @Test
  public void T_parse_NestedObject() {
    assertArrayEquals(
        new String[] {"user", "profile", "id"},
        VariantPath.parse("$.user.profile.id").objectFieldPath());
  }

  @Test
  public void T_parse_ArrayPaths() {
    VariantPath root = VariantPath.parse("$[0]");
    assertNotNull(root);
    assertTrue(root.containsArrayIndex());
    assertNull(root.objectFieldPath());

    VariantPath nested = VariantPath.parse("$.users[0].id");
    assertNotNull(nested);
    assertTrue(nested.containsArrayIndex());
    assertArrayEquals(new String[] {"users"}, nested.physicalProjectionPath());
    assertEquals("$.users[0].id", nested.toString());

    assertArrayEquals(new String[0], root.physicalProjectionPath());
  }

  @Test
  public void T_parse_InvalidPaths() {
    assertNull(VariantPath.parse(null));
    assertNull(VariantPath.parse("id"));
    assertNull(VariantPath.parse("$."));
    assertNull(VariantPath.parse("$[]"));
    assertNull(VariantPath.parse("$[-1]"));
    assertNull(VariantPath.parse("$.items[x]"));
  }
}
