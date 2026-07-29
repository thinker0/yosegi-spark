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
package jp.co.yahoo.yosegi.spark.pushdown;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.IsNull;
import org.apache.spark.sql.sources.Not;
import org.apache.spark.sql.sources.Or;
import org.junit.jupiter.api.Test;

class FilterConnectorFactoryV2Test {

  @Test
  void T_and_AllChildrenSupported_IsSupported() {
    final Filter filter = new And(new EqualTo("id", 1L), new EqualTo("name", "alice"));
    assertNotNull(FilterConnectorFactory.get(filter));
  }

  @Test
  void T_and_UnsupportedChild_IsRejectedAsWholeExpression() {
    final Filter filter = new And(new EqualTo("id", 1L), new IsNull("name"));
    assertNull(FilterConnectorFactory.get(filter));
  }

  @Test
  void T_or_UnsupportedChild_IsRejected() {
    final Filter filter = new Or(new EqualTo("id", 1L), new IsNull("name"));
    assertNull(FilterConnectorFactory.get(filter));
  }

  @Test
  void T_not_UnsupportedChild_IsRejected() {
    assertNull(FilterConnectorFactory.get(new Not(new IsNull("name"))));
  }

  @Test
  void T_not_SupportedChild_IsSupported() {
    assertNotNull(FilterConnectorFactory.get(new Not(new EqualTo("id", 1L))));
  }

  @Test
  void T_equalTo_NullLiteral_IsRejectedInsteadOfThrowing() {
    assertNull(FilterConnectorFactory.get(new EqualTo("id", null)));
  }
}
