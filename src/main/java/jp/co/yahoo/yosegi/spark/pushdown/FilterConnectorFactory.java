/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jp.co.yahoo.yosegi.spark.pushdown;

import java.util.Objects;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

import org.apache.spark.sql.sources.*;

import jp.co.yahoo.yosegi.message.objects.*;

import jp.co.yahoo.yosegi.spread.expression.*;
import jp.co.yahoo.yosegi.spread.column.filter.*;


/**
 * Translates supported Spark {@link Filter} objects to Yosegi expression nodes.
 *
 * <p>A {@code null} result means that the complete filter cannot be represented safely by the
 * Yosegi expression layer. Callers must retain Spark-side evaluation in that case.
 *
 * <p>Compound filters are all-or-none at this translation boundary. {@code AND}, {@code OR}, and
 * {@code NOT} return {@code null} when a required child is unsupported. In particular, building an
 * {@code OR} or {@code NOT} from only the supported portion would be an unsafe block-skip hint and
 * could remove rows that Spark should retain.
 *
 * <p>For DataSource V2 reads these expression nodes are optimization hints. Final Spark SQL
 * semantics are preserved by residual filters managed by the scan builder.
 */
public final class FilterConnectorFactory {
  private static final Map<Class, OperatorFilterFactory> gtDispatch = new HashMap<>();
  private static final Map<Class, OperatorFilterFactory> geDispatch = new HashMap<>();
  private static final Map<Class, OperatorFilterFactory> ltDispatch = new HashMap<>();
  private static final Map<Class, OperatorFilterFactory> leDispatch = new HashMap<>();
  private static final Map<Class, OperatorFilterFactory> eqDispatch = new HashMap<>();
  private static final Map<Class, ExpressionNodeFactory> dispatch   = new HashMap<>();

  static {
    gtDispatch.put(String.class, (v) -> new GtStringCompareFilter(v.toString()));
    geDispatch.put(String.class, (v) -> new GeStringCompareFilter(v.toString()));
    ltDispatch.put(String.class, (v) -> new LtStringCompareFilter(v.toString()));
    leDispatch.put(String.class, (v) -> new LeStringCompareFilter(v.toString()));
    eqDispatch.put(String.class, (v) -> new PerfectMatchStringFilter(v.toString()));

    setDispatchMap(gtDispatch, NumberFilterType.GT);
    setDispatchMap(geDispatch, NumberFilterType.GE);
    setDispatchMap(ltDispatch, NumberFilterType.LT);
    setDispatchMap(leDispatch, NumberFilterType.LE);
    setDispatchMap(eqDispatch, NumberFilterType.EQUAL);

    dispatch.put(GreaterThan.class, (f) -> {
      GreaterThan filter = (GreaterThan)f;
      Object value = filter.value();
      if (Objects.isNull(value)) return null;
      OperatorFilterFactory factory = gtDispatch.get(value.getClass());
      if (Objects.isNull(factory)) return null;
      return new ExecuterNode(new StringExtractNode(filter.attribute()), factory.apply(value));
    });

    dispatch.put(GreaterThanOrEqual.class, (f) -> {
      GreaterThanOrEqual filter = (GreaterThanOrEqual)f;
      Object value = filter.value();
      if (Objects.isNull(value)) return null;
      OperatorFilterFactory factory = geDispatch.get(value.getClass());
      if (Objects.isNull(factory)) return null;
      return new ExecuterNode(new StringExtractNode(filter.attribute()), factory.apply(value));
    });

    dispatch.put(LessThan.class, (f) -> {
      LessThan filter = (LessThan)f;
      Object value = filter.value();
      if (Objects.isNull(value)) return null;
      OperatorFilterFactory factory = ltDispatch.get(value.getClass());
      if (Objects.isNull(factory)) return null;
      return new ExecuterNode(new StringExtractNode(filter.attribute()), factory.apply(value));
    });

    dispatch.put(LessThanOrEqual.class, (f) -> {
      LessThanOrEqual filter = (LessThanOrEqual)f;
      Object value = filter.value();
      if (Objects.isNull(value)) return null;
      OperatorFilterFactory factory = leDispatch.get(value.getClass());
      if (Objects.isNull(factory)) return null;
      return new ExecuterNode(new StringExtractNode(filter.attribute()), factory.apply(value));
    });

    dispatch.put(EqualTo.class, (f) -> {
      EqualTo filter = (EqualTo)f;
      Object value = filter.value();
      if (Objects.isNull(value)) return null;
      OperatorFilterFactory factory = eqDispatch.get(value.getClass());
      if (Objects.isNull(factory)) return null;
      return new ExecuterNode(new StringExtractNode(filter.attribute()), factory.apply(value));
    });

    dispatch.put(StringStartsWith.class, (f) -> {
      StringStartsWith filter = (StringStartsWith)f;
      return new ExecuterNode(new StringExtractNode(filter.attribute()), new ForwardMatchStringFilter(filter.value()));
    });

    dispatch.put(StringEndsWith.class, (f) -> {
      StringEndsWith filter = (StringEndsWith)f;
      return new ExecuterNode(new StringExtractNode(filter.attribute()), new BackwardMatchStringFilter(filter.value()));
    });

    dispatch.put(StringContains.class, (f) -> {
      StringContains filter = (StringContains)f;
      return new ExecuterNode(new StringExtractNode(filter.attribute()), new PartialMatchStringFilter(filter.value()));
    });

    dispatch.put(And.class, (f) -> {
      And filter = (And)f;
      IExpressionNode left = get(filter.left());
      if (Objects.isNull(left)) {
        return null;
      }
      IExpressionNode right = get(filter.right());
      if (Objects.isNull(right)) {
        return null;
      }
      IExpressionNode result = new AndExpressionNode();
      result.addChildNode(left);
      result.addChildNode(right);
      return result;
    });

    dispatch.put(Or.class, (f) -> {
      Or filter = (Or)f;
      IExpressionNode result = new OrExpressionNode();
      IExpressionNode left = get(filter.left());
      if (Objects.isNull(left)) {
        return null;
      }
      IExpressionNode right = get(filter.right());
      if (Objects.isNull(right)) {
        return null;
      }
      result.addChildNode(left);
      result.addChildNode(right);
      return result;
    });

    dispatch.put(Not.class, (f) -> {
      Not filter = (Not)f;
      IExpressionNode child = get(filter.child());
      if (Objects.isNull(child)) {
        return null;
      }
      IExpressionNode result = new NotExpressionNode();
      result.addChildNode(child);
      return result;
    });

    dispatch.put(In.class, (f) -> {
      In filter = (In)f;
      Set<String> dic = new HashSet<String>();
      for (Object value : filter.values()) {
        if (!(value instanceof String)) return null;
        dic.add(value.toString());
      }
      return new ExecuterNode(new StringExtractNode(filter.attribute()), new StringDictionaryFilter(dic));
    });

    dispatch.put(EqualNullSafe.class, (f) -> null);
    dispatch.put(IsNull.class,        (f) -> null);
    dispatch.put(IsNotNull.class,     (f) -> null);
  }

  private FilterConnectorFactory() {}

  /**
   * Converts a Spark filter to a Yosegi expression when the complete expression is supported.
   *
   * @param filter Spark filter to translate
   * @return a safe Yosegi expression node, or {@code null} when translation is unsupported
   */
  public static IExpressionNode get(final Filter filter) {
    ExpressionNodeFactory factory = dispatch.get(filter.getClass());
    return (Objects.isNull(factory)) ? null : factory.apply(filter);
  }

  private static void setDispatchMap(Map<Class, OperatorFilterFactory> dispatch, final NumberFilterType type) {
    dispatch.put(Byte.class,    (v) -> new NumberFilter(type, new ByteObj(((Byte)v).byteValue())));
    dispatch.put(Short.class,   (v) -> new NumberFilter(type, new ShortObj(((Short)v).shortValue())));
    dispatch.put(Integer.class, (v) -> new NumberFilter(type, new IntegerObj(((Integer)v).intValue())));
    dispatch.put(Long.class,    (v) -> new NumberFilter(type, new LongObj(((Long)v).longValue())));
    dispatch.put(Float.class,   (v) -> new NumberFilter(type, new FloatObj(((Float)v).floatValue())));
    dispatch.put(Double.class,  (v) -> new NumberFilter(type, new DoubleObj(((Double)v).doubleValue())));
    dispatch.put(java.sql.Timestamp.class,  (v) -> new NumberFilter(type, new LongObj(((java.sql.Timestamp)v).getTime())));
  }

  @FunctionalInterface
  private static interface OperatorFilterFactory {
    IFilter apply(final Object value);
  }

  @FunctionalInterface
  private static interface ExpressionNodeFactory {
    IExpressionNode apply(final Filter filter);
  }
}

