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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
/**
 * Parsed representation of the Spark Variant path subset supported by Yosegi read pushdown.
 *
 * <p>A path is represented as ordered object-field and zero-based array-index segments, for
 * example {@code $.user.profile.name} or {@code $.users[0].id}. This is intentionally not a
 * general JSONPath implementation: parsing accepts only syntax that the V2 reader can reason about
 * safely.
 *
 * <p>Successfully parsing a path does not imply that Spark will accept physical pushdown.
 * {@link YosegiScanBuilder} separately validates the Variant backing schema and requires an exact
 * requested Spark leaf type.
 */
public final class VariantPath {
  /** Marker interface for a parsed object-field or array-index segment. */
  public interface Segment {}

  /** Object-field path segment such as {@code .user}. */
  public static final class Field implements Segment {
    private final String name;

    Field(final String name) {
      this.name = name;
    }

    public String name() {
      return name;
    }

    @Override
    public boolean equals(final Object o) {
      return o instanceof Field && name.equals(((Field) o).name);
    }

    @Override
    public int hashCode() {
      return name.hashCode();
    }

    @Override
    public String toString() {
      return "." + name;
    }
  }

  /** Zero-based array-index path segment such as {@code [0]}. */
  public static final class Index implements Segment {
    private final int index;

    Index(final int index) {
      this.index = index;
    }

    public int index() {
      return index;
    }

    @Override
    public boolean equals(final Object o) {
      return o instanceof Index && index == ((Index) o).index;
    }

    @Override
    public int hashCode() {
      return Integer.hashCode(index);
    }

    @Override
    public String toString() {
      return "[" + index + "]";
    }
  }

  private final List<Segment> segments;

  private VariantPath(final List<Segment> segments) {
    this.segments = Collections.unmodifiableList(new ArrayList<>(segments));
  }

  public static VariantPath parse(final String path) {
    if (path == null || path.isEmpty() || path.charAt(0) != '$') {
      return null;
    }
    final List<Segment> result = new ArrayList<>();
    int pos = 1;
    while (pos < path.length()) {
      final char marker = path.charAt(pos);
      if (marker == '.') {
        final int start = ++pos;
        while (pos < path.length() && path.charAt(pos) != '.' && path.charAt(pos) != '[') {
          pos++;
        }
        if (start == pos) {
          return null;
        }
        result.add(new Field(path.substring(start, pos)));
      } else if (marker == '[') {
        final int start = ++pos;
        while (pos < path.length() && Character.isDigit(path.charAt(pos))) {
          pos++;
        }
        if (start == pos || pos >= path.length() || path.charAt(pos) != ']') {
          return null;
        }
        try {
          result.add(new Index(Integer.parseInt(path.substring(start, pos))));
        } catch (NumberFormatException e) {
          return null;
        }
        pos++;
      } else {
        return null;
      }
    }
    return new VariantPath(result);
  }

  public List<Segment> segments() {
    return segments;
  }

  public boolean isRoot() {
    return segments.isEmpty();
  }

  public boolean containsArrayIndex() {
    for (Segment segment : segments) {
      if (segment instanceof Index) {
        return true;
      }
    }
    return false;
  }

  /** Returns a physical Yosegi column path when every segment is an object field. */
  public String[] objectFieldPath() {
    final String[] result = new String[segments.size()];
    for (int i = 0; i < segments.size(); i++) {
      if (!(segments.get(i) instanceof Field)) {
        return null;
      }
      result[i] = ((Field) segments.get(i)).name();
    }
    return result;
  }

  /**
   * Returns the physical Yosegi projection prefix. Array indexes are evaluated by the loader, so
   * projection stops before the first index.
   */
  public String[] physicalProjectionPath() {
    final List<String> result = new ArrayList<>();
    for (Segment segment : segments) {
      if (segment instanceof Index) {
        break;
      }
      if (!(segment instanceof Field)) {
        return null;
      }
      result.add(((Field) segment).name());
    }
    return result.toArray(new String[0]);
  }

  @Override
  public boolean equals(final Object o) {
    return o instanceof VariantPath && segments.equals(((VariantPath) o).segments);
  }

  @Override
  public int hashCode() {
    return Objects.hash(segments);
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder("$");
    for (Segment segment : segments) {
      builder.append(segment);
    }
    return builder.toString();
  }
}
