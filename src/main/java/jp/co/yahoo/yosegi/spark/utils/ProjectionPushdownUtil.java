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
package jp.co.yahoo.yosegi.spark.utils;

import jp.co.yahoo.yosegi.spark.v2.VariantStructUtil;

import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public final class ProjectionPushdownUtil {
  private ProjectionPushdownUtil() {}

  public static String createProjectionPushdownJson(final StructType requiredSchema) {
    final List<String> projections = new ArrayList<>();
    for (StructField field : requiredSchema.fields()) {
      if (field.dataType() instanceof StructType
          && VariantStructUtil.isVariantStruct((StructType) field.dataType())) {
        appendVariantProjections(projections, field);
      } else {
        projections.add(toJsonPath(field.name()));
      }
    }
    return "[" + String.join(",", projections) + "]";
  }

  private static void appendVariantProjections(
      final List<String> projections, final StructField variantField) {
    final StructType variantStruct = (StructType) variantField.dataType();
    for (StructField extractedField : variantStruct.fields()) {
      final String[] sourcePath =
          VariantStructUtil.getPhysicalProjectionPath(extractedField.metadata());
      final String[] projectionPath = new String[(sourcePath == null ? 0 : sourcePath.length) + 1];
      projectionPath[0] = variantField.name();
      if (sourcePath != null) {
        System.arraycopy(sourcePath, 0, projectionPath, 1, sourcePath.length);
      }
      final String projection = toJsonPath(projectionPath);
      if (!projections.contains(projection)) {
        projections.add(projection);
      }
    }
  }

  private static String toJsonPath(final String... names) {
    final StringBuilder builder = new StringBuilder("[");
    for (int i = 0; i < names.length; i++) {
      if (i > 0) {
        builder.append(',');
      }
      builder.append('"').append(escapeJson(names[i])).append('"');
    }
    return builder.append(']').toString();
  }

  private static String escapeJson(final String value) {
    return value.replace("\\", "\\\\").replace("\"", "\\\"");
  }
}
