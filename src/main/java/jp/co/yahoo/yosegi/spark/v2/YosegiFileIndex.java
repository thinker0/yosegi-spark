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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
/**
 * Driver-side file index, Hive-style partition discovery, and split-locality helper for V2 reads.
 *
 * <p>The index expands configured roots, recursively lists visible files, decodes Hive-style
 * {@code name=value} path segments, preserves partition-column order, and associates every file
 * with its raw partition values. {@code basePath} defines the partition discovery root when only a
 * subtree is loaded.
 *
 * <p>Partition values are kept in insertion order. That order is a correctness requirement because
 * the discovered Spark partition schema and the raw values later materialized on executors are
 * positionally aligned.
 *
 * <p>Hidden files are excluded. Conflicting partition layouts and files outside the configured
 * {@code basePath} fail fast rather than being interpreted heuristically.
 *
 * <p>Read 1.0 also uses fail-fast behavior for missing and corrupt files. Full compatibility with
 * Spark's {@code spark.sql.files.ignoreMissingFiles} and
 * {@code spark.sql.files.ignoreCorruptFiles} options is intentionally outside this class's 1.0
 * contract.
 */
public final class YosegiFileIndex {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private YosegiFileIndex() {}

  public static FileStatus[] listFiles(
      final Configuration configuration, final Map<String, String> options) throws IOException {
    return Arrays.stream(listPartitionedFiles(configuration, options).files())
        .map(PartitionedFile::fileStatus)
        .toArray(FileStatus[]::new);
  }

  public static Listing listPartitionedFiles(
      final Configuration configuration, final Map<String, String> options) throws IOException {
    final List<Path> roots = getPaths(options);
    final String basePathOption = YosegiV2Options.getIgnoreCase(options, YosegiV2Options.BASE_PATH);
    final List<PartitionedFile> files = new ArrayList<>();

    for (Path configuredRoot : roots) {
      final FileSystem fileSystem = configuredRoot.getFileSystem(configuration);
      final Path root = fileSystem.makeQualified(configuredRoot);
      final FileStatus rootStatus = fileSystem.getFileStatus(root);
      final Path partitionBase =
          basePathOption == null || basePathOption.isBlank()
              ? (rootStatus.isFile() ? root.getParent() : root)
              : fileSystem.makeQualified(new Path(basePathOption));
      collect(fileSystem, rootStatus, partitionBase, files);
    }

    files.sort(Comparator.comparing(file -> file.fileStatus().getPath().toString()));
    final List<String> partitionNames = validateAndGetPartitionNames(files);
    final StructField[] fields = new StructField[partitionNames.size()];
    for (int i = 0; i < partitionNames.size(); i++) {
      fields[i] = DataTypes.createStructField(partitionNames.get(i), DataTypes.StringType, true);
    }
    return new Listing(files.toArray(new PartitionedFile[0]), new StructType(fields));
  }

  public static String[] getPreferredLocations(
      final Configuration configuration,
      final FileStatus file,
      final long start,
      final long length)
      throws IOException {
    final FileSystem fileSystem = file.getPath().getFileSystem(configuration);
    final BlockLocation[] blocks =
        fileSystem.getFileBlockLocations(file, start, Math.max(1L, length));
    final Set<String> hosts = new LinkedHashSet<>();
    for (BlockLocation block : blocks) {
      for (String host : block.getHosts()) {
        hosts.add(host);
      }
    }
    return hosts.toArray(new String[0]);
  }

  public static String[] getPreferredLocations(
      final Configuration configuration, final FileStatus file) throws IOException {
    return getPreferredLocations(configuration, file, 0L, file.getLen());
  }

  private static List<Path> getPaths(final Map<String, String> options) throws IOException {
    final List<Path> paths = new ArrayList<>();
    final String path = YosegiV2Options.getIgnoreCase(options, YosegiV2Options.PATH);
    if (path != null && !path.isBlank()) {
      paths.add(new Path(path));
    }
    final String pathsJson = YosegiV2Options.getIgnoreCase(options, YosegiV2Options.PATHS);
    if (pathsJson != null && !pathsJson.isBlank()) {
      for (String value :
          OBJECT_MAPPER.readValue(pathsJson, new TypeReference<List<String>>() {})) {
        paths.add(new Path(value));
      }
    }
    if (paths.isEmpty()) {
      throw new IllegalArgumentException("The 'path' option is required for yosegi-v2.");
    }
    return paths;
  }

  private static void collect(
      final FileSystem fileSystem,
      final FileStatus status,
      final Path partitionBase,
      final List<PartitionedFile> files)
      throws IOException {
    if (isHidden(status.getPath())) {
      return;
    }
    if (status.isFile()) {
      files.add(
          new PartitionedFile(
              status, discoverPartitionValues(status.getPath(), partitionBase)));
      return;
    }
    for (FileStatus child : fileSystem.listStatus(status.getPath())) {
      collect(fileSystem, child, partitionBase, files);
    }
  }

  private static LinkedHashMap<String, String> discoverPartitionValues(
      final Path filePath, final Path partitionBase) {
    final List<String> segments = new ArrayList<>();
    Path current = filePath.getParent();
    while (current != null && !current.equals(partitionBase)) {
      segments.add(0, current.getName());
      current = current.getParent();
    }
    if (current == null) {
      throw new IllegalArgumentException(
          "File is outside the configured basePath: " + filePath + " basePath=" + partitionBase);
    }

    final LinkedHashMap<String, String> result = new LinkedHashMap<>();
    for (String segment : segments) {
      final int separator = segment.indexOf('=');
      if (separator <= 0) {
        continue;
      }
      final String name = decode(segment.substring(0, separator));
      final String value = decode(segment.substring(separator + 1));
      if (result.put(name, value) != null) {
        throw new IllegalArgumentException(
            "Duplicate partition column '" + name + "' in path: " + filePath);
      }
    }
    return result;
  }

  private static List<String> validateAndGetPartitionNames(final List<PartitionedFile> files) {
    List<String> expected = null;
    for (PartitionedFile file : files) {
      final List<String> actual = new ArrayList<>(file.partitionValues().keySet());
      if (expected == null) {
        expected = actual;
      } else if (!expected.equals(actual)) {
        throw new IllegalArgumentException(
            "Conflicting partition layouts. Expected "
                + expected
                + " but found "
                + actual
                + " in "
                + file.fileStatus().getPath());
      }
    }
    return expected == null ? List.of() : expected;
  }

  private static String decode(final String value) {
    // URLDecoder treats '+' as a space, while Hive-style partition paths use percent escaping and
    // a literal plus must stay a plus.
    return URLDecoder.decode(value.replace("+", "%2B"), StandardCharsets.UTF_8);
  }

  private static boolean isHidden(final Path path) {
    final String name = path.getName();
    return name.startsWith(".") || name.startsWith("_");
  }

  /** One physical file and the partition values encoded in its parent path. */
  public static final class PartitionedFile {
    private final FileStatus fileStatus;
    private final LinkedHashMap<String, String> partitionValues;

    private PartitionedFile(
        final FileStatus fileStatus, final LinkedHashMap<String, String> partitionValues) {
      this.fileStatus = fileStatus;
      this.partitionValues = new LinkedHashMap<>(partitionValues);
    }

    public FileStatus fileStatus() {
      return fileStatus;
    }

    public Map<String, String> partitionValues() {
      // Map.copyOf does not preserve the encounter order of the source map. Partition column
      // order must follow the directory hierarchy because it defines the StructType field order.
      return Collections.unmodifiableMap(new LinkedHashMap<>(partitionValues));
    }

    public String[] partitionValues(final StructType partitionSchema) {
      final String[] result = new String[partitionSchema.length()];
      for (int i = 0; i < partitionSchema.length(); i++) {
        result[i] = partitionValues.get(partitionSchema.fields()[i].name());
      }
      return result;
    }
  }

  /** Deterministic file listing plus its discovered Hive-style partition schema. */
  public static final class Listing {
    private final PartitionedFile[] files;
    private final StructType partitionSchema;

    private Listing(final PartitionedFile[] files, final StructType partitionSchema) {
      this.files = files.clone();
      this.partitionSchema = partitionSchema;
    }

    public PartitionedFile[] files() {
      return files.clone();
    }

    public StructType partitionSchema() {
      return partitionSchema;
    }
  }
}
