<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Yosegi Spark

Yosegi Spark is an Apache Spark plugin for reading and writing data in the Yosegi format.

This branch contains the Spark 4.2 DataSource V2 reader implementation. The DataSource V2 1.0 scope is **read only**.

## Documentation

- [Quick start](docs/quickstart.md) — start reading Yosegi with Spark 4.2 DataSource V2
- [Compatibility](COMPATIBILITY.md) — supported Java, Scala, Spark, V1/V2, and Read 1.0 feature matrix
- [Migrating from V1 to V2](MIGRATION_V1_TO_V2.md) — migration procedure, behavioral differences, and rollback
- [Data types](docs/types.md) — primitive, logical, complex, NULL, and physical/logical type rules
- [Partitioned datasets](docs/partition.md) — Hive-style partitions, `basePath`, pruning, and partition conflicts
- [Schema evolution](docs/schema-evolution.md) — multi-file merge, widening, nullable missing columns, and conflicts
- [Filter pushdown](docs/filter-pushdown.md) — supported filters, residual filters, and AND/OR/NOT safety
- [Spark Variant](docs/variant.md) — Variant paths, pushdown semantics, UNION, and known limitations

## Runtime requirements

- Java 17
- Scala 2.13
- Apache Spark 4.2
- Yosegi Spark built from this branch

## Data source names

Use the following format for the DataSource V2 reader:

```scala
spark.read
  .format("yosegi-v2")
  .load(path)
```

The existing DataSource V1 implementation remains available as:

```scala
spark.read
  .format("yosegi")
  .load(path)
```

The V1 and V2 readers are separate contracts. In particular, Spark Variant support described below is a V2 feature and is not part of V1/V2 black-box parity.

## DataSource V2 Read 1.0 scope

The V2 reader supports:

- DataSource V2 batch reads
- Column projection
- Regular filter pushdown with residual-filter safety
- Hive-style partition discovery and partition pruning
- `basePath`
- Multi-file reads and schema merge
- Byte-range file splitting
- Spark Variant full-value reads
- Variant direct, nested, and array paths
- Yosegi UNION values mapped to Spark Variant
- Explain-plan diagnostics for reader pushdown

The 1.0 release is intentionally read-only. See [Limitations](#limitations) for items outside the release scope.

## Basic read

Scala:

```scala
val df = spark.read
  .format("yosegi-v2")
  .load("/data/events")

df.select("id", "name").show()
```

SQL can be used after registering a temporary view:

```scala
df.createOrReplaceTempView("events")
```

```sql
SELECT id, name
FROM events
WHERE id >= 100
```

## Partitioned data

Hive-style paths are discovered automatically. For example:

```text
/data/events/date=2026-08-12/hour=13/part-00000.yosegi
```

The reader supports partition columns, multiple partition levels, URL-encoded partition values, NULL partitions, `basePath`, and partition pruning.

Example:

```scala
val df = spark.read
  .format("yosegi-v2")
  .option("basePath", "/data/events")
  .load("/data/events/date=2026-08-12")
```

```sql
SELECT *
FROM events
WHERE date = DATE '2026-08-12'
  AND hour = 13
```

Partition values are converted according to the resolved Spark schema. The tested 1.0 contract includes String, Integer, Long, Boolean, Date, Decimal, and NULL partition values. Hive's default partition marker is treated as NULL.

A partition column must not conflict with a physical data column of the same name. Such a conflict fails fast.

## Multi-file schema merge

Multiple Yosegi files can be read as one DataFrame. The 1.0 schema merge contract is deliberately conservative.

Examples of compatible merges:

| File A | File B | Result |
| --- | --- | --- |
| `INT` | `INT` | `INT` |
| `INT` | `LONG` | `LONG` |
| `FLOAT` | `DOUBLE` | `DOUBLE` |
| column exists | column missing | nullable column |

Examples of incompatible merges:

| File A | File B | Result |
| --- | --- | --- |
| `STRING` | `LONG` | error |
| `STRUCT` | `STRING` | error |

Nested structures are merged recursively where the types are compatible. Array elements and map values follow the same conservative merge rules. Map key types must remain compatible.

The 1.0 reader does **not** automatically convert incompatible schemas to Variant or UNION.

## Filter pushdown

The V2 reader can push supported regular column filters to Yosegi. The ordinary data-filter set includes supported equality/comparison, membership, boolean-composition, and string predicates. `EqualNullSafe`, `IS NULL`, and `IS NOT NULL` remain Spark residual predicates for ordinary data columns in Read 1.0.

Partition pruning is evaluated separately and supports null-safe equality, `IS NULL`, and `IS NOT NULL` on partition columns.

Pushdown is conservative. A predicate is pushed only when Yosegi can preserve Spark semantics.

For conjunctions, a supported child may be pushed while an unsupported child remains as a Spark residual filter. For disjunctions and negation, the reader does not partially push an expression when doing so could change query semantics.

Use `explain()` to inspect the effective contract:

```scala
df.filter("id >= 100").explain(true)
```

The V2 scan description exposes `ReadSchema`, `PushedFilters`, `PartitionFilters`, and pushed Variant extractions.

## Spark Variant

The V2 reader supports Spark 4.2 Variant reads.

### Full Variant

A Variant column can be selected directly:

```sql
SELECT v
FROM events
```

The reader reconstructs Spark Variant `value` and `metadata` from the Yosegi physical representation.

### Direct paths

```sql
SELECT variant_get(v, '$.id', 'bigint')
FROM events
```

```sql
SELECT variant_get(v, '$.name', 'string')
FROM events
```

### Nested paths

```sql
SELECT variant_get(v, '$.user.id', 'bigint')
FROM events
```

```sql
SELECT variant_get(v, '$.user.profile.name', 'string')
FROM events
```

### Array paths

```sql
SELECT variant_get(v, '$.items[0]', 'string')
FROM events
```

```sql
SELECT variant_get(v, '$.users[0].id', 'bigint')
FROM events
```

For array paths, the 1.0 reader reads the required array column and evaluates the element index in the loader. Element-level physical I/O projection is not part of 1.0.

### Pushdown semantics

Variant extraction is pushed only when the physical Yosegi leaf type exactly matches the Spark-requested type and Spark semantics can be preserved.

For example, an exact `LONG` to Spark `BIGINT` extraction can be pushed. A narrowing cast such as `LONG` to `INT`, or another conversion requiring Spark cast/overflow semantics, is left for Spark to evaluate.

This rule also applies to nested paths. The reader does not copy Spark's internal cast behavior into Yosegi.

`try_variant_get` missing-field behavior is handled safely through the same conservative contract.

## UNION and Variant

Yosegi UNION data is supported by the V2 Variant reader, including:

- Primitive values
- Object / SPREAD values
- Arrays
- MAP / STRUCT logical tags
- Recursive UNION values
- SQL NULL and Variant null
- Empty arrays and objects
- Objects, primitives, and NULL values inside arrays

The reader preserves the UNION type tag, source row position, value order, and NULL position while reconstructing Spark Variant values.

When a recursive UNION has lost schema-level semantic information, the reader does not infer Date, Timestamp, Decimal, or similar logical meanings from a raw LONG or BYTES value alone.

## Special Variant values

The full Variant path has contract coverage for:

- Decimal precision and scale
- Date
- Timestamp
- TimestampNTZ
- Float NaN
- Double positive and negative infinity
- Binary and empty binary

## Explain plan

For release diagnostics, inspect the Spark physical plan:

```scala
df.explain(true)
```

The V2 scan description reports the effective read schema and pushdown state. In particular, Variant extraction should show the following behavior:

- exact requested type: pushed to Yosegi
- cast/conversion required: evaluated by Spark

The plan is part of the 1.0 behavioral contract and is covered by end-to-end tests.

## Corrupt and missing files

DataSource V2 Read 1.0 uses a **fail-fast** contract for missing and corrupt Yosegi files.

Spark has `spark.sql.files.ignoreMissingFiles` and `spark.sql.files.ignoreCorruptFiles`, but V2 1.0 does not promise compatible ignore semantics for these options. Supporting those semantics consistently across file listing, schema inference, and executor-side reads is deferred beyond 1.0.

Do not rely on these Spark options to skip missing or corrupt Yosegi files when using `format("yosegi-v2")` in 1.0.

## Split behavior

The V2 reader plans byte-range input partitions. The 1.0 safety contract is that split boundaries must not produce missing or duplicate rows.

Tests cover whole-file and multi-split reads, tiny splits, boundaries inside Yosegi data, splits larger than the file, and empty files. Unique row IDs are used in safety tests to verify that every expected row is returned exactly once.

## Reader lifecycle

The reader releases underlying streams and column vectors on close and performs best-effort cleanup when initialization or close itself fails. Close is safe to call repeatedly.

When several cleanup operations fail, the first failure is preserved and later failures are attached as suppressed exceptions where possible.

These lifecycle guarantees are intended to keep Spark task failure, cancellation, and retry paths safe from avoidable resource leaks.

## Performance regression gate

The repository contains an opt-in V1/V2 performance regression harness. It is intentionally excluded from normal `mvn test` timing assertions because wall-clock thresholds are sensitive to CI and host load.

Run the release-gate benchmark explicitly:

```bash
mvn \
  -Dtest=YosegiDataSourceV1V2PerformanceTest \
  -Dyosegi.performance.enabled=true \
  test
```

The harness compares V1 and V2 for representative regular reads such as full projection, single-column projection, and selective filtering. Variant queries are recorded as a V2 baseline rather than compared with V1 because their contracts differ.

The performance gate is intended to detect major regressions, not to define a microbenchmark or promise a fixed throughput level.

## V1 and V2 compatibility

Black-box parity tests compare V1 (`format("yosegi")`) and V2 (`format("yosegi-v2")`) on the same Yosegi files for the common read contract, including primitive values, binary, Decimal, Date/Timestamp, Struct, Array, Map, SQL NULL, complex NULL, projection, filters, and partitions.

Variant is excluded from V1/V2 parity because the V2 Variant contract is intentionally different.

## Limitations

The following features are outside the DataSource V2 Read 1.0 scope and must not be treated as release blockers:

- DataSource V2 Writer
- Variant Writer
- Streaming
- Limit pushdown
- Aggregate pushdown
- Advanced statistics
- Array-element physical projection
- Large-scale FileIndex parallelization
- Advanced Variant performance tuning
- Automatic schema evolution to Variant / UNION
- Spark-compatible `ignoreMissingFiles` / `ignoreCorruptFiles` behavior

The existing V1 writer/read functionality is separate from this V2 Read 1.0 scope.

## Build and test

Compile and run the normal test suite with Maven:

```bash
mvn test
```

To build the package:

```bash
mvn clean package
```

## License

This project is licensed under the Apache License. See `LICENSE.txt` for details.

## Contributing

Contributions are welcome. Keep changes compatible with the documented reader contract and add or update tests when behavior changes.
