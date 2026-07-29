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

# Migrating from DataSource V1 to DataSource V2

This guide is for applications that currently read Yosegi through the Spark
DataSource V1 implementation and want to adopt the Spark 4.2 DataSource V2
reader.

For the supported runtime matrix, see [COMPATIBILITY.md](COMPATIBILITY.md).

## Before migrating

The V2 Read 1.0 runtime requires:

- Java 17
- Scala 2.13
- Apache Spark 4.2

V2 Read 1.0 is **read only**. If an application writes Yosegi through the
existing V1 writer, keep that write path on V1.

## Change the read format

V1:

```scala
val df = spark.read
  .format("yosegi")
  .load(path)
```

V2:

```scala
val df = spark.read
  .format("yosegi-v2")
  .load(path)
```

Do not change a V1 write path to `yosegi-v2` in Read 1.0.

## What should continue to work

The common V1/V2 read contract is covered by black-box parity tests for:

- primitive values
- Binary
- Decimal
- Date and Timestamp
- Struct
- Array
- Map
- SQL NULL
- NULL complex values
- column projection
- filters
- partitioned data

Applications should still validate their own representative datasets before
switching production workloads.

## What is new in V2

### Spark DataSource V2 planning

The reader participates in Spark's DataSource V2 scan planning and exposes
diagnostics such as the effective read schema and pushdown state.

Use:

```scala
df.explain(true)
```

The scan description includes information such as `ReadSchema`,
`PushedFilters`, `PartitionFilters`, and pushed Variant extractions.

### Spark Variant

Spark 4.2 Variant support is a V2 reader feature.

Examples:

```sql
SELECT v
FROM events
```

```sql
SELECT variant_get(v, '$.id', 'bigint')
FROM events
```

```sql
SELECT variant_get(v, '$.user.profile.name', 'string')
FROM events
```

See [docs/variant.md](docs/variant.md).

## Filter pushdown differences

Do not rely on whether a predicate happens to be executed inside the data
source. Rely on Spark query semantics.

V2 uses conservative pushdown:

- supported predicates may be pushed;
- unsupported predicates remain for Spark to evaluate;
- a supported child of an `AND` may be used safely while the unsupported
  condition remains residual;
- `OR` and `NOT` are not partially pushed when doing so could change results.

Use `explain(true)` when diagnosing a migration.

## Multi-file schema behavior

V2 Read 1.0 makes schema conflicts explicit.

Compatible widening includes:

- `INT` + `LONG` -> `LONG`
- `FLOAT` + `DOUBLE` -> `DOUBLE`
- a missing column -> nullable column

Incompatible combinations such as `STRING` + `LONG` or `STRUCT` + `STRING`
fail rather than being automatically converted to Variant or UNION.

If a V1 workload depends on a different implicit merge behavior, normalize the
input schema before migrating.

## Partitioned datasets

V2 supports Hive-style partition discovery, multiple levels, URL-encoded
partition values, `basePath`, NULL/default partition values, and partition
pruning.

Example:

```scala
val df = spark.read
  .format("yosegi-v2")
  .option("basePath", "/data/events")
  .load("/data/events/date=2026-08-12")
```

A partition column that conflicts with a physical data column of the same name
fails fast.

## Missing and corrupt files

This is an important migration difference.

V2 Read 1.0 uses fail-fast behavior for missing and corrupt Yosegi files. It
does not promise compatibility with Spark's
`spark.sql.files.ignoreMissingFiles` or
`spark.sql.files.ignoreCorruptFiles` behavior.

Applications that depend on those options should not assume identical V1/V2
behavior.

## Recommended migration procedure

1. Upgrade the runtime to the supported Java, Scala, and Spark versions.
2. Keep existing V1 write paths unchanged.
3. Run the same representative Yosegi dataset through both `yosegi` and
   `yosegi-v2`.
4. Compare schema and query results.
5. Include projection, filters, partitions, NULL values, and complex types used
   by the application.
6. Inspect `explain(true)` for important filter/partition queries.
7. Test missing/corrupt-file behavior if the workload depends on it.
8. Benchmark representative reads before changing production traffic.
9. Switch the read format to `yosegi-v2`.

## Rollback

Because V1 and V2 use different format names, a read-side rollback is normally
a configuration/code change from:

```text
yosegi-v2
```

back to:

```text
yosegi
```

Keep the V1 path available until application-specific migration validation is
complete.
