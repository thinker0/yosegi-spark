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

# Yosegi Spark quick start

This guide shows the shortest path to reading Yosegi with the Spark 4.2
DataSource V2 reader.

For the complete runtime matrix and limitations, see
[../COMPATIBILITY.md](../COMPATIBILITY.md). If you are moving an existing
application from V1, read
[../MIGRATION_V1_TO_V2.md](../MIGRATION_V1_TO_V2.md).

## Requirements

- Java 17
- Scala 2.13
- Apache Spark 4.2
- a Yosegi Spark build for this release line

Make the Yosegi and Yosegi Spark jars available to Spark using your normal
dependency or `--jars` configuration.

## Read a Yosegi dataset

Start with the V2 provider name:

```scala
val df = spark.read
  .format("yosegi-v2")
  .load("/tmp/example.yosegi")

df.show()
```

Select only the columns you need:

```scala
df.select("id", "name").show()
```

Filter normally through Spark:

```scala
df.filter("age >= 20").show()
```

Supported predicates may be pushed into Yosegi. Spark retains residual
predicates when the reader cannot safely evaluate them.

To inspect the scan:

```scala
df.filter("age >= 20").explain(true)
```

## Read partitioned data

For a Hive-style layout such as:

```text
/data/events/date=2026-08-12/hour=13/part-00000.yosegi
```

read the dataset directly:

```scala
val events = spark.read
  .format("yosegi-v2")
  .load("/data/events")
```

Or preserve the partition root while reading a subset:

```scala
val events = spark.read
  .format("yosegi-v2")
  .option("basePath", "/data/events")
  .load("/data/events/date=2026-08-12")
```

Partition predicates can then be used normally:

```scala
events
  .filter("date = DATE '2026-08-12' AND hour = 13")
  .show()
```

## Read Spark Variant

If a Yosegi column is exposed as Spark Variant, select the full value:

```scala
val df = spark.read
  .format("yosegi-v2")
  .load("/data/variant-events")

df.createOrReplaceTempView("events")
```

```sql
SELECT v
FROM events
```

Or extract a path:

```sql
SELECT variant_get(v, '$.id', 'bigint')
FROM events
```

Nested and array examples:

```sql
SELECT
  variant_get(v, '$.user.profile.name', 'string'),
  variant_get(v, '$.items[0]', 'string')
FROM events
```

For pushdown rules, UNION behavior, special values, and limitations, see
[variant.md](variant.md).

## Writing Yosegi

DataSource V2 Read 1.0 does not provide a V2 writer.

Existing applications that write Yosegi should continue to use the existing V1
write path. See [../MIGRATION_V1_TO_V2.md](../MIGRATION_V1_TO_V2.md).

## Next steps

- [Compatibility](../COMPATIBILITY.md)
- [V1 to V2 migration](../MIGRATION_V1_TO_V2.md)
- [Variant guide](variant.md)
- [Project README](../README.md)
