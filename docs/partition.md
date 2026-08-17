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

# Partitioned datasets

Yosegi Spark DataSource V2 Read 1.0 supports Hive-style partition discovery
and partition pruning.

## Layout

A typical layout is:

```text
/data/events/date=2026-08-12/hour=13/part-00000.yosegi
```

Read the partition root:

```scala
val df = spark.read
  .format("yosegi-v2")
  .load("/data/events")
```

The directory names are exposed as Spark columns.

## `basePath`

When reading only a subset of a partitioned tree, use `basePath` to preserve
the intended partition root:

```scala
val df = spark.read
  .format("yosegi-v2")
  .option("basePath", "/data/events")
  .load("/data/events/date=2026-08-12")
```

## Supported partition values

The Read 1.0 contract covers:

- String
- Integer
- Long
- Boolean
- Date
- Decimal
- NULL
- URL-encoded values
- multiple partition levels

Hive's default partition marker is treated as a NULL partition value.

## Partition pruning

Spark partition predicates are separated from data-column predicates and can
be used to avoid reading unrelated partition files.

Example:

```scala
df.filter("date = DATE '2026-08-12' AND hour = 13")
```

Use `explain(true)` to inspect the V2 scan and its `PartitionFilters`.

## Column order

Partition discovery preserves the partition-column order. The implementation
must not replace the ordered partition-value mapping with an unordered map.

## Partition/data-column conflict

A partition column must not have the same name as a physical data column.

For example, this layout is invalid when the Yosegi file itself also contains
a physical `date` column:

```text
/data/events/date=2026-08-12/part-00000.yosegi
```

The V2 reader fails fast rather than silently choosing one value.

## Multiple roots

A scan may contain multiple roots as long as their partition interpretation is
consistent with the selected base path and schema.

For complicated layouts, prefer an explicit `basePath` rather than relying on
implicit common-root discovery.

## Hidden files

Hidden files are excluded from normal data-file discovery.

## Related documentation

- [Quick start](quickstart.md)
- [Types](types.md)
- [Schema evolution](schema-evolution.md)
- [Filter pushdown](filter-pushdown.md)
- [Compatibility](../COMPATIBILITY.md)
