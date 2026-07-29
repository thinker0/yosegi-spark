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

# Yosegi Spark compatibility

This document describes the compatibility contract for the Spark 4.2 line of
Yosegi Spark and, in particular, the DataSource V2 reader.

## Spark 4.2 line

| Component | Supported version / contract |
| --- | --- |
| Java | 17 |
| Scala | 2.13 |
| Apache Spark | 4.2 |
| Yosegi Spark artifact | Scala 2.13 build |
| DataSource V1 format | `yosegi` |
| DataSource V2 format | `yosegi-v2` |
| DataSource V2 read | Supported |
| DataSource V2 write | Not supported in Read 1.0 |
| Spark Variant | Supported by the V2 reader |

The Spark, Scala, and Java versions above are a release-line contract. Do not
assume binary compatibility with a different Spark or Scala binary version.

## V1 and V2

The existing V1 implementation remains available:

```scala
spark.read
  .format("yosegi")
  .load(path)
```

The Spark 4.2 DataSource V2 reader is selected explicitly:

```scala
spark.read
  .format("yosegi-v2")
  .load(path)
```

V1 and V2 are separate Spark data source implementations. V2 Read 1.0 is
intended to provide a migration-quality replacement for the common V1 read
contract, but it is not a promise that every internal implementation detail or
every option has identical behavior.

Black-box parity tests cover the common read contract for primitive values,
Binary, Decimal, Date/Timestamp, Struct, Array, Map, SQL NULL, complex NULL,
projection, filters, and partitions.

Spark Variant is a V2 feature and is intentionally excluded from V1/V2 parity.

See [MIGRATION_V1_TO_V2.md](MIGRATION_V1_TO_V2.md) for migration guidance.

## DataSource V2 Read 1.0 feature compatibility

| Area | Read 1.0 |
| --- | --- |
| Batch read | Supported |
| Column pruning | Supported |
| Regular filter pushdown | Supported, conservatively |
| Residual filters | Supported |
| Hive-style partition discovery | Supported |
| Partition pruning | Supported |
| `basePath` | Supported |
| Multi-file read | Supported |
| Multi-file schema merge | Supported with conservative type rules |
| Byte-range split read | Supported |
| Full Spark Variant read | Supported |
| Direct Variant extraction | Supported |
| Nested Variant extraction | Supported |
| Array Variant extraction | Supported with 1.0 limitations |
| Yosegi UNION to Spark Variant | Supported |
| Explain-plan diagnostics | Supported |
| DataSource V2 Writer | Not supported |
| Streaming | Not supported |
| Limit pushdown | Not supported |
| Aggregate pushdown | Not supported |
| Advanced statistics | Not supported |

## Multi-file schema compatibility

Read 1.0 deliberately uses conservative schema evolution rules.

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

The reader does not automatically convert incompatible schemas to Variant or
UNION.

## Partition compatibility

The tested Read 1.0 partition contract includes:

- String
- Integer
- Long
- Boolean
- Date
- Decimal
- NULL
- multiple partition levels
- URL-encoded partition values
- Hive's default partition marker
- `basePath`
- partition pruning

A partition column must not conflict with a physical data column of the same
name. A conflict fails fast.

## Missing and corrupt files

Read 1.0 uses a **fail-fast** contract for missing and corrupt Yosegi files.

Spark provides `spark.sql.files.ignoreMissingFiles` and
`spark.sql.files.ignoreCorruptFiles`, but V2 Read 1.0 does not promise
Spark-compatible ignore semantics for these options.

## Variant compatibility

Variant support uses Spark 4.2's public
`SupportsPushDownVariantExtractions` API.

An extraction is pushed only when Yosegi can preserve Spark semantics. In
particular, an exact physical type match can be pushed, while an extraction
that requires cast, overflow, or other Spark conversion semantics is evaluated
by Spark.

See [docs/variant.md](docs/variant.md) for the detailed Variant contract.

## Known Read 1.0 limitations

The following are intentionally outside Read 1.0:

- DataSource V2 Writer
- Variant Writer
- Streaming
- Limit pushdown
- Aggregate pushdown
- advanced statistics
- array-element physical I/O projection
- large-scale FileIndex parallelization
- automatic incompatible-schema evolution to Variant/UNION
- Spark-compatible ignore-missing/ignore-corrupt behavior
- complete end-to-end top-level Array Variant support

These limitations are not indications that the existing V1 functionality has
been removed. They define the scope of the V2 Read 1.0 contract.
