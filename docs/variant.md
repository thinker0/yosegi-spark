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

# Spark Variant with Yosegi DataSource V2

Yosegi Spark DataSource V2 supports Spark 4.2 Variant reads through Spark's
public `SupportsPushDownVariantExtractions` API.

This document describes the Read 1.0 Variant contract. For a first read, start
with [quickstart.md](quickstart.md).

## Design rule

Variant pushdown is conservative:

```text
Yosegi can guarantee Spark-equivalent semantics
    -> accept pushdown

cast, overflow, timezone, or other Spark semantics are required
    -> do not accept pushdown
    -> let Spark evaluate the expression
```

The reader does not copy Spark's internal cast implementation into Yosegi.

## Full Variant

A Variant column can be selected directly:

```sql
SELECT v
FROM events
```

The reader reconstructs Spark Variant `value` and `metadata` from the Yosegi
physical representation.

## Direct path extraction

Examples:

```sql
SELECT variant_get(v, '$.id', 'bigint')
FROM events
```

```sql
SELECT variant_get(v, '$.name', 'string')
FROM events
```

A physical type that exactly matches the requested Spark type may be pushed.

For example, a Yosegi `LONG` requested as Spark `BIGINT` can be pushed when the
remaining semantics are compatible.

A narrowing conversion such as `LONG` to `INT` is not pushed. Spark performs
the conversion and owns overflow/cast behavior.

## Nested paths

Nested object paths are supported:

```sql
SELECT variant_get(v, '$.user.id', 'bigint')
FROM events
```

```sql
SELECT variant_get(v, '$.user.profile.name', 'string')
FROM events
```

The reader resolves path segments recursively and projects the required
physical columns. Missing fields and NULL intermediate objects are handled
without inventing values.

The same exact-type pushdown rule applies to nested leaves.

## Array paths

Object-contained arrays and arrays containing objects are supported:

```sql
SELECT variant_get(v, '$.items[0]', 'string')
FROM events
```

```sql
SELECT variant_get(v, '$.users[0].id', 'bigint')
FROM events
```

Read 1.0 reads the required array column and evaluates the array index in the
loader. It does not perform element-level physical I/O projection.

The contract includes:

- valid array indexes
- out-of-range indexes
- NULL arrays
- NULL elements
- empty arrays
- objects inside arrays
- primitive values inside arrays

Complete end-to-end support for a Variant whose top-level physical value is
itself an array, including paths such as `$[0]`, is outside the Read 1.0
contract.

## UNION

Yosegi UNION values can be reconstructed as Spark Variant values.

Covered shapes include:

- primitive values
- Object / SPREAD values
- arrays
- MAP / STRUCT logical tags
- nested and recursive UNION
- Variant null
- SQL NULL
- empty arrays and objects
- mixed objects, primitives, and NULL values inside arrays

The reader preserves the UNION type tag and source row position while
reconstructing values. Value order and NULL positions must not change.

## Special values and logical types

Full Variant coverage includes:

- Decimal precision and scale
- Date
- Timestamp
- TimestampNTZ
- Float NaN
- Double positive infinity
- Double negative infinity
- Binary
- empty Binary

Yosegi's physical `ColumnType` does not have dedicated Date, Timestamp, or
Decimal tags. When a normal typed schema is available, the Spark data type can
supply that semantic information.

A recursive UNION may lose that schema-level information. In that case the
reader must **not** guess, for example:

```text
LONG  -> Timestamp?
LONG  -> Date?
BYTES -> Decimal?
```

Raw physical representation alone is not enough to infer those logical types.

## Missing fields and `try_variant_get`

Missing fields follow the conservative pushdown contract. If the reader cannot
guarantee the requested semantics, Spark evaluates the expression.

`try_variant_get` is supported through the same rule rather than by
reimplementing Spark cast/error semantics in Yosegi.

## Explain plan

Use Spark's physical plan to diagnose Variant extraction:

```scala
df.explain(true)
```

The V2 scan description reports pushed Variant extractions together with the
effective read schema and other pushdown state.

Expected behavior:

```text
exact requested type -> pushed to Yosegi
cast required        -> Spark fallback
```

## Known limitations

Read 1.0 does not include:

- Variant Writer
- array-element physical I/O projection
- complete top-level Array Variant end-to-end support
- automatic incompatible-schema evolution to Variant/UNION
- advanced Variant performance tuning

These are intentionally separate from the Read 1.0 completion criteria.

## Related documentation

- [Quick start](quickstart.md)
- [Compatibility](../COMPATIBILITY.md)
- [V1 to V2 migration](../MIGRATION_V1_TO_V2.md)
