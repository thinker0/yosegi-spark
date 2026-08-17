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

# Multi-file schema and schema evolution

Yosegi Spark DataSource V2 can read multiple Yosegi files and merge their
schemas. Read 1.0 deliberately uses conservative schema-evolution rules.

The goal is predictable reads, not automatic conversion of every schema
conflict into a dynamic type.

## Added and missing columns

A column may exist in one file and be absent from another.

Example:

```text
file A: id LONG, name STRING
file B: id LONG
```

The merged schema contains `name` as a nullable column. Rows from file B
produce SQL NULL for `name`.

## Compatible numeric widening

Read 1.0 supports safe widening rules such as:

| File A | File B | Merged type |
| --- | --- | --- |
| `INT` | `INT` | `INT` |
| `INT` | `LONG` | `LONG` |
| `FLOAT` | `DOUBLE` | `DOUBLE` |

The same conservative rule is applied recursively where the implementation can
preserve the surrounding complex-type contract.

## Incompatible types

An incompatible schema is an error.

Examples:

| File A | File B | Result |
| --- | --- | --- |
| `STRING` | `LONG` | error |
| `STRUCT` | `STRING` | error |

Read 1.0 does not automatically promote incompatible columns to Spark Variant
or Yosegi UNION.

## Struct

Struct fields can be merged recursively when their field types are compatible.
A field that is missing from one side becomes nullable.

A Struct and a non-Struct value are incompatible.

## Array

Array element types must be merge-compatible. Numeric widening can be applied
when it preserves the array element contract.

Read 1.0 does not use Variant as an automatic escape hatch for incompatible
array elements.

## Map

Map keys require compatible key semantics. Map values can be merged
recursively when their types are compatible.

Schema evolution must not silently reinterpret map keys.

## Variant and UNION are not automatic schema evolution

Variant support and UNION decoding are explicit read features. They do not
mean that ordinary incompatible columns are automatically rewritten as
Variant.

This distinction is intentional: a stable typed schema should remain typed,
and an incompatible multi-file dataset should fail visibly.

## Operational guidance

For long-lived datasets:

1. Prefer stable physical column types.
2. Use compatible numeric widening when evolution is necessary.
3. Treat column removal as a nullable/missing-column change.
4. Avoid reusing a column name for a different logical type.
5. Validate a representative multi-file dataset before deployment.

## Related documentation

- [Types](types.md)
- [Partitioned datasets](partition.md)
- [Variant](variant.md)
- [Compatibility](../COMPATIBILITY.md)
