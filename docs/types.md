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

# Data types

This document describes the ordinary Spark data types handled by Yosegi Spark
and the important distinctions between physical Yosegi representation and
Spark logical types.

Variant has additional rules and is documented separately in
[variant.md](variant.md).

## Primitive values

The reader supports the ordinary primitive categories used by the existing
Yosegi Spark integration, including integral, floating-point, Boolean, String,
and Binary values.

The exact Spark schema is determined by the Yosegi schema and the Spark reader
mapping. Applications should use the Spark `DataFrame.schema` as the public
logical contract rather than depending on an internal Yosegi storage detail.

## Integral values

Integral values include Spark integer-width types represented by the reader's
typed schema.

For multi-file schema evolution, the important Read 1.0 widening rule is:

```text
INT + LONG -> LONG
```

A narrowing conversion is not silently introduced by schema merge.

For Variant extraction, a physical LONG requested as BIGINT can be pushed when
the types and semantics match exactly. A LONG requested as INT requires Spark
cast semantics and is not accepted as an exact-type Variant pushdown.

## Floating-point values

Read 1.0 supports the widening rule:

```text
FLOAT + DOUBLE -> DOUBLE
```

Full Variant reconstruction also preserves special floating-point values that
are supported by Spark Variant, including NaN and positive/negative infinity.

## String

String is a typed value. A String column is not automatically merged with an
unrelated numeric type.

For example:

```text
STRING + LONG -> schema conflict
```

String predicates may participate in normal filter pushdown when they have a
safe Yosegi translation.

## Boolean

Boolean values are supported as ordinary data columns and as typed partition
values.

## Binary

Binary and empty Binary values are supported.

Binary physical representation must not by itself be interpreted as a
higher-level logical type such as Decimal when the schema information needed
to establish that meaning has been lost.

## Decimal

Decimal precision and scale are part of the Spark logical type.

Yosegi does not have a dedicated physical `ColumnType` tag that by itself
fully identifies Spark Decimal semantics. The reader uses available typed
schema information to reconstruct the Spark value.

The reader must not guess that arbitrary bytes in a schema-less recursive
UNION are Decimal.

Decimal partition values are also part of the tested partition contract.

## Date

Spark Date values are supported when typed schema information identifies the
logical value.

Yosegi's physical type alone is not enough to infer that an arbitrary integral
value is a Date.

Date partition values are supported.

## Timestamp

Spark Timestamp values are supported through the typed schema.

Timestamp and TimestampNTZ are distinct Spark logical types. A Variant
extraction that would require converting between them is not treated as an
exact-type pushdown.

## TimestampNTZ

TimestampNTZ is supported by the Variant value reconstruction path when the
schema provides the required logical type.

The reader does not infer TimestampNTZ from a raw integral value after logical
schema information has been lost.

## Struct

Struct is supported as an ordinary Spark complex type.

The reader preserves field structure and SQL NULL semantics, including NULL
Struct values.

Multi-file schema merge can recursively merge compatible Struct fields. A
Struct cannot be silently merged with a String or another unrelated type.

## Array

Array is supported as an ordinary Spark complex type, including:

- empty arrays
- NULL arrays
- NULL elements
- complex values inside arrays where the schema permits them

Variant array extraction has additional behavior described in
[variant.md](variant.md).

## Map

Map is supported as an ordinary Spark complex type, including NULL Map values.

For multi-file schema evolution, map key semantics must remain compatible.
Compatible map values can be merged recursively.

## SQL NULL and complex NULL

SQL NULL is distinct from an empty collection or an empty object.

The reader and existing fixture writer preserve NULL semantics for primitive
and complex values, including NULL Struct, Array, and Map values and NULL
complex elements inside arrays.

## UNION

Yosegi UNION is not an ordinary Spark SQL primitive type. In the V2 Variant
reader, UNION values can be reconstructed as Spark Variant while preserving
the source type tag, row position, ordering, and NULL placement.

See [variant.md](variant.md).

## Physical type versus logical type

A key design rule is:

> Do not infer a Spark logical type from an ambiguous Yosegi physical type when
> the schema information required to establish that logical type is absent.

In particular, a recursive UNION may contain raw representations for which the
reader cannot safely decide:

```text
LONG  -> Date?
LONG  -> Timestamp?
BYTES -> Decimal?
```

Guessing would make the same physical value change meaning depending on reader
heuristics. Read 1.0 therefore keeps this behavior conservative.

## Related documentation

- [Schema evolution](schema-evolution.md)
- [Partitioned datasets](partition.md)
- [Filter pushdown](filter-pushdown.md)
- [Variant](variant.md)
- [Compatibility](../COMPATIBILITY.md)
