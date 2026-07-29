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

# Filter pushdown

Yosegi Spark DataSource V2 can push supported Spark filters into the Yosegi
reader. Pushdown is conservative: it is accepted only when it can be used
without changing Spark query results.

Spark remains responsible for residual predicates.

## Supported filter families

For ordinary data-column pushdown, Read 1.0 supports the filter families that can be translated safely by the Yosegi expression layer, including:

- equality
- greater-than / greater-than-or-equal
- less-than / less-than-or-equal
- `IN`
- `AND`
- `OR`
- `NOT`
- supported string predicates

`EqualNullSafe`, `IS NULL`, and `IS NOT NULL` are not pushed as ordinary data-column filters in Read 1.0. They remain Spark residual predicates.

Partition pruning has a separate evaluator and does support null-safe equality, `IS NULL`, and `IS NOT NULL` for partition columns. See [partition.md](partition.md).

Exact behavior also depends on the physical column type and whether the predicate can be represented safely by the Yosegi expression layer.

## Residual filters

A filter that is not pushed remains for Spark to evaluate.

For:

```text
A AND unsupported(B)
```

it can be safe to use `A` as a Yosegi-side pruning condition while Spark still
evaluates `B`, provided the pushed expression cannot remove a row that Spark
would keep.

Pushdown is an optimization, not a replacement for Spark semantics.

## `OR`

Partial `OR` pushdown is unsafe.

For:

```text
A OR unsupported(B)
```

pushing only `A` could cause Yosegi to skip data that would have matched `B`.

The reader therefore accepts compound pushdown only when the expression can be
handled safely as a whole.

## `NOT`

`NOT` has the same safety requirement: the child expression must have a
well-defined supported translation. An unsupported child must not be hidden
inside a translated `NOT` expression.

## Result parity

A query must return the same result whether a filter is pushed or evaluated by
Spark.

This is the primary correctness rule for the pushdown implementation.

## Explain plan

Use:

```scala
df.filter("id >= 100").explain(true)
```

The V2 scan description exposes `PushedFilters` and `PartitionFilters`, making
it possible to diagnose what was accepted by the reader.

## Variant extraction is separate

Regular filter pushdown and Spark Variant extraction pushdown are separate
contracts.

Variant extraction uses Spark 4.2's
`SupportsPushDownVariantExtractions` API and has its own exact-type/fallback
rules.

See [variant.md](variant.md).

## Related documentation

- [Types](types.md)
- [Partitioned datasets](partition.md)
- [Variant](variant.md)
- [Compatibility](../COMPATIBILITY.md)
