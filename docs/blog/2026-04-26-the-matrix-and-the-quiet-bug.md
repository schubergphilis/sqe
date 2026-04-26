---
title: "The Iceberg Matrix and the Quiet Bug Hiding in V3"
description: "We thought the V3 path worked. The unit tests said it worked. The matrix called it 'partial' and we agreed. Then we wrote eleven end-to-end tests and discovered Polaris had been silently rejecting every V3 column type for months."
pubDate: "2026-04-26"
author: "Jacob Verhoeks"
tags: ["iceberg", "v3", "polaris", "testing", "matrix"]
---

There is a public scoreboard for Iceberg engines. It lives at [icebergmatrix.org](https://icebergmatrix.org) and it counts what each engine actually supports across the spec. Sixty-three cells per engine. Three levels: full, partial, none. A simple structure that is brutal in practice.

SQE was sitting at 99 out of 189 points. Roughly half. The V2 column was almost full. The V3 column was almost empty. Every V3 cell read either "partial" with the same hand-waving note ("same writer path; not yet exercised") or "none" with the same admission ("V3 untested").

Partial is the word you use when the unit tests pass and you have not actually tried it.

I had not actually tried it.

## What we thought worked

The SQE write path detects V3 features. If you create a table with `TIMESTAMP_NS(9)` or with a `DEFAULT` literal, the handler upgrades the table to format-version 3. The code is clean, the test cases compile, the unit tests pass:

```rust
let needs_v3 = requires_v3_features(&ct.columns, &iceberg_schema);
let format_version = if needs_v3 {
    FormatVersion::V3
} else {
    self.format_version()
};

let table_creation = TableCreation::builder()
    .name(name.clone())
    .schema(iceberg_schema)
    .format_version(format_version)
    .build();

catalog.create_table(&namespace, table_creation).await?;
```

`TableCreation::builder().format_version(V3).build()` constructs a metadata object with `format_version: V3`. The unit tests confirm it. The matrix said partial because nothing tested it against a real catalog.

Half a day of writing eleven integration tests later, the gap between "the unit tests pass" and "the V3 path works" became very clear.

## The first failure

Eleven tests. All `#[ignore]` because they need the docker-compose stack. I lit them up.

Eleven failures, all the same shape:

```
"Invalid schema for v2:
- Invalid type for ts: timestamp_ns is not supported until v3"
```

Polaris was creating a v2 table from my CREATE TABLE statement. Then rejecting the v3 column type because the table it had just created was v2. The engine had told Polaris to make a v3 table. Polaris had ignored that.

## What iceberg-rust sends

iceberg-rust has a REST catalog implementation. When `create_table()` is called, it sends a `CreateTableRequest` over HTTP:

```rust
.json(&CreateTableRequest {
    name: creation.name,
    location: creation.location,
    schema: creation.schema,
    partition_spec: creation.partition_spec,
    write_order: creation.sort_order,
    stage_create: Some(false),
    properties: creation.properties,
})
```

Look at the fields. There is no `format_version`. The struct quietly drops it.

I checked the Iceberg REST spec. The OpenAPI definition for `CreateTableRequest` matches: name, location, schema, partition-spec, write-order, stage-create, properties. No dedicated format-version field.

So how is a REST client supposed to communicate "I want format-version 3"?

Through a property. The reserved table property `format-version` is what the Iceberg ecosystem uses for this. Java's Iceberg client sets it. PyIceberg sets it. iceberg-rust constructs the `TableMetadataBuilder` correctly from the local `TableCreation`, but the REST request itself drops the version on the floor and the server defaults to v2.

This is a one-line fix at the SQE layer. We forward `format-version` through the properties map ourselves:

```rust
let mut props = format_version_properties(format_version);
let table_creation = TableCreation::builder()
    .name(name.clone())
    .schema(iceberg_schema)
    .format_version(format_version)
    .properties(props)
    .build();
```

Polaris saw `format-version: 3` in the properties and built v3 metadata. The `timestamp_ns` column was accepted. The eleven tests turned green.

## The second silent bug

While I was in there, I tried `TBLPROPERTIES`. I had a test that read:

```sql
CREATE TABLE bloom_v3 (id BIGINT, ts TIMESTAMP_NS(9))
TBLPROPERTIES ('write.parquet.bloom-filter-columns' = 'id');
```

The test asserted `SHOW CREATE TABLE` would round-trip the property. The DDL came back without it. Not just without the bloom property. Without any user-set TBLPROPERTIES at all.

I checked the CREATE TABLE handler. The code reads `ct.columns`, builds a schema, calls the catalog. The `ct.table_properties` and `ct.with_options` fields on the parsed AST were never read. Every TBLPROPERTIES clause that ever passed through SQE had been silently dropped on the floor. The user typed it. The parser caught it. The handler ignored it.

```rust
pub(crate) fn merge_user_table_properties(
    props: &mut HashMap<String, String>,
    options: &[sqlparser::ast::SqlOption],
) {
    for opt in options {
        if let SqlOption::KeyValue { key, value } = opt {
            props.insert(key.value.clone(), sql_expr_to_property_string(value));
        }
    }
}
```

Six lines to fix. Two more lines in the CREATE handler to call it. The bloom property now round-trips. So does `write.delete.mode = 'merge-on-read'`. So does anything else a user sets.

This bug had existed since CREATE TABLE was first written. It would never have shown up under unit tests.

## What changed in the matrix

Eleven tests. Two bugs found. The matrix went from 99/189 (52.4%) to 127/189 (67.2%) in one block.

Then I went after the third gap. `FOR VERSION AS OF` had a runtime error: "schema provider does not support registering tables." The snapshot-pinned provider was being registered against the read-only Iceberg schema. The fix was to register it under `datafusion.public.<alias>` (a writable schema) and rewrite the SQL to use the alias. Same pattern that `FOR INCREMENTAL` already used. Different code path, no shared helper. Five-minute fix once you see it.

Then I added a type-promotion test (int to bigint on a V3 table). It worked the first time. The matrix moved another point.

Final: 129/189 (68.3%). Sixteen cells flipped from partial or none to full:

```
table-creation:v3        partial -> full
write-insert:v3          partial -> full
read-support:v3          partial -> full
copy-on-write:v3            none -> full
write-merge-update-delete   none -> full
merge-on-read:v3         partial -> full
position-deletes:v3      unknown -> full
equality-deletes:v3      partial -> full
schema-evolution:v3      partial -> full
statistics:v3               none -> full
cdc-support:v3           unknown -> full
time-travel:v3           partial -> full
type-promotion:v3        partial -> full
catalog-integration:v3      none -> full
polaris:v3                  none -> full
rest-catalog:v3             none -> full
```

Three commits. Thirteen integration tests. Two real bugs that had been hiding behind unit-test green checkmarks.

## What partial actually meant

Going through this exercise made the difference between "it should work" and "it works" tactile. Partial in the matrix used to mean "we believe the writer path is format-version agnostic and the V3 round-trip should work, but we have not tested it." That is a charitable interpretation of code we have not run.

The honest read of "partial" is "we do not know."

The integration tests are the difference. They are slow. They need a stack. They `#[ignore]` so unit-test runs do not break on a missing docker. Most engineers will write them last. Some will not write them at all. The matrix exists precisely because catalogs and engines lie to each other in ways that unit tests cannot see.

If the matrix says your engine is full on a cell, someone has run that cell against a real catalog. If it says partial, someone wrote the writer path and stopped. If it says unknown, someone shipped without measuring.

Pick the column you ship under.

## The one cell that stayed partial

The `bloom-filters:v3` cell did not flip. The write-time property now round-trips through the catalog and the parquet writer respects it on the coordinator path. The worker write path still does not wire the bloom property through. That is a real gap and the caveat in the matrix says so directly.

The matrix is most useful when the partial entries name what is actually missing. Not "untested." Not "future work." The exact line of code that needs to change. That is the difference between a status board and a punch list.

We are running a punch list now.

## The three things this taught me

First: integration tests are the only tests that matter for catalog interactions. If you do not have a real Polaris running and a real S3 backing it, you do not know whether your engine talks to either of them correctly. The cost of the docker-compose stack is the cost of knowing.

Second: read the wire protocol. iceberg-rust's `TableCreation` has a `format_version` field. The REST `CreateTableRequest` does not. Both are correct in their own scope. The bug lives in the gap between them. You cannot find that bug by reading either struct in isolation.

Third: the matrix is not a marketing tool. It is a quality artifact. Score 99/189 looks bad next to "passes 222 of 222 benchmark queries." Score 99/189 is honest about what we have not tested. Score 222/222 is true and incomplete: the benchmark suite does not exercise V3-only types or schema evolution or position deletes. The matrix does. Both numbers belong in the README. We had been showing one and hiding the other.

Build matters. Test matters. Measure matters. Ship the matrix score next to the benchmark score and let the reader see the whole picture.

The next cells are already in flight. HMS and Glue real implementations. The worker bloom-filter wiring. MERGE on V3 with a direct test. The score will keep moving.

The only number worth quoting is the one earned by tests that ran against a real stack today.
