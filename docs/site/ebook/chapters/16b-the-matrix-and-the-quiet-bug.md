# The Matrix and the Quiet Bug {#sec:matrix}

> The benchmark says you are fast.
> The matrix says what you actually do.

Benchmarks are the public face of a query engine. The matrix is the back of the kitchen. It is where the spec lives and where the lies surface.

We had been telling a benchmark story for weeks. Twenty-two TPC-H queries pass. Ninety-nine TPC-DS queries pass. Forty-three ClickBench queries pass. Two hundred and twenty-two queries across seven suites, all green. The number was true. The number was also incomplete. None of those queries used a TIMESTAMP_NS column. None of them set a write-default. None of them wrote a position-delete file or asked for an equality-delete update.

Iceberg has a v3 spec. We had been claiming partial support. Partial was charitable.

This chapter is the story of getting honest about what the engine actually does.

## What the matrix is

The Iceberg matrix is a public scoreboard at [icebergmatrix.org](https://icebergmatrix.org). Sixty-three cells per engine. Each cell maps to a feature of the Iceberg spec: position deletes, equality deletes, copy-on-write, merge-on-read, schema evolution, time travel, branching, partition evolution, the V3 type set (TIMESTAMP_NS, DEFAULT literals, variant, geometry, vectors), the various catalog backends, statistics, bloom filters, CDC. Three levels per cell: full, partial, none.

The matrix exists because Iceberg engines lie to each other in ways that unit tests cannot see. iceberg-rust's `TableCreation` accepts a `format_version`. The Iceberg REST `CreateTableRequest` does not have a `format_version` field. Both interfaces are correct in their own scope. The bug lives between them, in what gets serialised onto the wire.

You do not find that bug by reading either struct in isolation. You find it by writing a v3 CREATE TABLE statement, sending it to a real Polaris, and watching Polaris reject the v3 column type because the table it just created is v2.

## Where we started

Before this chapter's work, SQE sat at 99 out of 189 points on the matrix. Roughly 52%. The V2 column was almost full. The V3 column was almost empty.

| V3 cell | Level before | Note we had written |
|---|---|---|
| `table-creation:v3` | partial | "CREATE TABLE emits format-version: 3 when V3 features are used" |
| `write-insert:v3` | partial | "INSERT into V3 tables works when columns use nanosec timestamps" |
| `read-support:v3` | partial | "V3 tables produced by SQE are readable" |
| `copy-on-write:v3` | none | "V3 feature roundtrip untested" |
| `position-deletes:v3` | unknown | "No V3 integration test yet" |
| `equality-deletes:v3` | partial | "Same writer path as V2" |
| `merge-on-read:v3` | partial | "Same writer path and commit mechanism as V2" |
| `schema-evolution:v3` | partial | "ADD COLUMN with DEFAULT uses initial_default" |
| `time-travel:v3` | partial | "FOR SYSTEM_TIME AS OF works against V3 tables" |
| `cdc-support:v3` | unknown | "Library primitives shipped" |
| `statistics:v3` | none | "V3 untested" |
| `polaris:v3` | none | "V3 untested" |
| `rest-catalog:v3` | none | "V3-enabled REST catalogs untested" |

Read those notes carefully. Almost every one says the same thing in different words: "we wrote the writer path and stopped." Partial in this context did not mean "we tested the basic case and failed on edge cases." It meant "we have not pointed a real catalog at this code."

That is not a status. That is an admission.

## The first failure

I wrote eleven integration tests in `crates/sqe-coordinator/tests/v3_e2e.rs`. Each test stood up a fresh table with a V3-only column type, exercised one matrix cell, and asserted the post-state through the same TVFs (`table_files`, `table_snapshots`) the matrix evidence column already cited.

Every test was `#[ignore]` because each needs the docker-compose stack:

```bash
docker compose -f docker-compose.test.yml up -d
./scripts/bootstrap-test.sh
cargo test --package sqe-coordinator --test v3_e2e -- \
    --ignored --test-threads=1
```

I lit them up.

Eleven failures. All the same shape:

```
"Invalid schema for v2:
- Invalid type for ts: timestamp_ns is not supported until v3"
```

Polaris was creating a v2 table. Then rejecting the v3 column type because the table it had just created was v2. The engine had told Polaris to make a v3 table. Polaris had ignored that.

I went looking for where the version got dropped.

## The wire protocol

iceberg-rust has a REST catalog implementation. When `create_table()` is called, it sends a `CreateTableRequest` over HTTP. Here is what gets serialised:

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

I checked the Iceberg REST OpenAPI spec. The `CreateTableRequest` schema matches what iceberg-rust sends. No dedicated format-version field.

So how is a REST client supposed to communicate "I want format-version 3"?

Through a property. The reserved table property `format-version` is what the Iceberg ecosystem uses for this. Java's Iceberg client sets it. PyIceberg sets it. iceberg-rust constructs the local `TableMetadataBuilder` correctly from `TableCreation.format_version`, but the REST request itself drops the version on the floor and the server defaults to v2.

The fix is one line at the SQE layer:

```rust
let mut props = format_version_properties(format_version);
let table_creation = TableCreation::builder()
    .name(name.clone())
    .schema(iceberg_schema)
    .format_version(format_version)
    .properties(props)
    .build();
```

`format_version_properties` builds a single-entry HashMap mapping `format-version` to `"3"` (or `"2"` or `"1"`, matching whatever the engine decided). Polaris reads that property and creates v3 metadata. The v3 column type passes. The eleven tests turn green.

That is not unit-testable. There is no struct boundary where this bug shows up. Both sides of the wire look correct in isolation. The integration test is the only test that catches it.

## The second silent bug

While I was in the CREATE TABLE handler I noticed something else. The bloom filter test was failing:

```sql
CREATE TABLE bloom_v3 (id BIGINT, ts TIMESTAMP_NS(9))
TBLPROPERTIES ('write.parquet.bloom-filter-columns' = 'id');
```

The test asserted `SHOW CREATE TABLE` would round-trip the property. The DDL came back without it.

I went deeper. Not just the bloom property. Any TBLPROPERTIES.

```rust
let table_creation = TableCreation::builder()
    .name(name.clone())
    .schema(iceberg_schema)
    .format_version(format_version)
    .build();
```

The `ct.table_properties` and `ct.with_options` fields on the parsed AST were never read. Every TBLPROPERTIES clause that ever passed through SQE's CREATE TABLE handler had been silently dropped on the floor. The user typed it. The parser caught it. The handler ignored it.

This bug had existed since CREATE TABLE was first written. Nothing in the unit test suite would have caught it because nothing in the unit test suite asserted a property survived the round-trip through Polaris.

Six lines to fix:

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

Plus two lines in the CREATE handler to call it. The bloom property now round-trips. So does `write.delete.mode = 'merge-on-read'`. So does anything else a user sets.

While I was already in `SHOW CREATE TABLE`, I added emission of TBLPROPERTIES on read. Users who set a property can now verify the engine stored it. That sounds obvious in writing. It was not happening in code.

## The third gap

`FOR VERSION AS OF` had a different failure mode. Not silent. Loud. A runtime error every time:

```
Failed to register time-travel provider for v3_time_travel:
schema provider does not support registering tables
```

The pre-classifier was correctly stripping the clause before sqlparser-rs saw it. The engine was correctly resolving the snapshot id. The provider construction was correct. The registration step was wrong:

```rust
ctx.register_table(bare_table, Arc::new(provider))
```

`bare_table` is a name like `v3_time_travel`. The default schema in the SessionContext is the read-only Iceberg schema provider. It does not support `register_table`. It returns the error verbatim.

The `FOR INCREMENTAL` path had already solved this problem. It registers under `datafusion.public.<unique-alias>` (a writable MemoryCatalog schema) and rewrites the SQL to use the alias. The `FOR VERSION AS OF` path had been written separately and never picked up the same fix.

Five-minute change. Same pattern. Same `replace_table_reference` helper. Same writable schema. The runtime error stops, the snapshot-pinned scan returns the historical row count, and the cell flips to full.

There is a lesson in two paths solving the same problem differently. The lesson is not "write a shared helper." We did write a shared helper. The lesson is that the second writer of a similar feature needs to read the first one's commit log. They did not. I did not, before this. I noticed because the integration test surfaced the error.

## What changed

Three commits. Thirteen integration tests. Two real bugs found, one runtime gap fixed, sixteen matrix cells flipped from partial or none to full:

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

The matrix score moved from 99/189 (52.4%) to 129/189 (68.3%). One block of work. Half a day of integration test writing followed by two real fixes that no unit test would have surfaced.

## The cell that did not flip

`bloom-filters:v3` stayed partial. The write-time property now round-trips. The parquet writer respects it on the coordinator path. The worker write path still does not wire the bloom property through to its parquet writer config. That is a real gap and the matrix caveat names it directly.

The matrix is most useful when the partial entries name what is missing in the engine, not what is missing from the test suite. "Untested" is not a status. "Future work" is not a status. The exact line of code that needs to change is the status. That difference between a status board and a punch list is the difference between a marketing artifact and an engineering tool.

We are running a punch list now.

## Three rules from this work

**One: integration tests are the only tests that matter for catalog interactions.**

If you do not have a real Polaris running and a real S3 backing it, you do not know whether your engine talks to either of them correctly. The cost of the docker-compose stack is the cost of knowing. Most engineers will write integration tests last. Some will not write them at all. The matrix exists precisely because catalogs and engines lie to each other in ways unit tests cannot see.

**Two: read the wire protocol.**

`TableCreation` has a `format_version` field. `CreateTableRequest` does not. Both are correct in their own scope. The bug lives in the gap between them. You cannot find that bug by reading either struct alone.

This generalises. Every cross-process boundary is a place where state gets dropped or transformed. Local correctness does not survive serialisation. The only audit that catches this is the one that crosses the boundary.

**Three: a benchmark score and a matrix score answer different questions.**

"Passes 222 of 222 benchmark queries" answers a performance question. "129 of 189 matrix cells full" answers a capability question. Both are true. Both are partial. The benchmark suite does not exercise V3-only types or schema evolution or position deletes. The matrix does. Both numbers belong in the README.

We had been showing one and hiding the other. Score 99/189 looks bad next to "passes 222 of 222." Score 99/189 is honest about what we have not tested. We needed the honest version more than we needed the marketing version.

## What comes next

The remaining partial cells are named in the matrix with their gap. Worker bloom filters. Spark cross-engine reads (the test ships as `#[ignore]` because the Spark stack is not in the docker-compose). MERGE on V3 needs a direct test, not a transitive coverage claim through UPDATE. PostgreSQL JDBC catalog is pinned to upstream iceberg-rust adoption.

The remaining `none` cells are real gaps with real reasons:

- HMS and Glue need real implementations (the current code is config scaffolding only)
- Variant, geometry, and vector types are blocked on upstream iceberg-rust and arrow-rs work that has not landed
- Hidden partitioning on V3 needs PARTITIONED BY support in CREATE TABLE that does not exist yet

Each one has an issue link or an upstream tracker. The matrix tells you what to work on next without anyone needing to write a roadmap document. That is the highest compliment I can pay it.

The next time someone asks "is your engine production-ready," you will not need to answer with adjectives. Show them the matrix score and the benchmark score next to each other. Let the reader decide.

The only number worth quoting is the one earned by tests that ran against a real stack today.
