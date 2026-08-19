# Hive External Tables Design (cycle 1: read path and policy identity)

**Date:** 2026-08-19
**Status:** design, decisions recorded
**Scope:** cycle 1 of a five-cycle program (roadmap below)

## Goal

Query Hive-style external tables on S3-compatible storage with Athena/Glue
CREATE EXTERNAL TABLE syntax, in both embedded and server mode, without a
metastore. CSV, line-delimited JSON, and Parquet. Partitions included.

Table definitions live in a sidecar manifest under a warehouse root in object
storage, not in Polaris or Nessie. Any process that can read the data can read
the definition, so the embedded CLI, a coordinator pod, and a future worker all
resolve the same table with no new service and no shared local disk.

Storing definitions in Glue or HMS instead stays a future feature. The manifest
is shaped as Glue's `TableInput` so that becomes a field mapping rather than a
redesign.

## Roadmap

All five areas are in scope for the program. Cycle 1 is what this document
specifies; each later cycle gets its own design, plan, and MR.

| Cycle | Scope | Why in this order |
|---|---|---|
| 1 | DDL surface, manifest, warehouse root, partition discovery, credentials, LOCATION guard, tag-source routing, coordinator-local execution | The manifest format and the table's policy identity are inherited by every later cycle |
| 2 | `ALTER TABLE ADD/DROP PARTITION`, `MSCK REPAIR TABLE`, `SHOW PARTITIONS`, Athena partition projection, persisted partition index | Needs the manifest partition slots from cycle 1. Also the main fast-query unlock (see below) |
| 3 | Tag-based row filters and column masks on Hive scans, Ranger resource registration | Builds on cycle 1 identity. Cycle 1 already enforces grant-based policy and fails closed on tags |
| 4 | `INSERT INTO` and CTAS writing into partition directories | Independent of cycles 2 and 3. Carries the no-atomic-commit caveat |
| 5 | Hive scans executed on workers | `ScanTask` is Parquet-shaped and Iceberg-field-id oriented today. The protocol change deserves its own design |

## What was measured first

All probes on 2026-08-19 against `target/dev-release/sqe-cli 0.37.0`.

**1. The embedded read path already works.** DataFusion's native DDL registers,
discovers hive partitions, and prunes on a partition predicate:

```sql
CREATE EXTERNAL TABLE sales_a (id INT, amount DOUBLE, dt VARCHAR)
  STORED AS CSV PARTITIONED BY (dt) LOCATION '<dir>/hive/sales/';
SELECT * FROM sales_a;                          -- 3 rows across dt=2026-01-01, dt=2026-01-02
SELECT * FROM sales_a WHERE dt = '2026-01-02';  -- 1 row
```

`DESCRIBE` reports `dt Utf8View NOT NULL`, so the partition column is part of
the table schema. This is session-scoped and in memory. Nothing persists.

**2. Server mode has no external-table path at all.**
`crates/sqe-sql/src/classifier.rs:665` classifies any `CreateTable` without a
query as `StatementKind::CreateTable`, and
`crates/sqe-coordinator/src/query_handler.rs:1307` hands that to
`write_handler.handle_create_table`, which is the Iceberg path. The `external`
flag is never read anywhere in `sqe-sql`.

**3. Athena syntax does not parse.**

```
CREATE EXTERNAL TABLE t (id INT) PARTITIONED BY (dt string)
  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' ...
=> ParserError("Expected: end of statement or ;, found: ROW at Line: 1, Column: 87")
```

`PARTITIONED BY (dt string)` as a separate typed clause, `ROW FORMAT SERDE`,
`WITH SERDEPROPERTIES`, `STORED AS TEXTFILE`, and `TBLPROPERTIES` all fail.
DataFusion also requires partition columns to appear in the main column list,
which is the opposite of Hive and Athena.

**4. Format keywords are narrower than Athena's.** `STORED AS CSV`, `JSON`, and
`PARQUET` resolve. `STORED AS NDJSON` and `STORED AS AVRO` both fail with
`Unable to create table with format NDJSON! Could not find FileFormat`.
DataFusion's JSON format is line-delimited, so `JSON` is the NDJSON reader.

**5. LOCATION is unguarded.** This succeeded with no reachability check and no
path check:

```sql
CREATE EXTERNAL TABLE s3t (id INT) STORED AS PARQUET LOCATION 's3://nope-bucket/prefix/';
```

`crates/sqe-coordinator/src/session_context.rs:418` (CAT-01) deliberately does
not call `enable_url_table()` on the coordinator, because `ListingTableFactory`
resolves paths without ever calling `TvfPolicy::check`, which let an
authenticated user read `/etc/shadow`. The DDL path is the same bypass through a
different door.

**6. Tag fail-closed would deny every Hive query.**
`crates/sqe-policy/src/plan_rewriter.rs:226` calls `TagSource::column_tags` for
every non-reserved `TableScan`, and `None` injects `lit(false)` at line 244.
`crates/sqe-coordinator/src/tag_source_impl.rs:100` returns `None` for any table
absent from the Iceberg `TableMetadataCache`. A Hive table is never in that
cache, so with a tag source configured every Hive query returns zero rows.
There is already a precedent escape hatch for virtual schemas at
`plan_rewriter.rs:223` (`is_reserved_virtual_ref`), so the fix is routing.

**7. Partition pruning only prunes the listing in one case.**
`datafusion-catalog-listing-54.1.0/src/helpers.rs:292`
(`evaluate_partition_prefix`) builds a listing prefix from a leading run of
equality-pinned partition columns and breaks at the first unconstrained one. A
range predicate, an `IN` list, or a filter on a non-leading partition column
falls back to a full recursive LIST of the table prefix followed by in-memory
filtering. On a lake with tens of thousands of partitions that listing dominates
the query.

**8. The policy key drops the catalog.**
`crates/sqe-policy/src/plan_rewriter.rs:725` (`resolve_policy_key`) returns
`(namespace, table)` from the schema and table components only. The test at line
1522 asserts it: `TableReference::full("cat", "hr", "employees")` keys as
`("hr", "employees")`.

So a grant, row filter, or column mask written for the Iceberg table
`hr.employees` would also govern a Hive table `lake.hr.employees`. That is
privilege escalation, because the person who picks the Hive table's name is
whoever can write a manifest, and its LOCATION points wherever their credentials
reach. Cycle 1 needs an explicit resource-naming rule, recorded below.

**9. sqlparser already parses the whole Athena DDL, and two things it does not.**
sqlparser 0.62 parses the full form in both `GenericDialect` and `HiveDialect`,
so cycle 1 needs no new parser. `CreateTable` carries `external: true`,
`hive_distribution: PARTITIONED { columns }` with typed partition keys,
`hive_formats.row_format` as `SERDE { class }` or `DELIMITED { delimiters }`,
`hive_formats.serde_properties`, `hive_formats.storage` as `FileFormat` or
`IOF { input_format, output_format }`, `file_format`, `location`, and
`table_options: CreateTableOptions::TableProperties(..)`.

Two things do not parse, in either dialect:

```
... LOCATION 's3://b/p/' WITH (SECRET = 'partner')
=> Expected: end of statement, found: WITH
CREATE EXTERNAL TABLE t (c MAP<STRING,INT>) ...
=> Expected: '(', found: <
```

The first removes the need for a grammar extension: the secret goes in
`TBLPROPERTIES ('sqe.secret' = 'name')`, which parses today and is where Glue
and Athena put everything else anyway.

The second needs a correction to an earlier reading of this same probe. The
failure is `MAP<...>` specifically, not angle-bracket types as a class:
`supports_map_literal_with_angle_brackets()` is false in every dialect except
Spark's. `ARRAY<STRING>` and `STRUCT<x:INT>` both parse under `GenericDialect`
and render with their brackets intact, so they reach the extraction layer and
are rejected there by name.

The consequence for cycle 1 is the same either way, scalar columns and scalar
partition keys, but the rejection lives in two places and the error a user sees
differs: a clear SQE message for `array` and `struct`, and a raw sqlparser
message for `map`. Both are documented, and neither is silent.

**10. DataFusion's listing cache is off on purpose.**
`crates/sqe-catalog/src/lazy_object_store.rs:64` builds the runtime with
`with_list_files_cache_limit(0)`. That is the #363 fix: an infinite-TTL listing
cache served a stale file size and produced a corrupt-footer error that
self-healed only on restart. Cycle 1 does not re-enable it.

## Cycle 1 scope

In:

- `CREATE EXTERNAL TABLE` with Athena/Glue syntax, `DROP TABLE`,
  `SHOW CREATE TABLE`, `SHOW TABLES`, `information_schema`
- Sidecar manifest read and write, versioned, Glue-shaped
- `ATTACH` of a Hive warehouse root, plus TOML config for server mode
- Hive-style `k=v/` partition discovery with typed partition values
- Credential binding: session credentials by default, optional named secret
- LOCATION through the fail-closed `[storage.tvf]` allowlist, at create and at
  every load
- `CatalogRoutedTagSource` so Hive tables are not denied and not unmasked
- Size-only statistics, so planning does not read a footer per file
- A resource-naming rule that keeps Hive and Iceberg policy keys disjoint
- Coordinator-local execution in server mode

Out, by cycle:

- Explicit partition DDL, `MSCK REPAIR`, partition projection (cycle 2)
- Tag-based masks and filters, Ranger resource registration (cycle 3)
- Any write to a Hive table (cycle 4)
- Worker execution of Hive scans (cycle 5)
- ORC, Avro, SequenceFile, RCFile. Rejected by name, not silently
- Bucketed tables, skewed tables, Hive views, `STORED BY` handlers
- Hive angle-bracket nested types. Rejected by name (finding 9)

## SQL surface

Parsed in a new `crates/sqe-sql/src/hive_ddl.rs`, which produces a
`HiveTableSpec`. Both the manifest writer and the provider consume that struct.
The parse runs before DataFusion sees the SQL, in the same position where
`ATTACH` and `CREATE SECRET` are already intercepted.

```sql
CREATE EXTERNAL TABLE [IF NOT EXISTS] [catalog.][db.]name (
  col type [COMMENT 'text'], ...
)
[COMMENT 'text']
[PARTITIONED BY (col type, ...)]
[ROW FORMAT DELIMITED FIELDS TERMINATED BY 'c' [ESCAPED BY 'c'] [LINES TERMINATED BY 'c']
 | ROW FORMAT SERDE 'class' [WITH SERDEPROPERTIES ('k' = 'v', ...)]]
[STORED AS TEXTFILE | PARQUET | JSON | CSV
 | STORED AS INPUTFORMAT 'class' OUTPUTFORMAT 'class']
LOCATION 'url'
[TBLPROPERTIES ('k' = 'v', ...)]
```

There are no grammar extensions. Every clause above is Athena's own, and
sqlparser 0.62 parses all of it (finding 9), so a DDL script copied out of
Athena runs unchanged when it uses a supported format. SQE-specific settings
ride in `TBLPROPERTIES` under an `sqe.` prefix, which is where Glue and Athena
already put engine-specific settings:

| Property | Meaning |
|---|---|
| `sqe.secret` | Name of a secret to use for this table's location instead of session credentials |

An unrecognized `sqe.`-prefixed property is an error. A non-prefixed unknown
property is preserved in the manifest and ignored, because Athena DDL carries
properties (`classification`, `compressionType`, `EXTERNAL`) that are metadata
for other tools.

### SerDe allow-list

No SerDe is implemented. A closed allow-list maps a SerDe class onto one of the
three formats, and anything else is rejected with the class name in the message.

| SerDe class | Format | Honored SERDEPROPERTIES |
|---|---|---|
| `org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe` | csv | `field.delim`, `serialization.format`, `escape.delim` |
| `org.apache.hadoop.hive.serde2.OpenCSVSerde` | csv | `separatorChar`, `quoteChar`, `escapeChar` |
| `org.openx.data.jsonserde.JsonSerDe` | json | `ignore.malformed.json` |
| `org.apache.hive.hcatalog.data.JsonSerDe` | json | `ignore.malformed.json` |
| `org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe` | parquet | none |

`ROW FORMAT DELIMITED FIELDS TERMINATED BY` is sugar for `LazySimpleSerDe` with
`field.delim`.

`STORED AS TEXTFILE` maps to csv, `PARQUET` to parquet, `JSON` and `JSONFILE`
to json, and `CSV` to csv. `STORED AS INPUTFORMAT` accepts
`org.apache.hadoop.mapred.TextInputFormat` and
`org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat`, and rejects
anything else. When both a SerDe and a `STORED AS` are given and they disagree,
the SerDe wins and the mismatch is a warning, matching Hive.

An unknown key inside `WITH SERDEPROPERTIES` is an error naming the key, not a
silent drop. Silently ignoring `separatorChar` or a `mapping.*` rule changes
query results, and a wrong answer is worse than a rejected DDL.

### Semantics decisions

**CSV header is off unless asked.** Hive TEXTFILE is headerless. DataFusion's
`CsvFormat` defaults `has_header = true`, so the default is inverted at the
mapping layer. `skip.header.line.count = 1` sets `has_header = true`, `0` sets
it false, and any value above 1 is an error, because DataFusion expresses the
setting as a boolean and a silent off-by-N row count is not acceptable.

**No delimiter means `\001`.** `LazySimpleSerDe` with no `field.delim` uses
ctrl-A, as Hive does. Athena users who wrote comma-separated data without
declaring a delimiter get a one-column table, exactly as in Athena. Documented
with the fix (`FIELDS TERMINATED BY ','`).

**Column binding differs per format.** CSV binds by ordinal, and the declared
column list is authoritative. Parquet and JSON bind by name, case-insensitively.
Name-matching a positional format is the same defect class as the field-id scan
bug fixed in !784, where a projection matched by Parquet name and returned
another column's values after a rename.

**Partition columns are declared separately and land last.** They must not
appear in the main column list, as in Hive and Athena. The rewrite appends them
to the DataFusion column list and passes them as `table_partition_cols`, so the
resulting Arrow schema has data columns first and partition columns after, which
is Hive's column order.

**Partition values are typed from the path.** `string`, `varchar`, `int`,
`bigint`, and `date` are supported partition key types. A path segment that does
not parse to the declared type makes that partition unreadable and raises an
error naming the segment, rather than being silently skipped.
`__HIVE_DEFAULT_PARTITION__` normalizes to NULL.

**Identifiers fold to lowercase**, consistent with the decision recorded for
issue #337 and with Hive and Glue, which lowercase all table and column names.
Quoted identifiers keep their case in the manifest and are matched
case-insensitively for Parquet and JSON binding.

### Deviations from Athena, stated

1. `OpenCSVSerde` in Athena types every column as string. SQE honors the
   declared column types and casts. Athena's behavior is a workaround for its
   own SerDe, not a semantic anyone wants.
2. Hive angle-bracket nested types (`array<string>`, `map<string,int>`,
   `struct<x:int>`) are rejected with an error naming the type, because
   sqlparser parses neither form (finding 9). Cycle 1 is scalar columns and
   scalar partition keys. A pre-parse type rewrite is cycle-2 work, and the
   error says so.
3. `projection.*` partition projection properties are rejected in cycle 1 with a
   message pointing at cycle 2, rather than being ignored. Ignoring them would
   silently produce a full table scan where the user asked for a computed
   partition set.
4. `has_encrypted_data`, `STORED BY`, bucketing, and skew clauses are rejected.
5. `DROP TABLE ... PURGE` is rejected. `DROP TABLE` removes the manifest and
   never touches data, so there is no purge semantics to offer yet.

## Manifest format

One object per table, under the warehouse root. The data lives wherever
`storage_descriptor.location` points, which may be a different bucket.

```
<root>/<database>/<table>/_sqe/table.json
```

```json
{
  "manifest_version": 1,
  "table_type": "EXTERNAL_TABLE",
  "database_name": "sales",
  "name": "orders",
  "owner": "jacob",
  "created_at": "2026-08-19T10:00:00Z",
  "comment": null,
  "partition_keys": [ { "name": "dt", "type": "string", "comment": null } ],
  "parameters": {
    "skip.header.line.count": "1",
    "classification": "csv",
    "sqe.secret": "partner"
  },
  "storage_descriptor": {
    "location": "s3://raw/orders/",
    "input_format": "org.apache.hadoop.mapred.TextInputFormat",
    "output_format": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
    "compressed": false,
    "serde_info": {
      "serialization_library": "org.apache.hadoop.hive.serde2.OpenCSVSerde",
      "parameters": { "separatorChar": "," }
    },
    "columns": [
      { "name": "id", "type": "bigint", "comment": null },
      { "name": "amount", "type": "double", "comment": "in cents" }
    ],
    "sort_columns": []
  },
  "sqe": {
    "secret": "partner",
    "column_tags": {},
    "partition_index": null,
    "statistics": null
  }
}
```

Field names above the `sqe` key mirror Glue's `TableInput` and
`StorageDescriptor`. A future Glue or HMS backend maps the top level onto
`TableInput` and the `sqe` block onto Glue table `Parameters`, so no cycle has
to reshape the format.

Types are stored as SQL type text (`bigint`, `varchar(32)`, `decimal(18,2)`,
`array<string>`) and parsed to Arrow on load. The manifest does not pin an Arrow
or DataFusion version.

Slots reserved now and populated later: `sqe.partition_index` (cycle 2),
`sqe.column_tags` (cycle 3), `sqe.statistics` (cycle 2 `ANALYZE`). A manifest in
object storage read by mixed engine versions is a wire format, so it is
versioned from its first write.

### The manifest is untrusted input

Anyone who can write to the warehouse prefix can plant a manifest. The loader
treats it as a network payload, not as engine-authored state:

- JSON schema validated. Unknown `manifest_version` above the known maximum is
  rejected, not parsed best-effort. Unknown fields inside a known version are
  ignored so a newer writer stays readable.
- Caps: object size, column count, partition-key count, partition-index entry
  count. Each cap is a config value with a documented default.
- `serde_info.serialization_library` checked against the allow-list on load, not
  only at create.
- `storage_descriptor.location` re-validated through
  `StorageConfig::tvf.check_path` on **every load**. A planted manifest pointing
  at `file:///var/run/secrets/kubernetes.io/serviceaccount/token` hits the same
  fail-closed guard the file TVFs use. `file://` locations are rejected outright
  on the coordinator and allowed in embedded mode only when
  `allow_local_paths` is set, matching current TVF behavior.

### Concurrency

One object per table, so the blast radius of a conflict is one table. `CREATE`
uses a conditional put (if-none-match) where the store supports it and
`IF NOT EXISTS` semantics come from that. Where the store does not support
conditional put, `CREATE` is a read-then-write with documented last-writer-wins,
and the doc says so plainly rather than implying atomicity.

## Warehouse root

A root is attached, not implied. `TYPE hive` is already bound to HMS-Iceberg at
`crates/sqe-sql/src/attach.rs:64`, so the new kind is `hive_external`:

```sql
ATTACH 'lake' (TYPE hive_external, LOCATION 's3://bucket/wh/');
```

Server mode also accepts roots from TOML so a deployment has them at startup
without a per-session `ATTACH`. Embedded mode accepts a `--hive-root NAME=URL`
flag, repeatable, matching the existing `--catalog NAME=PATH` shape.

Databases are the first prefix level under the root, so `SHOW SCHEMAS` is a
delimiter-scoped LIST and `SHOW TABLES` is a LIST of one database prefix plus a
GET per manifest. `ATTACH` validates the root through `check_path` like any
other location.

`build_catalog` in `crates/sqe-catalog/src/mount.rs` returns
`Arc<dyn iceberg::Catalog>`, which a Hive root is not. A Hive root registers a
DataFusion `CatalogProvider` directly, through a sibling dispatch in front of
`build_catalog` rather than by widening its return type.

## Providers and execution

New module `crates/sqe-catalog/src/hive/`:

| File | Responsibility |
|---|---|
| `manifest.rs` | Serde types, validation, caps, read and write, versioning |
| `provider.rs` | `HiveCatalogProvider`, `HiveSchemaProvider`, table construction |
| `serde_map.rs` | SerDe and `STORED AS` allow-list, `ListingOptions` mapping |
| `partition.rs` | Partition discovery, typing, TTL cache, pruning helpers |
| `stats.rs` | Statistics from object sizes and Parquet footers |

`HiveSchemaProvider::table(name)` loads the manifest through the metadata cache,
maps the SerDe to a `FileFormat`, builds `ListingOptions` with
`table_partition_cols`, binds an object store from session credentials or the
named secret, and returns a `ListingTable`. `table_names()` comes from the
cached LIST.

Embedded mode intercepts `StatementKind::CreateExternalTable` in
`EmbeddedClient::execute` alongside the existing `ATTACH` and `CREATE SECRET`
interception, before `ctx.sql()`. Server mode adds a `hive_ddl` handler in the
coordinator, so the statement never reaches `write_handler.handle_create_table`
and its Iceberg path. Both call the same module.

In server mode, Hive scans execute coordinator-local, as the file TVFs do today.
Server mode does not mean distributed scan parity, and the docs say so until
cycle 5.

## Statement dispatch

Every handler that resolves a table name now has a Hive root to consider. The
branch lives in one place, a `resolve_table_kind(table_ref)` helper in the
coordinator that answers Iceberg or Hive from the registered catalogs, so no
handler grows its own detection.

| Statement | Cycle 1 behavior |
|---|---|
| `CREATE EXTERNAL TABLE` | Supported. Routed to the Hive DDL handler |
| `DROP TABLE` | Supported. Deletes the manifest, never the data. `PURGE` rejected |
| `SHOW TABLES`, `SHOW SCHEMAS`, `information_schema` | Supported through the provider |
| `SHOW CREATE TABLE` | Supported. Renders Athena-compatible DDL from the manifest |
| `DESCRIBE`, `SHOW COLUMNS` | Supported through the provider schema |
| `SHOW STATS`, `ANALYZE` | Rejected, naming cycle 2 |
| `ALTER TABLE ADD/DROP PARTITION`, `MSCK REPAIR`, `SHOW PARTITIONS` | Rejected, naming cycle 2 |
| `SET TAGS`, `SET MASKING POLICY` | Rejected, naming cycle 3 |
| `INSERT`, CTAS, `MERGE`, `UPDATE`, `DELETE`, `TRUNCATE`, `INSERT OVERWRITE` | Rejected, naming cycle 4 |
| `ALTER TABLE` rename, column add/drop/rename, `DROP NESTED COLUMN`, `COMMENT ON`, `ALTER TABLE SET TBLPROPERTIES` | Rejected. Schema evolution on an external table is a manifest edit, and it is not cycle-1 work |
| `PARTITION EVOLUTION`, `RefDdl` (branches and tags), time travel | Rejected. These are Iceberg snapshot features with no Hive equivalent |

Every rejection names `hive_external` and the cycle that will support it, so the
message tells an operator what to do rather than only what failed. Each row is a
test case, which is how the list stays true as the cycles land.

## Credentials

Session credentials by default, so a Hive read runs as the calling user and the
no-service-account model holds. `TBLPROPERTIES ('sqe.secret' = 'name')` names a
secret for buckets outside the user's token scope. The manifest stores the secret **name**
only; material is resolved per session against the existing `SecretStore`, and a
missing secret is an error at query time naming the secret.

A secret referenced in a manifest is not an authorization decision. The
allowlist and the policy layer still apply, so a secret cannot be used to reach
a location the deployment forbids.

## Policy, tags, audit

### Resource naming

Finding 8 rules out relying on the existing key as-is. The rule for cycle 1:

**A Hive table's policy namespace is `<root>.<database>`, and an Iceberg table's
stays the dotted Iceberg namespace.** The Hive root name is part of the key; the
Iceberg catalog name still is not. So `lake.sales.orders` keys as
`("lake.sales", "orders")` and no grant written for Iceberg `sales` can reach it.

This is deliberately asymmetric, because making the catalog part of the key for
every table kind would invalidate every Ranger policy already written against
the `hive_database` convention. That change is worth doing and is not cycle-1
work.

Two guards come with the rule:

- `ATTACH ... TYPE hive_external` rejects a root name that collides with a
  registered catalog name.
- The residual collision is an Iceberg multi-level namespace whose dotted form
  equals `<root>.<database>`, for example an Iceberg namespace literally named
  `lake.sales`. Cycle 1 does not detect that. It is recorded as a known
  limitation, the mitigation is the root-naming convention, and the permanent
  fix is catalog-in-key for all table kinds.

The change is confined to `resolve_policy_key`, which gains a branch that
prefixes the root name when the reference names a Hive catalog. The rewriter's
enforcement logic is untouched: it resolves whatever key that function returns,
so grant-based row filters and column masks apply to Hive tables through the
existing path. A test pins the Iceberg key so the branch cannot change it.

### Tag routing


A `CatalogRoutedTagSource` in the coordinator is required, not optional. It
dispatches on catalog name: Iceberg catalogs to the existing `CacheTagSource`,
attached Hive roots to a new `HiveTagSource` that reads `sqe.column_tags` from
the cached manifest. Without it, finding 6 above applies and every Hive query
returns zero rows.

`HiveTagSource` returns `Some(map)` on a manifest cache hit, including
`Some(empty)` for the untagged cycle-1 case, and `None` on a cache miss or an
unreadable manifest, which fails closed exactly as the Iceberg source does. It
does not invent a third state.

Routing defaults matter as much as the routes. An unknown or unmatched catalog
name routes to the Iceberg `CacheTagSource`, which fails closed. It never routes
to `Some(empty)`. A permissive default in a dispatcher is a silent mask bypass,
and the dispatcher is the one place where adding a catalog kind later could
introduce one.

`TagSource::column_tags` is synchronous, so `HiveTagSource` can only answer from
cache. The invariant it depends on: plan rewriting runs after LogicalPlan
construction, which has already forced `HiveSchemaProvider::table()` and
therefore the manifest load. That ordering is load-bearing, so it is asserted by
a test rather than left as a comment.

Audit events are emitted from the Hive DDL handler.
`crates/sqe-coordinator/src/audit_resources.rs:80` only sees DataFusion
`CreateExternalTable` logical plans, and the server path does not produce one.
The resource name is the same three-part name policy uses, so audit and policy
agree on identity.

## Fast query

| Lever | Cycle 1 | Later |
|---|---|---|
| Partition listing | No caching. Every query lists, subject to the leading-equality prefix optimization from finding 7. Finding 10 rules out re-enabling DataFusion's listing cache | Cycle 2 is the listing cycle: `sqe.partition_index` persisted in the manifest, invalidated by manifest ETag rather than by a timer, plus partition projection so a cold read lists nothing |
| Parquet pruning | Row-group pruning from footer statistics at execution time, footers reused through the existing `footer_cache.rs` | |
| Scan parallelism | Parquet split by row group using the existing `task_split_target_size`; CSV and JSON parallelized across files up to `target_partitions` | Cycle 5 moves scans onto workers |
| Optimizer statistics | Size-only: cardinality estimated from file count and bytes, with `ListingOptions::with_collect_stat(false)`. Collecting real statistics reads a Parquet footer per file at plan time, which is the latency this row exists to avoid | Cycle 2 `ANALYZE TABLE` persists real counts into `sqe.statistics`, so planning gets accurate without paying per-query |
| Pushdown | Parquet gets projection and predicate pushdown. CSV and JSON get projection only, filters above the scan | |

Cycle 1 delegates file listing to `ListingTable`. Cycle 2 is where the provider
starts building the file list itself from the partition index, because that is
what an index is for. Splitting it that way keeps cycle 1's provider small and
keeps the invalidation question, which is what #363 was really about, inside the
cycle that introduces cached state.

Statistics are not optional polish. `JoinStrategyRule` and
`star_schema_reorder` plan from statistics, and the SMJ regression in !724 showed
what happens when they widen to Unknown. A Hive table with no statistics is
exactly that case, so cycle 1 supplies size-derived estimates.

Compressed CSV and JSON are not splittable, so one file is one thread. The docs
state the file-sizing guidance rather than leaving users to discover it from a
flat profile.

Benchmark plan: a partitioned fixture in CSV and Parquet, measured cold and warm
cache, with and without a partition-pinning predicate, so the listing cost from
finding 7 is visible as its own number.

## Error handling

Every rejection names the thing rejected: the SerDe class, the format keyword,
the SERDEPROPERTIES key, the TBLPROPERTIES value, the manifest field, the object
path. A manifest failure reports the object path, because the operator's next
step is to look at that object.

Errors are typed through the existing `SqeError` variants. A missing manifest is
a table-not-found, an unreadable manifest is a config error, a location failing
the allowlist is a permission error, and a missing secret is a config error
naming the secret.

## Testing

TDD per the repo rules. Test first, then implementation.

- `sqe-sql`: every DDL form in the grammar above, each SerDe mapping, each
  rejection path, and lowercase folding. Table-driven.
- `sqe-catalog` manifest: round-trip, unknown version rejected, unknown field
  inside a known version ignored, each cap enforced, disallowed SerDe class on
  load, `file://` and metadata-endpoint locations rejected on load.
- `sqe-catalog` read: local-filesystem fixtures for partitioned CSV, JSON, and
  Parquet. Header on and off, ordinal binding for CSV, name binding for Parquet
  after a column reorder, typed partition values, `__HIVE_DEFAULT_PARTITION__`,
  and partition pruning with and without an equality-pinned leading column.
- `sqe-coordinator`: a Hive query is not denied with a tag source active; a
  manifest cache miss does deny; an unmatched catalog name routes to the
  fail-closed Iceberg source; a grant written for Iceberg `hr.employees` does
  NOT govern Hive `lake.hr.employees` (finding 8); a grant written for
  `lake.hr` does; the manifest is in cache by the time the rewriter runs; an
  audit event is emitted with the same name policy used.
- `sqe-coordinator` dispatch: one case per row of the statement dispatch table,
  asserting support or a rejection naming `hive_external` and its cycle.
- Integration: a MinIO or RustFS-backed root through `make test-integration`,
  covering `ATTACH`, `CREATE EXTERNAL TABLE`, `SHOW TABLES`, a partitioned read,
  and `DROP TABLE` leaving the data in place.

## Rollback

The feature is additive and gated. A `hive-external` cargo feature gates the
module, named to avoid confusion with the existing `hms` feature, which is Hive
Metastore for Iceberg tables. It is default-on, because the CSV, JSON, and
Parquet readers it uses are already compiled for the file TVFs, so its binary
cost is roughly zero, like `hadoop`. No Hive root attached means no behavior
change on any existing path. The one
change to shared code is the tag-source routing, which is a dispatch in front of
the existing `CacheTagSource` and keeps the Iceberg behavior byte-identical when
no Hive catalog is registered. That routing is covered by a test asserting the
Iceberg path still fails closed on a cache miss.

Rollback is removing the root from config or `DETACH`, which leaves manifests in
object storage as inert JSON.
