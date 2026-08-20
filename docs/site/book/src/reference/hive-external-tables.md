# Hive External Tables

SQE reads Hive-style external tables on S3-compatible storage with Athena and Glue `CREATE EXTERNAL TABLE` syntax. No metastore is involved. The table definition lives in a small JSON manifest next to the data, in a warehouse root you attach. Any process that can read that root, the embedded CLI, a coordinator, a future worker, resolves the same table.

Cycle 1 is the read path: attach a root, create and drop tables, read CSV, line-delimited JSON, and Parquet, with Hive-style partitions. It is the first of five planned cycles. Four things are explicitly out of scope and covered at the end: explicit partition DDL, tag-based masks and filters, writes, and worker-side execution.

There are no SQE grammar extensions. Every clause below is Athena's own. SQE-specific settings ride inside `TBLPROPERTIES` under an `sqe.` prefix, which is where Glue and Athena put engine-specific settings anyway. Today there is exactly one: `sqe.secret`.

## Attaching a warehouse root

A root is attached, not implied. Three ways, matching the shapes SQE already uses for catalogs and secrets.

**SQL, session-scoped:**

```sql
ATTACH 's3://bucket/wh/' AS lake (TYPE hive_external);
```

**Config, at coordinator startup:**

```toml
[hive]
manifest_max_bytes = 1048576
manifest_max_columns = 4096
manifest_max_partition_keys = 32

[[hive.roots]]
name     = "lake"
location = "s3://bucket/wh/"
```

**Embedded CLI, repeatable flag:**

```bash
sqe-cli --embedded --hive-root lake=s3://bucket/wh/
```

A root name cannot collide with a registered catalog name; `ATTACH` rejects the collision. `TYPE hive` still means Hive Metastore for Iceberg tables. `TYPE hive_external` is a different thing: a warehouse root for tables that are not Iceberg at all.

Attaching a root does not list or read anything. The check is a path-reachability check against the storage allowlist described below, not a network call against the root's contents.

## Creating a table

```sql
CREATE EXTERNAL TABLE lake.sales.orders (
  id BIGINT,
  amount DOUBLE
)
PARTITIONED BY (dt STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',')
STORED AS TEXTFILE
LOCATION 's3://raw/orders/'
TBLPROPERTIES ('skip.header.line.count' = '1', 'sqe.secret' = 'partner');
```

The DDL above is copied from Athena's own syntax and parses unchanged. Every column and property is real: `id` and `amount` are the data columns, `dt` is a partition column declared separately and never repeated in the column list, and the table reads from `s3://raw/orders/` using a secret named `partner` instead of the coordinator's own storage credentials.

A statement that fails to classify as `CREATE EXTERNAL TABLE` is an error naming the problem, never a silent fall-through to a different table kind.

### The one clause a lot of copied Athena DDL gets wrong

`STORED AS CSV` and `STORED AS JSON` do not parse. sqlparser's format keywords are exactly `TEXTFILE`, `SEQUENCEFILE`, `ORC`, `PARQUET`, `AVRO`, `RCFILE`, and `JSONFILE`. For a comma-separated file the working form is:

```sql
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
```

Bare `TEXTFILE` on its own means Hive's control-A delimiter (`\001`), not comma. If your data has no `FIELDS TERMINATED BY` clause and no `ROW FORMAT SERDE`, expect a one-column table, exactly as you would in Athena. For JSON, the canonical Athena form that parses is `ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'`; `STORED AS JSONFILE` also works and resolves to the same reader.

## What you can actually read

CSV, line-delimited JSON, and Parquet.

ORC, Avro, SequenceFile, and RCFile all parse in the DDL and are recorded faithfully in the manifest, with their real Hadoop input and output format classes, so `SHOW CREATE TABLE` renders a truthful DDL for a table this cycle cannot read. They are rejected at read time by class name, naming the format, not silently dropped from the manifest.

### SerDe allow-list

Five SerDe classes are supported. Anything else is rejected with the class name in the error.

| SerDe class | Format | Honored SERDEPROPERTIES |
|---|---|---|
| `org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe` | CSV | `field.delim`, `serialization.format`, `escape.delim` |
| `org.apache.hadoop.hive.serde2.OpenCSVSerde` | CSV | `separatorChar`, `quoteChar`, `escapeChar` |
| `org.openx.data.jsonserde.JsonSerDe` | JSON | `ignore.malformed.json` |
| `org.apache.hive.hcatalog.data.JsonSerDe` | JSON | `ignore.malformed.json` |
| `org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe` | Parquet | none |

`ROW FORMAT DELIMITED FIELDS TERMINATED BY` is sugar for `LazySimpleSerDe` with `field.delim` set. `STORED AS TEXTFILE` maps to CSV, `PARQUET` to Parquet, `JSONFILE` to JSON. When a DDL gives both a `ROW FORMAT SERDE` and a `STORED AS`, and they disagree, the SerDe wins and the mismatch is a warning, matching Hive.

An unknown key inside `WITH SERDEPROPERTIES` is an error naming the key. Silently ignoring `separatorChar` changes query results, and a wrong answer is worse than a rejected DDL.

## Four semantics decisions

**CSV header is off unless asked.** Hive `TEXTFILE` is headerless by convention, the opposite of DataFusion's own CSV default. `skip.header.line.count = 1` turns the header on, `0` leaves it off, and any value above `1` is an error: DataFusion expresses this as a boolean, so an off-by-N row count would be silent otherwise.

**Column binding differs by format.** CSV binds by position, and the declared column list is authoritative. Parquet and JSON bind by name, case-insensitively. Matching a positional format by name, or a named format by position, is the same defect class fixed in the small-file scan bug (!784), where a projection matched by Parquet column name and returned another column's values after a rename.

**Partition columns land last.** They are declared in their own `PARTITIONED BY` clause, never in the main column list, exactly as Hive requires. The resulting Arrow schema has data columns first and partition columns after.

**`__HIVE_DEFAULT_PARTITION__` reads as NULL.** Consistently, in a projection, in a filter, and in a `GROUP BY`. An earlier cut of this feature let a filter treat the sentinel as NULL while a plain `SELECT` still returned the literal string, which would have made `WHERE dt IS NULL` and `SELECT dt` disagree about the same row. Both now agree.

Partition values are typed from the path: `string`, `varchar`, `int`, `bigint`, and `date` are supported. A path segment that does not parse to the declared type makes that partition unreadable with an error naming the segment, rather than being silently skipped.

Octal escapes work in delimiters, so `FIELDS TERMINATED BY '\001'` behaves exactly like the no-delimiter default. A delimiter byte above 127 is rejected with an explanation: Hive's own defaults and everything a real deployment picks (control characters, printable ASCII) sit inside the safe range, and a byte above it cannot be represented as one UTF-8 byte in the property's string storage.

## Five deviations from Athena

1. **`OpenCSVSerde` types every column as string in Athena.** SQE honors your declared column types and casts. Athena's all-string behavior is a workaround for its own SerDe, not a semantic anyone actually wants.
2. **Hive angle-bracket nested types are rejected.** `array<string>`, `struct<x:int>`, and `map<string,int>` all fail. `array` and `struct` parse and are rejected by name with the column named in the error; `map` fails inside the parser itself, before SQE ever sees it, because sqlparser only supports Spark's angle-bracket map literal. Cycle 1 is scalar columns and scalar partition keys only.
3. **`projection.*` partition-projection properties are rejected**, not ignored, with a message pointing at cycle 2. Ignoring them would silently turn a computed partition set into a full table scan.
4. **`has_encrypted_data`, `STORED BY`, bucketing, and skew clauses are rejected.**
5. **`DROP TABLE ... PURGE` is rejected.** `DROP TABLE` deletes the manifest and never touches data, so there is no purge semantics to offer yet.

## What DROP TABLE does

`DROP TABLE` deletes the sidecar manifest. It never touches the underlying data, regardless of `PURGE`, which is rejected outright rather than silently ignored: an external table's `DROP` has nothing to purge.

## Credentials, plainly

Reads authenticate to object storage with the coordinator's own storage credentials, or with a named secret from `TBLPROPERTIES ('sqe.secret' = '...')`. Not with the calling user's credentials.

The credential model is a real difference from how SQE treats Iceberg tables. An Iceberg read gets per-user credentials because Polaris vends them per table against the caller's bearer token. Nothing plays that role for an arbitrary `s3://raw/orders/` prefix: there is no catalog managing it, so there is no per-user credential to vend. The engine has no mechanism to build an S3 client for a raw prefix scoped to one user, and building one is a project of its own, not a cycle-1 task. The file table-valued functions (`read_parquet`, `read_csv`, `read_json`) already work exactly this way.

Authorization still runs per user, in two places that do not depend on the credential:

- The `[storage.tvf]` prefix allowlist. `allowed_object_store_prefixes` supports `{user}` substitution, and it is evaluated with the calling session's identity on every manifest load, not only at `CREATE EXTERNAL TABLE` time.
- The policy layer. Grants, row filters, and column masks apply to a Hive table the same way they apply to an Iceberg one (see the resource-naming rule below). Cycle 1 enforces grant-based policy; tag-based row filters and masks land in cycle 3.

Put plainly: for Iceberg tables SQE has no service account. For Hive external tables it effectively has one, bounded by a per-user prefix allowlist and per-user policy. If that distinction matters to your deployment, keep Hive roots out of buckets that are not uniformly readable by the engine's own identity. A secret named in a manifest does not widen this: the allowlist and the policy layer still apply on top of it, so a secret cannot reach a location the deployment has not already allowed.

## Resource naming, and why it matters

A Hive table's policy namespace is `<root>.<database>`, not the database alone. `lake.sales.orders` keys as `("lake.sales", "orders")`. An Iceberg table keeps its plain dotted namespace, with no catalog prefix, unchanged from before this feature existed.

The asymmetry is deliberate. Without it, a grant written for Iceberg `sales.employees` would also govern a Hive table someone else named `lake.sales.employees`, and the person who names a Hive table is whoever can write a manifest, pointed wherever their credentials reach. That is privilege escalation through naming, not a hypothetical. `ATTACH ... TYPE hive_external` refuses a root name that collides with a registered catalog name to keep this rule enforceable. The one case it does not detect is an Iceberg namespace whose own dotted name happens to equal `<root>.<database>` (for example, an Iceberg namespace literally named `lake.sales`); avoiding that naming collision is on you until a later cycle makes the catalog part of every table kind's key.

## Performance, plainly

**Cycle 1 lists on every query.** There is no persisted partition index yet. What prunes the listing today is pinning the leading partition columns with equality: `WHERE dt = '2026-01-02'` narrows the listing to that prefix. A range predicate, an `IN` list, or a filter on a partition column that is not the leading one falls back to listing the entire table prefix and filtering in memory. On a table with tens of thousands of partitions, that listing dominates the query.

**Compressed CSV and JSON are one file per thread.** Neither format is splittable once compressed, so a single large compressed file runs on a single thread regardless of how many cores are available. Size files with that in mind: many moderately sized files parallelize; one giant file does not, no matter the partition count.

Parquet gets real row-group pruning from footer statistics and splits by row group for parallelism. CSV and JSON get file-level parallelism up to `target_partitions`, and only projection pushdown, not predicate pushdown; filters run above the scan.

Cycle 1 uses size-derived statistics (file count and byte size), not a Parquet footer read per file at plan time, so a join against a Hive table does not plan blind while staying cheap to plan. Cycle 2's `ANALYZE TABLE` replaces this with persisted real counts.

Cycle 2 removes the listing cost by persisting the partition list itself in the manifest, maintained by `MSCK REPAIR TABLE` (full re-list) and `ALTER TABLE ... ADD PARTITION` (one entry, no re-scan).

## Server mode

In server mode, Hive scans execute coordinator-local, the same way the file table-valued functions do today. Coordinator-local execution is not distributed scan parity. Worker execution of Hive scans is cycle 5, because `ScanTask` today is shaped for Parquet and Iceberg field IDs, and giving it a Hive-shaped alternative is its own protocol change.

## Statement support today

| Statement | Cycle 1 behavior |
|---|---|
| `CREATE EXTERNAL TABLE` | Supported |
| `DROP TABLE` | Supported. Deletes the manifest, never the data. `PURGE` rejected |
| `SHOW TABLES FROM <root>.<database>`, `DESCRIBE`, `SHOW COLUMNS`, `information_schema` | Supported. `SHOW TABLES` and `SHOW CREATE TABLE` are policy-gated: a fully-denied table is omitted/errors like a missing one, and a restricted column is dropped from `SHOW CREATE TABLE`'s rendered DDL. `information_schema` itself is not policy-filtered for a Hive root, same as it is not for an Iceberg catalog: this is pre-existing parity, not a Hive-specific gap |
| `SHOW CREATE TABLE` | Supported. Renders Athena-compatible DDL from the manifest |
| `SHOW SCHEMAS`, `SHOW SCHEMAS IN <root>` | `SHOW SCHEMAS` is not parsed at all in embedded mode, against a Hive root or an Iceberg catalog alike: this is a pre-existing embedded-mode gap, not something this feature introduces. In server mode it works for Iceberg catalogs, but has no dedicated path for a Hive root yet and fails with a generic catalog error. `SELECT catalog_name, schema_name FROM information_schema.schemata WHERE catalog_name = '<root>'` works in both modes and lists the root's databases; use that, or `SHOW TABLES FROM <root>.<database>` against a database you already know |
| `ANALYZE` | Rejected, naming cycle 2 |
| `ALTER TABLE ... ADD PARTITION` | Rejected, naming cycle 2 |
| `MSCK REPAIR TABLE`, `SHOW PARTITIONS` | Rejected with a generic not-implemented error. These statements do not yet reach SQE's own statement classifier at all, so the message does not yet name `hive_external` or a cycle. Cycle 2 adds the classifier arms when it implements the statements, and the message becomes specific for free |
| `ALTER TABLE ... SET TAGS`, masking policy statements | Rejected, naming cycle 3 |
| `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE` | Rejected, naming cycle 4 |
| `ALTER TABLE` rename, column add/drop/rename, `COMMENT ON` | Rejected outright. Schema evolution on an external table is a manifest edit, not scheduled work |
| Partition evolution, branches, tags, time travel | Rejected outright. These are Iceberg snapshot features with no Hive equivalent |

Every rejection names `hive_external` and, where one is planned, the cycle that adds support, so the error tells you what to do rather than only what failed. Two statements are absent from this table on purpose. `CREATE EXTERNAL TABLE ... AS SELECT` cannot reach any handler at all, Hive-aware or not: sqlparser's own grammar for `external: true` has no `AS SELECT` path in any dialect, so there is nothing to name. A plain `CREATE TABLE lake.x.y AS SELECT` (no `EXTERNAL`) against a Hive name does parse and does reach the ordinary Iceberg `CreateTable` dispatch, which carries no Hive-aware check, so it does not fail with a `hive_external` message; and `MERGE` against a Hive target is not yet gated by a Hive-aware check either. Neither path has been run to confirm what error surfaces instead. Both are writes, so both land in cycle 4 regardless of which error they produce today.

## Known limitations, and what to do about them

**Every query lists the partition directory unless the leading partition columns are equality-pinned.** On a large partition count this dominates query time. Restructure filters to pin the leading partition columns with `=` where you can, and expect the listing cost until cycle 2's partition index ships.

**A compressed CSV or JSON file is one thread, however many cores you have.** Split large compressed files at write time rather than relying on in-engine parallelism to compensate.

**Reads use engine credentials or a named secret, never the caller's own.** Do not point a Hive root at a bucket your users should not uniformly be able to read through the engine's identity. Use the `[storage.tvf]` allowlist and grants to bound exposure per user; do not rely on the credential itself to do that job.

**`array`, `struct`, and `map` columns are not supported.** A table with a nested column needs those columns projected out at the source or flattened before SQE reads it. There is no workaround inside SQE for cycle 1.

**No `MSCK REPAIR TABLE` or `SHOW PARTITIONS` yet.** Both fail with a generic not-implemented error rather than one naming `hive_external`, because neither statement reaches SQE's own statement classifier at all. A table with partitions added out of band from `CREATE EXTERNAL TABLE` still reads correctly through directory listing; there is no persisted partition catalog to repair or show. Cycle 2 changes that, and the error becomes specific at the same time.

**`SHOW SCHEMAS` does not list a Hive root's databases.** This is not a Hive-specific gap: `SHOW SCHEMAS` is not parsed in embedded mode at all, against any catalog kind, and in server mode it has no dedicated Hive path yet, so it fails with a generic catalog error against a Hive root. `SELECT catalog_name, schema_name FROM information_schema.schemata WHERE catalog_name = '<root>'` works in both modes and is the reliable way to list a root's databases; `SHOW TABLES FROM <root>.<database>` works too once you know the database name.

**Server mode runs Hive scans on the coordinator, not on workers.** A distributed deployment gets no scan parallelism across workers for a Hive table today. Size the coordinator accordingly, or wait for cycle 5.

**A grant against an Iceberg namespace never governs a same-named Hive database, and vice versa.** A prior draft of this page understated the actual exposure here: the policy engine and the tag route both key off the table reference's catalog component, and a two-part reference (`sales.orders`, no catalog) carries no catalog component at all. Attaching a Hive root under the SAME name as the session's default catalog would have let an unqualified reference reach that root while policy resolution still took the Iceberg branch. SQE now refuses to attach a Hive root under that name, at both `[[hive.roots]]` startup and `ATTACH`, so the collision cannot occur. One narrower corner case remains open: an Iceberg namespace whose dotted name literally equals `<root>.<database>` still produces the same policy key as a Hive table in that root's database. Name your Hive roots to avoid that collision until a later cycle closes it structurally.

## What is next

Cycle 2 adds explicit partition DDL (`ALTER TABLE ... ADD/DROP PARTITION`, `MSCK REPAIR TABLE`, `SHOW PARTITIONS`), Athena-style partition projection, and the persisted partition index that removes the per-query listing cost described above. Cycle 3 adds tag-based row filters and column masks on Hive scans. Cycle 4 adds writes (`INSERT INTO`, CTAS) into partition directories. Cycle 5 moves Hive scan execution onto workers.
