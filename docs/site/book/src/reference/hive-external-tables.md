# Hive External Tables

SQE reads Hive-style external tables on S3-compatible storage with Athena and Glue `CREATE EXTERNAL TABLE` syntax. No metastore is involved. The table definition lives in a small JSON manifest next to the data, in a warehouse root you attach. Any process that can read that root, the embedded CLI, a coordinator, a future worker, resolves the same table.

Cycle 2 is the partition-index path: `MSCK REPAIR TABLE`, `ALTER TABLE ... ADD/DROP PARTITION`, `SHOW PARTITIONS`, Athena partition projection, and `ANALYZE TABLE`. Cycle 1 remains the read path underneath. Three things stay out of scope and are covered at the end: tag-based masks and filters, writes, and worker-side execution.

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
metadata_recheck_secs = 10   # serve a cached manifest this long before asking the metastore again
metadata_ttl_secs = 60       # Glue only (no change token): refetch a manifest after this long
metadata_timeout_secs = 10   # cut a metastore call here; serve the last known manifest if there is one

[[hive.roots]]
name     = "lake"
location = "s3://bucket/wh/"
```

**Embedded CLI, repeatable flag:**

```bash
sqe-cli --embedded --hive-root lake=s3://bucket/wh/
```

Against anything other than real AWS S3, the embedded CLI also needs to be told where the store is:

```bash
sqe-cli --embedded \
  --s3-endpoint http://localhost:9000 \
  --hive-root lake=s3://bucket/wh/
```

Embedded mode loads no `sqe.toml`, so `--s3-endpoint`, `--s3-access-key`, `--s3-secret-key`, and `--s3-region` are how it learns about a store. Each falls back to `SQE_STORAGE__S3_*` (the coordinator's own config names) and then to the standard `AWS_*` variables, so `AWS_ENDPOINT_URL` and `AWS_ACCESS_KEY_ID` work as you would expect. Prefer the environment for the secret; a command line is visible in `ps`. An endpoint override switches addressing to path-style, because `bucket.host:port` does not resolve for a self-hosted store; `--s3-virtual-hosted` turns that back off for a store with wildcard DNS. Without any of this the engine talks to real AWS and, with no static key, falls through to the instance-metadata service and times out.

A runnable end-to-end example, including the data generator and the assertions, is in [`quickstart/hive-external-s3/`](https://github.com/getsqe/sqe/tree/main/quickstart/hive-external-s3).

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

### Athena and Trino views in Glue

A Glue entry with `TableType = VIRTUAL_VIEW` is an Athena (or Trino) view: no storage, and the definition in `ViewOriginalText` as a `/* Presto View: <base64 JSON> */` payload. SQE decodes the payload, runs the view's Trino-dialect SQL through the same front door a user statement takes (source-level Trino fallbacks, the compatibility AST rewrites), registers the engine's Trino function set (`date_parse`, `from_iso8601_timestamp`, `sequence`, `array_join`, and the rest), and plans it against the same catalog with the view's own database as the default schema. A body the DuckDB planner dialect rejects is parsed once more with the generic dialect, which is what makes `CROSS JOIN UNNEST(SEQUENCE(..)) t (p)` plan. Four Trino spellings that sqlparser, chrono or the planner refused are handled on the way: `trim(BOTH FROM x)` becomes `TRIM(x)` (`LEADING`/`TRAILING` to `LTRIM`/`RTRIM`), `date_parse('202601', '%Y%m')` fills the fields the format does not name with their lowest value, as Trino does, the `U&'...'` form Trino uses to serialize any literal with a non-ASCII character becomes a plain literal, and `split(s, delimiter, limit)` is the Trino function with its limit (the last element holds the rest) rather than an alias on DataFusion's two-argument `string_to_array`. `FROM orders`, `FROM "sales"."orders"` and Athena's `FROM awsdatacatalog.sales.orders` all resolve, views over views work, and `information_schema.tables` reports the entry as a `VIEW`. Base tables load through the ordinary provider, so row filters and column masks apply to the underlying scans, not to the view text.

A view whose SQL this release cannot plan (an unsupported function, a base table in a format SQE cannot read) is hidden with an ERROR log line naming the view and the planner's reason, the same way an unreadable table is. Hive-native views, whose `ViewOriginalText` is plain SQL rather than a Presto payload, are not supported and are hidden with a message that says so.

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
3. **`projection.*` partition-projection properties are implemented.** Unknown keys and malformed types error by name at CREATE and on every manifest load. They are never silently ignored.
4. **`has_encrypted_data`, `STORED BY`, bucketing, and skew clauses are rejected.**
5. **`DROP TABLE ... PURGE` is rejected.** `DROP TABLE` deletes the manifest and never touches data, so there is no purge semantics to offer yet.

## What DROP TABLE does

`DROP TABLE` deletes the sidecar manifest. It never touches the underlying data, regardless of `PURGE`, which is rejected outright rather than silently ignored: an external table's `DROP` has nothing to purge.

## Credentials, plainly

Reads authenticate to object storage with the coordinator's own storage credentials, or with a named secret from `TBLPROPERTIES ('sqe.secret' = '...')`. Not with the calling user's credentials.

The credential model is a real difference from how SQE treats Iceberg tables. An Iceberg read gets per-user credentials because Polaris vends them per table against the caller's bearer token. Nothing plays that role for an arbitrary `s3://raw/orders/` prefix: there is no catalog managing it, so there is no per-user credential to vend. The engine has no mechanism to build an S3 client for a raw prefix scoped to one user, and building one is a project of its own, not a cycle-1 task. The file table-valued functions (`read_parquet`, `read_csv`, `read_json`) already work exactly this way.

Authorization still runs per user, in two places that do not depend on the credential:

- The `[storage.tvf]` prefix allowlist. `allowed_object_store_prefixes` supports `{user}` substitution, and it is evaluated with the calling session's identity on every manifest load, not only at `CREATE EXTERNAL TABLE` time.
- The policy layer. Row filters and column masks apply to a Hive table the same way they apply to an Iceberg one (see the resource-naming rule below). Table-level access is decided here too, and differently from Iceberg: with `[policy] engine = "ranger"` a Hive table is readable only when a Ranger access policy grants `select` on it to the user, a role or a group, and a table with no grant reads as not found. Grant first, deny by default, because no catalog stands in front of the root to say no. `[policy.ranger] hive_access_policies = false` turns the gate off. Details in [Fine-grained access control](../features/fine-grained-access-control.md).

Put plainly: for Iceberg tables SQE has no service account. For Hive external tables it effectively has one, bounded by a per-user prefix allowlist and per-user policy. If that distinction matters to your deployment, keep Hive roots out of buckets that are not uniformly readable by the engine's own identity. A secret named in a manifest does not widen this: the allowlist and the policy layer still apply on top of it, so a secret cannot reach a location the deployment has not already allowed.

## Resource naming, and why it matters

A Hive table's policy namespace is `<root>.<database>`, not the database alone. `lake.sales.orders` keys as `("lake.sales", "orders")`. An Iceberg table keeps its plain dotted namespace, with no catalog prefix, unchanged from before this feature existed.

The asymmetry is deliberate. Without it, a grant written for Iceberg `sales.employees` would also govern a Hive table someone else named `lake.sales.employees`, and the person who names a Hive table is whoever can write a manifest, pointed wherever their credentials reach. That is privilege escalation through naming, not a hypothetical. `ATTACH ... TYPE hive_external` refuses a root name that collides with a registered catalog name to keep this rule enforceable. The one case it does not detect is an Iceberg namespace whose own dotted name happens to equal `<root>.<database>` (for example, an Iceberg namespace literally named `lake.sales`); avoiding that naming collision is on you until a later cycle makes the catalog part of every table kind's key.

## Performance, plainly

**A populated `sqe.partition_index` plans from the index.** Equality, `IN`, range, and `IS NULL` predicates on partition columns prune entries before any file group is built. An absent index keeps cycle-1 listing, including leading-equality prefix pruning. Projection-enabled tables compute the partition set at plan time and never list the table prefix to discover partitions.

**Compressed CSV and JSON are one file per thread.** Neither format is splittable once compressed, so a single large compressed file runs on a single thread regardless of how many cores are available. Size files with that in mind: many moderately sized files parallelize; one giant file does not, no matter the partition count.

Parquet gets real row-group pruning from footer statistics and splits by row group for parallelism. CSV and JSON get file-level parallelism up to `target_partitions`, and only projection pushdown, not predicate pushdown; filters run above the scan.

Without a prior `ANALYZE TABLE`, planning uses size-derived statistics from the index (file counts and bytes captured at ADD/REPAIR time) or from a one-shot listing when there is no index. `ANALYZE TABLE` persists real row counts into `sqe.statistics` and planning uses those when present. Parquet counts come from footers; CSV and JSON pay a full decode once.

## Server mode

Verified through a real `sqe-server` process over Flight SQL, not only in-process: the quickstart's `./run.sh --server` boots a coordinator that attaches its root from `[[hive.roots]]`, runs the same DDL and the same queries as the embedded path, restarts the coordinator, and reads the tables back from the manifests. Same row counts, same rendered DDL.

Two things a server-mode deployment needs that embedded mode does not. `[storage.tvf] allowed_object_store_prefixes` is mandatory: server mode fails closed on object-store paths, so with no allowlist every `s3://` read is denied, including the manifest load behind a Hive table. And nothing else has to be reachable. A coordinator whose `[catalog]` points at an Iceberg endpoint nobody is listening on still boots, attaches its Hive roots, and answers every Hive query, which is what a Hive-only deployment looks like.

The exception is metadata statements that enumerate catalogs. `information_schema.tables` and `DESCRIBE` walk **every** registered catalog before any `WHERE` clause narrows them, so an unreachable Iceberg catalog fails those statements even for a query that only named a Hive root. `SHOW TABLES FROM <root>.<database>` does not, and is the reliable listing in server mode. In embedded mode it is the reverse: `SHOW TABLES` is not parsed and `information_schema` is what works.

## Worker execution

Hive scans can run on workers. It is opt-in:

```toml
[query]
distributed_hive_scans = true
```

Default off. With the flag off, every Hive scan runs coordinator-local, exactly as before.

One `ScanTask` per surviving partition. The coordinator lists the partitions that survived pruning and ships their file paths and sizes, rather than shipping a prefix for the worker to expand: placement without sizes is blind, and one partition holding a thousand files would otherwise land wholesale on one worker. The listing is bounded by the partition index, so only survivors are listed, never the whole table.

All three formats run on workers: Parquet, CSV, and line-delimited JSON. A CSV or JSON task covers exactly one file and reads it whole. There is no byte-range splitting, because neither format has row groups and a byte offset is not a record boundary. A table with one very large CSV file gets no intra-file parallelism; a table with many files across many partitions parallelizes fully.

Partition columns are directory-encoded and absent from every data file, so the worker materializes them as constant arrays, typed from the table's declared schema. The typing rule is imported from the coordinator's own `partition_scalar` rather than reimplemented, and `__HIVE_DEFAULT_PARTITION__` resolves to NULL on both paths. A differential test suite runs the same query coordinator-local and worker-side and compares rows and schemas, per format, because the failure to design against here is not a crash: it is a partition column silently typed `varchar` on one path, or a header row counted as data.

### The fallback contract

Not every scan distributes, and a scan that does not is never a query failure. It runs coordinator-local and says why.

| Situation | What happens |
|---|---|
| `distributed_hive_scans = false` | Coordinator-local, unmarked. This is the default state; marking every scan in a non-distributed deployment would be noise. |
| No partition entries planned | Coordinator-local, marked. A table with no partition index has no per-partition unit of work to distribute. |
| Partition listing failed | Coordinator-local, marked with the listing error. |
| No worker reader for the format | Coordinator-local, marked with the format. No format answers this today. |

"Marked" means the plan carries a `HiveLocalScanExec` node and plain `EXPLAIN` prints the reason:

```
HiveLocalScanExec: coordinator-local, no partition entries to distribute; scan runs coordinator-local
```

The marker exists because a silent fallback is the worst outcome available. The query still returns correct rows, so nothing looks wrong, and the only symptom is a cluster that never gets any work. Run `EXPLAIN` if you expected a Hive scan to distribute and it did not.

### What stays coordinator-local regardless

Predicate and projection pushdown are not pushed into a distributed Hive scan. A task carries no pushed-down predicate and always projects the full schema; the `Filter` and `Projection` above the scan still apply, so results are correct, but a worker reads more of each file than a coordinator-local scan would. Partition pruning is unaffected: it happens on the coordinator before either path exists, so both inherit the same surviving partitions.

Hive DDL, `MSCK REPAIR`, `SHOW PARTITIONS`, `ANALYZE`, and manifest access all stay on the coordinator.

## Statement support today

| Statement | Behavior |
|---|---|
| `CREATE EXTERNAL TABLE` | Supported |
| `DROP TABLE` | Supported. Deletes the manifest, never the data. `PURGE` rejected |
| `SHOW TABLES FROM <root>.<database>`, `DESCRIBE`, `SHOW COLUMNS`, `information_schema` | Supported. `SHOW TABLES` and `SHOW CREATE TABLE` are policy-gated: a fully-denied table is omitted/errors like a missing one, and a restricted column is dropped from `SHOW CREATE TABLE`'s rendered DDL. `information_schema` itself is not policy-filtered for a Hive root, same as it is not for an Iceberg catalog: this is pre-existing parity, not a Hive-specific gap |
| `SHOW CREATE TABLE` | Supported. Renders Athena-compatible DDL from the manifest |
| `SHOW SCHEMAS`, `SHOW SCHEMAS IN <root>` | `SHOW SCHEMAS` is not parsed at all in embedded mode, against a Hive root or an Iceberg catalog alike: this is a pre-existing embedded-mode gap, not something this feature introduces. In server mode it works for Iceberg catalogs, but has no dedicated path for a Hive root yet and fails with a generic catalog error. `SELECT catalog_name, schema_name FROM information_schema.schemata WHERE catalog_name = '<root>'` works in both modes and lists the root's databases; use that, or `SHOW TABLES FROM <root>.<database>` against a database you already know |
| `ANALYZE TABLE` | Supported on Hive tables. Persists row counts into `sqe.statistics`. Same policy gate as reads. Iceberg `ANALYZE` remains a no-op (#329) |
| `ALTER TABLE ... ADD PARTITION` | Supported. Appends one index entry, never touches data. `IF NOT EXISTS` honored. Refused on projection-enabled tables |
| `ALTER TABLE ... DROP PARTITION` | Supported. Removes the index entry, never the data. `PURGE` rejected, naming cycle 4. Refused on projection-enabled tables (projection owns the partition set) |
| `MSCK REPAIR TABLE` | Supported. Relists the table location and rewrites the index. The remedy for a stale index. Junk paths (non-`k=v` dirs, untypeable values, files at the table root) are skipped, matching Hive and Athena. No-op on projection-enabled tables |
| `SHOW PARTITIONS` | Supported. Reads the index when present, lists when absent |
| `SHOW VIEWS FROM <root>.<database>` | Supported. The Athena / Trino views (`TableType = VIRTUAL_VIEW`) among the tables the caller may see, one `View` column, from one database listing. `SHOW TABLES` lists views and tables together, as Trino does; this is how a crawler tells them apart without `information_schema.tables`, which enumerates the whole catalog to answer for one schema |
| `ALTER TABLE ... SET TAGS`, masking policy statements | Rejected, naming cycle 3 |
| `INSERT INTO` | **Supported.** Appends into `k=v/` partition directories, refreshes the partition index, and drops the cached session context so the next query sees the rows. See [Writes](#writes) |
| `INSERT OVERWRITE`, `REPLACE INTO` | Rejected, naming why it cannot be made atomic |
| `CREATE TABLE ... AS SELECT` | **Supported**, with a required `LOCATION`. See [CTAS](#ctas). `CREATE OR REPLACE` is refused. Previously this reached the Iceberg dispatch with no Hive-aware check at all |
| `UPDATE`, `DELETE`, `TRUNCATE` | Rejected outright. All three need the same primitive `INSERT OVERWRITE` needs, and a Hive external table does not have it |
| `ALTER TABLE` rename, column add/drop/rename, `COMMENT ON` | Rejected outright. Schema evolution on an external table is a manifest edit, not scheduled work |
| Partition evolution, branches, tags, time travel | Rejected outright. These are Iceberg snapshot features with no Hive equivalent |

Every rejection names `hive_external` and, where one is planned, the cycle that adds support, so the error tells you what to do rather than only what failed.

One correction to an earlier version of this page. It said a plain `CREATE TABLE lake.x.y AS SELECT` "does reach the ordinary Iceberg `CreateTable` dispatch, which carries no Hive-aware check". That was accurate and it was a bug, not a documentation gap: the statement took the Iceberg write path against a Hive name. It now fails with a `hive_external` message naming cycle 4. `CREATE EXTERNAL TABLE ... AS SELECT` still cannot reach any handler, because sqlparser's grammar for `external: true` has no `AS SELECT` path in any dialect, so there is nothing to name. `MERGE` against a Hive target is still not gated by a Hive-aware check.

## Writes

`INSERT INTO` lands rows in the table's `k=v/` partition directories, one directory per distinct partition value in the inserted data. Partition columns are written into the directory NAME and never into the data file, which is what keeps the files readable by Athena, Spark, and Trino.

```sql
INSERT INTO lake.sales.orders VALUES (6, 60.5, '2026-01-02');
INSERT INTO lake.sales.orders SELECT id, amount, dt FROM staging.orders;
```

The partition column goes last, matching the table's column order as `SHOW CREATE TABLE` renders it.

### The visibility contract

**A single file appears atomically. A multi-file write does not.**

An object store makes a `PUT` visible whole or not at all, so no reader ever sees a half-written file. On a local filesystem the same holds for a different reason: the object-store layer writes to a staging path and then renames, and `rename(2)` within one filesystem is atomic. Neither guarantee is SQE's own, and SQE does not add a second one on top.

But a write that produces several files has no commit that makes them appear together, and there is nothing SQE can do about that: a Hive external table has no metadata pointer to swap. Its content is whatever is in the directory, which is exactly the property that lets other engines read it.

So a concurrent reader may see a subset of an in-progress write, and an interrupted write leaves the files it had already landed. Those files hold real rows, not corruption. **If you need snapshot isolation on write, use an Iceberg table.**

### One metadata round trip per table, not one per lookup

Planning a statement resolves every table reference it names through the catalog, `information_schema` resolves every table in a database, and the policy layer reads the same manifest to find column tags. Before this, each of those lookups cost a `HEAD` on the manifest to compare its ETag, and for CSV and JSON tables a listing of the data prefix to read the compression codec off a file name, and for tables with neither `ANALYZE` statistics nor a partition index a second listing to size the table for the planner. A twelve-table dbt step paid all of that twelve times over.

Now a manifest that the metastore confirmed less than `metadata_recheck_secs` ago (default 10) is served as is, and what the two listings learned is kept on the manifest's cache entry until the manifest reloads or the cache TTL (`metadata_ttl_secs`, default 60) expires. Every `CREATE`, `ALTER`, `DROP`, `MSCK REPAIR` and `INSERT` in the coordinator drops the entry for the table it changed, so your own writes are visible immediately. The window only delays what another engine wrote to the same manifest; set `metadata_recheck_secs = 0` to check on every lookup. Glue-backed roots have no change token and already refreshed on the TTL alone; they now honour that TTL under continuous querying, where the previous release kept extending it.

### Glue: one listing warms the whole database

Glue's `GetTables` returns every table's full definition, so a Glue-backed root treats the listing as the fetch. The pre-plan refresh, `SHOW TABLES` and `information_schema` load a database's manifests into the cache in one paginated call, and the `SELECT` that follows finds its table already there. Before this a `SHOW TABLES` against a 618-table database made 618 `GetTable` calls to check each table's policy, which took eight seconds; now it makes the one listing it needed anyway. `information_schema.tables` also answers a table's kind from the manifest alone rather than building the table, so listing a database no longer lists every table's data prefix for size statistics.

When the TTL expires and the refetched definition is unchanged, the entry keeps the file facts it had learned (codec, listed bytes), so an expiry on a quiet table costs the `GetTable` and nothing on the object store. For a read-mostly Glue catalog, raising `metadata_ttl_secs` to a few hundred seconds mostly removes those round trips.

### Timeouts, and refreshing on demand

Every metastore call on a root is cut at `metadata_timeout_secs` (default 10; `0` disables the bound), including the SDK's own connect and operation clocks for Glue. A table the cache has seen is then served from its last known manifest with a `serving stale metadata, refresh timed out` warning; a table it has never seen fails with a `catalog metadata timeout` error naming the call; a listing that times out keeps the previous snapshot, the way a failed listing already did. Before this, a throttled `GetTable` stalled the query in planning with nothing in the log.

A longer TTL needs a way to drop an entry when a table changes outside SQE. Three paths do:

- `CALL system.refresh_catalog_cache(table => 'glue.db.t')` drops that table's manifest, `schema => 'glue.db'` drops the database's; both also drop the caller's own session view, as the bare call always did. No privilege needed, and the cost is bounded by the scope named.
- The MCP tool `refresh_metadata` is the same call with `catalog`, `schema` and `table` arguments, rate-limited per scope so a looping assistant cannot flood the metastore. A not-found answer from `list_tables`, `describe_table`, `get_table_ddl` or `sample_rows` ends with a hint to call it once.
- `POST /api/v1/catalogs/refresh` (admin) drops every Hive manifest alongside the session and REST-catalog caches, for the out-of-band rebind case.


### The partition index is refreshed for you

Reads plan from `sqe.partition_index` rather than listing the table prefix, so a write into a partition the index does not mention would otherwise be invisible. Every `INSERT` refreshes the index after the data lands, using the same discovery `MSCK REPAIR TABLE` runs, and then drops the cached session context so the next query sees the rows.

The ordering is deliberate: data first, then the index. The reverse would make the index promise files that do not exist. This way the only possible in-between state is data that is intact and not yet visible, and the error says so:

```
hive_external: the rows were WRITTEN but are not yet visible: refreshing the
partition index failed (...). The data files are on storage and intact. Run
`MSCK REPAIR TABLE lake.sales.orders` to make them visible.
```

The cost is one prefix listing per write, which is the work the index exists to avoid on reads. An incremental update is the right optimization later.

### CTAS

```sql
CREATE TABLE lake.sales.daily
  PARTITIONED BY (dt string)
  STORED AS PARQUET
  LOCATION 's3://bucket/warehouse/sales/daily/'
AS SELECT id, amount, dt FROM lake.sales.orders;
```

Three rules, each of which fails loudly rather than guessing.

**`LOCATION` is required.** SQE does not choose data paths for external tables, the same way `CREATE EXTERNAL TABLE` requires one. A CTAS without it errors instead of writing your data somewhere you did not name.

**Partition columns come last in the SELECT.** Hive stores them in the directory name and the read path expects them at the end of the schema. A partition column elsewhere in the projection is an error naming the column that is actually in that position, not a silent reorder: reordering would move your data into a different column.

Both `PARTITIONED BY (dt string)` and `PARTITIONED BY (dt)` work. The first declares the type, the second takes it from the query. Column types for the stored columns always come from the query, so a type with no Hive equivalent fails at `CREATE` rather than producing a table nobody can read.

**Format defaults to Parquet**, not Hive's `TEXTFILE`. A CTAS is SQE writing the files, and writing text would discard the types the query just established. `STORED AS` overrides it.

**`CREATE OR REPLACE` is refused.** `DROP TABLE` on a Hive external table removes the manifest and never the data, so a replacement would write new files into a directory that still holds the old ones and the table would read back as the union of both. Drop the table, delete its data directory, then create it again.

CTAS runs in two steps, manifest then data, which is the opposite order from `INSERT`. A failure between them leaves an empty table: `SELECT` returns nothing, `SHOW CREATE TABLE` works, `DROP TABLE` cleans up, and re-running fixes it. Writing the data first would leave files under a path no table names, and `MSCK REPAIR TABLE` could not help because repair needs a manifest to repair.

### Why INSERT OVERWRITE is refused

An overwrite would have to delete the partition's objects and then write new ones. No object store can make those two steps atomic, so an interruption between them loses the partition. That is real data loss from a statement that reads as routine, so it is refused rather than approximated. `REPLACE INTO` is refused for the same reason and through the same check, because it sets a different parser flag and would otherwise execute as a plain append.

## Known limitations, and what to do about them

**A stale index omits partitions, never invents them.** Files that landed out of band after the last `MSCK REPAIR` or `ADD PARTITION` are invisible until you repair. SQE does not list the table prefix at plan time to double-check the index, because that listing is the cost the index exists to remove. `MSCK REPAIR TABLE` is the named recovery path. Deleting the `sqe.partition_index` slot from the manifest restores cycle-1 discovery.

**A table with a `__HIVE_DEFAULT_PARTITION__` directory under a non-string partition key cannot be written to.** Reads work: the sentinel resolves to NULL, and that is tested on both the coordinator-local and worker paths. Writes fail with `Cannot cast string '__HIVE_DEFAULT_PARTITION__' to value of Int32 type`, because the write path lists the table's existing partition directories and casts each value to the column's declared type, and Hive's sentinel is a string. A string-typed partition key is unaffected. To write into such a table, declare the partition key as `string`, or move the sentinel partition's files aside first.

**A CSV or JSON file is one task, however many cores you have.** Not only compressed files: neither format has row groups, so there is no boundary to split on, and a worker reads each text file whole. Split large text files at write time rather than relying on in-engine parallelism to compensate. Parquet splits within a file.

**Reads use engine credentials or a named secret, never the caller's own.** Do not point a Hive root at a bucket your users should not uniformly be able to read through the engine's identity. Use the `[storage.tvf]` allowlist and grants to bound exposure per user; do not rely on the credential itself to do that job.

**`array`, `struct`, and `map` columns are not supported.** A table with a nested column needs those columns projected out at the source or flattened before SQE reads it. There is no workaround inside SQE for cycle 1.

**Partition projection.** `projection.enabled=true` plus per-column `projection.<col>.type` of `enum`, `integer`, `date`, or `injected` computes the partition set at plan time. Locations follow the standard `k=v/` layout under the table location. `injected` columns take an equality or `IN` literal from the query when one is present, and NULL (`__HIVE_DEFAULT_PARTITION__`) otherwise. `ADD PARTITION` / `DROP PARTITION` on a projected table is an error: projection owns the partition set. `MSCK REPAIR TABLE` succeeds as a no-op, so an ETL script that always repairs after a load does not fail. Unknown `projection.*` keys fail at CREATE and on every load, naming the key.

**`SHOW SCHEMAS` does not list a Hive root's databases.** This is not a Hive-specific gap: `SHOW SCHEMAS` is not parsed in embedded mode at all, against any catalog kind, and in server mode it has no dedicated Hive path yet, so it fails with a generic catalog error against a Hive root. `SELECT catalog_name, schema_name FROM information_schema.schemata WHERE catalog_name = '<root>'` works in both modes and is the reliable way to list a root's databases; `SHOW TABLES FROM <root>.<database>` works too once you know the database name.

**Distributed Hive scans are off by default.** Set `query.distributed_hive_scans = true` to turn them on. With the flag off, or for a table that cannot distribute, scans run coordinator-local and `EXPLAIN` says so; see [Worker execution](#worker-execution) for the fallback contract.

**A grant against an Iceberg namespace never governs a same-named Hive database, and vice versa.** A prior draft of this page understated the actual exposure here: the policy engine and the tag route both key off the table reference's catalog component, and a two-part reference (`sales.orders`, no catalog) carries no catalog component at all. Attaching a Hive root under the SAME name as the session's default catalog would have let an unqualified reference reach that root while policy resolution still took the Iceberg branch. SQE now refuses to attach a Hive root under that name, at both `[[hive.roots]]` startup and `ATTACH`, so the collision cannot occur. One narrower corner case remains open: an Iceberg namespace whose dotted name literally equals `<root>.<database>` still produces the same policy key as a Hive table in that root's database. Name your Hive roots to avoid that collision until a later cycle closes it structurally.

## What is next

Cycle 3 adds tag-based row filters and column masks on Hive scans.

Cycle 4 is in for reads-into-writes: `INSERT INTO` and CTAS both work (see [Writes](#writes)). `INSERT OVERWRITE`, `DELETE`, `UPDATE` and `MERGE` remain refused, all for the same reason: a Hive external table has no atomic multi-file commit to build them on.

Cycle 5, worker execution, has shipped and is described under [Worker execution](#worker-execution). Two pieces of it are still open: predicate and projection pushdown into a distributed Hive task, and a streaming CSV/JSON decode so a large text file does not have to be resident whole.
