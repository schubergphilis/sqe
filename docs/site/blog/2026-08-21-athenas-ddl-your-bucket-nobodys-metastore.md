---
title: "Athena's DDL, your bucket, nobody's metastore"
description: "SQE now reads Hive-style external tables over CSV, line-delimited JSON, and partitioned Parquet on any S3-compatible store, using Athena's own CREATE EXTERNAL TABLE syntax. There is no Hive Metastore, no Glue API call, and no catalog service of any kind behind the table: the definition is a JSON manifest next to the data, shaped like a Glue TableInput. This post is what emulating Athena and Glue actually requires, which parts we copied deliberately, the four Athena behaviours we refused to copy, and the credential difference that is a real change from how SQE treats Iceberg."
pubDate: "2026-08-21"
author: "Jacob Verhoeks"
tags:
  - "hive"
  - "athena"
  - "glue"
  - "s3"
  - "external-tables"
  - "datafusion"
---

A lot of data is already in a bucket, in an open file format, with a Hive-shaped directory layout, and a pile of `CREATE EXTERNAL TABLE` statements that Athena understands. Converting all of it to Iceberg to ask one question about it is a bad trade. So SQE now reads it where it lies.

Cycle 1 is the read path: attach a warehouse root, create and drop tables, read CSV, line-delimited JSON, and Parquet, with Hive-style `k=v/` partitions. No metastore process. No Glue API call. The table definition is a small JSON manifest sitting next to the data.

Every number and every error message below is copied from a run that finished, most of them from `quickstart/hive-external-s3/`, which generates its own data and asserts 21 invariants.

## What "emulating Athena and Glue" actually means

Athena gives you three separable things, and they are worth pulling apart before claiming to emulate any of them.

**A DDL grammar.** `CREATE EXTERNAL TABLE`, `ROW FORMAT SERDE`, `WITH SERDEPROPERTIES`, `PARTITIONED BY`, `STORED AS`, `LOCATION`, `TBLPROPERTIES`. We parse the real thing rather than a lookalike, by extending sqlparser instead of forking it, so copied Athena DDL runs unchanged in the common case.

**SerDe semantics.** What a delimiter means, whether a header row is data, how a partition value is typed. Copying the grammar and not the semantics is worse than not copying either, because the query returns a number instead of an error.

**A catalog.** Glue holds a `TableInput` per table: a `StorageDescriptor` with columns, a `SerdeInfo`, a location, `partition_keys` beside it, `parameters` for everything else. Athena reads it over an API.

The grammar and the semantics we implement. The catalog we replace, and the replacement is the interesting decision: one JSON object per table, at `<root>/<database>/<table>/_sqe/table.json`, using **Glue's own field names**.

```json
{
  "manifest_version": 1,
  "table_type": "EXTERNAL_TABLE",
  "database_name": "demo",
  "name": "events_parquet",
  "partition_keys": [
    { "name": "dt", "type": "string", "comment": null },
    { "name": "region", "type": "string", "comment": null }
  ],
  "parameters": {},
  "storage_descriptor": {
    "location": "s3://lake/data/events_parquet/",
    "input_format": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
    "output_format": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
    "serde_info": {
      "serialization_library": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
      "parameters": {}
    },
    "columns": [ { "name": "event_id", "type": "bigint", "comment": null } ],
    "sort_columns": []
  },
  "sqe": { "secret": null, "column_tags": {}, "partition_index": null, "statistics": null }
}
```

The field names are not cosmetic. A Glue-backed catalog later becomes a field mapping rather than a redesign, and a test asserts the on-disk key names rather than round-tripping through our own serializer, because a `#[serde(rename)]` would change serialize and deserialize together and keep a round-trip test green while the JSON silently stopped matching Glue.

Anything SQE-specific lives under an `sqe` key, which is where Glue and Athena put engine-specific settings anyway. Today `sqe.secret` is the only one that does something.

The manifest is untrusted input. Anyone who can write to the warehouse prefix can plant one, so it is size-capped before it is deserialized (1 MiB, 4096 columns, 32 partition keys by default), and its `location` is re-checked against the storage allowlist on **every** load, not only when it was created. A guard that only runs at create time protects nothing against a file written by any other path.

## The DDL that works

Copied from Athena, unchanged:

```sql
CREATE EXTERNAL TABLE lake.demo.events_parquet (
  event_id BIGINT,
  user_id  BIGINT,
  kind     STRING,
  amount   DOUBLE
)
PARTITIONED BY (dt STRING, region STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION 's3://lake/data/events_parquet/';
```

Attach the root first, three ways: `ATTACH 's3://lake/wh/' AS lake (TYPE hive_external)` in SQL, `[[hive.roots]]` in config, or `--hive-root lake=s3://lake/wh/` on the embedded CLI. Attaching lists nothing and reads nothing. It is a path check, not a network call.

Two clauses in copied Athena DDL do not parse, and both are worth knowing before you paste a hundred of them.

**`STORED AS CSV` and `STORED AS JSON` are not format keywords.** The set is exactly `TEXTFILE`, `SEQUENCEFILE`, `ORC`, `PARQUET`, `AVRO`, `RCFILE`, `JSONFILE`. For comma-separated data the working form is `ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE`.

**Bare `TEXTFILE` means control-A, not comma.** Hive's `LazySimpleSerDe` defaults its field delimiter to `\001`, and so does Athena. A table declared over comma-separated data with no delimiter clause is a one-column table in both engines. We kept that default rather than being helpfully different, because a table that reads differently in two engines is worse than a table that reads badly in both. Octal escapes work, so `FIELDS TERMINATED BY '\001'` and no clause at all behave identically.

## Five SerDes, and why anything else is an error

No SerDe is implemented. Each allow-listed class maps onto a reader DataFusion already has.

| SerDe class | Format | Honoured SERDEPROPERTIES |
|---|---|---|
| `org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe` | CSV | `field.delim`, `serialization.format`, `escape.delim` |
| `org.apache.hadoop.hive.serde2.OpenCSVSerde` | CSV | `separatorChar`, `quoteChar`, `escapeChar` |
| `org.openx.data.jsonserde.JsonSerDe` | JSON | `ignore.malformed.json` |
| `org.apache.hive.hcatalog.data.JsonSerDe` | JSON | `ignore.malformed.json` |
| `org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe` | Parquet | none |

An unknown SerDe class is an error naming the class. An unknown key inside `WITH SERDEPROPERTIES` is an error naming the key. Silently ignoring `separatorChar` changes the query's answer, and a rejected DDL is cheaper to debug than a wrong number.

ORC, Avro, SequenceFile and RCFile parse, and their real Hadoop input and output format classes are recorded faithfully in the manifest, so `SHOW CREATE TABLE` renders truthful DDL for a table this cycle cannot read. They are rejected at read time, by class name, naming the format. An earlier cut dropped the unrecognised keyword instead, which meant an ORC table's manifest forgot it was ORC: a lossy record of what the user actually declared, written into a file that outlives the release that wrote it.

JSON means one object per line, in both the `.json` and `.ndjson` cases. A `[{...},{...}]` array document is not readable, in SQE or in Athena. The extension is cosmetic; the framing is what matters.

## Partitions come from the path

`dt=2026-01-02/region=eu/` is where `dt` and `region` come from. They are declared in `PARTITIONED BY`, never repeated in the column list, and they land last in the resulting schema. `string`, `varchar`, `int`, `bigint` and `date` are typed from the path segment; a segment that does not parse to its declared type makes that partition unreadable with an error naming the segment, rather than being skipped quietly.

`__HIVE_DEFAULT_PARTITION__` reads as NULL, consistently: in a projection, in a filter, and in a `GROUP BY`. An earlier cut had a filter treating the sentinel as NULL while a plain `SELECT` still returned the literal string, so `WHERE dt IS NULL` and `SELECT dt` disagreed about the same row. Half-emulating a sentinel is worse than not handling it, because both answers look plausible.

## Four Athena behaviours we refused to copy

**`OpenCSVSerde` types every column as string in Athena.** We honour your declared types and cast. Athena's all-string behaviour is a workaround for its own SerDe, not a semantic anyone wants.

**`projection.*` partition projection is rejected, not ignored.** Ignoring it turns a computed partition set into a full table scan, which is a performance cliff disguised as a working table. Cycle 2 implements it.

**`DROP TABLE ... PURGE` is rejected.** `DROP TABLE` deletes the manifest and never touches data. An external table has nothing to purge, so we refuse the word rather than accepting it and doing nothing.

**Nested types are rejected with the column named.** `array<string>` and `struct<x:int>` parse and are refused; `map<string,int>` fails inside the parser before we see it. Cycle 1 is scalar columns and scalar partition keys.

`STORED BY`, bucketing, skew clauses and `has_encrypted_data` are likewise refused rather than accepted-and-ignored.

## The credential difference, stated plainly

For Iceberg tables, SQE has no service account: every query runs as the authenticated user, and Polaris vends per-table credentials against that user's bearer token.

Nothing plays that role for an arbitrary `s3://raw/orders/` prefix. There is no catalog managing it, so there is no per-user credential to vend, and building that mechanism is a project rather than a task. Hive reads therefore authenticate with the engine's storage credentials, or with a named `SECRET` from `TBLPROPERTIES ('sqe.secret' = '...')`. The file table-valued functions have always worked this way.

Authorization still runs per user, in two places that do not depend on the credential. The `[storage.tvf] allowed_object_store_prefixes` allowlist supports `{user}` substitution and is evaluated on every manifest load. The policy layer applies grants exactly as it does to an Iceberg table.

Put plainly: for Iceberg tables SQE has no service account, and for Hive external tables it effectively has one, bounded by a per-user prefix allowlist and per-user policy. If that distinction matters where you work, keep Hive roots out of buckets that are not uniformly readable by the engine's own identity.

One related decision has security weight. A Hive table's policy key is `<root>.<database>`, not the bare database. `lake.sales.orders` keys as `("lake.sales", "orders")` while an Iceberg table keeps its plain dotted namespace. Without the asymmetry, a grant written for Iceberg `sales.employees` would also govern a Hive table that someone named `lake.sales.employees`, pointed wherever their credentials reach. That is privilege escalation through naming, so `ATTACH` refuses a root name that collides with a catalog name, and refuses one that collides with the session default catalog, in order to keep the rule enforceable.

## Performance, without the marketing

**Cycle 1 lists on every query.** There is no persisted partition index yet. What prunes the listing is pinning the leading partition columns with equality: `WHERE dt = '2026-01-02'` narrows to that prefix. A range predicate, an `IN` list, or a filter on a non-leading partition column lists the whole table prefix and filters in memory. On tens of thousands of partitions, the listing dominates.

**Compressed CSV and JSON are one file per thread.** Neither format is splittable once compressed. Size files accordingly: many moderate files parallelize, one giant file does not, whatever the core count.

Parquet gets row-group pruning from footer statistics and splits by row group. Cycle 1 uses size-derived statistics rather than reading a footer per file at plan time, so a join against a Hive table does not plan blind while staying cheap to plan.

## Two deployment shapes, one SQL

Embedded, no server and no catalog:

```bash
sqe-cli --embedded --memory \
  --s3-endpoint http://localhost:19100 \
  --hive-root lake=s3://lake/wh/
```

Server mode is the same SQL over Flight SQL against a coordinator that attached its roots from `[[hive.roots]]`. The quickstart runs both paths over the same bucket with the same DDL file, the same query file and the same 21 assertions: 21 of 21 in each, with row counts checked against what the generator recorded rather than against the engine's own answer. At the default size that is 3.96 million rows across four tables, in both modes. The embedded path has also run `--size-mb 1024`: 63.4 million rows, 73 seconds from generation to the last assertion, on a laptop.

The server-mode config points `[catalog]` at a port nobody is listening on, deliberately. A coordinator serving only Hive roots needs the object store and nothing else, and it boots, attaches, and answers every Hive query with no Iceberg endpoint reachable at all.

Two things a server needs that embedded does not. `[storage.tvf] allowed_object_store_prefixes` is mandatory: server mode fails closed on object-store paths, so with no allowlist every `s3://` read is denied, the manifest load included. And metadata statements that enumerate catalogs behave differently: `information_schema.tables` and `DESCRIBE` walk every registered catalog before any `WHERE` clause narrows them, so an unreachable Iceberg catalog fails those statements even for a query that only named a Hive root. `SHOW TABLES FROM <root>.<database>` does not, and is the reliable listing in server mode. In embedded mode it is the reverse, because `SHOW TABLES` is not parsed there at all.

## Three things that will waste your afternoon

**A cycle-1 scan does not filter by file extension.** Everything under a table's `LOCATION` is that table's data. Two formats under one prefix means one gets parsed by the wrong reader, and a `LOCATION` containing the manifest directory hands `table.json` to the CSV reader. One prefix per table, and keep the warehouse root out of the data path.

**RustFS serves stale listing metadata after an overwrite.** Measured on `rustfs/rustfs:latest`: replace an object with a different-sized body and `LIST` keeps reporting the previous size and mtime while `GET` serves the new bytes. A delete-then-write does not clear it. Dropping and recreating the bucket does not clear it. Only restarting the store does. Since any reader plans byte ranges from the listed size, a stale size cuts the last row in half, and you get `Csv error: incorrect number of fields` on a file that is provably well-formed, or the row count of the data that used to be there. `aws s3 ls` reports the same stale size, which is what proved it was the store and not the engine. Worth knowing beyond one quickstart: if you overwrite data files in place under a RustFS-backed table, queries can return stale results until the store restarts.

**A binary older than the feature boots fine and serves nothing.** `[[hive.roots]]` is unknown config to a pre-Hive coordinator, so it starts perfectly, attaches no root, and every query fails with "unknown catalog". Two separate debugging detours went into that one before the quickstart started printing each binary's path and build date and hard-failing when no root attaches.

## What comes next

Cycle 2 adds explicit partition DDL (`ALTER TABLE ... ADD/DROP PARTITION`, `MSCK REPAIR TABLE`, `SHOW PARTITIONS`), Athena-style partition projection, and the persisted partition index that removes the per-query listing cost. Cycle 3 puts tag-based row filters and column masks on Hive scans. Cycle 4 adds writes into partition directories. Cycle 5 moves Hive scan execution onto workers, which needs `ScanTask` to grow a Hive-shaped alternative to its Parquet and Iceberg field-id shape.

The reference page is `docs/site/book/src/reference/hive-external-tables.md`, and the runnable version of everything above is `quickstart/hive-external-s3/`.
