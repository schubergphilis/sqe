---
title: "The partition list lives next to the data"
description: "Cycle 2 of SQE's Hive external tables replaces the per-query prefix LIST with a partition index persisted in the sidecar manifest, maintained by MSCK REPAIR TABLE and ALTER TABLE ADD/DROP PARTITION. Athena asks Glue for the partition list over an API. SQE reads one ETag-cached JSON object sitting beside the files. On a 449-partition slice of the public Bitcoin dataset a pinned query goes from 99 ms to 1.9 ms cold, and DuckDB on the same files stays at 44 ms because it re-globs every query. Partition projection is implemented too, and measurably slower than the index on the same pin."
pubDate: "2026-08-23"
author: "Jacob Verhoeks"
tags:
  - "hive"
  - "athena"
  - "glue"
  - "duckdb"
  - "partitions"
  - "benchmarks"
---

Cycle 1 shipped the read path for Hive external tables: Athena's `CREATE EXTERNAL TABLE` grammar, five SerDes, a sidecar JSON manifest shaped like a Glue `TableInput`, and no metastore anywhere. It also listed the table's prefix on every single query. Only a leading run of equality-pinned partition columns pruned that listing, and everything else walked the whole prefix and filtered in memory.

Cycle 2 removes the listing. The partition list is now persisted in the slot cycle 1 reserved for it, and a read plans from the index instead of asking the object store what exists.

Every number below comes from a run that finished. The JSON is committed under `benchmarks/results/`, the working notes are in `docs/evidence/perf/hive-cycle2-partition-index.md`, and the SQE numbers are `dev-release` profile rather than `release`, so they are directional and not a committed baseline.

## Where the partition list lives

Athena and SQE agree on the maintenance DDL and disagree on where the answer is stored.

Athena keeps partitions in Glue. `MSCK REPAIR TABLE` populates them, `ALTER TABLE ADD PARTITION` appends one, and at plan time the engine asks Glue for the list over an API call. The catalog is a service on the other side of the network, and the partition list is state that service owns.

SQE keeps the same list in `sqe.partition_index`, inside the manifest already sitting at `<root>/<database>/<table>/_sqe/table.json`, next to the data it describes. Reading it is one object GET, and the result is cached keyed on the manifest's ETag rather than on a timer, so a manifest that changed is reloaded and a manifest that did not is free.

```json
"sqe": {
  "partition_index": {
    "format_version": 1,
    "entries": [
      {
        "values": { "date": "2024-01-01" },
        "location": "s3://lake/data/btc_blocks/date=2024-01-01/",
        "file_count": 1,
        "total_bytes": 56689
      }
    ]
  }
}
```

An entry carries its own location, because a Hive partition is allowed to live outside the table prefix. Every one of those locations is re-validated against the `[storage.tvf]` allowlist on each manifest load, not only when it was written, for the same reason cycle 1 re-checks `storage_descriptor.location`: the manifest is a file in a bucket, and anyone who can write to that prefix can plant one.

Values are typed on load from the declared partition key types, so `date=2024-01-01` becomes a date and not a string that sorts correctly by luck. A value that cannot be typed is a load error naming the entry. `__HIVE_DEFAULT_PARTITION__` normalizes to NULL, the same as it does on the listing path, and the cycle-1 filter rewriting runs on the indexed path so the two agree.

There is a cap, `manifest_max_partition_index_entries`, checked on load and on repair. A million-partition table should fail closed rather than produce a manifest object nobody can read.

## The DDL is the DDL you already have

```sql
MSCK REPAIR TABLE lake.demo.btc_blocks;

ALTER TABLE lake.demo.btc_blocks
  ADD IF NOT EXISTS PARTITION (date = '2026-08-23')
  LOCATION 's3://lake/data/btc_blocks/date=2026-08-23/';

ALTER TABLE lake.demo.btc_blocks DROP PARTITION (date = '2009-01-03');

SHOW PARTITIONS lake.demo.btc_blocks;
```

`ADD PARTITION` is a read-modify-write of one entry, no re-scan. `DROP PARTITION` removes the entry and never touches data, and `PURGE` is refused rather than accepted and ignored, the same call cycle 1 made for `DROP TABLE ... PURGE`. `SHOW PARTITIONS` reads the index when there is one and falls back to listing when there is not.

`MSCK REPAIR TABLE` is the recursive re-list, and it skips rather than fails. Directories that are not `k=v`, values that will not type, keys that were never declared, files sitting at the table root: all skipped, the way Hive and Athena skip them. A repair that aborts on the first junk directory is a repair nobody can run on a real bucket.

## Staleness, stated plainly

The index can be wrong in exactly one direction. Files can arrive under a prefix that has no index entry, and a query then returns fewer rows than the bucket holds. The index cannot invent a partition that does not exist, because entries are written by repair or by explicit DDL and validated on load.

Missing partitions rather than wrong values is the trade, and `MSCK REPAIR TABLE` is the recovery. Same contract Hive and Athena have offered for years, and the reason both of them ship the statement.

## What it costs and what it saves

Local `file://` fixture, `dev-release`, cold is the first query on a fresh session and warm is the third.

| case | listing cold/warm (ms) | indexed cold/warm (ms) | MSCK (ms) |
|---|---|---|---|
| csv 256, full scan | 329 / 79 | 63 / 25 | 27 |
| csv 256, pin | 57 / 28 | 1.8 / 0.8 | |
| csv 1024, full scan | 628 / 239 | 219 / 56 | 115 |
| csv 1024, pin | 219 / 111 | 2.3 / 1.1 | |
| csv 4096, pin | 1248 / 445 | 11 / 4.3 | 458 |
| parquet 256, full scan | 406 / 48 | 57 / 7.7 | 32 |
| parquet 1024, pin | 321 / 106 | 10 / 1.4 | 204 |

The pin is where the index earns its place: at 4096 partitions, 1248 ms of listing becomes 11 ms of index read, about 110x cold.

A full unpinned scan still opens every file. The index removes the recursive walk and nothing else, which is why 1024-wide CSV goes from 628 ms to 219 ms rather than to single digits. Anyone quoting the 110x for a full scan is quoting the wrong row.

The index is also not free. Building it at 4096 partitions costs 458 ms of repair on local disk, and on real object storage the same operation is a recursive LIST at network latency. One build, then every subsequent pin is a few milliseconds.

## A real public dataset

Synthetic fixtures prove the mechanism and nothing about the shape of real data, so the next target was `s3://aws-public-blockchain/v1.0/btc/blocks/`: 6437 live `date=` prefixes running from 2009-01-03 to today, one small snappy Parquet per day.

Anonymous `aws s3 ls` from a laptop, which is the LIST cost a cycle-1 scan or an `MSCK REPAIR` pays before any decode:

| prefix | wall (ms) | objects / prefixes |
|---|---|---|
| `btc/blocks/` prefix list | 2377 to 4321 | 6437 prefixes |
| `btc/blocks/` recursive, the MSCK analogue | 6470 | 6501 files, 328 MiB |
| `btc/blocks/date=2024-01-01/` recursive | 934 to 1103 | 1 file, 55 KiB |

A Bitcoin-scale repair is that 6.5-second recursive LIST. Per query, cycle 1 would pay the 2 to 4 seconds of prefix listing.

SQE cannot read that bucket yet, because unsigned public-bucket access is not wired up and Hive reads authenticate with engine credentials. So 449 partitions came down to local disk first: all of 2009 plus the first quarter of 2024, 15 MiB, in 11 seconds.

## SQE against DuckDB, on the same files

Same 449 files, same hot page cache, same answers. DuckDB 1.5.5 in-process, cold is the first `execute` and warm is the median of the next six. Both engines returned 45 869 blocks, 155 blocks on 2024-01-01, and 36 977 562 transactions.

| query | DuckDB cold/warm | SQE listing | SQE indexed |
|---|---|---|---|
| `count(*)` all | 70 / 62 | 196 / 55 | 121 / **23** |
| `WHERE date = '2024-01-01'` | 44 / 44 | 99 / 48 | **1.9 / 0.7** |
| `sum(transaction_count)` pin | 45 / 44 | 96 / 49 | **1.5 / 0.9** |
| `sum(transaction_count)` all | 62 / 63 | | 144 / **27** |
| live S3 pin, unsigned | 1116 / 369 | not supported | not supported |

Four things worth separating.

**Correctness is not the story.** Both engines print the same integers. DuckDB printed them first, and SQE matched them, which is how the run was validated in the first place.

**The pin is a persisted-metadata win, not a decode win.** DuckDB globs `date=*/*.parquet` on every query and prunes to one file; its own `EXPLAIN` says `Scanning Files: 1/449`. The glob is what costs 44 ms, and it costs it again every time. SQE's listing path sits in the same band at 99 ms cold and 48 ms warm. After `MSCK REPAIR`, SQE reads the index, skips the glob entirely, and lands at 1.9 ms cold and sub-millisecond warm. Nothing about SQE's Parquet reader got faster. The engine simply stopped asking a question it had already answered.

**On a full scan DuckDB is the steadier engine cold.** 70 ms against SQE's 121 ms indexed and 196 ms listing, where SQE's cold includes Hive provider construction and, on the listing path, a union across listing tables. Warm, once the index exists, SQE comes out ahead at 23 to 27 ms against DuckDB's 62.

**DuckDB can query the public bucket today and SQE cannot.** 1.1 s cold and 370 ms warm for a one-day pin straight off `s3://`, no credentials. SQE needs engine credentials for any object-store read, which is why this comparison ran on copied files.

Fairness notes, because they change how much the table is worth: both engines hit a hot page cache, neither run is cold-disk, the SQE build is `dev-release`, and 15 MiB of data is not a decode benchmark. What it measures is planning, and planning is what cycle 2 changed.

## Partition projection is implemented, and it is not always the fast path

Cycle 1 documented Athena's `projection.*` properties as rejected. The rejection was never actually implemented, so they flowed into `parameters` and were silently ignored, which turns a computed partition set into a full table scan. Cycle 2 closes both halves by implementing the feature.

```sql
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.date.type' = 'date',
  'projection.date.range' = '2009-01-03,2024-03-31',
  'projection.date.format' = 'yyyy-MM-dd'
)
```

Projection computes the partition set at plan time from the config and never lists storage. On the same Bitcoin pin it costs 11 ms cold against the index's 1.9 ms. Both skip the prefix LIST, and projection is still the slower of the two, because it materializes every calendar day in the declared range including the holes in early 2009 and prunes afterward. The index stores only the directories that exist.

Athena has projection because a Glue round trip per query is worth avoiding. When the partition list is already a local file, the workaround costs more than the thing it was working around. Projection stays worth having for tables where partitions are genuinely dense and arrive continuously, since it needs no repair at all. `MSCK REPAIR` on a projection-enabled table is a successful no-op, and `ADD`/`DROP PARTITION` stay errors, because projection owns the partition set.

## Also in this cycle

`ANALYZE TABLE` persists row counts into `sqe.statistics`: Parquet counts come from footers through the existing footer cache, CSV and JSON from a full decode. Planning uses them when present and falls back to size-derived estimates otherwise, so a join against a Hive table stops guessing without paying a footer read per query.

The statement classifier grew arms for `MSCK`, `SHOW PARTITIONS`, and the add and drop partition operations, so an unsupported variant names `hive_external` and the reason instead of saying "Statement type not supported".

## What comes next

Cycle 3 puts tag-based row filters and column masks on Hive scans. Cycle 4 adds writes into partition directories. Cycle 5 moves Hive scan execution onto workers, which needs `ScanTask` to grow a Hive-shaped alternative to its Parquet and Iceberg field-id shape. Until then Hive scans run coordinator-local by construction.

The reference page is `docs/site/book/src/reference/hive-external-tables.md`, the measurements are in `docs/evidence/perf/hive-cycle2-partition-index.md`, and the runnable version is `quickstart/hive-external-s3/`.
