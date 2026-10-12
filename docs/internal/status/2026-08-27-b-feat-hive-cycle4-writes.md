**STATUS 2026-08-27 (b): Hive external tables, cycle 4 writes started, `INSERT INTO` works, on `feat/hive-cycle4-writes`.**

`INSERT INTO` a Hive external table appends into `k=v/` partition directories, refreshes `sqe.partition_index`, and drops the cached session context. The writer was not the work: `HiveTableProvider::insert_into` already delegated to its inner `ListingTable`, and DataFusion 55 already carried `table_partition_cols` into `FileSinkConfig`, so cycle 4 is routing plus bookkeeping.

Two bugs found by tests rather than by reasoning. The partition index goes stale on write, so an insert into a new partition landed its file and stayed invisible -- the strongest read optimization in the Hive path was also what made the first write look like a lost write. And a `SELECT` before a write caches the table provider in that session context, so the next `SELECT` on it returned pre-write rows; the Iceberg arms already called `invalidate_session_cache` and the Hive arm did not.

Also fixed: CTAS had NO Hive check and fell into the Iceberg write path. `DELETE`/`UPDATE`/`TRUNCATE` said "cycle 4 adds it" and now say plainly unsupported, since all three need the primitive `INSERT OVERWRITE` needs.

**CTAS landed too.** Manifest first, then data -- the opposite order from INSERT, and the inversion is the point: a failure after the manifest leaves an empty table that DROP cleans up, while data-first would leave orphaned files under a path no table names, which `MSCK REPAIR` cannot recover because repair needs a manifest to repair. `LOCATION` is required (SQE does not invent data paths for external tables), partition columns must be the trailing SELECT columns (a violation errors rather than reordering), and `CREATE OR REPLACE` is refused because DROP leaves the data and the result would be the union.

Two sqlparser traps worth knowing: Hive's `PARTITIONED BY` lands in `hive_distribution`, not `partition_by`; and `LOCATION` lands in `hive_formats.location`, not `ct.location`. Reading the wrong field in either case yields a silently wrong table.

**5.2 needed no code.** `object_store`'s LocalFileSystem already stages and renames (`put` -> `new_staged_upload` then `std::fs::rename`; multipart `complete` renames too), so per-file atomicity is the dependency's guarantee, not SQE's. Verified without a race by asserting no staging leftover survives and every file present parses as complete parquet. That is twice this cycle that reading the dependency first turned an implementation task into a test (the CSV header was the other).

**Cycle 4 is complete.** The quickstart (7.1) now writes into a new partition and a second engine reads it back: 26 checks passed, 0 failed against a real RustFS bucket. `process 2 still sees the written row` is the load-bearing one -- process 2 attached the same root and read nothing but the bucket, so it can only see the row if the partition-index refresh was persisted rather than kept in memory.

**NEXT: cycle 3**, the last unstarted cycle: tag-based row filters and column masks on Hive scans, plus Ranger resource registration. Two tests already on main (`4f769d7`, `68228b6`) suggest masking may already reach a Hive scan by construction, so scope it by checking that first -- the same approach that turned two cycle-4 tasks into tests instead of code.

---
