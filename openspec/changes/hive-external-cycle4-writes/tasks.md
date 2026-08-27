# Cycle 4 tasks

## 1. Routing

The exact sites, located before starting so they are not rediscovered.
`reject_if_hive_table(session, ref, label, cycle)` in `query_handler.rs` is the
one rejection helper; four call sites carry `cycle = 4`:

| Line | Statement | Cycle 4 delivers it? |
|---|---|---|
| 1613 | `INSERT` | yes |
| 1645 | `DELETE` | no |
| 1665 | `UPDATE` | no |
| 1739 | `TRUNCATE TABLE` | no (rewrites to `DELETE FROM`) |

- [x] 1.1 Stop rejecting `INSERT INTO` at line 1613 and let it reach
      `HiveTableProvider::insert_into`. Routed through the session context in
      `hive_write::handle_insert`, because DataFusion's own INSERT planning is
      what calls `insert_into` at all.
- [x] 1.2 **Re-label the three that cycle 4 does NOT deliver.** They currently
      say "cycle 4 adds it", which becomes a false promise the moment cycle 4
      ships without them. `DELETE`, `UPDATE` and `TRUNCATE` all need the same
      missing primitive (see design.md decision 4), so they move to `cycle = 0`
      -- a plain "not supported" -- or to whichever cycle actually takes them
      on. Leaving them at 4 is the kind of wrong that only shows up in a
      support conversation.
- [x] 1.3 **CTAS is not rejected today, it is UNROUTED.** Now rejected with
      `cycle = 4` so it fails cleanly instead of taking the Iceberg branch.
      ROUTING it is still open (1.4). `StatementKind::Ctas`
      at line 1576 has no `reject_if_hive_table` call at all; it goes straight to
      `write_handler.handle_ctas_streaming`, which is the Iceberg write path. So
      `CREATE TABLE <hive-root>.db.t AS SELECT ...` does not fail cleanly today,
      it takes the wrong branch. Route it, and add the rejection for the interim
      if CTAS lands after `INSERT`.
- [ ] 1.4 CTAS is two operations, not one: write the sidecar manifest (the
      cycle-1 `create_external_table` path) and then the data. Decide and record
      which happens first, on the same reasoning as design.md's decision 2. A
      manifest with no data reads as an empty table; data with no manifest is
      invisible and unnamed.
- [x] 1.5 Test: the statements are routed, not merely un-rejected. Assert on the
      rows landing, because "no longer returns NotImplemented" is also what a
      silently-dropped write looks like.

## 2. The sink

- [x] 2.1 Pin `keep_partition_by_columns = false` for Hive writes rather than
      inheriting DataFusion's default.
- [x] 2.2 Test: no written data file contains a partition column. Read the
      parquet/CSV/JSON file directly, not through the table -- the read path
      projects partition columns back IN, so a table-level SELECT looks correct
      either way. This is the assertion that distinguishes them.
- [x] 2.3 CSV header. CONFIRMED already correct by construction: `serde_map::listing_options`
      builds one `CsvFormat` with `with_has_header(r.has_header)`, and
      DataFusion's `CsvFormat` uses that flag for both the reader's skip and the
      writer's emit, so read and write cannot disagree by construction. Verify
      with 4.2/4.3 FIRST. If it holds, this task is a test and not a change --
      which beats adding a redundant setting nobody can later remove because
      nobody knows whether it mattered.
- [x] 2.4 Refuse overwrite. Matched on the PARSED statement's `overwrite` AND
      `replace_into` flags -- `REPLACE INTO` sets the second, and checking only
      the first would let it reach the sink and append. The message names what an overwrite
      would have to do and why it cannot be atomic.
- [x] 2.5 Test: `INSERT OVERWRITE` errors (unit-level; the table-unchanged half
      needs the routed end-to-end path). An
      overwrite that appended would also "not error" if the test only checked
      the statement's return.

## 3. Index maintenance

- [x] 3.1 After the data lands, refresh `sqe.partition_index`.
      `refresh_index_after_write` delegates to the SAME discovery
      `MSCK REPAIR TABLE` runs, so the write path and the recovery path cannot
      disagree about what the table contains. Cost: a full prefix LIST per
      write. An incremental update needs the written partition values threaded
      out of the sink and is the right later optimization.
- [x] 3.2 Invalidate the manifest cache for the table. Without this the 60s TTL
      can serve the pre-write index, so a correct index update is still
      invisible for up to a minute.
- [x] 3.3 An index update that fails AFTER the data landed reports that the rows
      were written but are not yet visible, and names `MSCK REPAIR TABLE`. A
      generic error sends the user looking for data that is in the bucket.
- [x] 3.4 Test: a write into a NEW partition is visible to the next query with no
      manual repair. **Measured the gap first**: without the refresh the row's
      file is on disk and the next SELECT returns the old five rows. The test
      asserts the 5 BEFORE asserting the 6, so it cannot pass by the row having
      been visible all along.
- [x] 3.6 **Invalidate the cached session context after a write.** Found by
      4.3, not predicted by the design. A `SELECT` before the write caches the
      table provider in that `SessionContext`; the next `SELECT` on the same
      context returns the PRE-write rows while a fresh context returns the
      correct ones. So refreshing the index without also dropping the cached
      context makes a correct refresh invisible -- indistinguishable from the
      refresh failing. The Iceberg CTAS and INSERT arms already called
      `invalidate_session_cache`; the Hive arm did not.

      The testing half matters as much: a test reusing one context does not
      match production, which builds one per statement. `read_back` therefore
      takes the FIXTURE and builds a fresh session per read, or the test fails
      on the caching rather than on what it was written to check.

- [ ] 3.5 Test: the ordering. Simulate an index-update failure and assert the
      data files exist and `MSCK REPAIR TABLE` recovers them. This is the test
      that proves data-first was actually implemented, rather than index-first
      that happens to work when nothing fails.

## 4. Round-trip per format

Write, then read back through the normal read path, and compare counts AND
values. A write test passes on a file nobody can read correctly.

- [x] 4.1 Parquet.
- [x] 4.2 CSV with `skip.header.line.count = 1`.
- [x] 4.3 CSV with `skip.header.line.count = 0`. Both, because the two failures
      are opposite (a real row eaten vs a phantom row) and a test of one is
      passed by a writer that gets the other wrong. **Writing this one found a
      different bug**: it failed with the written row MISSING, not duplicated,
      and the cause was neither the writer nor the header. See 3.6.
- [x] 4.4 Line-delimited JSON.
- [ ] 4.5 A typed non-string partition key and `__HIVE_DEFAULT_PARTITION__`,
      round-tripped. Cycle 5's differential suite pinned these on the READ side;
      a write has to produce directory names the read side then parses back.

## 5. Concurrency and durability

- [ ] 5.1 Test: two concurrent inserts into the same partition both land, with
      neither's files overwritten. Static file names have already cost this repo
      an INSERT failure at scale in the Iceberg path; the symptom under
      concurrency is a lost file, not an error.
- [ ] 5.2 Local-filesystem writes go to a temporary name and rename within the
      directory, so a reader never sees a torn file. Object-store `PUT` is
      already whole-or-nothing and needs nothing.
- [ ] 5.3 Test: an interrupted write leaves the table readable.

## 6. Docs

- [ ] 6.1 Reference page: the visibility contract, in the words of design.md's
      decision 1. Per-object atomicity, no multi-file atomicity, what a
      concurrent reader can see, and "if you need snapshot isolation on write,
      use an Iceberg table".
- [ ] 6.2 The statement-support table gains `INSERT INTO` and CTAS, and records
      `INSERT OVERWRITE` / `DELETE` / `UPDATE` / `MERGE` as refused with the
      reason.
- [ ] 6.3 `nextsteps.md` and README roadmap.

## 7. Quickstart

- [ ] 7.1 Extend `quickstart/hive-external-s3/` with a write: insert into a
      partitioned table, restart the engine, read it back. The restart is the
      point -- it proves the index was persisted rather than held in the
      session's cache.
